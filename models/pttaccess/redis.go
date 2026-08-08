// Package pttaccess persists process-independent PTT access controls in Redis.
//
// The store is intentionally separate from the HTTP client. This lets every
// caller share cooldowns, fixed-window request reservations, and recent block
// observations without weakening Redis failures into an in-memory fallback.
package pttaccess

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	redigo "github.com/garyburd/redigo/redis"
)

const (
	keyPrefix          = "ptt:access"
	blockTTL           = 24 * time.Hour
	blockSequenceTTL   = 48 * time.Hour
	maxSafeLuaInteger  = int64(1<<53 - 1)
	hourWindowSeconds  = int64((time.Hour) / time.Second)
	dayWindowSeconds   = int64((24 * time.Hour) / time.Second)
	hourCounterTTL     = 2 * time.Hour
	dailyCounterTTL    = 48 * time.Hour
	corruptErrorPrefix = "PTTACCESS_CORRUPT:"
)

var (
	ErrInvalidConfig = errors.New("invalid PTT access Redis configuration")
	ErrInvalidTime   = errors.New("invalid PTT access time")
	ErrCorruptData   = errors.New("corrupt PTT access Redis data")
)

// Store is the process-independent PTT access-state contract.
type Store interface {
	LoadCooldown(context.Context) (time.Time, error)
	ExtendCooldown(context.Context, time.Time) error
	ReserveRequest(context.Context, time.Time) (time.Time, error)
	RecordBlock(context.Context, time.Time, time.Duration, int, time.Duration) (int64, time.Time, error)
}

// Connector opens one Redis connection. The injectable form keeps the store
// independently testable without replacing the application-wide Redis pool.
type Connector func() (redigo.Conn, error)

// RedisConfig contains immutable request limits and an optional connector.
// Both request limits must be positive.
type RedisConfig struct {
	Connect     Connector
	HourlyLimit int64
	DailyLimit  int64
}

// Redis persists PTT access state. One instance is safe for concurrent use.
type Redis struct {
	connect     Connector
	hourlyLimit int64
	dailyLimit  int64
}

var _ Store = (*Redis)(nil)

// NewRedis validates config and constructs a Redis-backed access store.
func NewRedis(config RedisConfig) (*Redis, error) {
	if config.HourlyLimit <= 0 || config.HourlyLimit > maxSafeLuaInteger {
		return nil, fmt.Errorf("%w: hourly limit must be between 1 and %d", ErrInvalidConfig, maxSafeLuaInteger)
	}
	if config.DailyLimit <= 0 || config.DailyLimit > maxSafeLuaInteger {
		return nil, fmt.Errorf("%w: daily limit must be between 1 and %d", ErrInvalidConfig, maxSafeLuaInteger)
	}
	connector := config.Connect
	if connector == nil {
		connector = func() (redigo.Conn, error) {
			return connections.Redis(), nil
		}
	}
	return &Redis{
		connect:     connector,
		hourlyLimit: config.HourlyLimit,
		dailyLimit:  config.DailyLimit,
	}, nil
}

func (store *Redis) cooldownKey() string      { return keyPrefix + ":cooldown" }
func (store *Redis) blockKey() string         { return keyPrefix + ":blocks" }
func (store *Redis) blockSequenceKey() string { return keyPrefix + ":block-sequence" }

func (store *Redis) hourKey(bucket int64) string {
	return fmt.Sprintf("%s:requests:hour:%d", keyPrefix, bucket)
}

func (store *Redis) dayKey(bucket int64) string {
	return fmt.Sprintf("%s:requests:day:%d", keyPrefix, bucket)
}

// LoadCooldown returns the greatest persisted cooldown deadline. A missing key
// is represented by the zero time; malformed data and Redis errors are not.
func (store *Redis) LoadCooldown(ctx context.Context) (time.Time, error) {
	connection, err := store.connection(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer connection.Close()

	reply, err := connection.Do("GET", store.cooldownKey())
	if err != nil {
		return time.Time{}, store.commandError(ctx, "load cooldown", err)
	}
	if reply == nil {
		return time.Time{}, nil
	}
	milliseconds, err := redigo.Int64(reply, nil)
	if err != nil || milliseconds <= 0 || milliseconds > maxSafeLuaInteger {
		return time.Time{}, fmt.Errorf("%w: invalid cooldown timestamp", ErrCorruptData)
	}
	return time.UnixMilli(milliseconds), nil
}

// ExtendCooldown atomically persists max(current, until). It never shortens an
// existing cooldown, including when multiple processes extend it concurrently.
func (store *Redis) ExtendCooldown(ctx context.Context, until time.Time) error {
	milliseconds, err := validMilliseconds(until)
	if err != nil {
		return err
	}
	_, err = store.eval(ctx, "extend cooldown", extendCooldownScript,
		[]string{store.cooldownKey()}, milliseconds, maxSafeLuaInteger)
	return err
}

// ReserveRequest atomically admits one request into both the aligned UTC hour
// and UTC day windows. When either limit is full, neither counter changes and
// retryAt is the latest reset among the exhausted windows.
func (store *Redis) ReserveRequest(ctx context.Context, now time.Time) (retryAt time.Time, err error) {
	nowMilliseconds, err := validMilliseconds(now)
	if err != nil {
		return time.Time{}, err
	}
	nowSeconds := nowMilliseconds / int64(time.Second/time.Millisecond)
	hourBucket := nowSeconds / hourWindowSeconds
	dayBucket := nowSeconds / dayWindowSeconds
	hourResetMilliseconds := (hourBucket + 1) * hourWindowSeconds * int64(time.Second/time.Millisecond)
	dayResetMilliseconds := (dayBucket + 1) * dayWindowSeconds * int64(time.Second/time.Millisecond)

	reply, err := store.eval(ctx, "reserve request", reserveRequestScript,
		[]string{store.hourKey(hourBucket), store.dayKey(dayBucket)},
		store.hourlyLimit,
		store.dailyLimit,
		hourResetMilliseconds,
		dayResetMilliseconds,
		hourCounterTTL.Milliseconds(),
		dailyCounterTTL.Milliseconds(),
		maxSafeLuaInteger,
	)
	if err != nil {
		return time.Time{}, err
	}
	values, err := redigo.Values(reply, nil)
	if err != nil || len(values) != 2 {
		return time.Time{}, fmt.Errorf("%w: invalid reservation response", ErrCorruptData)
	}
	var admitted int
	var retryMilliseconds int64
	if _, err = redigo.Scan(values, &admitted, &retryMilliseconds); err != nil {
		return time.Time{}, fmt.Errorf("%w: decode reservation response: %v", ErrCorruptData, err)
	}
	switch admitted {
	case 1:
		if retryMilliseconds != 0 {
			return time.Time{}, fmt.Errorf("%w: admitted reservation has a retry deadline", ErrCorruptData)
		}
		return time.Time{}, nil
	case 0:
		if retryMilliseconds <= nowMilliseconds || retryMilliseconds > maxSafeLuaInteger {
			return time.Time{}, fmt.Errorf("%w: invalid reservation retry deadline", ErrCorruptData)
		}
		return time.UnixMilli(retryMilliseconds), nil
	default:
		return time.Time{}, fmt.Errorf("%w: unknown reservation status %d", ErrCorruptData, admitted)
	}
}

// RecordBlock atomically records one timestamp in an exact sliding 24-hour
// window and max-extends the shared cooldown. Count and circuit-breaker state
// can therefore never be persisted without their corresponding cooldown.
func (store *Redis) RecordBlock(
	ctx context.Context,
	now time.Time,
	minimum time.Duration,
	threshold int,
	breaker time.Duration,
) (int64, time.Time, error) {
	nowMilliseconds, err := validMilliseconds(now)
	if err != nil {
		return 0, time.Time{}, err
	}
	minimumMilliseconds, err := positiveDurationMilliseconds("minimum block cooldown", minimum)
	if err != nil {
		return 0, time.Time{}, err
	}
	breakerMilliseconds, err := positiveDurationMilliseconds("circuit breaker cooldown", breaker)
	if err != nil {
		return 0, time.Time{}, err
	}
	if threshold <= 0 || int64(threshold) > maxSafeLuaInteger {
		return 0, time.Time{}, fmt.Errorf("%w: invalid circuit breaker threshold", ErrInvalidConfig)
	}
	if nowMilliseconds > maxSafeLuaInteger-minimumMilliseconds ||
		nowMilliseconds > maxSafeLuaInteger-breakerMilliseconds {
		return 0, time.Time{}, fmt.Errorf("%w: cooldown deadline is outside the supported range", ErrInvalidTime)
	}

	reply, err := store.eval(ctx, "record block and cooldown", recordBlockScript,
		[]string{store.blockKey(), store.blockSequenceKey(), store.cooldownKey()},
		nowMilliseconds,
		nowMilliseconds-blockTTL.Milliseconds(),
		blockTTL.Milliseconds(),
		blockSequenceTTL.Milliseconds(),
		minimumMilliseconds,
		threshold,
		breakerMilliseconds,
		maxSafeLuaInteger,
	)
	if err != nil {
		return 0, time.Time{}, err
	}
	values, err := redigo.Values(reply, nil)
	if err != nil || len(values) != 2 {
		return 0, time.Time{}, fmt.Errorf("%w: invalid block response", ErrCorruptData)
	}
	var count, untilMilliseconds int64
	if _, err = redigo.Scan(values, &count, &untilMilliseconds); err != nil || count <= 0 ||
		untilMilliseconds <= nowMilliseconds || untilMilliseconds > maxSafeLuaInteger {
		return 0, time.Time{}, fmt.Errorf("%w: invalid block response", ErrCorruptData)
	}
	return count, time.UnixMilli(untilMilliseconds), nil
}

func (store *Redis) connection(ctx context.Context) (redigo.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || store.connect == nil {
		return nil, fmt.Errorf("%w: Redis connector is not configured", ErrInvalidConfig)
	}
	connection, err := store.connect()
	if err != nil {
		return nil, fmt.Errorf("connect Redis for PTT access state: %w", err)
	}
	if connection == nil {
		return nil, errors.New("PTT access Redis connector returned nil")
	}
	return connection, nil
}

func (store *Redis) eval(
	ctx context.Context,
	operation, script string,
	keys []string,
	values ...interface{},
) (interface{}, error) {
	connection, err := store.connection(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	args := redigo.Args{}.Add(script).Add(len(keys)).AddFlat(keys).AddFlat(values)
	reply, err := connection.Do("EVAL", args...)
	if err != nil {
		return nil, store.commandError(ctx, operation, err)
	}
	return reply, nil
}

func (store *Redis) commandError(ctx context.Context, operation string, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if strings.Contains(err.Error(), corruptErrorPrefix) {
		return fmt.Errorf("%w: %s", ErrCorruptData, operation)
	}
	return fmt.Errorf("%s in Redis for PTT access state: %w", operation, err)
}

func validMilliseconds(value time.Time) (int64, error) {
	if value.IsZero() {
		return 0, fmt.Errorf("%w: timestamp is zero", ErrInvalidTime)
	}
	milliseconds := value.UnixMilli()
	if milliseconds <= 0 || milliseconds > maxSafeLuaInteger {
		return 0, fmt.Errorf("%w: timestamp is outside the supported range", ErrInvalidTime)
	}
	return milliseconds, nil
}

func positiveDurationMilliseconds(name string, value time.Duration) (int64, error) {
	if value <= 0 || value.Milliseconds() <= 0 || value.Milliseconds() > maxSafeLuaInteger {
		return 0, fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, name)
	}
	return value.Milliseconds(), nil
}

const extendCooldownScript = `
local raw = redis.call('GET', KEYS[1])
if raw then
  local current = tonumber(raw)
  if not string.match(raw, '^[1-9]%d*$') or not current or current > tonumber(ARGV[2]) then
    return redis.error_reply('PTTACCESS_CORRUPT: cooldown')
  end
  if current >= tonumber(ARGV[1]) then
    return current
  end
end
redis.call('SET', KEYS[1], ARGV[1])
return tonumber(ARGV[1])
`

const reserveRequestScript = `
local function count(key)
  local raw = redis.call('GET', key)
  if not raw then
    return 0
  end
  local value = tonumber(raw)
  local canonical = raw == '0' or string.match(raw, '^[1-9]%d*$')
  if not canonical or not value or value > tonumber(ARGV[7]) then
    error('PTTACCESS_CORRUPT: request count')
  end
  return value
end

local hourly = count(KEYS[1])
local daily = count(KEYS[2])
local hourFull = hourly >= tonumber(ARGV[1])
local dayFull = daily >= tonumber(ARGV[2])
if hourFull or dayFull then
  local retryAt = 0
  if hourFull then retryAt = tonumber(ARGV[3]) end
  if dayFull and tonumber(ARGV[4]) > retryAt then retryAt = tonumber(ARGV[4]) end
  return {0, retryAt}
end

hourly = redis.call('INCR', KEYS[1])
if hourly == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[5]) end
daily = redis.call('INCR', KEYS[2])
if daily == 1 then redis.call('PEXPIRE', KEYS[2], ARGV[6]) end
return {1, 0}
`

const recordBlockScript = `
local function canonical_integer(raw, allow_zero)
  if not raw then return nil end
  local canonical = (allow_zero and raw == '0') or string.match(raw, '^[1-9]%d*$')
  local value = tonumber(raw)
  if not canonical or not value or value > tonumber(ARGV[8]) then
    return nil
  end
  return value
end

local block_type = redis.call('TYPE', KEYS[1]).ok
if block_type ~= 'none' and block_type ~= 'zset' then
  return redis.error_reply('PTTACCESS_CORRUPT: block event type')
end

local sequence_raw = redis.call('GET', KEYS[2])
if sequence_raw and not canonical_integer(sequence_raw, true) then
  return redis.error_reply('PTTACCESS_CORRUPT: block sequence')
end

local cooldown_raw = redis.call('GET', KEYS[3])
local cooldown = nil
if cooldown_raw then
  cooldown = canonical_integer(cooldown_raw, false)
  if not cooldown then
    return redis.error_reply('PTTACCESS_CORRUPT: cooldown')
  end
end

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
local sequence = redis.call('INCR', KEYS[2])
if sequence > tonumber(ARGV[8]) then
  return redis.error_reply('PTTACCESS_CORRUPT: block sequence overflow')
end
redis.call('PEXPIRE', KEYS[2], ARGV[4])
local member = ARGV[1] .. ':' .. tostring(sequence)
redis.call('ZADD', KEYS[1], ARGV[1], member)
redis.call('PEXPIRE', KEYS[1], ARGV[3])
local count = redis.call('ZCARD', KEYS[1])

local delay = tonumber(ARGV[5])
if count >= tonumber(ARGV[6]) and tonumber(ARGV[7]) > delay then
  delay = tonumber(ARGV[7])
end
local until_at = tonumber(ARGV[1]) + delay
if until_at > tonumber(ARGV[8]) then
  return redis.error_reply('PTTACCESS_CORRUPT: cooldown overflow')
end
if cooldown and cooldown > until_at then until_at = cooldown end
redis.call('SET', KEYS[3], tostring(until_at))
return {count, until_at}
`
