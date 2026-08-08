package outbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	redigo "github.com/garyburd/redigo/redis"
)

const (
	defaultPrefix         = "ptta:discord:outbox"
	defaultDoneTTL        = 30 * 24 * time.Hour
	defaultCounterKey     = "counter:alert"
	defaultCounterChannel = "alert-counter"
	defaultRecoverLimit   = 100
	maxFailureClassLength = 128
	maxMessageIDLength    = 512
)

// Store is the delivery-worker and producer-facing durable outbox contract.
// Keeping jobs behind this interface permits deterministic failure tests
// without replacing the production Redis implementation.
type Store interface {
	Enqueue(context.Context, Item) (EnqueueResult, error)
	Claim(context.Context, time.Duration) (*ClaimedItem, error)
	AckChunk(context.Context, string, string, string, time.Duration) (AckResult, error)
	Retry(context.Context, string, string, time.Duration, string) (RetryResult, error)
	RecoverExpired(context.Context, int) ([]string, error)
	PendingCount(context.Context) (int64, error)
	HighWatermarkReached(context.Context) (bool, int64, error)
}

// RedisConfig configures the durable Redis outbox.
type RedisConfig struct {
	// Connect defaults to the application's shared Redis pool.
	Connect func() redigo.Conn
	// Prefix isolates every outbox key. It defaults to ptta:discord:outbox.
	Prefix string
	// DoneTTL controls durable delivery dedupe. Zero selects 30 days; a
	// negative duration retains done markers indefinitely.
	DoneTTL time.Duration
	// HighWatermark rejects new, non-duplicate events when the number of
	// ready plus leased events reaches this value. Zero disables the limit.
	HighWatermark  int64
	CounterKey     string
	CounterChannel string
}

// Redis implements the durable outbox on one Redis instance. All state
// transitions are fenced and performed by Lua scripts.
type Redis struct {
	connect        func() redigo.Conn
	prefix         string
	doneTTL        time.Duration
	highWatermark  int64
	counterKey     string
	counterChannel string
}

var _ Store = (*Redis)(nil)

// NewRedis constructs a durable outbox. Configuration is immutable after
// construction, so one instance can safely be shared by all producers and the
// single Discord delivery worker.
func NewRedis(config RedisConfig) *Redis {
	connect := config.Connect
	if connect == nil {
		connect = connections.Redis
	}
	prefix := strings.TrimRight(strings.TrimSpace(config.Prefix), ":")
	if prefix == "" {
		prefix = defaultPrefix
	}
	doneTTL := config.DoneTTL
	if doneTTL == 0 {
		doneTTL = defaultDoneTTL
	}
	counterKey := strings.TrimSpace(config.CounterKey)
	if counterKey == "" {
		counterKey = defaultCounterKey
	}
	counterChannel := strings.TrimSpace(config.CounterChannel)
	if counterChannel == "" {
		counterChannel = defaultCounterChannel
	}
	return &Redis{
		connect:        connect,
		prefix:         prefix,
		doneTTL:        doneTTL,
		highWatermark:  config.HighWatermark,
		counterKey:     counterKey,
		counterChannel: counterChannel,
	}
}

func (store *Redis) readyKey() string         { return store.prefix + ":ready" }
func (store *Redis) leasedKey() string        { return store.prefix + ":leased" }
func (store *Redis) itemPrefix() string       { return store.prefix + ":item:" }
func (store *Redis) itemKey(id string) string { return store.itemPrefix() + id }
func (store *Redis) donePrefix() string       { return store.prefix + ":done:" }
func (store *Redis) doneKey(id string) string { return store.donePrefix() + id }
func (store *Redis) webhookNotBeforeKey() string {
	return store.prefix + ":webhook-not-before"
}

// Enqueue durably admits an immutable item. Repeating the same event and
// payload is idempotent whether it is pending or already done. Reusing the
// event ID for a different payload fails with ErrEventConflict.
func (store *Redis) Enqueue(ctx context.Context, item Item) (EnqueueResult, error) {
	if err := item.Validate(); err != nil {
		return EnqueueResult{}, err
	}
	chunksJSON, err := json.Marshal(item.Chunks)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("marshal outbox chunks: %w", err)
	}
	countAlert := 0
	if item.CountAlert {
		countAlert = 1
	}
	reply, err := store.eval(ctx, enqueueScript,
		[]string{store.readyKey(), store.leasedKey(), store.itemKey(item.EventID), store.doneKey(item.EventID)},
		item.EventID,
		item.Kind,
		string(chunksJSON),
		len(item.Chunks),
		countAlert,
		payloadHash(item),
		store.highWatermark,
	)
	if err != nil {
		return EnqueueResult{}, err
	}
	values, err := redigo.Values(reply, nil)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("decode enqueue result: %w", err)
	}
	var status int
	if _, err = redigo.Scan(values, &status); err != nil {
		return EnqueueResult{}, fmt.Errorf("scan enqueue result: %w", err)
	}
	result := EnqueueResult{EventID: item.EventID, Status: EnqueueStatus(status)}
	switch result.Status {
	case EnqueueCreated, EnqueueAlreadyPending, EnqueueAlreadyDone:
		return result, nil
	default:
		return EnqueueResult{}, fmt.Errorf("%w: unknown enqueue status %d", ErrCorruptData, status)
	}
}

// Claim atomically leases the oldest due event. A nil item means no event is
// currently due. leaseDuration must exceed the longest single-chunk HTTP
// attempt; AckChunk renews it after every confirmed non-final chunk.
func (store *Redis) Claim(ctx context.Context, leaseDuration time.Duration) (*ClaimedItem, error) {
	leaseMS, err := positiveMilliseconds("lease duration", leaseDuration)
	if err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		return nil, fmt.Errorf("create outbox lease token: %w", err)
	}
	reply, err := store.eval(ctx, claimScript,
		[]string{store.readyKey(), store.leasedKey(), store.webhookNotBeforeKey()},
		store.itemPrefix(), token, leaseMS, defaultRecoverLimit,
	)
	if err != nil {
		return nil, err
	}
	values, err := redigo.Values(reply, nil)
	if errors.Is(err, redigo.ErrNil) || (err == nil && len(values) == 0) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("decode claim result: %w", err)
	}
	var (
		eventID, kind, chunksJSON, storedPayloadHash, storedToken string
		nextChunk, countAlert                                     int
		attempts, recoveries, generation, leaseUntilMS            int64
	)
	if _, err = redigo.Scan(values,
		&eventID, &kind, &chunksJSON, &nextChunk, &countAlert,
		&attempts, &recoveries, &storedPayloadHash, &storedToken,
		&generation, &leaseUntilMS,
	); err != nil {
		return nil, fmt.Errorf("scan claim result: %w", err)
	}
	var chunks []string
	if err = json.Unmarshal([]byte(chunksJSON), &chunks); err != nil {
		return nil, fmt.Errorf("%w: decode immutable chunks", ErrCorruptData)
	}
	claimed := &ClaimedItem{
		Item: Item{
			EventID:    eventID,
			Kind:       kind,
			Chunks:     chunks,
			CountAlert: countAlert == 1,
		},
		NextChunk:       nextChunk,
		Attempts:        attempts,
		Recoveries:      recoveries,
		LeaseToken:      storedToken,
		LeaseGeneration: generation,
		LeaseUntilMS:    leaseUntilMS,
	}
	if err = claimed.Item.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptData, err)
	}
	if storedToken != token || storedPayloadHash != payloadHash(claimed.Item) {
		return nil, fmt.Errorf("%w: claimed payload or token does not match", ErrCorruptData)
	}
	if _, err = claimed.CurrentChunk(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// AckChunk records one confirmed Discord message ID. A non-final ack renews the
// lease. The final ack atomically writes the done marker and removes the
// pending item. Counter increment and publication are best-effort side effects
// that never hold a confirmed Discord delivery open.
func (store *Redis) AckChunk(
	ctx context.Context,
	eventID, token, discordMessageID string,
	leaseDuration time.Duration,
) (AckResult, error) {
	if err := validateTransitionIdentity(eventID, token); err != nil {
		return AckResult{}, err
	}
	if strings.TrimSpace(discordMessageID) == "" || len(discordMessageID) > maxMessageIDLength {
		return AckResult{}, fmt.Errorf("%w: invalid Discord message ID", ErrInvalidItem)
	}
	leaseMS, err := positiveMilliseconds("lease duration", leaseDuration)
	if err != nil {
		return AckResult{}, err
	}
	doneTTLMS := int64(-1)
	if store.doneTTL > 0 {
		doneTTLMS = store.doneTTL.Milliseconds()
		if doneTTLMS <= 0 {
			doneTTLMS = 1
		}
	}
	reply, err := store.eval(ctx, ackChunkScript,
		[]string{
			store.readyKey(), store.leasedKey(), store.itemKey(eventID),
			store.doneKey(eventID), store.counterKey,
		},
		eventID, token, discordMessageID, leaseMS, doneTTLMS, store.counterChannel,
	)
	if err != nil {
		return AckResult{}, err
	}
	values, err := redigo.Values(reply, nil)
	if err != nil {
		return AckResult{}, fmt.Errorf("decode ack result: %w", err)
	}
	var completed, alreadyDone, nextChunk int
	var alertCount int64
	if _, err = redigo.Scan(values, &completed, &alreadyDone, &nextChunk, &alertCount); err != nil {
		return AckResult{}, fmt.Errorf("scan ack result: %w", err)
	}
	result := AckResult{
		Completed:   completed == 1,
		AlreadyDone: alreadyDone == 1,
		NextChunk:   nextChunk,
	}
	if alertCount >= 0 {
		result.AlertCount = &alertCount
	}
	return result, nil
}

// Retry atomically returns a leased event to the ready schedule. failureClass
// must be a non-secret classifier such as "discord_http_429" or
// "network_timeout"; raw errors and webhook URLs must not be persisted.
// Webhook-wide failures also max-extend the shared delivery pause in the same
// transition. Retry never drops an item, regardless of the attempt count.
func (store *Redis) Retry(
	ctx context.Context,
	eventID, token string,
	delay time.Duration,
	failureClass string,
) (RetryResult, error) {
	if err := validateTransitionIdentity(eventID, token); err != nil {
		return RetryResult{}, err
	}
	if err := validateFailureClass(failureClass); err != nil {
		return RetryResult{}, err
	}
	delayMS, err := nonNegativeMilliseconds("retry delay", delay)
	if err != nil {
		return RetryResult{}, err
	}
	pauseAll := 0
	if shouldPauseDiscordBacklog(failureClass) {
		pauseAll = 1
	}
	reply, err := store.eval(ctx, retryScript,
		[]string{
			store.readyKey(), store.leasedKey(), store.itemKey(eventID), store.doneKey(eventID),
			store.webhookNotBeforeKey(),
		},
		eventID, token, delayMS, failureClass, pauseAll,
	)
	if err != nil {
		return RetryResult{}, err
	}
	values, err := redigo.Values(reply, nil)
	if err != nil {
		return RetryResult{}, fmt.Errorf("decode retry result: %w", err)
	}
	var alreadyDone int
	var attempts, nextAttemptMS int64
	if _, err = redigo.Scan(values, &alreadyDone, &attempts, &nextAttemptMS); err != nil {
		return RetryResult{}, fmt.Errorf("scan retry result: %w", err)
	}
	return RetryResult{
		AlreadyDone:   alreadyDone == 1,
		Attempts:      attempts,
		NextAttemptMS: nextAttemptMS,
	}, nil
}

// RecoverExpired moves expired leases back to the ready schedule and clears
// their tokens. Any stale worker is fenced out from subsequent transitions.
func (store *Redis) RecoverExpired(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultRecoverLimit
	}
	reply, err := store.eval(ctx, recoverExpiredScript,
		[]string{store.readyKey(), store.leasedKey()},
		store.itemPrefix(), limit,
	)
	if err != nil {
		return nil, err
	}
	ids, err := redigo.Strings(reply, nil)
	if errors.Is(err, redigo.ErrNil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("decode recovered event IDs: %w", err)
	}
	return ids, nil
}

// PendingCount returns the number of ready plus leased events.
func (store *Redis) PendingCount(ctx context.Context) (int64, error) {
	reply, err := store.eval(ctx, pendingCountScript,
		[]string{store.readyKey(), store.leasedKey()},
	)
	if err != nil {
		return 0, err
	}
	count, err := redigo.Int64(reply, nil)
	if err != nil {
		return 0, fmt.Errorf("decode outbox pending count: %w", err)
	}
	return count, nil
}

// HighWatermarkReached reports the configured admission state. Enqueue performs
// the same check atomically, so this method is advisory rather than an admission
// lock.
func (store *Redis) HighWatermarkReached(ctx context.Context) (reached bool, count int64, err error) {
	count, err = store.PendingCount(ctx)
	if err != nil {
		return false, 0, err
	}
	return store.highWatermark > 0 && count >= store.highWatermark, count, nil
}

func (store *Redis) eval(ctx context.Context, script string, keys []string, values ...interface{}) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || store.connect == nil {
		return nil, errors.New("outbox Redis connector is not configured")
	}
	conn := store.connect()
	if conn == nil {
		return nil, errors.New("outbox Redis connector returned nil")
	}
	defer conn.Close()
	args := redigo.Args{}.Add(script).Add(len(keys)).AddFlat(keys).AddFlat(values)
	reply, err := conn.Do("EVAL", args...)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, mapScriptError(err)
	}
	return reply, nil
}

func validateTransitionIdentity(eventID, token string) error {
	if !validEventID(eventID) {
		return fmt.Errorf("%w: invalid event ID", ErrInvalidItem)
	}
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: lease token is required", ErrInvalidItem)
	}
	return nil
}

func validateFailureClass(value string) error {
	if value == "" || len(value) > maxFailureClassLength {
		return fmt.Errorf("%w: invalid retry failure class", ErrInvalidItem)
	}
	for _, char := range value {
		valid := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || strings.ContainsRune("_.:-", char)
		if !valid {
			return fmt.Errorf("%w: retry failure class must not contain raw error text", ErrInvalidItem)
		}
	}
	return nil
}

func shouldPauseDiscordBacklog(failureClass string) bool {
	switch failureClass {
	case "discord_http_401",
		"discord_http_403",
		"discord_http_404",
		"discord_http_410",
		"discord_http_429",
		"discord_webhook_not_configured",
		"discord_webhook_invalid":
		return true
	default:
		return false
	}
}

func positiveMilliseconds(name string, duration time.Duration) (int64, error) {
	if duration <= 0 {
		return 0, fmt.Errorf("%w: %s must be positive", ErrInvalidItem, name)
	}
	milliseconds := duration.Milliseconds()
	if milliseconds <= 0 {
		milliseconds = 1
	}
	return milliseconds, nil
}

func nonNegativeMilliseconds(name string, duration time.Duration) (int64, error) {
	if duration < 0 {
		return 0, fmt.Errorf("%w: %s must not be negative", ErrInvalidItem, name)
	}
	return duration.Milliseconds(), nil
}

func leaseToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func mapScriptError(err error) error {
	message := err.Error()
	var target error
	switch {
	case strings.Contains(message, "OUTBOX_EVENT_CONFLICT"):
		target = ErrEventConflict
	case strings.Contains(message, "OUTBOX_NOT_FOUND"):
		target = ErrItemNotFound
	case strings.Contains(message, "OUTBOX_STALE_LEASE"):
		target = ErrStaleLease
	case strings.Contains(message, "OUTBOX_LEASE_EXPIRED"):
		target = ErrLeaseExpired
	case strings.Contains(message, "OUTBOX_INVALID_STATE"):
		target = ErrInvalidState
	case strings.Contains(message, "OUTBOX_HIGH_WATERMARK"):
		target = ErrHighWatermark
	case strings.Contains(message, "OUTBOX_WRONG_TYPE"):
		target = ErrWrongRedisType
	case strings.Contains(message, "OUTBOX_CORRUPT"), strings.Contains(message, "OUTBOX_COUNTER_INVALID"):
		target = ErrCorruptData
	default:
		return err
	}
	return fmt.Errorf("%w: Redis rejected an outbox transition", target)
}

const luaHelpers = `
local function key_type(key)
    local value = redis.call('TYPE', key)
    if type(value) == 'table' then
        return value['ok']
    end
    return value
end

local function require_type(key, expected)
    local actual = key_type(key)
    if actual ~= 'none' and actual ~= expected then
        error('OUTBOX_WRONG_TYPE')
    end
end

local function now_ms()
    local value = redis.call('TIME')
    return tonumber(value[1]) * 1000 + math.floor(tonumber(value[2]) / 1000)
end
`

const enqueueScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'hash')
require_type(KEYS[4], 'hash')

local event_id = ARGV[1]
local payload_hash = ARGV[6]
local item_exists = redis.call('EXISTS', KEYS[3]) == 1
local done_exists = redis.call('EXISTS', KEYS[4]) == 1
if item_exists and done_exists then
    error('OUTBOX_CORRUPT')
end
if done_exists then
    if redis.call('HGET', KEYS[4], 'payload_hash') ~= payload_hash then
        error('OUTBOX_EVENT_CONFLICT')
    end
    return {3}
end
if item_exists then
    if redis.call('HGET', KEYS[3], 'payload_hash') ~= payload_hash then
        error('OUTBOX_EVENT_CONFLICT')
    end
    local state = redis.call('HGET', KEYS[3], 'state')
    if state == 'ready' then
        redis.call('ZREM', KEYS[2], event_id)
        if not redis.call('ZSCORE', KEYS[1], event_id) then
            local next_attempt = tonumber(redis.call('HGET', KEYS[3], 'next_attempt_ms')) or now_ms()
            redis.call('ZADD', KEYS[1], next_attempt, event_id)
        end
    elseif state == 'leased' then
        redis.call('ZREM', KEYS[1], event_id)
        if not redis.call('ZSCORE', KEYS[2], event_id) then
            local lease_until = tonumber(redis.call('HGET', KEYS[3], 'lease_until_ms'))
            if not lease_until then
                error('OUTBOX_CORRUPT')
            end
            redis.call('ZADD', KEYS[2], lease_until, event_id)
        end
    else
        error('OUTBOX_INVALID_STATE')
    end
    return {2}
end

local high_watermark = tonumber(ARGV[7]) or 0
if high_watermark > 0 then
    local pending = redis.call('ZCARD', KEYS[1]) + redis.call('ZCARD', KEYS[2])
    if pending >= high_watermark then
        error('OUTBOX_HIGH_WATERMARK')
    end
end

local current = now_ms()
redis.call('HSET', KEYS[3],
    'schema', '1',
    'event_id', event_id,
    'kind', ARGV[2],
    'chunks_json', ARGV[3],
    'chunk_count', ARGV[4],
    'next_chunk', '0',
    'count_alert', ARGV[5],
    'payload_hash', payload_hash,
    'created_at_ms', current,
    'next_attempt_ms', current,
    'attempts', '0',
    'recoveries', '0',
    'lease_generation', '0',
    'state', 'ready')
redis.call('ZADD', KEYS[1], current, event_id)
return {1}
`

const claimScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'string')

local item_prefix = ARGV[1]
local token = ARGV[2]
local lease_ms = tonumber(ARGV[3])
local scan_limit = tonumber(ARGV[4]) or 100
local current = now_ms()
local pause_raw = redis.call('GET', KEYS[3])
if pause_raw then
    if not string.match(pause_raw, '^[1-9]%d*$') or not tonumber(pause_raw) then
        error('OUTBOX_CORRUPT')
    end
    local pause_until = tonumber(pause_raw)
    if pause_until > current then return {} end
    redis.call('DEL', KEYS[3])
end
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', current, 'LIMIT', 0, scan_limit)

for _, event_id in ipairs(candidates) do
    local item_key = item_prefix .. event_id
    require_type(item_key, 'hash')
    if redis.call('EXISTS', item_key) == 0 then
        redis.call('ZREM', KEYS[1], event_id)
        redis.call('ZREM', KEYS[2], event_id)
    else
        if redis.call('HGET', item_key, 'state') ~= 'ready' then
            error('OUTBOX_INVALID_STATE')
        end
        local chunk_count = tonumber(redis.call('HGET', item_key, 'chunk_count'))
        local next_chunk = tonumber(redis.call('HGET', item_key, 'next_chunk'))
        if not chunk_count or not next_chunk or chunk_count <= 0 or next_chunk < 0 or next_chunk >= chunk_count then
            error('OUTBOX_CORRUPT')
        end
        local lease_until = current + lease_ms
        redis.call('ZREM', KEYS[1], event_id)
        redis.call('ZADD', KEYS[2], lease_until, event_id)
        local generation = redis.call('HINCRBY', item_key, 'lease_generation', 1)
        redis.call('HSET', item_key,
            'state', 'leased',
            'lease_token', token,
            'lease_until_ms', lease_until)
        return {
            event_id,
            redis.call('HGET', item_key, 'kind'),
            redis.call('HGET', item_key, 'chunks_json'),
            next_chunk,
            redis.call('HGET', item_key, 'count_alert'),
            redis.call('HGET', item_key, 'attempts'),
            redis.call('HGET', item_key, 'recoveries'),
            redis.call('HGET', item_key, 'payload_hash'),
            token,
            generation,
            lease_until
        }
    end
end
return {}
`

const ackChunkScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'hash')
require_type(KEYS[4], 'hash')

local event_id = ARGV[1]
local token = ARGV[2]
local message_id = ARGV[3]
local lease_ms = tonumber(ARGV[4])
local done_ttl_ms = tonumber(ARGV[5])
local counter_channel = ARGV[6]

local item_exists = redis.call('EXISTS', KEYS[3]) == 1
local done_exists = redis.call('EXISTS', KEYS[4]) == 1
if not item_exists then
    if done_exists then
        local alert_count = tonumber(redis.call('HGET', KEYS[4], 'alert_count')) or -1
        local chunk_count = tonumber(redis.call('HGET', KEYS[4], 'chunk_count')) or 0
        return {1, 1, chunk_count, alert_count}
    end
    error('OUTBOX_NOT_FOUND')
end
if done_exists then
    error('OUTBOX_CORRUPT')
end
if redis.call('HGET', KEYS[3], 'lease_token') ~= token then
    error('OUTBOX_STALE_LEASE')
end
if redis.call('HGET', KEYS[3], 'state') ~= 'leased' then
    error('OUTBOX_INVALID_STATE')
end

local current = now_ms()
local lease_until = tonumber(redis.call('HGET', KEYS[3], 'lease_until_ms'))
if not lease_until then
    error('OUTBOX_CORRUPT')
end
if lease_until <= current then
    error('OUTBOX_LEASE_EXPIRED')
end
if not redis.call('ZSCORE', KEYS[2], event_id) then
    error('OUTBOX_INVALID_STATE')
end

local chunk_count = tonumber(redis.call('HGET', KEYS[3], 'chunk_count'))
local next_chunk = tonumber(redis.call('HGET', KEYS[3], 'next_chunk'))
if not chunk_count or not next_chunk or chunk_count <= 0 or next_chunk < 0 or next_chunk >= chunk_count then
    error('OUTBOX_CORRUPT')
end
local next_value = next_chunk + 1
if next_value < chunk_count then
    local renewed_until = current + lease_ms
    redis.call('HSET', KEYS[3],
        'message_id:' .. next_chunk, message_id,
        'next_chunk', next_value,
        'lease_until_ms', renewed_until)
    redis.call('ZADD', KEYS[2], renewed_until, event_id)
    return {0, 0, next_value, -1}
end

for index = 0, next_chunk - 1 do
    if not redis.call('HGET', KEYS[3], 'message_id:' .. index) then
        error('OUTBOX_CORRUPT')
    end
end

local count_alert = redis.call('HGET', KEYS[3], 'count_alert') == '1'
local alert_count = -1

redis.call('HSET', KEYS[4],
    'schema', '1',
    'payload_hash', redis.call('HGET', KEYS[3], 'payload_hash'),
    'kind', redis.call('HGET', KEYS[3], 'kind'),
    'chunk_count', chunk_count,
    'completed_at_ms', current,
    'message_id:' .. next_chunk, message_id)
for index = 0, next_chunk - 1 do
    redis.call('HSET', KEYS[4],
        'message_id:' .. index,
        redis.call('HGET', KEYS[3], 'message_id:' .. index))
end
if done_ttl_ms and done_ttl_ms > 0 then
    redis.call('PEXPIRE', KEYS[4], done_ttl_ms)
end
redis.call('ZREM', KEYS[1], event_id)
redis.call('ZREM', KEYS[2], event_id)
redis.call('DEL', KEYS[3])

-- Discord has already accepted the message, so delivery completion is the
-- authoritative transition above. Preserve malformed counters for operators
-- to inspect, and contain every accounting failure so it cannot cause a resend.
if count_alert then
    local stored_count = redis.pcall('GET', KEYS[5])
    local counter_valid = not (type(stored_count) == 'table' and stored_count['err'])
    if counter_valid and stored_count and not string.match(stored_count, '^%d+$') then
        counter_valid = false
    end
    if counter_valid then
        local increment = redis.pcall('INCR', KEYS[5])
        if not (type(increment) == 'table' and increment['err']) then
            local incremented_count = tonumber(increment)
            if incremented_count and incremented_count >= 0 then
                alert_count = incremented_count
                redis.pcall('HSET', KEYS[4], 'alert_count', alert_count)
                redis.pcall('PUBLISH', counter_channel, alert_count)
            end
        end
    end
end
return {1, 0, chunk_count, alert_count}
`

const retryScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'hash')
require_type(KEYS[4], 'hash')
require_type(KEYS[5], 'string')

local event_id = ARGV[1]
local token = ARGV[2]
local delay_ms = tonumber(ARGV[3]) or 0
local failure_class = ARGV[4]
local pause_all = ARGV[5] == '1'
local pause_raw = redis.call('GET', KEYS[5])
local pause_until = nil
if pause_raw then
    if not string.match(pause_raw, '^[1-9]%d*$') or not tonumber(pause_raw) then
        error('OUTBOX_CORRUPT')
    end
    pause_until = tonumber(pause_raw)
end
local item_exists = redis.call('EXISTS', KEYS[3]) == 1
local done_exists = redis.call('EXISTS', KEYS[4]) == 1
if not item_exists then
    if done_exists then
        return {1, 0, 0}
    end
    error('OUTBOX_NOT_FOUND')
end
if done_exists then
    error('OUTBOX_CORRUPT')
end
if redis.call('HGET', KEYS[3], 'lease_token') ~= token then
    error('OUTBOX_STALE_LEASE')
end
if redis.call('HGET', KEYS[3], 'state') ~= 'leased' then
    error('OUTBOX_INVALID_STATE')
end

local current = now_ms()
local lease_until = tonumber(redis.call('HGET', KEYS[3], 'lease_until_ms'))
if not lease_until then
    error('OUTBOX_CORRUPT')
end
if lease_until <= current then
    error('OUTBOX_LEASE_EXPIRED')
end

local next_attempt = current + delay_ms
if pause_all then
    local effective_pause = next_attempt
    if pause_until and pause_until > effective_pause then effective_pause = pause_until end
    if effective_pause > current then
        redis.call('SET', KEYS[5], tostring(effective_pause), 'PX', effective_pause - current)
    else
        redis.call('DEL', KEYS[5])
    end
end
local attempts = redis.call('HINCRBY', KEYS[3], 'attempts', 1)
redis.call('HSET', KEYS[3],
    'state', 'ready',
    'next_attempt_ms', next_attempt,
    'last_error_class', failure_class,
    'last_error_at_ms', current)
redis.call('HDEL', KEYS[3], 'lease_token', 'lease_until_ms')
redis.call('ZREM', KEYS[2], event_id)
redis.call('ZADD', KEYS[1], next_attempt, event_id)
return {0, attempts, next_attempt}
`

const recoverExpiredScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')

local item_prefix = ARGV[1]
local limit = tonumber(ARGV[2]) or 100
local current = now_ms()
local candidates = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', current, 'LIMIT', 0, limit)
local recovered = {}

for _, event_id in ipairs(candidates) do
    local item_key = item_prefix .. event_id
    require_type(item_key, 'hash')
    if redis.call('EXISTS', item_key) == 0 then
        redis.call('ZREM', KEYS[2], event_id)
    else
        if redis.call('HGET', item_key, 'state') ~= 'leased' then
            error('OUTBOX_INVALID_STATE')
        end
        local stored_until = tonumber(redis.call('HGET', item_key, 'lease_until_ms'))
        local scheduled_until = tonumber(redis.call('ZSCORE', KEYS[2], event_id))
        if not stored_until or not scheduled_until or stored_until ~= scheduled_until then
            error('OUTBOX_CORRUPT')
        end
        redis.call('HINCRBY', item_key, 'recoveries', 1)
        redis.call('HSET', item_key,
            'state', 'ready',
            'next_attempt_ms', current)
        redis.call('HDEL', item_key, 'lease_token', 'lease_until_ms')
        redis.call('ZREM', KEYS[2], event_id)
        redis.call('ZADD', KEYS[1], current, event_id)
        table.insert(recovered, event_id)
    end
end
return recovered
`

const pendingCountScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
return redis.call('ZCARD', KEYS[1]) + redis.call('ZCARD', KEYS[2])
`
