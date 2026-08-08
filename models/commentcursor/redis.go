package commentcursor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	redigo "github.com/garyburd/redigo/redis"
)

const redisKeyPrefix = "commentcursor:"

var (
	ErrStateNotFound      = errors.New("comment cursor state is missing")
	ErrStateCorrupt       = errors.New("comment cursor state is corrupt")
	ErrArticleCodeEmpty   = errors.New("comment cursor article code is empty")
	ErrTransitionConflict = errors.New("comment cursor transition conflicts with newer state")
)

// Connector opens one Redis connection. The injectable form keeps the cursor
// independently testable without changing the process-wide Redis pool.
type Connector func() (redigo.Conn, error)

// RedisStore persists one cursor state per PTT article code.
type RedisStore struct {
	connector Connector
}

func NewRedisStore() *RedisStore {
	return &RedisStore{connector: func() (redigo.Conn, error) {
		return connections.Redis(), nil
	}}
}

func NewRedisStoreWithConnector(connector Connector) *RedisStore {
	return &RedisStore{connector: connector}
}

// Load distinguishes an absent key, malformed persisted JSON, and Redis
// transport/command errors through separate error chains.
func (store *RedisStore) Load(articleCode string) (State, error) {
	key, err := cursorKey(articleCode)
	if err != nil {
		return State{}, err
	}
	connection, err := store.connect()
	if err != nil {
		return State{}, fmt.Errorf("connect Redis for comment cursor: %w", err)
	}
	defer connection.Close()

	payload, err := redigo.Bytes(connection.Do("GET", key))
	if errors.Is(err, redigo.ErrNil) {
		return State{}, ErrStateNotFound
	}
	if err != nil {
		return State{}, fmt.Errorf("load comment cursor: %w", err)
	}
	state, err := decodeState(payload)
	if err != nil {
		return State{}, err
	}
	return state, nil
}

func (store *RedisStore) Save(articleCode string, state State) error {
	key, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	if err := validateState(state); err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode comment cursor: %w", err)
	}
	connection, err := store.connect()
	if err != nil {
		return fmt.Errorf("connect Redis for comment cursor: %w", err)
	}
	defer connection.Close()
	response, err := redigo.String(connection.Do("SET", key, payload))
	if err != nil {
		return fmt.Errorf("save comment cursor: %w", err)
	}
	if response != "OK" {
		return fmt.Errorf("save comment cursor: unexpected Redis response %q", response)
	}
	return nil
}

// Initialize creates a missing version-3 cursor, or conditionally replaces a
// legacy v1/v2 cursor after Load reported ErrStateUpgradeNeeded. It never
// overwrites another version-3 lifecycle and never discards a pending
// transition. This is the recovery counterpart to Reset, which is reserved for
// an operator-requested new subscription lifecycle.
func (store *RedisStore) Initialize(articleCode string, state State, allowLegacy bool) error {
	key, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	payload, err := marshalState(state, "initialize")
	if err != nil {
		return err
	}
	legacy := 0
	if allowLegacy {
		legacy = 1
	}
	_, err = store.evalTransition(
		"initialize comment cursor",
		initializeScript,
		[]string{key, pendingKeyFromCursorKey(key)},
		payload,
		legacy,
	)
	return err
}

// Reset starts a new lifecycle cursor and atomically discards any staged
// transition from an earlier subscription lifecycle.
func (store *RedisStore) Reset(articleCode string, state State) error {
	key, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	if err := validateState(state); err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode reset comment cursor: %w", err)
	}
	connection, err := store.connect()
	if err != nil {
		return fmt.Errorf("connect Redis for comment cursor reset: %w", err)
	}
	defer connection.Close()
	if _, err := connection.Do("MULTI"); err != nil {
		return fmt.Errorf("begin comment cursor reset: %w", err)
	}
	discard := true
	defer func() {
		if discard {
			_, _ = connection.Do("DISCARD")
		}
	}()
	if err := connection.Send("SET", key, payload); err != nil {
		return fmt.Errorf("queue comment cursor reset: %w", err)
	}
	if err := connection.Send("DEL", pendingKeyFromCursorKey(key)); err != nil {
		return fmt.Errorf("queue pending comment transition reset: %w", err)
	}
	results, err := redigo.Values(connection.Do("EXEC"))
	discard = false
	if err != nil {
		return fmt.Errorf("reset comment cursor: %w", err)
	}
	if len(results) != 2 {
		return fmt.Errorf("reset comment cursor: unexpected Redis response length %d", len(results))
	}
	for _, result := range results {
		if transactionErr, ok := result.(redigo.Error); ok {
			return fmt.Errorf("reset comment cursor: %w", transactionErr)
		}
	}
	return nil
}

// Advance atomically moves an event-free cursor transition forward only while
// the complete source state is still current and no notification transition is
// staged. Repeating an already-applied transition is idempotent.
func (store *RedisStore) Advance(articleCode string, source, next State) error {
	key, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	if source.Epoch != next.Epoch {
		return fmt.Errorf("%w: cursor epoch changed during advance", ErrInvalidState)
	}
	sourcePayload, err := marshalState(source, "source")
	if err != nil {
		return err
	}
	nextPayload, err := marshalState(next, "next")
	if err != nil {
		return err
	}
	_, err = store.evalTransition(
		"advance comment cursor",
		advanceScript,
		[]string{key, pendingKeyFromCursorKey(key)},
		sourcePayload,
		nextPayload,
	)
	return err
}

func marshalState(state State, operation string) ([]byte, error) {
	if err := validateState(state); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode %s comment cursor: %w", operation, err)
	}
	return payload, nil
}

func (store *RedisStore) evalTransition(
	operation, script string,
	keys []string,
	values ...interface{},
) (interface{}, error) {
	connection, err := store.connect()
	if err != nil {
		return nil, fmt.Errorf("connect Redis to %s: %w", operation, err)
	}
	defer connection.Close()
	args := redigo.Args{}.Add(script).Add(len(keys)).AddFlat(keys).AddFlat(values)
	reply, err := connection.Do("EVAL", args...)
	if err == nil {
		return reply, nil
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "COMMENT_CURSOR_CONFLICT"):
		return nil, fmt.Errorf("%w: %s", ErrTransitionConflict, operation)
	case strings.Contains(message, "COMMENT_CURSOR_CORRUPT"):
		return nil, fmt.Errorf("%w: %s", ErrStateCorrupt, operation)
	default:
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
}

func (store *RedisStore) connect() (redigo.Conn, error) {
	if store == nil || store.connector == nil {
		return nil, errors.New("comment cursor Redis connector is nil")
	}
	connection, err := store.connector()
	if err != nil {
		return nil, err
	}
	if connection == nil {
		return nil, errors.New("comment cursor Redis connector returned nil")
	}
	return connection, nil
}

func decodeState(payload []byte) (State, error) {
	var envelope struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return State{}, fmt.Errorf("%w: invalid JSON", ErrStateCorrupt)
	}
	if envelope.Version == 1 || envelope.Version == legacyStateVersion {
		var legacy struct {
			Version    int            `json:"version"`
			Generation uint64         `json:"generation,omitempty"`
			Tail       []OccurrenceID `json:"tail"`
		}
		if err := decodeStrictJSON(payload, &legacy); err != nil || legacy.Tail == nil ||
			(envelope.Version == legacyStateVersion && legacy.Generation == 0) {
			return State{}, fmt.Errorf("%w: invalid legacy state", ErrStateCorrupt)
		}
		for _, id := range legacy.Tail {
			if !validOccurrenceID(id) {
				return State{}, fmt.Errorf("%w: invalid legacy occurrence", ErrStateCorrupt)
			}
		}
		return State{}, ErrStateUpgradeNeeded
	}
	var state State
	if err := decodeStrictJSON(payload, &state); err != nil {
		return State{}, fmt.Errorf("%w: invalid JSON", ErrStateCorrupt)
	}
	if err := validateState(state); err != nil {
		return State{}, fmt.Errorf("%w: %v", ErrStateCorrupt, err)
	}
	return state, nil
}

func decodeStrictJSON(payload []byte, destination interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func cursorKey(articleCode string) (string, error) {
	if strings.TrimSpace(articleCode) == "" {
		return "", ErrArticleCodeEmpty
	}
	return redisKeyPrefix + articleCode, nil
}

const cursorTransitionLuaHelpers = `
local function key_type(key)
  local value = redis.call('TYPE', key)
  if type(value) == 'table' then return value['ok'] end
  return value
end

local function require_string_or_missing(key)
  local actual = key_type(key)
  if actual ~= 'none' and actual ~= 'string' then
    error('COMMENT_CURSOR_CORRUPT')
  end
end
`

const initializeScript = cursorTransitionLuaHelpers + `
require_string_or_missing(KEYS[1])
require_string_or_missing(KEYS[2])
if redis.call('EXISTS', KEYS[2]) == 1 then
  error('COMMENT_CURSOR_CONFLICT')
end

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then return 0 end
if current then
  if ARGV[2] ~= '1' then error('COMMENT_CURSOR_CONFLICT') end
  local ok, decoded = pcall(cjson.decode, current)
  if not ok or type(decoded) ~= 'table' or
     (decoded['version'] ~= 1 and decoded['version'] ~= 2) then
    error('COMMENT_CURSOR_CONFLICT')
  end
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
`

const advanceScript = cursorTransitionLuaHelpers + `
require_string_or_missing(KEYS[1])
require_string_or_missing(KEYS[2])
if redis.call('EXISTS', KEYS[2]) == 1 then
  error('COMMENT_CURSOR_CONFLICT')
end
local current = redis.call('GET', KEYS[1])
if current == ARGV[2] then return 0 end
if current ~= ARGV[1] then error('COMMENT_CURSOR_CONFLICT') end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`
