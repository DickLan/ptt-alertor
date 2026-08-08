package commentcursor

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	redigo "github.com/garyburd/redigo/redis"
)

const (
	pendingVersion    = 1
	maxPendingContent = 16 << 20
)

var (
	ErrPendingNotFound = errors.New("comment transition is not staged")
)

// PendingEvent is frozen before the first outbox admission attempt. Replaying
// it after a Redis/network/process failure therefore reuses the exact event ID
// and payload even if PTT comments are edited or deleted in the meantime.
type PendingEvent struct {
	CanonicalParts []string `json:"canonicalParts"`
	Content        string   `json:"content"`
	CountAlert     bool     `json:"countAlert"`
}

// Pending is a durable transition plan. The cursor cannot advance beyond Next
// until Event is admitted to the Discord outbox and the article diagnostic
// snapshot is updated.
type Pending struct {
	Version          int              `json:"version"`
	SourceRevision   string           `json:"sourceRevision"`
	Next             State            `json:"next"`
	Comments         article.Comments `json:"comments"`
	LastPushDateTime time.Time        `json:"lastPushDateTime"`
	Event            PendingEvent     `json:"event"`
}

func NewPending(
	source State,
	next State,
	comments article.Comments,
	lastPushDateTime time.Time,
	event PendingEvent,
) (Pending, error) {
	if err := validateState(source); err != nil {
		return Pending{}, err
	}
	if source.Epoch != next.Epoch {
		return Pending{}, errors.New("comment transition changes lifecycle epoch")
	}
	pending := Pending{
		Version:          pendingVersion,
		SourceRevision:   source.Revision,
		Next:             next,
		Comments:         append(article.Comments(nil), comments...),
		LastPushDateTime: lastPushDateTime,
		Event: PendingEvent{
			CanonicalParts: append([]string(nil), event.CanonicalParts...),
			Content:        event.Content,
			CountAlert:     event.CountAlert,
		},
	}
	if pending.Comments == nil {
		pending.Comments = make(article.Comments, 0)
	}
	if err := validatePending(pending); err != nil {
		return Pending{}, err
	}
	return pending, nil
}

func validatePending(pending Pending) error {
	if pending.Version != pendingVersion {
		return fmt.Errorf("invalid comment transition version %d", pending.Version)
	}
	if !validFixedHex(pending.SourceRevision, 64) {
		return errors.New("invalid comment transition source revision")
	}
	if err := validateState(pending.Next); err != nil {
		return err
	}
	if pending.SourceRevision == pending.Next.Revision {
		return errors.New("comment transition does not change the cursor")
	}
	if pending.Comments == nil || !equalOccurrenceIDs(OccurrenceIDs(pending.Comments), pending.Next.Snapshot) {
		return errors.New("comment transition snapshot does not match comments")
	}
	if len(pending.Event.CanonicalParts) == 0 || len(pending.Event.CanonicalParts) > 16 {
		return errors.New("comment transition canonical identity is invalid")
	}
	for _, part := range pending.Event.CanonicalParts {
		if strings.TrimSpace(part) == "" || len(part) > 1024 {
			return errors.New("comment transition canonical identity has an invalid part")
		}
	}
	if pending.Event.Content == "" || len(pending.Event.Content) > maxPendingContent {
		return errors.New("comment transition content is invalid")
	}
	return nil
}

func (store *RedisStore) LoadPending(articleCode string) (Pending, error) {
	key, err := pendingKey(articleCode)
	if err != nil {
		return Pending{}, err
	}
	connection, err := store.connect()
	if err != nil {
		return Pending{}, fmt.Errorf("connect Redis for comment transition: %w", err)
	}
	defer connection.Close()
	payload, err := redigo.Bytes(connection.Do("GET", key))
	if errors.Is(err, redigo.ErrNil) {
		return Pending{}, ErrPendingNotFound
	}
	if err != nil {
		return Pending{}, fmt.Errorf("load comment transition: %w", err)
	}
	var pending Pending
	if err := decodeStrictJSON(payload, &pending); err != nil {
		return Pending{}, fmt.Errorf("decode comment transition: %w", err)
	}
	if err := validatePending(pending); err != nil {
		return Pending{}, fmt.Errorf("decode comment transition: %w", err)
	}
	return pending, nil
}

// StagePending writes an immutable transition only while the complete source
// cursor is still current. A repeated identical stage is idempotent; a newer
// lifecycle, event-free advance, or different staged plan wins with
// ErrTransitionConflict.
func (store *RedisStore) StagePending(articleCode string, source State, pending Pending) error {
	key, err := pendingKey(articleCode)
	if err != nil {
		return err
	}
	if err := validateState(source); err != nil {
		return err
	}
	if err := validatePending(pending); err != nil {
		return err
	}
	if pending.SourceRevision != source.Revision || pending.Next.Epoch != source.Epoch {
		return fmt.Errorf("%w: staged transition source does not match", ErrInvalidState)
	}
	sourcePayload, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("encode staged comment cursor source: %w", err)
	}
	payload, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("encode comment transition: %w", err)
	}
	cursor, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	_, err = store.evalTransition(
		"stage comment transition",
		stagePendingScript,
		[]string{cursor, key},
		sourcePayload,
		payload,
	)
	return err
}

// CommitPending atomically advances the cursor and removes exactly the staged
// transition that was admitted to the outbox. Reset and every other CAS
// transition are serialized by Redis, so an older epoch can never overwrite a
// newer subscription lifecycle.
func (store *RedisStore) CommitPending(articleCode string, source State, pending Pending) error {
	key, err := pendingKey(articleCode)
	if err != nil {
		return err
	}
	if err := validateState(source); err != nil {
		return err
	}
	if err := validatePending(pending); err != nil {
		return err
	}
	sourceIsNext := source.Epoch == pending.Next.Epoch && source.Revision == pending.Next.Revision
	if !sourceIsNext && (pending.SourceRevision != source.Revision || pending.Next.Epoch != source.Epoch) {
		return fmt.Errorf("%w: committed transition source does not match", ErrInvalidState)
	}
	sourcePayload, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("encode committed comment cursor source: %w", err)
	}
	nextPayload, err := json.Marshal(pending.Next)
	if err != nil {
		return fmt.Errorf("encode committed comment cursor next state: %w", err)
	}
	pendingPayload, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("encode committed comment transition: %w", err)
	}
	cursor, err := cursorKey(articleCode)
	if err != nil {
		return err
	}
	_, err = store.evalTransition(
		"commit comment transition",
		commitPendingScript,
		[]string{cursor, key},
		sourcePayload,
		nextPayload,
		pendingPayload,
	)
	return err
}

func pendingKey(articleCode string) (string, error) {
	key, err := cursorKey(articleCode)
	if err != nil {
		return "", err
	}
	return pendingKeyFromCursorKey(key), nil
}

func pendingKeyFromCursorKey(cursor string) string {
	return cursor + ":pending"
}

const stagePendingScript = cursorTransitionLuaHelpers + `
require_string_or_missing(KEYS[1])
require_string_or_missing(KEYS[2])
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  error('COMMENT_CURSOR_CONFLICT')
end
local staged = redis.call('GET', KEYS[2])
if staged then
  if staged == ARGV[2] then return 0 end
  error('COMMENT_CURSOR_CONFLICT')
end
redis.call('SET', KEYS[2], ARGV[2])
return 1
`

const commitPendingScript = cursorTransitionLuaHelpers + `
require_string_or_missing(KEYS[1])
require_string_or_missing(KEYS[2])
local current = redis.call('GET', KEYS[1])
local staged = redis.call('GET', KEYS[2])

if current == ARGV[2] then
  if staged and staged ~= ARGV[3] then error('COMMENT_CURSOR_CONFLICT') end
  if staged then redis.call('DEL', KEYS[2]) end
  return 0
end
if current ~= ARGV[1] or staged ~= ARGV[3] then
  error('COMMENT_CURSOR_CONFLICT')
end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('DEL', KEYS[2])
return 1
`
