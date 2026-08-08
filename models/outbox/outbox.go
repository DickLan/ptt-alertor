// Package outbox provides durable, at-least-once notification storage.
//
// Event identifiers describe the logical event, not a delivery attempt. Callers
// should use stable, collision-resistant source identities such as a complete
// PTT article code or canonical URL. In particular, a Unix-second article ID is
// not unique enough to identify an event.
package outbox

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	eventIDDomain = "ptt-alertor:discord-outbox:event:v1"

	// These limits are duplicated at this persistence boundary deliberately:
	// corrupt or future callers must not turn one logical event into an
	// unbounded number of Discord requests or Redis bytes.
	MaxChunksPerItem    = 8
	MaxChunkUTF16Units  = 2000
	MaxItemPayloadBytes = 64 * 1024
)

var (
	// ErrInvalidItem means an item is missing a required immutable field.
	ErrInvalidItem = errors.New("invalid outbox item")
	// ErrEventConflict means the same event ID was used for different payloads.
	ErrEventConflict = errors.New("outbox event payload conflicts with the stored payload")
	// ErrItemNotFound means an operation referenced neither a pending nor a done item.
	ErrItemNotFound = errors.New("outbox item not found")
	// ErrStaleLease means a worker no longer owns the item it tried to mutate.
	ErrStaleLease = errors.New("outbox lease token is stale")
	// ErrLeaseExpired means the worker's otherwise matching lease has expired.
	ErrLeaseExpired = errors.New("outbox lease expired")
	// ErrInvalidState means the durable item is not in the required state.
	ErrInvalidState = errors.New("outbox item is in an invalid state")
	// ErrCorruptData means a durable item violates an outbox invariant.
	ErrCorruptData = errors.New("outbox data is corrupt")
	// ErrHighWatermark means admitting another item would exceed the configured limit.
	ErrHighWatermark = errors.New("outbox high watermark reached")
	// ErrWrongRedisType means an outbox key collides with incompatible Redis data.
	ErrWrongRedisType = errors.New("outbox Redis key has the wrong type")
)

// Item is the immutable portion of one logical Discord notification.
// Chunks must already respect Discord's per-message content limit. The outbox
// stores the exact chunk list once and resumes from the persisted chunk index.
type Item struct {
	EventID    string
	Kind       string
	Chunks     []string
	CountAlert bool
}

// NewItem validates and copies a notification payload. canonicalIdentityParts
// must contain the full stable source identity, not a truncated timestamp.
func NewItem(kind string, canonicalIdentityParts, chunks []string, countAlert bool) (Item, error) {
	item := Item{
		EventID:    DeterministicEventID(kind, canonicalIdentityParts...),
		Kind:       kind,
		Chunks:     append([]string(nil), chunks...),
		CountAlert: countAlert,
	}
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	return item, nil
}

// DeterministicEventID returns a lowercase SHA-256 identifier for a logical
// event. Each component is length-prefixed, so ("ab", "c") and ("a", "bc")
// cannot alias. String normalization is deliberately left to the caller because
// board names, PTT codes, canonical links, thresholds, and revisions have
// different normalization rules.
func DeterministicEventID(kind string, canonicalIdentityParts ...string) string {
	hash := sha256.New()
	writeHashPart(hash, eventIDDomain)
	writeHashPart(hash, kind)
	for _, part := range canonicalIdentityParts {
		writeHashPart(hash, part)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashPart(hash hashWriter, part string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(part)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write([]byte(part))
}

// Validate checks the immutable notification fields.
func (item Item) Validate() error {
	if !validEventID(item.EventID) {
		return fmt.Errorf("%w: event ID must be a lowercase SHA-256 digest", ErrInvalidItem)
	}
	if strings.TrimSpace(item.Kind) == "" {
		return fmt.Errorf("%w: kind is required", ErrInvalidItem)
	}
	if len(item.Kind) > 128 || !utf8.ValidString(item.Kind) {
		return fmt.Errorf("%w: kind is too long or invalid UTF-8", ErrInvalidItem)
	}
	if len(item.Chunks) == 0 {
		return fmt.Errorf("%w: at least one chunk is required", ErrInvalidItem)
	}
	if len(item.Chunks) > MaxChunksPerItem {
		return fmt.Errorf("%w: chunk count exceeds %d", ErrInvalidItem, MaxChunksPerItem)
	}
	totalBytes := 0
	for index, chunk := range item.Chunks {
		if chunk == "" {
			return fmt.Errorf("%w: chunk %d is empty", ErrInvalidItem, index)
		}
		if !utf8.ValidString(chunk) {
			return fmt.Errorf("%w: chunk %d is invalid UTF-8", ErrInvalidItem, index)
		}
		if utf16Length(chunk) > MaxChunkUTF16Units {
			return fmt.Errorf("%w: chunk %d exceeds %d UTF-16 units", ErrInvalidItem, index, MaxChunkUTF16Units)
		}
		totalBytes += len(chunk)
		if totalBytes > MaxItemPayloadBytes {
			return fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidItem, MaxItemPayloadBytes)
		}
	}
	return nil
}

func utf16Length(value string) int {
	units := 0
	for _, r := range value {
		units++
		if r > 0xffff {
			units++
		}
	}
	return units
}

func validEventID(id string) bool {
	if len(id) != sha256.Size*2 {
		return false
	}
	for _, char := range id {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func payloadHash(item Item) string {
	hash := sha256.New()
	writeHashPart(hash, "ptt-alertor:discord-outbox:payload:v1")
	writeHashPart(hash, item.Kind)
	if item.CountAlert {
		writeHashPart(hash, "1")
	} else {
		writeHashPart(hash, "0")
	}
	for _, chunk := range item.Chunks {
		writeHashPart(hash, chunk)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// EnqueueStatus describes whether Enqueue created or found a durable event.
type EnqueueStatus int

const (
	EnqueueCreated EnqueueStatus = iota + 1
	EnqueueAlreadyPending
	EnqueueAlreadyDone
)

// EnqueueResult is returned for both newly admitted and idempotent events.
type EnqueueResult struct {
	EventID string
	Status  EnqueueStatus
}

// ClaimedItem is an immutable item plus its durable delivery cursor and lease.
type ClaimedItem struct {
	Item
	NextChunk       int
	Attempts        int64
	Recoveries      int64
	LeaseToken      string
	LeaseGeneration int64
	LeaseUntilMS    int64
}

// CurrentChunk returns the next unsent immutable chunk.
func (item ClaimedItem) CurrentChunk() (string, error) {
	if item.NextChunk < 0 || item.NextChunk >= len(item.Chunks) {
		return "", fmt.Errorf("%w: next chunk %d of %d", ErrCorruptData, item.NextChunk, len(item.Chunks))
	}
	return item.Chunks[item.NextChunk], nil
}

// AckResult describes a confirmed chunk transition.
type AckResult struct {
	Completed   bool
	AlreadyDone bool
	NextChunk   int
	// AlertCount is set only when the completed item requested counter updates.
	AlertCount *int64
}

// RetryResult describes a durable requeue. Retry never deletes the item.
type RetryResult struct {
	AlreadyDone   bool
	Attempts      int64
	NextAttemptMS int64
}
