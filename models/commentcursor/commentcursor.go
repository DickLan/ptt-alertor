// Package commentcursor tracks ordered PTT comment occurrences without relying
// on PTT's minute-resolution timestamps alone.
package commentcursor

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

const (
	StateVersion       = 3
	legacyStateVersion = 2
	maxSnapshotEntries = 100_000
	epochBytes         = 16
)

var (
	// ErrCursorNotFound is retained for source compatibility. Version 3 uses a
	// no-loss, at-least-once suffix when it cannot match an occurrence.
	ErrCursorNotFound     = errors.New("comment cursor has no common occurrence")
	ErrInvalidState       = errors.New("comment cursor state is invalid")
	ErrStateUpgradeNeeded = errors.New("comment cursor state requires a version 3 baseline")
	ErrEpochGeneration    = errors.New("comment cursor epoch generation failed")
)

// OccurrenceID combines a content fingerprint with its one-based occurrence
// ordinal among comments with that exact fingerprint. The ordinal distinguishes
// repeated, otherwise identical comments posted within the same minute.
type OccurrenceID string

// State is the authoritative ordered snapshot for one tracking lifecycle.
// Epoch prevents done-marker collisions after unsubscribe/re-subscribe or
// cursor-key recovery. Revision is a hash chain, so a structural state cycle
// never reuses an earlier transition identity.
type State struct {
	Version  int            `json:"version"`
	Epoch    string         `json:"epoch"`
	Revision string         `json:"revision"`
	Snapshot []OccurrenceID `json:"snapshot"`
}

// Difference contains both the newly observed suffix and the only state that
// may be persisted after its notification transition is durable.
type Difference struct {
	NewComments article.Comments
	Next        State
}

// OccurrenceIDs returns stable ordered IDs for one complete PTT comment list.
func OccurrenceIDs(comments article.Comments) []OccurrenceID {
	ids := make([]OccurrenceID, 0, len(comments))
	ordinals := make(map[string]uint64)
	for _, comment := range comments {
		fingerprint := commentFingerprint(comment)
		ordinals[fingerprint]++
		ids = append(ids, OccurrenceID(fingerprint+":"+strconv.FormatUint(ordinals[fingerprint], 10)))
	}
	return ids
}

// NewState creates the first authoritative cursor for a tracking lifecycle.
func NewState(comments article.Comments) (State, error) {
	epoch, err := NewEpoch()
	if err != nil {
		return State{}, err
	}
	return newStateWithEpoch(epoch, comments)
}

// NewEpoch returns a cryptographically random lifecycle identifier.
func NewEpoch() (string, error) {
	value := make([]byte, epochBytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("%w: %v", ErrEpochGeneration, err)
	}
	return hex.EncodeToString(value), nil
}

func newStateWithEpoch(epoch string, comments article.Comments) (State, error) {
	snapshot := OccurrenceIDs(comments)
	if snapshot == nil {
		snapshot = make([]OccurrenceID, 0)
	}
	state := State{
		Version:  StateVersion,
		Epoch:    epoch,
		Snapshot: append([]OccurrenceID(nil), snapshot...),
	}
	if state.Snapshot == nil {
		state.Snapshot = make([]OccurrenceID, 0)
	}
	state.Revision = chainedRevision(state.Epoch, "initial", state.Snapshot)
	if err := validateState(state); err != nil {
		return State{}, err
	}
	return state, nil
}

// Diff finds the longest current prefix that can still be explained as an
// ordered subsequence of the previous complete snapshot. The first current
// occurrence that cannot be matched starts the new suffix; once a new comment
// appears, later comments cannot be old because PTT appends pushes in order.
// Exact occurrence IDs deliberately fail conservative when deletion renumbers
// duplicates: extra at-least-once notifications are preferable to a false LCS
// anchor that permanently hides a genuinely new comment.
func Diff(previous State, current article.Comments) (Difference, error) {
	if err := validateState(previous); err != nil {
		return Difference{}, err
	}

	currentSnapshot := OccurrenceIDs(current)
	boundary := len(currentSnapshot)
	searchFrom := 0
	for currentIndex, currentID := range currentSnapshot {
		matchedAt := -1
		for previousIndex := searchFrom; previousIndex < len(previous.Snapshot); previousIndex++ {
			if previous.Snapshot[previousIndex] == currentID {
				matchedAt = previousIndex
				break
			}
		}
		if matchedAt < 0 {
			boundary = currentIndex
			break
		}
		searchFrom = matchedAt + 1
	}

	next := State{
		Version:  StateVersion,
		Epoch:    previous.Epoch,
		Revision: previous.Revision,
		Snapshot: append([]OccurrenceID(nil), currentSnapshot...),
	}
	if next.Snapshot == nil {
		next.Snapshot = make([]OccurrenceID, 0)
	}
	if !equalOccurrenceIDs(previous.Snapshot, next.Snapshot) {
		next.Revision = chainedRevision(previous.Epoch, previous.Revision, next.Snapshot)
	}
	if err := validateState(next); err != nil {
		return Difference{}, err
	}
	return Difference{
		NewComments: cloneComments(current[boundary:]),
		Next:        next,
	}, nil
}

// Revision returns the persisted hash-chain head after strict validation.
func Revision(state State) (string, error) {
	if err := validateState(state); err != nil {
		return "", err
	}
	return state.Revision, nil
}

func chainedRevision(epoch, source string, snapshot []OccurrenceID) string {
	hasher := sha256.New()
	writeFingerprintField(hasher, "ptt-alertor-comment-cursor-v3")
	writeFingerprintField(hasher, epoch)
	writeFingerprintField(hasher, source)
	for _, id := range snapshot {
		writeFingerprintField(hasher, string(id))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func cloneComments(comments article.Comments) article.Comments {
	if len(comments) == 0 {
		return make(article.Comments, 0)
	}
	return append(article.Comments(nil), comments...)
}

func commentFingerprint(comment article.Comment) string {
	hasher := sha256.New()
	writeFingerprintField(hasher, comment.Tag)
	writeFingerprintField(hasher, comment.UserID)
	writeFingerprintField(hasher, comment.Content)
	minute := comment.DateTime.UTC().Truncate(time.Minute).Format(time.RFC3339)
	writeFingerprintField(hasher, minute)
	return hex.EncodeToString(hasher.Sum(nil))
}

func writeFingerprintField(hasher hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(value))
}

func validateState(state State) error {
	if state.Version != StateVersion {
		return fmt.Errorf("%w: version %d", ErrInvalidState, state.Version)
	}
	if !validFixedHex(state.Epoch, epochBytes*2) {
		return fmt.Errorf("%w: epoch is malformed", ErrInvalidState)
	}
	if !validFixedHex(state.Revision, sha256.Size*2) {
		return fmt.Errorf("%w: revision is malformed", ErrInvalidState)
	}
	if state.Snapshot == nil {
		return fmt.Errorf("%w: snapshot is missing", ErrInvalidState)
	}
	if len(state.Snapshot) > maxSnapshotEntries {
		return fmt.Errorf("%w: snapshot has %d entries", ErrInvalidState, len(state.Snapshot))
	}
	seen := make(map[OccurrenceID]struct{}, len(state.Snapshot))
	for _, id := range state.Snapshot {
		if !validOccurrenceID(id) {
			return fmt.Errorf("%w: malformed occurrence ID", ErrInvalidState)
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: duplicate occurrence ID", ErrInvalidState)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validFixedHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func equalOccurrenceIDs(left, right []OccurrenceID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validOccurrenceID(id OccurrenceID) bool {
	value := string(id)
	separator := strings.LastIndexByte(value, ':')
	if separator != sha256.Size*2 || separator == len(value)-1 {
		return false
	}
	fingerprintBytes, err := hex.DecodeString(value[:separator])
	if err != nil || hex.EncodeToString(fingerprintBytes) != value[:separator] {
		return false
	}
	ordinalText := value[separator+1:]
	ordinal, err := strconv.ParseUint(ordinalText, 10, 64)
	return err == nil && ordinal > 0 && strconv.FormatUint(ordinal, 10) == ordinalText
}
