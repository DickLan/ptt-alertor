package commentcursor

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

const testEpoch = "00112233445566778899aabbccddeeff"

func TestOccurrenceIDsCoverFieldsMinuteAndOrdinal(t *testing.T) {
	baseTime := time.Date(2026, time.July, 13, 10, 15, 5, 0, time.FixedZone("CST", 8*60*60))
	base := article.Comment{Tag: "推 ", UserID: "alice", Content: ": hello", DateTime: baseTime}
	sameMinute := base
	sameMinute.DateTime = baseTime.Add(45 * time.Second)

	baseID := OccurrenceIDs(article.Comments{base})[0]
	sameMinuteID := OccurrenceIDs(article.Comments{sameMinute})[0]
	if baseID != sameMinuteID {
		t.Fatalf("same-minute IDs differ: %q != %q", baseID, sameMinuteID)
	}
	repeated := OccurrenceIDs(article.Comments{base, sameMinute})
	if fingerprintPart(repeated[0]) != fingerprintPart(repeated[1]) ||
		!strings.HasSuffix(string(repeated[0]), ":1") || !strings.HasSuffix(string(repeated[1]), ":2") {
		t.Fatalf("repeated occurrence IDs = %#v", repeated)
	}

	variants := []article.Comment{base, base, base, base}
	variants[0].Tag = "噓 "
	variants[1].UserID = "bob"
	variants[2].Content = ": changed"
	variants[3].DateTime = baseTime.Add(time.Minute)
	for _, variant := range variants {
		if fingerprintPart(OccurrenceIDs(article.Comments{variant})[0]) == fingerprintPart(baseID) {
			t.Fatalf("variant %#v did not change fingerprint", variant)
		}
	}
}

func TestDiffFindsRepeatedCommentAddedInSameMinute(t *testing.T) {
	minute := time.Date(2026, time.July, 13, 11, 20, 0, 0, time.UTC)
	first := article.Comment{Tag: "推 ", UserID: "alice", Content: ": same", DateTime: minute.Add(5 * time.Second)}
	second := first
	second.DateTime = minute.Add(50 * time.Second)
	previous := mustState(t, article.Comments{first})

	difference, err := Diff(previous, article.Comments{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(difference.NewComments, article.Comments{second}) {
		t.Fatalf("new comments = %#v, want second occurrence", difference.NewComments)
	}
	if difference.Next.Epoch != previous.Epoch || difference.Next.Revision == previous.Revision {
		t.Fatalf("next state did not retain epoch and advance revision: %#v", difference.Next)
	}
}

func TestDiffDoesNotLoseNewCommentBeforeRecreatedOldFingerprint(t *testing.T) {
	start := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	a := article.Comment{Tag: "推 ", UserID: "a", Content: ": A", DateTime: start}
	b := article.Comment{Tag: "推 ", UserID: "b", Content: ": B", DateTime: start.Add(time.Minute)}
	x := article.Comment{Tag: "推 ", UserID: "x", Content: ": X", DateTime: start.Add(2 * time.Minute)}
	previous := mustState(t, article.Comments{a, b})

	// Old B was deleted, then X and a byte-identical B were appended. A global
	// LCS incorrectly anchors on the final B and loses X; prefix-subsequence
	// matching must start the at-least-once suffix at X.
	difference, err := Diff(previous, article.Comments{a, x, b})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(difference.NewComments, article.Comments{x, b}) {
		t.Fatalf("new suffix = %#v, want X and recreated B", difference.NewComments)
	}
}

func TestDiffDeletionOnlyIsAnOldSubsequence(t *testing.T) {
	start := time.Date(2026, 7, 13, 12, 30, 0, 0, time.UTC)
	comment := func(name string, minute int) article.Comment {
		return article.Comment{Tag: "推 ", UserID: name, Content: ": " + name, DateTime: start.Add(time.Duration(minute) * time.Minute)}
	}
	a, b, c := comment("a", 0), comment("b", 1), comment("c", 2)
	difference, err := Diff(mustState(t, article.Comments{a, b, c}), article.Comments{a, c})
	if err != nil {
		t.Fatal(err)
	}
	if len(difference.NewComments) != 0 || difference.NewComments == nil {
		t.Fatalf("deletion-only diff = %#v, want non-nil empty", difference.NewComments)
	}
}

func TestDiffConservativelyResendsAfterDuplicateRenumber(t *testing.T) {
	minute := time.Date(2026, 7, 13, 13, 0, 0, 0, time.UTC)
	a := article.Comment{Tag: "推 ", UserID: "same", Content: ": repeated", DateTime: minute}
	x := article.Comment{Tag: "推 ", UserID: "anchor", Content: ": x", DateTime: minute.Add(time.Minute)}
	previous := mustState(t, article.Comments{a, a, x})
	difference, err := Diff(previous, article.Comments{a, x, a})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(difference.NewComments, article.Comments{a}) {
		t.Fatalf("conservative suffix = %#v, want appended A after ordinal renumber", difference.NewComments)
	}
}

func TestDiffWithoutCommonOccurrenceReturnsAll(t *testing.T) {
	start := time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC)
	old := article.Comment{Tag: "推 ", UserID: "old", Content: ": old", DateTime: start}
	current := article.Comments{{Tag: "推 ", UserID: "new", Content: ": new", DateTime: start}}
	previous := mustState(t, article.Comments{old})
	copyBefore := previous
	copyBefore.Snapshot = append([]OccurrenceID(nil), previous.Snapshot...)

	difference, err := Diff(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(difference.NewComments, current) || !reflect.DeepEqual(previous, copyBefore) {
		t.Fatalf("difference=%#v previous=%#v", difference, previous)
	}
}

func TestVersionThreeKeepsCompleteSnapshotBeyondLegacyTail(t *testing.T) {
	start := time.Date(2026, 7, 13, 14, 0, 0, 0, time.UTC)
	comments := make(article.Comments, 80)
	for index := range comments {
		comments[index] = article.Comment{
			Tag: "推 ", UserID: "user", Content: ": " + string(rune(0x4e00+index)),
			DateTime: start.Add(time.Duration(index) * time.Minute),
		}
	}
	state := mustState(t, comments)
	if len(state.Snapshot) != len(comments) || !reflect.DeepEqual(state.Snapshot, OccurrenceIDs(comments)) {
		t.Fatalf("snapshot length = %d, want %d complete entries", len(state.Snapshot), len(comments))
	}

	// Delete more than the old 32-entry tail and append one comment. The full
	// snapshot still identifies the survivor prefix and the new suffix.
	newComment := article.Comment{Tag: "推 ", UserID: "new", Content: ": new", DateTime: start.Add(100 * time.Minute)}
	current := append(append(article.Comments(nil), comments[:20]...), newComment)
	difference, err := Diff(state, current)
	if err != nil || !reflect.DeepEqual(difference.NewComments, article.Comments{newComment}) {
		t.Fatalf("large deletion diff = (%#v, %v)", difference, err)
	}
}

func TestRevisionHashChainNeverReusesStateCycle(t *testing.T) {
	minute := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	a := article.Comment{Tag: "推 ", UserID: "same", Content: ": same", DateTime: minute}
	one := article.Comments{a}
	two := article.Comments{a, a}
	first := mustState(t, one)
	added, err := Diff(first, two)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := Diff(added.Next, one)
	if err != nil {
		t.Fatal(err)
	}
	if !equalOccurrenceIDs(first.Snapshot, deleted.Next.Snapshot) {
		t.Fatalf("cycled snapshot differs")
	}
	if first.Revision == deleted.Next.Revision {
		t.Fatalf("state cycle reused revision %q", first.Revision)
	}
	secondAdded, err := Diff(deleted.Next, two)
	if err != nil || secondAdded.Next.Revision == added.Next.Revision {
		t.Fatalf("second transition reused first revision: (%#v, %v)", secondAdded, err)
	}
}

func TestDifferentTrackingEpochsNeverReuseInitialRevision(t *testing.T) {
	comment := article.Comment{Tag: "推 ", UserID: "same", Content: ": same", DateTime: time.Now()}
	first, err := newStateWithEpoch(testEpoch, article.Comments{comment})
	if err != nil {
		t.Fatal(err)
	}
	second, err := newStateWithEpoch("ffeeddccbbaa99887766554433221100", article.Comments{comment})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision == second.Revision {
		t.Fatalf("different lifecycle epochs reused revision %q", first.Revision)
	}
}

func TestStateValidationRejectsMalformedValues(t *testing.T) {
	if _, err := Diff(State{}, nil); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Diff() error = %v, want ErrInvalidState", err)
	}
	state := mustState(t, nil)
	state.Snapshot = nil
	if _, err := Revision(state); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Revision() error = %v, want ErrInvalidState", err)
	}
}

func TestOccurrenceIDValidationRejectsNonCanonicalForms(t *testing.T) {
	id := OccurrenceIDs(article.Comments{{
		Tag: "推 ", UserID: "user", Content: ": value", DateTime: time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC),
	}})[0]
	fingerprint := fingerprintPart(id)
	for name, invalid := range map[string]OccurrenceID{
		"uppercase fingerprint": OccurrenceID(strings.ToUpper(fingerprint) + ":1"),
		"leading-zero ordinal":  OccurrenceID(fingerprint + ":01"),
	} {
		t.Run(name, func(t *testing.T) {
			if validOccurrenceID(invalid) {
				t.Fatalf("validOccurrenceID(%q) = true", invalid)
			}
		})
	}
}

func mustState(t *testing.T, comments article.Comments) State {
	t.Helper()
	state, err := newStateWithEpoch(testEpoch, comments)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func fingerprintPart(id OccurrenceID) string {
	value := string(id)
	return value[:strings.LastIndexByte(value, ':')]
}
