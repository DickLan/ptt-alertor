package commentcursor

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	miniredis "github.com/alicebob/miniredis/v2"
	redigo "github.com/garyburd/redigo/redis"
)

func TestRedisStoreRoundTripAndMissing(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))

	if _, err := store.Load("M.1.A.001"); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("missing Load() error = %v", err)
	}
	state := mustState(t, article.Comments{{
		Tag: "推 ", UserID: "alice", Content: ": hello", DateTime: time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC),
	}})
	if err := store.Save("M.1.A.001", state); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load("M.1.A.001")
	if err != nil || !reflect.DeepEqual(loaded, state) {
		t.Fatalf("loaded = %#v, %v; want %#v", loaded, err, state)
	}
}

func TestRedisStoreReportsLegacyStateNeedsBaseline(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	validID := OccurrenceIDs(article.Comments{{Tag: "推 ", UserID: "a", Content: ": a", DateTime: time.Now()}})[0]
	for name, payload := range map[string]string{
		"version one": `{"version":1,"tail":[]}`,
		"version two": `{"version":2,"generation":7,"tail":["` + string(validID) + `"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			server.Set(redisKeyPrefix+"M.legacy", payload)
			if _, err := store.Load("M.legacy"); !errors.Is(err, ErrStateUpgradeNeeded) {
				t.Fatalf("Load() error = %v, want ErrStateUpgradeNeeded", err)
			}
		})
	}
}

func TestRedisStoreDistinguishesCorruptState(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	corruptPayloads := []string{
		`{`,
		`{"version":3,"epoch":"bad","revision":"bad","snapshot":[]}`,
		`{"version":3,"epoch":"00112233445566778899aabbccddeeff","revision":"` + strings64("a") + `","snapshot":null}`,
		`{"version":2,"generation":0,"tail":[]}`,
		`{"version":1,"tail":null}`,
		`{"version":1,"tail":[],"unknown":true}`,
		`{"version":99}`,
	}
	for _, payload := range corruptPayloads {
		server.Set(redisKeyPrefix+"M.corrupt", payload)
		_, err := store.Load("M.corrupt")
		if !errors.Is(err, ErrStateCorrupt) {
			t.Errorf("payload %q error = %v, want ErrStateCorrupt", payload, err)
		}
	}
}

func TestPendingTransitionRoundTripNXAndReset(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	code := "M.2.A.002"
	oldComment := article.Comment{Tag: "推 ", UserID: "old", Content: ": old", DateTime: time.Now()}
	newComment := article.Comment{Tag: "推 ", UserID: "new", Content: ": new", DateTime: time.Now().Add(time.Minute)}
	source := mustState(t, article.Comments{oldComment})
	difference, err := Diff(source, article.Comments{oldComment, newComment})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPending(source, difference.Next, article.Comments{oldComment, newComment}, newComment.DateTime, PendingEvent{
		CanonicalParts: []string{"nba", code, source.Epoch, source.Revision, difference.Next.Revision},
		Content:        "one frozen payload",
		CountAlert:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(code, source); err != nil {
		t.Fatal(err)
	}
	if err := store.StagePending(code, source, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.StagePending(code, source, pending); err != nil {
		t.Fatalf("duplicate StagePending() error = %v", err)
	}
	loaded, err := store.LoadPending(code)
	if err != nil || loaded.Version != pending.Version || loaded.SourceRevision != pending.SourceRevision ||
		!reflect.DeepEqual(loaded.Next, pending.Next) || !reflect.DeepEqual(loaded.Event, pending.Event) ||
		!equalOccurrenceIDs(OccurrenceIDs(loaded.Comments), OccurrenceIDs(pending.Comments)) ||
		!loaded.LastPushDateTime.Equal(pending.LastPushDateTime) {
		t.Fatalf("loaded pending = %#v, %v; want %#v", loaded, err, pending)
	}

	newLifecycle, err := newStateWithEpoch("ffeeddccbbaa99887766554433221100", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(code, newLifecycle); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadPending(code); !errors.Is(err, ErrPendingNotFound) {
		t.Fatalf("pending survived reset: %v", err)
	}
	loadedState, err := store.Load(code)
	if err != nil || !reflect.DeepEqual(loadedState, newLifecycle) {
		t.Fatalf("reset state = %#v, %v", loadedState, err)
	}
}

func TestRedisCursorTransitionsUseFullStateCAS(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	code := "M.3.A.003"
	oldComment := article.Comment{Tag: "推 ", UserID: "old", Content: ": old", DateTime: time.Now()}
	newComment := article.Comment{Tag: "推 ", UserID: "new", Content: ": new", DateTime: time.Now().Add(time.Minute)}
	source := mustState(t, article.Comments{oldComment})
	difference, err := Diff(source, article.Comments{oldComment, newComment})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPending(source, difference.Next, article.Comments{oldComment, newComment}, newComment.DateTime, PendingEvent{
		CanonicalParts: []string{"nba", code, source.Epoch, source.Revision, difference.Next.Revision},
		Content:        "frozen payload",
		CountAlert:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	deletion, err := Diff(source, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save(code, source); err != nil {
		t.Fatal(err)
	}
	if err := store.StagePending(code, source, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(code, source, deletion.Next); !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("Advance() over pending error = %v, want ErrTransitionConflict", err)
	}
	if err := store.CommitPending(code, source, pending); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(code)
	if err != nil || !reflect.DeepEqual(loaded, difference.Next) {
		t.Fatalf("committed state = %#v, %v", loaded, err)
	}
	if err := store.CommitPending(code, difference.Next, pending); err != nil {
		t.Fatalf("idempotent CommitPending() error = %v", err)
	}

	if err := store.Reset(code, source); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(code, source, deletion.Next); err != nil {
		t.Fatal(err)
	}
	if err := store.StagePending(code, source, pending); !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("StagePending() after advance error = %v, want ErrTransitionConflict", err)
	}
}

func TestRedisResetCannotBeOverwrittenByConcurrentPendingCommit(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	code := "M.4.A.004"
	oldComment := article.Comment{Tag: "推 ", UserID: "old", Content: ": old", DateTime: time.Now()}
	newComment := article.Comment{Tag: "推 ", UserID: "new", Content: ": new", DateTime: time.Now().Add(time.Minute)}
	source := mustState(t, article.Comments{oldComment})
	difference, err := Diff(source, article.Comments{oldComment, newComment})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPending(source, difference.Next, article.Comments{oldComment, newComment}, newComment.DateTime, PendingEvent{
		CanonicalParts: []string{"nba", code, source.Epoch, source.Revision, difference.Next.Revision},
		Content:        "frozen payload",
	})
	if err != nil {
		t.Fatal(err)
	}

	for iteration := 0; iteration < 50; iteration++ {
		if err := store.Reset(code, source); err != nil {
			t.Fatal(err)
		}
		if err := store.StagePending(code, source, pending); err != nil {
			t.Fatal(err)
		}
		newLifecycle, stateErr := NewState(nil)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			_ = store.CommitPending(code, source, pending)
		}()
		go func() {
			defer workers.Done()
			<-start
			if resetErr := store.Reset(code, newLifecycle); resetErr != nil {
				t.Errorf("Reset() error = %v", resetErr)
			}
		}()
		close(start)
		workers.Wait()

		loaded, loadErr := store.Load(code)
		if loadErr != nil || !reflect.DeepEqual(loaded, newLifecycle) {
			t.Fatalf("iteration %d state = %#v, %v; old commit overwrote reset", iteration, loaded, loadErr)
		}
		if _, pendingErr := store.LoadPending(code); !errors.Is(pendingErr, ErrPendingNotFound) {
			t.Fatalf("iteration %d pending survived reset: %v", iteration, pendingErr)
		}
	}
}

func TestRedisInitializeDoesNotOverwriteNewerLifecycle(t *testing.T) {
	server := startCursorRedis(t)
	defer server.Close()
	store := NewRedisStoreWithConnector(miniredisConnector(server))
	code := "M.5.A.005"
	baseline := mustState(t, nil)
	if err := store.Initialize(code, baseline, false); err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(code, baseline, false); err != nil {
		t.Fatalf("idempotent Initialize() error = %v", err)
	}
	other, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(code, other, false); !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("Initialize() overwrite error = %v, want ErrTransitionConflict", err)
	}

	legacyCode := "M.legacy.initialize"
	server.Set(redisKeyPrefix+legacyCode, `{"version":2,"generation":7,"tail":[]}`)
	if err := store.Initialize(legacyCode, other, true); err != nil {
		t.Fatalf("legacy Initialize() error = %v", err)
	}
	loaded, err := store.Load(legacyCode)
	if err != nil || !reflect.DeepEqual(loaded, other) {
		t.Fatalf("legacy initialized state = %#v, %v", loaded, err)
	}
}

func TestRedisStorePreservesConnectionErrorsAndRejectsInput(t *testing.T) {
	dialError := errors.New("dial failed")
	store := NewRedisStoreWithConnector(func() (redigo.Conn, error) { return nil, dialError })
	state := mustState(t, nil)
	if _, err := store.Load("M.1.A.001"); !errors.Is(err, dialError) {
		t.Fatalf("Load() error = %v", err)
	}
	if err := store.Save("M.1.A.001", state); !errors.Is(err, dialError) {
		t.Fatalf("Save() error = %v", err)
	}
	if err := store.Reset("M.1.A.001", state); !errors.Is(err, dialError) {
		t.Fatalf("Reset() error = %v", err)
	}
	if err := store.Initialize("M.1.A.001", state, false); !errors.Is(err, dialError) {
		t.Fatalf("Initialize() error = %v", err)
	}
	if err := store.Save("", state); !errors.Is(err, ErrArticleCodeEmpty) {
		t.Fatalf("empty code error = %v", err)
	}
	if err := store.Save("M.invalid", State{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("invalid state error = %v", err)
	}
}

func startCursorRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func miniredisConnector(server *miniredis.Miniredis) Connector {
	return func() (redigo.Conn, error) { return redigo.Dial("tcp", server.Addr()) }
}

func strings64(value string) string {
	result := ""
	for len(result) < 64 {
		result += value
	}
	return result[:64]
}
