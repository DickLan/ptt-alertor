package stockwatch

import (
	"context"
	"errors"
	"fmt"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"strings"
	"sync"
	"testing"
	"time"
)

var testStart = time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)

func fixture(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	r := miniredis.RunT(t)
	r.SetTime(testStart)
	pool := &redis.Pool{MaxIdle: 8, MaxActive: 32, Wait: true, Dial: func() (redis.Conn, error) { return redis.Dial("tcp", r.Addr()) }}
	t.Cleanup(func() { pool.Close() })
	s := New(&Store{Connect: pool.Get, Prefix: "test:stock"}, true, "https://discord.com/api/webhooks/123/test-stock", "https://discord.com/api/webhooks/456/test-old", strings.Repeat("t", 64))
	if !s.Ready() {
		t.Fatal(s.ConfigError)
	}
	return s, r
}
func good(author, title, suffix string, second int64) article.Article {
	return article.Article{Code: fmt.Sprintf("M.%d.A.%s", second, suffix), ID: int(second), Author: author, Title: title}
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func due(r *miniredis.Miniredis, round int) {
	interval := time.Duration(IntervalSeconds) * time.Second
	r.SetTime(testStart.Add(time.Duration(round) * interval))
	r.FastForward(interval)
}
func fetchRows(s *Service, rows article.Articles) {
	s.Fetch = func(_ context.Context, b *board.Board) error {
		b.OnlineArticles = rows
		b.NewArticles = rows
		return nil
	}
}

func TestEmptyScheduleAndActivationIdentity(t *testing.T) {
	s, r := fixture(t)
	calls := 0
	s.Fetch = func(context.Context, *board.Board) error { calls++; return errors.New("offline") }
	must(t, s.Tick(context.Background()))
	if calls != 0 {
		t.Fatal("empty list crawled")
	}
	must(t, s.Store.Add([]string{"Yipi1357", "YIPI1357"}))
	a, _ := s.Store.Authors()
	if len(a) != 1 {
		t.Fatal(a)
	}
	before, _ := s.Store.State()
	must(t, s.Store.Add([]string{"yipi1357"}))
	after, _ := s.Store.State()
	if before["next_attempt_at_ms"] != after["next_attempt_at_ms"] {
		t.Fatal("duplicate reset schedule")
	}
	must(t, s.Tick(context.Background()))
	if calls != 0 {
		t.Fatal("polled early")
	}
	due(r, 1)
	if s.Tick(context.Background()) == nil {
		t.Fatal("expected fetch failure")
	}
	if calls != 1 {
		t.Fatal(calls)
	}
	// A new service instance (restart) respects the persisted failed-attempt schedule.
	next := New(s.Store, true, "https://discord.com/api/webhooks/123/test-stock", "", strings.Repeat("t", 64))
	next.Fetch = s.Fetch
	must(t, next.Tick(context.Background()))
	if calls != 1 {
		t.Fatal("restart retried before the configured interval")
	}
	state, _ := s.Store.State()
	if state["last_success_at_ms"] != "" || state["consecutive_failures"] != "1" {
		t.Fatal(state)
	}
	must(t, s.Store.Remove("YIPI1357"))
	must(t, s.Tick(context.Background()))
	if calls != 1 {
		t.Fatal("removed list crawled")
	}
	must(t, s.Store.Add([]string{"yipi1357"}))
	a2, _ := s.Store.Authors()
	if a[0].Generation == a2[0].Generation {
		t.Fatal("re-add reused generation")
	}
}

func TestFirstPollAllPostKindsAndFullCodeDedup(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Yipi1357"}))
	ts := testStart.Unix()
	rows := article.Articles{good("YIPI1357", "[標的] before", "OLD", ts-1), good("yipi1357", "Re: [討論] same second", "001", ts), good("yipi1357", "Fw: [新聞] same second", "002", ts), good("yipi1357", "[心得] later", "003", ts+1), good("yipi13570", "[標的] not exact author", "004", ts+1)}
	fetchRows(s, rows)
	due(r, 1)
	must(t, s.Tick(context.Background()))
	n, e := s.Outbox.PendingCount(context.Background())
	must(t, e)
	if n != 3 {
		t.Fatalf("got %d notifications", n)
	}
	rows[1].Title = "edited title"
	fetchRows(s, rows)
	due(r, 2)
	must(t, s.Tick(context.Background()))
	n, _ = s.Outbox.PendingCount(context.Background())
	if n != 3 {
		t.Fatal("editing or retry duplicated events")
	}
	item, e := s.Outbox.Claim(context.Background(), time.Minute)
	must(t, e)
	allowed, e := s.Eligible(context.Background(), item)
	must(t, e)
	if !allowed {
		t.Fatal("active author ineligible")
	}
	must(t, s.Store.Remove("yipi1357"))
	allowed, e = s.Eligible(context.Background(), item)
	must(t, e)
	if allowed {
		t.Fatal("removed author eligible")
	}
	must(t, s.Store.Add([]string{"yipi1357"}))
	allowed, e = s.Eligible(context.Background(), item)
	must(t, e)
	if allowed {
		t.Fatal("old generation eligible after re-add")
	}
}

func TestSyntheticCursorAndFencedEpoch(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha"}))
	due(r, 1)
	a, e := s.Store.Begin()
	must(t, e)
	if a == nil {
		t.Fatal("no due attempt")
	}
	d := cursorDriver{store: s.Store, attempt: a, boundary: testStart.Unix()}
	rows, initialized, e := d.GetArticlesE("Stock")
	must(t, e)
	if !initialized || len(rows) != 1 || rows[0].ID != int(testStart.Unix()-1) || rows[0].Code != "" || r.Exists(s.Store.key("cursor")) {
		t.Fatal("synthetic cursor invalid or persisted")
	}
	r.Set(s.Store.key("cursor"), "corrupt")
	if _, _, e = d.GetArticlesE("Stock"); e == nil {
		t.Fatal("corrupt cursor disguised as missing")
	}
	r.Del(s.Store.key("cursor"))
	must(t, s.Store.Remove("alpha"))
	must(t, s.Store.Add([]string{"Beta"}))
	if s.Store.Finish(a, article.Articles{good("Alpha", "x", "001", testStart.Unix())}, "", 1, 1) == nil {
		t.Fatal("stale epoch wrote cursor")
	}
	if r.Exists(s.Store.key("cursor")) {
		t.Fatal("stale cursor exists")
	}
}

func TestOnlyOneConcurrentAttemptAndIndependentCounter(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha"}))
	due(r, 1)
	var wg sync.WaitGroup
	got := make(chan *Attempt, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, e := s.Store.Begin()
			if e != nil {
				t.Error(e)
			}
			if a != nil {
				got <- a
			}
		}()
	}
	wg.Wait()
	close(got)
	if len(got) != 1 {
		t.Fatal("concurrent attempts", len(got))
	}
	r.Set("counter:alert", "77")
	r.Set("user:untouched", "old-user")
	item, e := outbox.NewItem("stockwatch", []string{"test"}, []string{"test"}, true)
	must(t, e)
	_, e = s.Outbox.Enqueue(context.Background(), item)
	must(t, e)
	c, e := s.Outbox.Claim(context.Background(), time.Minute)
	must(t, e)
	_, e = s.Outbox.AckChunk(context.Background(), c.EventID, c.LeaseToken, "123456", time.Minute)
	must(t, e)
	old, _ := r.Get("counter:alert")
	own, _ := r.Get(s.Store.key("sent_count"))
	if old != "77" || own != "1" {
		t.Fatal("counter isolation", old, own)
	}
}

func TestMalformedThreeAttemptsAndGapVisible(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha"}))
	goodRow := good("Other", "valid", "001", testStart.Unix()+1)
	bad := article.Article{Author: "Alpha", Code: "malformed", Title: "broken"}
	fetchRows(s, article.Articles{goodRow, bad, bad})
	for round := 1; round <= 3; round++ {
		due(r, round)
		e := s.Tick(context.Background())
		if (round < 3) != (e != nil) {
			t.Fatalf("round %d: %v", round, e)
		}
	}
	state, _ := s.Store.State()
	if state["skipped_invalid"] != "1" || state["last_error"] != "" {
		t.Fatal(state)
	}
	saved, _ := r.Get(s.Store.key("cursor"))
	if strings.Contains(saved, "malformed") {
		t.Fatal("malformed cursor persisted")
	}
	s.Fetch = func(context.Context, *board.Board) error { return board.ErrCatchUpPageLimit }
	due(r, 4)
	if s.Tick(context.Background()) == nil {
		t.Fatal("gap hidden")
	}
	state, _ = s.Store.State()
	if state["last_error"] != "ptt_catchup_limit" {
		t.Fatal(state)
	}
	current, _ := r.Get(s.Store.key("cursor"))
	if current != saved {
		t.Fatal("gap advanced cursor")
	}
}

func TestRemovalDuringFetchAndRedisFailureFailClosed(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha", "Beta"}))
	due(r, 1)
	s.Fetch = func(_ context.Context, b *board.Board) error {
		must(t, s.Store.Remove("Alpha"))
		b.OnlineArticles = article.Articles{good("Alpha", "new", "001", testStart.Unix()+2)}
		b.NewArticles = b.OnlineArticles
		return nil
	}
	must(t, s.Tick(context.Background()))
	n, _ := s.Outbox.PendingCount(context.Background())
	if n != 0 {
		t.Fatal("removed author enqueued")
	}
	r.Del(s.Store.key("authors"))
	r.Set(s.Store.key("authors"), "wrong-type")
	if _, e := s.Store.Authors(); e == nil {
		t.Fatal("storage failure shown as empty")
	}
	if _, e := s.Snapshot(context.Background()); e == nil {
		t.Fatal("storage failure hidden from admin")
	}
}

func TestWebhookDoesNotFallBack(t *testing.T) {
	s, _ := fixture(t)
	for _, hook := range []string{"", "https://discord.com/api/webhooks/456/test-old"} {
		n := New(s.Store, true, hook, "https://discord.com/api/webhooks/456/test-old", strings.Repeat("t", 64))
		if n.Ready() {
			t.Fatal("accepted missing/shared webhook")
		}
	}
}

func TestDisplayedAuthorRequiresCompleteID(t *testing.T) {
	for raw, want := range map[string]string{"Alpha": "alpha", " ALPHA (小明) ": "alpha", "Alpha(名字有括號 (x))": "alpha", "Alpha0 (different)": "alpha0", "SomeAlpha (nickname)": "somealpha", "Alpha Beta": "", "<Alpha>": "", "(Alpha)": ""} {
		if got := authorKey(raw); got != want {
			t.Errorf("authorKey(%q)=%q; want %q", raw, got, want)
		}
	}
}

func TestMalformedRegistryFailureVisible(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha"}))
	due(r, 1)
	r.Set(s.Store.key("invalid"), "wrong-type")
	fetchRows(s, article.Articles{good("Other", "valid", "001", testStart.Unix()), {Author: "Alpha", Code: "malformed"}})
	if s.Tick(context.Background()) == nil {
		t.Fatal("malformed registry error hidden")
	}
	state, e := s.Store.State()
	must(t, e)
	if state["last_error"] != "malformed_registry_unavailable" {
		t.Fatal(state)
	}
}
