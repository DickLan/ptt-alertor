package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestArticleScopeActualCollectorAndDiscordReceiver(t *testing.T) {
	var mu sync.Mutex
	received := []string{}
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		mu.Lock()
		received = append(received, body.Content)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"123456789"}`)
	}))
	defer receiver.Close()
	r := miniredis.RunT(t)
	r.SetTime(time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC))
	pool := &redis.Pool{Dial: func() (redis.Conn, error) { return redis.Dial("tcp", r.Addr()) }}
	defer pool.Close()
	s := stockwatch.New(&stockwatch.Store{Connect: pool.Get, Prefix: "test:scope-delivery"}, true, receiver.URL, "", strings.Repeat("t", 64))
	ctx := context.Background()
	if e := s.Store.Add([]string{"Alpha", "Beta", "Gamma"}); e != nil {
		t.Fatal(e)
	}
	if e := s.Store.SetArticleScope("Gamma", "targets"); e != nil {
		t.Fatal(e)
	}
	ts := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC).Unix()
	rows := article.Articles{}
	for i, name := range []string{"Alpha", "Beta", "Gamma"} {
		for j, title := range []string{"[新聞] news", "Re: [標的] target"} {
			rows = append(rows, article.Article{Code: fmt.Sprintf("M.%d.A.%03d", ts, i*2+j), ID: int(ts), Author: name, Title: title})
		}
	}
	s.Fetch = func(_ context.Context, b *board.Board) error {
		b.NewArticles = rows
		b.OnlineArticles = rows
		return nil
	}
	interval := time.Duration(stockwatch.IntervalSeconds) * time.Second
	r.SetTime(time.Unix(ts+stockwatch.IntervalSeconds, 0))
	r.FastForward(interval)
	if e := s.Tick(ctx); e != nil {
		t.Fatal(e)
	}
	// Alpha was all when queued and narrows before delivery; Beta stays all.
	if e := s.Store.SetArticleScope("Alpha", "targets"); e != nil {
		t.Fatal(e)
	}
	worker := newDiscordOutboxWorker(s.Outbox, s.Client)
	worker.eligible = s.Eligible
	for i := 0; i < 6; i++ {
		item, e := s.Outbox.Claim(ctx, time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if item == nil {
			break
		}
		worker.deliverClaimed(ctx, item)
	}
	mu.Lock()
	defer mu.Unlock()
	snap, e := s.Snapshot(ctx)
	if e != nil || snap.Pending != 0 || snap.SentCount != 4 || len(received) != 4 {
		t.Fatal(e, snap, len(received))
	}
	for _, content := range received {
		if (strings.Contains(content, "作者：Alpha") || strings.Contains(content, "作者：Gamma")) && strings.Contains(content, "[新聞]") {
			t.Fatal("unwanted news delivered")
		}
	}
	if e := s.Store.SetArticleScope("Alpha", "all"); e != nil {
		t.Fatal(e)
	}
	item, e := s.Outbox.Claim(ctx, time.Minute)
	if e != nil || item != nil {
		t.Fatal("cancelled item revived")
	}
}

func TestScopeCancellationFailureDoesNotSendOrAck(t *testing.T) {
	for _, kind := range []string{"expired", "stale", "missing", "done"} {
		t.Run(kind, func(t *testing.T) {
			s, r, _ := newDiscordWorkerTestOutbox(t)
			ctx := context.Background()
			i, _ := outbox.NewItem("scope", []string{kind}, []string{"never send"}, true)
			s.Enqueue(ctx, i)
			item, _ := s.Claim(ctx, time.Minute)
			switch kind {
			case "expired":
				r.SetTime(discordOutboxTestNow.Add(2 * time.Minute))
				r.FastForward(2 * time.Minute)
			case "stale":
				item.LeaseToken = "other-token"
			case "missing":
				r.Del("test:discord:outbox:item:" + item.EventID)
			case "done":
				if e := s.Cancel(ctx, item.EventID, item.LeaseToken); e != nil {
					t.Fatal(e)
				}
			}
			// Use the actual helper's prefix for missing-state simulation below.
			if kind == "missing" {
				for _, key := range r.Keys() {
					if strings.Contains(key, ":item:") && strings.HasSuffix(key, item.EventID) {
						r.Del(key)
					}
				}
			}
			calls := 0
			worker := newDiscordOutboxWorker(s, discordChunkSenderFunc(func(context.Context, string) (string, error) { calls++; return "123", nil }))
			worker.eligible = func(context.Context, *outbox.ClaimedItem) (bool, error) { return false, nil }
			worker.deliverClaimed(ctx, item)
			if calls != 0 {
				t.Fatal("cancel failure sent HTTP")
			}
			if r.Exists("counter:alert") {
				t.Fatal("cancel counted delivery")
			}
		})
	}
}

func TestScopePartialDeliveryPreservesMessageAndDoesNotResume(t *testing.T) {
	s, r, _ := newDiscordWorkerTestOutbox(t)
	ctx := context.Background()
	i, _ := outbox.NewItem("scope-partial", []string{"one"}, []string{"first", "second"}, true)
	s.Enqueue(ctx, i)
	item, _ := s.Claim(ctx, time.Minute)
	calls := 0
	worker := newDiscordOutboxWorker(s, discordChunkSenderFunc(func(context.Context, string) (string, error) { calls++; return "123456", nil }))
	worker.eligible = func(context.Context, *outbox.ClaimedItem) (bool, error) { return calls == 0, nil }
	worker.deliverClaimed(ctx, item)
	if calls != 1 {
		t.Fatal(calls)
	}
	found := false
	for _, key := range r.Keys() {
		if strings.Contains(key, ":done:") && strings.HasSuffix(key, item.EventID) {
			found = true
			if r.HGet(key, "state") != "cancelled" || r.HGet(key, "message_id:0") != "123456" {
				t.Fatal("partial message lost")
			}
		}
	}
	if !found {
		t.Fatal("missing cancellation tombstone")
	}
	again, e := s.Claim(ctx, time.Minute)
	if e != nil || again != nil {
		t.Fatal("partial event resent")
	}
}
