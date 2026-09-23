package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOptionalEligibilityCancelsOrRetriesWithoutHTTP(t *testing.T) {
	for _, lookupError := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "redis-error"}[lookupError], func(t *testing.T) {
			s, _, _ := newDiscordWorkerTestOutbox(t)
			ctx := context.Background()
			i, e := outbox.NewItem("stockwatch", []string{"cancel"}, []string{"one"}, true)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.Enqueue(ctx, i); e != nil {
				t.Fatal(e)
			}
			c, e := s.Claim(ctx, time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			calls := 0
			w := newDiscordOutboxWorker(s, discordChunkSenderFunc(func(context.Context, string) (string, error) { calls++; return "123", nil }))
			w.eligible = func(context.Context, *outbox.ClaimedItem) (bool, error) {
				if lookupError {
					return false, errors.New("storage unavailable")
				}
				return false, nil
			}
			w.deliverClaimed(ctx, c)
			if calls != 0 {
				t.Fatal("unauthorized send")
			}
			n, _ := s.PendingCount(ctx)
			if lookupError && n != 1 || !lookupError && n != 0 {
				t.Fatal(n)
			}
		})
	}
}

func TestEligibilityCheckedAgainBetweenChunks(t *testing.T) {
	s, _, _ := newDiscordWorkerTestOutbox(t)
	ctx := context.Background()
	i, _ := outbox.NewItem("stockwatch", []string{"two"}, []string{"one", "two"}, true)
	s.Enqueue(ctx, i)
	c, _ := s.Claim(ctx, time.Minute)
	calls := 0
	w := newDiscordOutboxWorker(s, discordChunkSenderFunc(func(context.Context, string) (string, error) { calls++; return "123", nil }))
	w.eligible = func(context.Context, *outbox.ClaimedItem) (bool, error) { return calls == 0, nil }
	w.deliverClaimed(ctx, c)
	if calls != 1 {
		t.Fatal(calls)
	}
	n, _ := s.PendingCount(ctx)
	if n != 0 {
		t.Fatal(n)
	}
}

func TestStockRuntimeOwnHTTPReceiverWhenOriginalJobsDisabled(t *testing.T) {
	t.Setenv("JOBS_ENABLED", "false")
	var sent atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Query().Get("wait") != "true" {
			t.Error("invalid webhook request")
		}
		var body map[string]interface{}
		if e := json.NewDecoder(r.Body).Decode(&body); e != nil {
			t.Error(e)
		}
		if body["content"] != "isolated stock notification" {
			t.Error("unexpected payload")
		}
		sent.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"123456789"}`))
	}))
	defer receiver.Close()
	r := miniredis.RunT(t)
	pool := &redis.Pool{MaxIdle: 8, MaxActive: 30, Wait: true, Dial: func() (redis.Conn, error) { return redis.Dial("tcp", r.Addr()) }}
	defer pool.Close()
	s := stockwatch.New(&stockwatch.Store{Connect: pool.Get, Prefix: "test:runtime"}, true, receiver.URL+"/stock", "https://discord.com/api/webhooks/456/old", strings.Repeat("t", 64))
	if !s.Ready() {
		t.Fatal(s.ConfigError)
	}
	if e := s.Store.Add([]string{"Alpha"}); e != nil {
		t.Fatal(e)
	}
	authors, _ := s.Store.Authors()
	kind := "stockwatch:alpha:" + authors[0].Generation + ":M.1789027200.A.001"
	i, _ := outbox.NewItem(kind, []string{"runtime"}, []string{"isolated stock notification"}, true)
	s.Outbox.Enqueue(context.Background(), i)
	s.Fetch = func(context.Context, *board.Board) error {
		t.Error("runtime fetched before due")
		return errors.New("unexpected")
	}
	stop := StartStockWatch(s)
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, e := s.Snapshot(context.Background())
		if e == nil && snapshot.SentCount == 1 && len(snapshot.Recent) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	snapshot, e := s.Snapshot(context.Background())
	if e != nil || sent.Load() != 1 || snapshot.SentCount != 1 || len(snapshot.Recent) != 1 || snapshot.Pending != 0 {
		t.Fatal("runtime delivery", e, sent.Load(), snapshot)
	}
	if r.Exists("counter:alert") {
		t.Fatal("original counter changed")
	}
}
