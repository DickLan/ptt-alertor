package outbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCancelFencedNoFakeDelivery(t *testing.T) {
	s, r, _ := newTestOutbox(t, 10)
	ctx := context.Background()
	item := mustItem(t, "stockwatch", []string{"cancel"}, []string{"one", "two"}, true)
	if _, e := s.Enqueue(ctx, item); e != nil {
		t.Fatal(e)
	}
	c, e := s.Claim(ctx, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, c.EventID, "wrong-token"); !errors.Is(e, ErrStaleLease) {
		t.Fatal(e)
	}
	if _, e = s.AckChunk(ctx, c.EventID, c.LeaseToken, "real-first-message", time.Minute); e != nil {
		t.Fatal(e)
	}
	if e = s.Cancel(ctx, c.EventID, c.LeaseToken); e != nil {
		t.Fatal(e)
	}
	if n, _ := s.PendingCount(ctx); n != 0 {
		t.Fatal(n)
	}
	if r.HGet(s.doneKey(item.EventID), "state") != "cancelled" || r.HGet(s.doneKey(item.EventID), "message_id:0") != "real-first-message" || r.HGet(s.doneKey(item.EventID), "message_id:1") != "" {
		t.Fatal("invalid cancellation marker")
	}
	if r.Exists(s.counterKey) {
		t.Fatal("cancel counted as delivery")
	}
	if _, e = s.Enqueue(ctx, item); e != nil {
		t.Fatal(e)
	}
	if n, _ := s.PendingCount(ctx); n != 0 {
		t.Fatal("cancelled item re-enqueued")
	}
}
