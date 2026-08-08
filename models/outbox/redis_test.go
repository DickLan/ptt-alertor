package outbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redigo "github.com/garyburd/redigo/redis"
)

var testNow = time.Date(2026, time.July, 13, 12, 0, 0, 0, time.UTC)

func newTestOutbox(t *testing.T, highWatermark int64) (*Redis, *miniredis.Miniredis, *redigo.Pool) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(testNow)
	pool := &redigo.Pool{
		MaxIdle:   8,
		MaxActive: 128,
		Wait:      true,
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", server.Addr())
		},
	}
	t.Cleanup(func() { _ = pool.Close() })
	store := NewRedis(RedisConfig{
		Connect:        pool.Get,
		Prefix:         "test:discord:outbox",
		DoneTTL:        24 * time.Hour,
		HighWatermark:  highWatermark,
		CounterKey:     "counter:alert",
		CounterChannel: "alert-counter",
	})
	return store, server, pool
}

func mustItem(t *testing.T, kind string, identity, chunks []string, countAlert bool) Item {
	t.Helper()
	item, err := NewItem(kind, identity, chunks, countAlert)
	if err != nil {
		t.Fatalf("NewItem() error = %v", err)
	}
	return item
}

func TestDeterministicEventIDUsesUnambiguousFullIdentity(t *testing.T) {
	first := DeterministicEventID("article", "NBA", "M.1720862400.A.001")
	if first != DeterministicEventID("article", "NBA", "M.1720862400.A.001") {
		t.Fatal("same canonical identity produced different event IDs")
	}
	if first == DeterministicEventID("article", "NBA", "M.1720862400.A.002") {
		t.Fatal("different same-second PTT article codes collided")
	}
	if DeterministicEventID("kind", "ab", "c") == DeterministicEventID("kind", "a", "bc") {
		t.Fatal("length-ambiguous identity parts collided")
	}
	if DeterministicEventID("article", "NBA", "M.1.A.001") == DeterministicEventID("comment", "NBA", "M.1.A.001") {
		t.Fatal("different event kinds collided")
	}
}

func TestNewItemCopiesAndValidatesImmutableChunks(t *testing.T) {
	chunks := []string{"first", "second"}
	item := mustItem(t, "article", []string{"NBA", "M.1.A.001"}, chunks, true)
	chunks[0] = "mutated"
	if item.Chunks[0] != "first" {
		t.Fatalf("NewItem retained caller slice: chunks = %#v", item.Chunks)
	}

	invalid := item
	invalid.EventID = "not-a-sha256"
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("invalid event ID error = %v, want ErrInvalidItem", err)
	}
	invalid = item
	invalid.Chunks = []string{""}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("empty chunk error = %v, want ErrInvalidItem", err)
	}

	invalid = item
	invalid.Chunks = make([]string, MaxChunksPerItem+1)
	for index := range invalid.Chunks {
		invalid.Chunks[index] = "bounded"
	}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("excess chunk count error = %v, want ErrInvalidItem", err)
	}

	invalid = item
	invalid.Chunks = []string{strings.Repeat("😀", MaxChunkUTF16Units/2+1)}
	if err := invalid.Validate(); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("oversized UTF-16 chunk error = %v, want ErrInvalidItem", err)
	}
}

func TestRedisEnqueueIsConcurrentIdempotentAndPayloadImmutable(t *testing.T) {
	store, _, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "M.1.A.001"}, []string{"original"}, true)

	const callers = 32
	results := make(chan EnqueueResult, callers)
	errorsCh := make(chan error, callers)
	var workers sync.WaitGroup
	for index := 0; index < callers; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := store.Enqueue(context.Background(), item)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	workers.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent Enqueue() error = %v", err)
	}
	created, pending := 0, 0
	for result := range results {
		switch result.Status {
		case EnqueueCreated:
			created++
		case EnqueueAlreadyPending:
			pending++
		default:
			t.Fatalf("unexpected enqueue status %v", result.Status)
		}
	}
	if created != 1 || pending != callers-1 {
		t.Fatalf("created = %d, pending = %d; want 1 and %d", created, pending, callers-1)
	}
	if count, err := store.PendingCount(context.Background()); err != nil || count != 1 {
		t.Fatalf("PendingCount() = %d, %v; want 1, nil", count, err)
	}

	conflict := item
	conflict.Chunks = []string{"different"}
	if _, err := store.Enqueue(context.Background(), conflict); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("conflicting Enqueue() error = %v, want ErrEventConflict", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if claimed == nil || claimed.Chunks[0] != "original" {
		t.Fatalf("claimed immutable chunks = %#v, want original", claimed)
	}
}

func TestRedisHighWatermarkIsEnforcedAtomically(t *testing.T) {
	store, _, _ := newTestOutbox(t, 1)
	first := mustItem(t, "article", []string{"NBA", "M.1.A.001"}, []string{"first"}, true)
	second := mustItem(t, "article", []string{"NBA", "M.1.A.002"}, []string{"second"}, true)
	if _, err := store.Enqueue(context.Background(), first); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	duplicate, err := store.Enqueue(context.Background(), first)
	if err != nil || duplicate.Status != EnqueueAlreadyPending {
		t.Fatalf("duplicate Enqueue() = %#v, %v", duplicate, err)
	}
	if _, err = store.Enqueue(context.Background(), second); !errors.Is(err, ErrHighWatermark) {
		t.Fatalf("second Enqueue() error = %v, want ErrHighWatermark", err)
	}
	reached, count, err := store.HighWatermarkReached(context.Background())
	if err != nil || !reached || count != 1 {
		t.Fatalf("HighWatermarkReached() = %t, %d, %v; want true, 1, nil", reached, count, err)
	}
}

func TestRedisClaimIsExclusiveAcrossConcurrentWorkers(t *testing.T) {
	store, _, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "M.1.A.001"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	const workersCount = 32
	claimedCh := make(chan *ClaimedItem, workersCount)
	errorsCh := make(chan error, workersCount)
	var workers sync.WaitGroup
	for index := 0; index < workersCount; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			claimed, err := store.Claim(context.Background(), time.Minute)
			if err != nil {
				errorsCh <- err
				return
			}
			if claimed != nil {
				claimedCh <- claimed
			}
		}()
	}
	workers.Wait()
	close(claimedCh)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent Claim() error = %v", err)
	}
	var claimed []*ClaimedItem
	for item := range claimedCh {
		claimed = append(claimed, item)
	}
	if len(claimed) != 1 {
		t.Fatalf("successful claims = %d, want 1", len(claimed))
	}
	if claimed[0].LeaseGeneration != 1 || claimed[0].LeaseToken == "" {
		t.Fatalf("claim fencing = generation %d token %q", claimed[0].LeaseGeneration, claimed[0].LeaseToken)
	}
	if count, err := store.PendingCount(context.Background()); err != nil || count != 1 {
		t.Fatalf("PendingCount() = %d, %v; want leased item", count, err)
	}
}

func TestRedisPersistsChunkProgressFencesStaleWorkersAndCountsOnce(t *testing.T) {
	store, server, pool := newTestOutbox(t, 0)
	item := mustItem(t, "comment", []string{"NBA", "M.1.A.001", "revision-1"}, []string{"one", "two", "three"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	first, err := store.Claim(context.Background(), time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first Claim() = %#v, %v", first, err)
	}
	if _, err = store.AckChunk(context.Background(), item.EventID, "wrong-token", "discord-0", time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale AckChunk() error = %v, want ErrStaleLease", err)
	}
	ack, err := store.AckChunk(context.Background(), item.EventID, first.LeaseToken, "discord-0", time.Minute)
	if err != nil || ack.Completed || ack.NextChunk != 1 {
		t.Fatalf("first AckChunk() = %#v, %v", ack, err)
	}
	retry, err := store.Retry(context.Background(), item.EventID, first.LeaseToken, 0, "discord_503")
	if err != nil || retry.Attempts != 1 {
		t.Fatalf("Retry() = %#v, %v", retry, err)
	}

	second, err := store.Claim(context.Background(), time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second Claim() = %#v, %v", second, err)
	}
	if second.LeaseToken == first.LeaseToken || second.LeaseGeneration != 2 || second.NextChunk != 1 {
		t.Fatalf("second claim fencing/progress = %#v", second)
	}
	if current, err := second.CurrentChunk(); err != nil || current != "two" {
		t.Fatalf("CurrentChunk() = %q, %v; want two", current, err)
	}
	if _, err = store.AckChunk(context.Background(), item.EventID, first.LeaseToken, "stale", time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old worker AckChunk() error = %v, want ErrStaleLease", err)
	}
	ack, err = store.AckChunk(context.Background(), item.EventID, second.LeaseToken, "discord-1", time.Minute)
	if err != nil || ack.Completed || ack.NextChunk != 2 {
		t.Fatalf("second AckChunk() = %#v, %v", ack, err)
	}

	// Use a fresh direct connection for Pub/Sub; a subscribed connection must not
	// be returned to the command pool.
	direct, err := redigo.Dial("tcp", server.Addr(), redigo.DialReadTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("dial Pub/Sub Redis: %v", err)
	}
	pubsub := redigo.PubSubConn{Conn: direct}
	defer pubsub.Close()
	if err = pubsub.Subscribe(store.counterChannel); err != nil {
		t.Fatalf("subscribe counter channel: %v", err)
	}
	if subscription, ok := pubsub.Receive().(redigo.Subscription); !ok || subscription.Count != 1 {
		t.Fatalf("subscription confirmation = %#v", subscription)
	}

	ack, err = store.AckChunk(context.Background(), item.EventID, second.LeaseToken, "discord-2", time.Minute)
	if err != nil || !ack.Completed || ack.AlreadyDone || ack.NextChunk != 3 || ack.AlertCount == nil || *ack.AlertCount != 1 {
		t.Fatalf("final AckChunk() = %#v, %v", ack, err)
	}
	message, ok := pubsub.Receive().(redigo.Message)
	if !ok || string(message.Data) != "1" || message.Channel != store.counterChannel {
		t.Fatalf("counter publication = %#v", message)
	}

	conn := pool.Get()
	counter, counterErr := redigo.Int64(conn.Do("GET", store.counterKey))
	_ = conn.Close()
	if counterErr != nil || counter != 1 {
		t.Fatalf("counter:alert = %d, %v; want 1", counter, counterErr)
	}
	idempotentAck, err := store.AckChunk(context.Background(), item.EventID, second.LeaseToken, "discord-2", time.Minute)
	if err != nil || !idempotentAck.Completed || !idempotentAck.AlreadyDone {
		t.Fatalf("idempotent final AckChunk() = %#v, %v", idempotentAck, err)
	}
	result, err := store.Enqueue(context.Background(), item)
	if err != nil || result.Status != EnqueueAlreadyDone {
		t.Fatalf("post-completion Enqueue() = %#v, %v", result, err)
	}
	if count, err := store.PendingCount(context.Background()); err != nil || count != 0 {
		t.Fatalf("PendingCount() = %d, %v; want 0", count, err)
	}
}

func TestRedisRecoversExpiredLeaseAndRejectsExpiredOrOldToken(t *testing.T) {
	store, server, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"Stock", "M.2.A.001"}, []string{"payload"}, false)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	first, err := store.Claim(context.Background(), time.Second)
	if err != nil || first == nil {
		t.Fatalf("Claim() = %#v, %v", first, err)
	}
	server.SetTime(testNow.Add(2 * time.Second))
	if _, err = store.AckChunk(context.Background(), item.EventID, first.LeaseToken, "too-late", time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired AckChunk() error = %v, want ErrLeaseExpired", err)
	}
	recovered, err := store.RecoverExpired(context.Background(), 10)
	if err != nil || len(recovered) != 1 || recovered[0] != item.EventID {
		t.Fatalf("RecoverExpired() = %#v, %v", recovered, err)
	}
	if _, err = store.Retry(context.Background(), item.EventID, first.LeaseToken, 0, "shutdown"); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("recovered stale Retry() error = %v, want ErrStaleLease", err)
	}
	second, err := store.Claim(context.Background(), time.Second)
	if err != nil || second == nil {
		t.Fatalf("recovered Claim() = %#v, %v", second, err)
	}
	if second.LeaseToken == first.LeaseToken || second.LeaseGeneration != 2 || second.Recoveries != 1 {
		t.Fatalf("recovered fencing = %#v", second)
	}
	ack, err := store.AckChunk(context.Background(), item.EventID, second.LeaseToken, "confirmed", time.Second)
	if err != nil || !ack.Completed || ack.AlertCount != nil {
		t.Fatalf("recovered final AckChunk() = %#v, %v", ack, err)
	}
}

func TestRedisRetryNeverDropsAfterRepeatedFailures(t *testing.T) {
	store, _, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "M.3.A.001"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	for attempt := int64(1); attempt <= 20; attempt++ {
		claimed, err := store.Claim(context.Background(), time.Minute)
		if err != nil || claimed == nil {
			t.Fatalf("Claim() at attempt %d = %#v, %v", attempt, claimed, err)
		}
		result, err := store.Retry(context.Background(), item.EventID, claimed.LeaseToken, 0, "discord_404")
		if err != nil || result.Attempts != attempt {
			t.Fatalf("Retry() at attempt %d = %#v, %v", attempt, result, err)
		}
		if count, err := store.PendingCount(context.Background()); err != nil || count != 1 {
			t.Fatalf("PendingCount() after attempt %d = %d, %v", attempt, count, err)
		}
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil || claimed.Attempts != 20 {
		t.Fatalf("final Claim() = %#v, %v; item was dropped or attempts lost", claimed, err)
	}
}

func TestRedisRetryDelayUsesRedisTimeAndPreservesItem(t *testing.T) {
	store, server, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "M.5.A.001"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	retry, err := store.Retry(context.Background(), item.EventID, claimed.LeaseToken, time.Minute, "discord_http_429")
	if err != nil || retry.Attempts != 1 {
		t.Fatalf("Retry() = %#v, %v", retry, err)
	}
	if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() before retry deadline = %#v, %v; want nil", next, err)
	}
	server.SetTime(testNow.Add(59 * time.Second))
	if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() one second before deadline = %#v, %v; want nil", next, err)
	}
	server.SetTime(testNow.Add(time.Minute))
	next, err := store.Claim(context.Background(), time.Minute)
	if err != nil || next == nil || next.EventID != item.EventID || next.Attempts != 1 {
		t.Fatalf("Claim() at retry deadline = %#v, %v", next, err)
	}
}

func TestRedis429RetryPausesTheWholeWebhookBacklogDurably(t *testing.T) {
	store, server, pool := newTestOutbox(t, 0)
	first := mustItem(t, "article", []string{"NBA", "M.5.A.010"}, []string{"first"}, false)
	second := mustItem(t, "article", []string{"NBA", "M.5.A.011"}, []string{"second"}, false)
	for _, item := range []Item{first, second} {
		if _, err := store.Enqueue(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	if _, err := store.Retry(context.Background(), claimed.EventID, claimed.LeaseToken, 45*time.Second, "discord_http_429"); err != nil {
		t.Fatal(err)
	}

	restarted := NewRedis(RedisConfig{
		Connect:        pool.Get,
		Prefix:         "test:discord:outbox",
		DoneTTL:        24 * time.Hour,
		CounterKey:     "counter:alert",
		CounterChannel: "alert-counter",
	})
	if next, err := restarted.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("restarted Claim() during global pause = %#v, %v", next, err)
	}
	server.SetTime(testNow.Add(44*time.Second + 999*time.Millisecond))
	if next, err := restarted.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() before global deadline = %#v, %v", next, err)
	}
	server.SetTime(testNow.Add(45 * time.Second))
	next, err := restarted.Claim(context.Background(), time.Minute)
	if err != nil || next == nil || next.EventID == claimed.EventID {
		t.Fatalf("Claim() at global deadline = %#v, %v; want other ready item", next, err)
	}
}

func TestRedisPermanentWebhookFailurePausesTheWholeBacklog(t *testing.T) {
	store, server, _ := newTestOutbox(t, 0)
	first := mustItem(t, "article", []string{"NBA", "M.5.A.020"}, []string{"first"}, false)
	second := mustItem(t, "article", []string{"NBA", "M.5.A.021"}, []string{"second"}, false)
	for _, item := range []Item{first, second} {
		if _, err := store.Enqueue(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	delay := 30 * time.Minute
	if _, err = store.Retry(
		context.Background(),
		claimed.EventID,
		claimed.LeaseToken,
		delay,
		"discord_http_404",
	); err != nil {
		t.Fatal(err)
	}
	if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() during permanent webhook pause = %#v, %v", next, err)
	}
	server.SetTime(testNow.Add(delay - time.Millisecond))
	if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() before permanent webhook pause deadline = %#v, %v", next, err)
	}
	server.SetTime(testNow.Add(delay))
	next, err := store.Claim(context.Background(), time.Minute)
	if err != nil || next == nil || next.EventID == claimed.EventID {
		t.Fatalf("Claim() at permanent webhook pause deadline = %#v, %v; want other ready item", next, err)
	}
}

func TestDiscordFailuresThatPauseTheWholeBacklog(t *testing.T) {
	for _, failureClass := range []string{
		"discord_http_401",
		"discord_http_403",
		"discord_http_404",
		"discord_http_410",
		"discord_http_429",
		"discord_webhook_not_configured",
		"discord_webhook_invalid",
	} {
		if !shouldPauseDiscordBacklog(failureClass) {
			t.Errorf("shouldPauseDiscordBacklog(%q) = false, want true", failureClass)
		}
	}
	for _, failureClass := range []string{
		"discord_http_400",
		"discord_http_408",
		"discord_http_500",
		"discord_delivery_unconfirmed",
		"discord_transport",
	} {
		if shouldPauseDiscordBacklog(failureClass) {
			t.Errorf("shouldPauseDiscordBacklog(%q) = true, want false", failureClass)
		}
	}
}

func TestRedisWebhookPauseMaxExtendsAndRetryValidationIsAtomic(t *testing.T) {
	t.Run("longer pause cannot be shortened", func(t *testing.T) {
		store, server, _ := newTestOutbox(t, 0)
		items := []Item{
			mustItem(t, "article", []string{"NBA", "M.6.A.001"}, []string{"one"}, false),
			mustItem(t, "article", []string{"NBA", "M.6.A.002"}, []string{"two"}, false),
		}
		for _, item := range items {
			if _, err := store.Enqueue(context.Background(), item); err != nil {
				t.Fatal(err)
			}
		}
		first, _ := store.Claim(context.Background(), time.Minute)
		second, _ := store.Claim(context.Background(), time.Minute)
		if first == nil || second == nil {
			t.Fatal("two workers could not lease both test items")
		}
		if _, err := store.Retry(context.Background(), first.EventID, first.LeaseToken, 90*time.Second, "discord_http_429"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Retry(context.Background(), second.EventID, second.LeaseToken, 10*time.Second, "discord_http_429"); err != nil {
			t.Fatal(err)
		}
		server.SetTime(testNow.Add(89 * time.Second))
		if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
			t.Fatalf("shorter 429 reduced global pause: %#v, %v", next, err)
		}
		server.SetTime(testNow.Add(90 * time.Second))
		if next, err := store.Claim(context.Background(), time.Minute); err != nil || next == nil {
			t.Fatalf("Claim() at max pause deadline = %#v, %v", next, err)
		}
	})

	t.Run("wrong pause type does not partially release lease", func(t *testing.T) {
		store, _, pool := newTestOutbox(t, 0)
		item := mustItem(t, "article", []string{"NBA", "M.7.A.001"}, []string{"payload"}, false)
		if _, err := store.Enqueue(context.Background(), item); err != nil {
			t.Fatal(err)
		}
		claimed, _ := store.Claim(context.Background(), time.Minute)
		connection := pool.Get()
		_, _ = connection.Do("LPUSH", store.webhookNotBeforeKey(), "wrong-type")
		_ = connection.Close()
		if _, err := store.Retry(context.Background(), claimed.EventID, claimed.LeaseToken, time.Minute, "discord_http_429"); !errors.Is(err, ErrWrongRedisType) {
			t.Fatalf("Retry() wrong pause type error = %v, want ErrWrongRedisType", err)
		}
		connection = pool.Get()
		state, stateErr := redigo.String(connection.Do("HGET", store.itemKey(item.EventID), "state"))
		_ = connection.Close()
		if stateErr != nil || state != "leased" {
			t.Fatalf("item state after rejected Retry = %q, %v; want leased", state, stateErr)
		}
	})

	t.Run("non-429 retry does not pause other events", func(t *testing.T) {
		store, _, _ := newTestOutbox(t, 0)
		items := []Item{
			mustItem(t, "article", []string{"NBA", "M.8.A.001"}, []string{"one"}, false),
			mustItem(t, "article", []string{"NBA", "M.8.A.002"}, []string{"two"}, false),
		}
		for _, item := range items {
			_, _ = store.Enqueue(context.Background(), item)
		}
		claimed, _ := store.Claim(context.Background(), time.Minute)
		if _, err := store.Retry(context.Background(), claimed.EventID, claimed.LeaseToken, time.Minute, "discord_http_503"); err != nil {
			t.Fatal(err)
		}
		if next, err := store.Claim(context.Background(), time.Minute); err != nil || next == nil || next.EventID == claimed.EventID {
			t.Fatalf("non-429 blocked other event: %#v, %v", next, err)
		}
	})
}

func TestRedisFinalAckCompletesWhenAlertCounterCannotBeUpdated(t *testing.T) {
	tests := []struct {
		name   string
		poison func(redigo.Conn, string) error
		verify func(redigo.Conn, string) error
	}{
		{
			name: "malformed value",
			poison: func(conn redigo.Conn, key string) error {
				_, err := conn.Do("SET", key, "not-an-integer")
				return err
			},
			verify: func(conn redigo.Conn, key string) error {
				value, err := redigo.String(conn.Do("GET", key))
				if err == nil && value != "not-an-integer" {
					return errors.New("malformed counter was changed")
				}
				return err
			},
		},
		{
			name: "negative value",
			poison: func(conn redigo.Conn, key string) error {
				_, err := conn.Do("SET", key, "-1")
				return err
			},
			verify: func(conn redigo.Conn, key string) error {
				value, err := redigo.String(conn.Do("GET", key))
				if err == nil && value != "-1" {
					return errors.New("negative counter was changed")
				}
				return err
			},
		},
		{
			name: "increment overflow",
			poison: func(conn redigo.Conn, key string) error {
				_, err := conn.Do("SET", key, "9223372036854775807")
				return err
			},
			verify: func(conn redigo.Conn, key string) error {
				value, err := redigo.String(conn.Do("GET", key))
				if err == nil && value != "9223372036854775807" {
					return errors.New("overflowing counter was changed")
				}
				return err
			},
		},
		{
			name: "wrong Redis type",
			poison: func(conn redigo.Conn, key string) error {
				_, err := conn.Do("LPUSH", key, "wrong-type")
				return err
			},
			verify: func(conn redigo.Conn, key string) error {
				length, err := redigo.Int(conn.Do("LLEN", key))
				if err == nil && length != 1 {
					return errors.New("wrong-type counter was changed")
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, _, pool := newTestOutbox(t, 0)
			item := mustItem(t, "article", []string{"NBA", "counter-failure", test.name}, []string{"payload"}, true)
			if _, err := store.Enqueue(context.Background(), item); err != nil {
				t.Fatalf("Enqueue() error = %v", err)
			}
			claimed, err := store.Claim(context.Background(), time.Minute)
			if err != nil || claimed == nil {
				t.Fatalf("Claim() = %#v, %v", claimed, err)
			}
			conn := pool.Get()
			if err = test.poison(conn, store.counterKey); err != nil {
				_ = conn.Close()
				t.Fatalf("poison alert counter: %v", err)
			}
			_ = conn.Close()

			messageID := "confirmed"
			ack, err := store.AckChunk(context.Background(), item.EventID, claimed.LeaseToken, messageID, time.Minute)
			if err != nil || !ack.Completed || ack.AlreadyDone || ack.NextChunk != 1 || ack.AlertCount != nil {
				t.Fatalf("final AckChunk() = %#v, %v", ack, err)
			}
			if count, countErr := store.PendingCount(context.Background()); countErr != nil || count != 0 {
				t.Fatalf("PendingCount() = %d, %v; want 0, nil", count, countErr)
			}

			conn = pool.Get()
			itemExists, itemErr := redigo.Bool(conn.Do("EXISTS", store.itemKey(item.EventID)))
			doneExists, doneErr := redigo.Bool(conn.Do("EXISTS", store.doneKey(item.EventID)))
			storedMessageID, messageErr := redigo.String(conn.Do("HGET", store.doneKey(item.EventID), "message_id:0"))
			_, alertCountErr := redigo.String(conn.Do("HGET", store.doneKey(item.EventID), "alert_count"))
			counterErr := test.verify(conn, store.counterKey)
			_ = conn.Close()
			if itemErr != nil || doneErr != nil || itemExists || !doneExists {
				t.Fatalf("completed state: item=%t/%v done=%t/%v", itemExists, itemErr, doneExists, doneErr)
			}
			if messageErr != nil || storedMessageID != messageID {
				t.Fatalf("done message ID = %q, %v; want %q", storedMessageID, messageErr, messageID)
			}
			if !errors.Is(alertCountErr, redigo.ErrNil) {
				t.Fatalf("done alert_count error = %v, want redis.ErrNil", alertCountErr)
			}
			if counterErr != nil {
				t.Fatalf("counter was not conservatively preserved: %v", counterErr)
			}

			idempotent, err := store.AckChunk(context.Background(), item.EventID, claimed.LeaseToken, messageID, time.Minute)
			if err != nil || !idempotent.Completed || !idempotent.AlreadyDone || idempotent.AlertCount != nil {
				t.Fatalf("idempotent AckChunk() = %#v, %v", idempotent, err)
			}
			enqueue, err := store.Enqueue(context.Background(), item)
			if err != nil || enqueue.Status != EnqueueAlreadyDone {
				t.Fatalf("post-completion Enqueue() = %#v, %v", enqueue, err)
			}
			if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
				t.Fatalf("Claim() after completed AckChunk() = %#v, %v; want nil", next, err)
			}
		})
	}
}

func TestRedisFinalAckCompletesWhenCounterPublicationFails(t *testing.T) {
	store, _, pool := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "publish-failure"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}

	// Miniredis does not expose per-command failure hooks for commands invoked
	// inside EVAL. Keep the production script intact except for making its
	// best-effort PUBLISH call fail with the wrong arity.
	const publishCall = "redis.pcall('PUBLISH', counter_channel, alert_count)"
	if strings.Count(ackChunkScript, publishCall) != 1 {
		t.Fatal("AckChunk script must contain exactly one best-effort PUBLISH call")
	}
	faultScript := strings.Replace(
		ackChunkScript,
		publishCall,
		"redis.pcall('PUBLISH', counter_channel)",
		1,
	)
	reply, err := store.eval(
		context.Background(),
		faultScript,
		[]string{
			store.readyKey(), store.leasedKey(), store.itemKey(item.EventID),
			store.doneKey(item.EventID), store.counterKey,
		},
		item.EventID, claimed.LeaseToken, "confirmed", time.Minute.Milliseconds(),
		store.doneTTL.Milliseconds(), store.counterChannel,
	)
	if err != nil {
		t.Fatalf("fault-injected final AckChunk script error = %v", err)
	}
	values, err := redigo.Values(reply, nil)
	if err != nil {
		t.Fatalf("decode fault-injected AckChunk result: %v", err)
	}
	var completed, alreadyDone, nextChunk int
	var alertCount int64
	if _, err = redigo.Scan(values, &completed, &alreadyDone, &nextChunk, &alertCount); err != nil {
		t.Fatalf("scan fault-injected AckChunk result: %v", err)
	}
	if completed != 1 || alreadyDone != 0 || nextChunk != 1 || alertCount != 1 {
		t.Fatalf(
			"fault-injected AckChunk result = {%d %d %d %d}, want {1 0 1 1}",
			completed, alreadyDone, nextChunk, alertCount,
		)
	}

	if count, countErr := store.PendingCount(context.Background()); countErr != nil || count != 0 {
		t.Fatalf("PendingCount() = %d, %v; want 0, nil", count, countErr)
	}
	conn := pool.Get()
	counter, counterErr := redigo.Int64(conn.Do("GET", store.counterKey))
	itemExists, itemErr := redigo.Bool(conn.Do("EXISTS", store.itemKey(item.EventID)))
	doneExists, doneErr := redigo.Bool(conn.Do("EXISTS", store.doneKey(item.EventID)))
	_ = conn.Close()
	if counterErr != nil || counter != 1 {
		t.Fatalf("counter after failed publication = %d, %v; want 1, nil", counter, counterErr)
	}
	if itemErr != nil || doneErr != nil || itemExists || !doneExists {
		t.Fatalf("completed state: item=%t/%v done=%t/%v", itemExists, itemErr, doneExists, doneErr)
	}

	idempotent, err := store.AckChunk(context.Background(), item.EventID, claimed.LeaseToken, "confirmed", time.Minute)
	if err != nil || !idempotent.Completed || !idempotent.AlreadyDone || idempotent.AlertCount == nil || *idempotent.AlertCount != 1 {
		t.Fatalf("idempotent AckChunk() = %#v, %v", idempotent, err)
	}
	if next, err := store.Claim(context.Background(), time.Minute); err != nil || next != nil {
		t.Fatalf("Claim() after failed publication = %#v, %v; want nil", next, err)
	}
}

func TestRedisNonCountingItemDoesNotDependOnCounterType(t *testing.T) {
	store, _, pool := newTestOutbox(t, 0)
	item := mustItem(t, "broadcast", []string{"request-id"}, []string{"payload"}, false)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	conn := pool.Get()
	if _, err = conn.Do("LPUSH", store.counterKey, "wrong-type"); err != nil {
		t.Fatalf("create wrong-type counter: %v", err)
	}
	_ = conn.Close()
	ack, err := store.AckChunk(context.Background(), item.EventID, claimed.LeaseToken, "confirmed", time.Minute)
	if err != nil || !ack.Completed || ack.AlertCount != nil {
		t.Fatalf("non-counting AckChunk() = %#v, %v", ack, err)
	}
}

func TestRedisRetryRejectsRawErrorOrWebhookText(t *testing.T) {
	store, _, _ := newTestOutbox(t, 0)
	item := mustItem(t, "article", []string{"NBA", "M.7.A.001"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	if _, err = store.Retry(
		context.Background(), item.EventID, claimed.LeaseToken, 0,
		"https://discord.com/api/webhooks/123/secret",
	); !errors.Is(err, ErrInvalidItem) {
		t.Fatalf("Retry() raw webhook error = %v, want ErrInvalidItem", err)
	}
	if count, err := store.PendingCount(context.Background()); err != nil || count != 1 {
		t.Fatalf("PendingCount() after rejected retry = %d, %v; want leased item", count, err)
	}
}

func TestRedisWrongTypeFailsBeforePartialEnqueue(t *testing.T) {
	store, _, pool := newTestOutbox(t, 0)
	conn := pool.Get()
	if _, err := conn.Do("SET", store.readyKey(), "wrong-type"); err != nil {
		t.Fatalf("poison ready key: %v", err)
	}
	_ = conn.Close()
	item := mustItem(t, "article", []string{"NBA", "M.4.A.001"}, []string{"payload"}, true)
	if _, err := store.Enqueue(context.Background(), item); !errors.Is(err, ErrWrongRedisType) {
		t.Fatalf("Enqueue() error = %v, want ErrWrongRedisType", err)
	}
	conn = pool.Get()
	exists, err := redigo.Bool(conn.Do("EXISTS", store.itemKey(item.EventID)))
	_ = conn.Close()
	if err != nil || exists {
		t.Fatalf("partial item exists = %t, %v; want false", exists, err)
	}
}
