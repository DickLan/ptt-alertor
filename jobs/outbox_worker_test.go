package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	miniredis "github.com/alicebob/miniredis/v2"
	redigo "github.com/garyburd/redigo/redis"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
)

var discordOutboxTestNow = time.Date(2026, time.July, 13, 12, 0, 0, 0, time.UTC)

type discordChunkSenderFunc func(context.Context, string) (string, error)

func (send discordChunkSenderFunc) SendChunk(ctx context.Context, content string) (string, error) {
	return send(ctx, content)
}

type retryRecord struct {
	delay        time.Duration
	failureClass string
}

type recordingRetryStore struct {
	outbox.Store
	retries chan retryRecord
}

func (store *recordingRetryStore) Retry(
	ctx context.Context,
	eventID, token string,
	delay time.Duration,
	failureClass string,
) (outbox.RetryResult, error) {
	result, err := store.Store.Retry(ctx, eventID, token, delay, failureClass)
	if err == nil {
		store.retries <- retryRecord{delay: delay, failureClass: failureClass}
	}
	return result, err
}

func newDiscordWorkerTestOutbox(t *testing.T) (*outbox.Redis, *miniredis.Miniredis, *redigo.Pool) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(discordOutboxTestNow)
	pool := &redigo.Pool{
		MaxIdle:   8,
		MaxActive: 32,
		Wait:      true,
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", server.Addr())
		},
	}
	t.Cleanup(func() { _ = pool.Close() })
	store := outbox.NewRedis(outbox.RedisConfig{
		Connect:        pool.Get,
		Prefix:         "test:jobs:discord:outbox",
		DoneTTL:        time.Hour,
		HighWatermark:  10_000,
		CounterKey:     "counter:alert",
		CounterChannel: "alert-counter",
	})
	return store, server, pool
}

func newFastDiscordOutboxWorker(store outbox.Store, client discordChunkSender) *discordOutboxWorker {
	worker := newDiscordOutboxWorker(store, client)
	worker.leaseDuration = time.Second
	worker.idlePoll = time.Millisecond
	worker.recoveryInterval = 5 * time.Millisecond
	worker.transientBase = 0
	worker.transientMax = 0
	return worker
}

func enqueueWorkerTestItem(t *testing.T, store outbox.Store, chunks []string, countAlert bool) outbox.Item {
	t.Helper()
	item, err := outbox.NewItem("test", []string{"NBA", "M.1720862400.A.001"}, chunks, countAlert)
	if err != nil {
		t.Fatalf("NewItem() error = %v", err)
	}
	if _, err = store.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	return item
}

func runDiscordWorkerForTest(worker *discordOutboxWorker) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()
	return cancel, done
}

func waitForDiscordWorkerCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for Discord outbox worker condition")
}

func stopDiscordWorker(t *testing.T, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Discord outbox worker did not stop")
	}
}

func TestDiscordOutboxWorkerResumesAtFailedChunk(t *testing.T) {
	store, _, pool := newDiscordWorkerTestOutbox(t)
	enqueueWorkerTestItem(t, store, []string{"one", "two", "three"}, true)

	var (
		mu    sync.Mutex
		calls []string
	)
	sender := discordChunkSenderFunc(func(_ context.Context, content string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, content)
		if len(calls) == 2 {
			return "", &discord.HTTPError{StatusCode: 503}
		}
		return fmt.Sprintf("message-%d", len(calls)), nil
	})
	worker := newFastDiscordOutboxWorker(store, sender)
	cancel, done := runDiscordWorkerForTest(worker)

	waitForDiscordWorkerCondition(t, func() bool {
		count, err := store.PendingCount(context.Background())
		return err == nil && count == 0
	})
	stopDiscordWorker(t, cancel, done)

	mu.Lock()
	gotCalls := append([]string(nil), calls...)
	mu.Unlock()
	wantCalls := []string{"one", "two", "two", "three"}
	if strings.Join(gotCalls, "|") != strings.Join(wantCalls, "|") {
		t.Fatalf("sent chunks = %#v, want %#v", gotCalls, wantCalls)
	}
	conn := pool.Get()
	count, err := redigo.Int64(conn.Do("GET", "counter:alert"))
	_ = conn.Close()
	if err != nil || count != 1 {
		t.Fatalf("counter:alert = %d, %v; want 1, nil", count, err)
	}
}

func TestDiscordOutboxWorkerRecoversExpiredLeaseOnRestart(t *testing.T) {
	store, server, _ := newDiscordWorkerTestOutbox(t)
	item := enqueueWorkerTestItem(t, store, []string{"restart payload"}, false)
	claimed, err := store.Claim(context.Background(), time.Second)
	if err != nil || claimed == nil || claimed.EventID != item.EventID {
		t.Fatalf("initial Claim() = %#v, %v", claimed, err)
	}
	server.SetTime(discordOutboxTestNow.Add(2 * time.Second))

	delivered := make(chan string, 1)
	sender := discordChunkSenderFunc(func(_ context.Context, content string) (string, error) {
		delivered <- content
		return "restart-message", nil
	})
	cancel, done := runDiscordWorkerForTest(newFastDiscordOutboxWorker(store, sender))
	waitForDiscordWorkerCondition(t, func() bool {
		count, countErr := store.PendingCount(context.Background())
		return countErr == nil && count == 0
	})
	stopDiscordWorker(t, cancel, done)

	select {
	case content := <-delivered:
		if content != "restart payload" {
			t.Fatalf("delivered content = %q", content)
		}
	default:
		t.Fatal("recovered item was not delivered")
	}
}

func TestDiscordOutboxWorkerCancellationLeavesRecoverableLease(t *testing.T) {
	store, server, _ := newDiscordWorkerTestOutbox(t)
	enqueueWorkerTestItem(t, store, []string{"cancel payload"}, false)
	started := make(chan struct{})
	var startOnce sync.Once
	blockingSender := discordChunkSenderFunc(func(ctx context.Context, _ string) (string, error) {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		return "", ctx.Err()
	})
	worker := newFastDiscordOutboxWorker(store, blockingSender)
	cancel, done := runDiscordWorkerForTest(worker)

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start Discord send")
	}
	stopDiscordWorker(t, cancel, done)
	if count, err := store.PendingCount(context.Background()); err != nil || count != 1 {
		t.Fatalf("pending after cancellation = %d, %v; want leased item", count, err)
	}
	if claimed, err := store.Claim(context.Background(), time.Second); err != nil || claimed != nil {
		t.Fatalf("Claim() before lease expiry = %#v, %v; want nil", claimed, err)
	}

	server.SetTime(discordOutboxTestNow.Add(2 * time.Second))
	restartedSender := discordChunkSenderFunc(func(_ context.Context, content string) (string, error) {
		if content != "cancel payload" {
			t.Errorf("restarted content = %q", content)
		}
		return "restarted-message", nil
	})
	restartCancel, restartDone := runDiscordWorkerForTest(newFastDiscordOutboxWorker(store, restartedSender))
	waitForDiscordWorkerCondition(t, func() bool {
		count, err := store.PendingCount(context.Background())
		return err == nil && count == 0
	})
	stopDiscordWorker(t, restartCancel, restartDone)
}

func TestDiscordOutboxWorkerRetainsPermanentWebhookFailureWithLongDelay(t *testing.T) {
	redisStore, _, _ := newDiscordWorkerTestOutbox(t)
	enqueueWorkerTestItem(t, redisStore, []string{"permanent failure"}, false)
	store := &recordingRetryStore{
		Store:   redisStore,
		retries: make(chan retryRecord, 1),
	}
	sender := discordChunkSenderFunc(func(context.Context, string) (string, error) {
		return "", &discord.HTTPError{StatusCode: 404}
	})
	worker := newDiscordOutboxWorker(store, sender)
	worker.idlePoll = time.Millisecond
	cancel, done := runDiscordWorkerForTest(worker)

	select {
	case retry := <-store.retries:
		if retry.failureClass != "discord_http_404" {
			t.Errorf("failure class = %q", retry.failureClass)
		}
		if retry.delay != defaultDiscordPermanentRetryBase {
			t.Errorf("retry delay = %s, want %s", retry.delay, defaultDiscordPermanentRetryBase)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("permanent failure was not retained for retry")
	}
	stopDiscordWorker(t, cancel, done)
	if count, err := redisStore.PendingCount(context.Background()); err != nil || count != 1 {
		t.Fatalf("pending permanent failure = %d, %v; want 1", count, err)
	}
}

func TestDiscordOutboxWorkerHonorsExplicitRateLimitDelay(t *testing.T) {
	redisStore, _, _ := newDiscordWorkerTestOutbox(t)
	enqueueWorkerTestItem(t, redisStore, []string{"rate limited"}, false)
	store := &recordingRetryStore{
		Store:   redisStore,
		retries: make(chan retryRecord, 1),
	}
	sender := discordChunkSenderFunc(func(context.Context, string) (string, error) {
		return "", &discord.HTTPError{StatusCode: 429, RetryAfter: 45 * time.Second}
	})
	worker := newDiscordOutboxWorker(store, sender)
	worker.idlePoll = time.Millisecond
	cancel, done := runDiscordWorkerForTest(worker)

	select {
	case retry := <-store.retries:
		if retry.failureClass != "discord_http_429" || retry.delay != 45*time.Second {
			t.Fatalf("retry = %#v, want discord_http_429 after 45s", retry)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rate-limited event was not released for delayed retry")
	}
	stopDiscordWorker(t, cancel, done)
}

func TestDiscordOutboxWorkerDoesNotHitNextBacklogItemDuringWebhook429Pause(t *testing.T) {
	store, server, _ := newDiscordWorkerTestOutbox(t)
	for index, content := range []string{"first backlog item", "second backlog item"} {
		item, err := outbox.NewItem("test", []string{"NBA", fmt.Sprintf("M.429.A.%03d", index)}, []string{content}, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.Enqueue(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	var calls int32
	sender := discordChunkSenderFunc(func(context.Context, string) (string, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return "", &discord.HTTPError{StatusCode: 429, RetryAfter: 45 * time.Second}
		}
		return fmt.Sprintf("message-%d", call), nil
	})
	worker := newFastDiscordOutboxWorker(store, sender)
	cancel, done := runDiscordWorkerForTest(worker)
	waitForDiscordWorkerCondition(t, func() bool { return atomic.LoadInt32(&calls) == 1 })
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		stopDiscordWorker(t, cancel, done)
		t.Fatalf("Discord calls during global Retry-After = %d, want 1", got)
	}

	server.SetTime(discordOutboxTestNow.Add(45 * time.Second))
	waitForDiscordWorkerCondition(t, func() bool { return atomic.LoadInt32(&calls) >= 2 })
	stopDiscordWorker(t, cancel, done)
}

func TestEnqueueDiscordNotificationPersistsUTF16SafeChunks(t *testing.T) {
	store, _, _ := newDiscordWorkerTestOutbox(t)
	originalOutbox := notificationOutbox
	notificationOutbox = store
	t.Cleanup(func() { notificationOutbox = originalOutbox })

	content := strings.Repeat("文", 1_999) + "\n" + strings.Repeat("😀", 1_001) + "tail"
	err := enqueueDiscordNotification(
		context.Background(),
		"article",
		[]string{"NBA", "M.1720862400.A.009"},
		content,
		true,
	)
	if err != nil {
		t.Fatalf("enqueueDiscordNotification() error = %v", err)
	}
	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	if len(claimed.Chunks) < 2 {
		t.Fatalf("chunk count = %d, want multiple chunks", len(claimed.Chunks))
	}
	if got := strings.Join(claimed.Chunks, ""); got != content {
		t.Fatal("persisted chunks do not reconstruct the input")
	}
	for index, chunk := range claimed.Chunks {
		if units := len(utf16.Encode([]rune(chunk))); units > discord.MaxContentLength {
			t.Errorf("chunk %d has %d UTF-16 units", index, units)
		}
	}
}

func TestEnqueueDiscordNotificationTruncatesPathologicalLargePayload(t *testing.T) {
	store, _, _ := newDiscordWorkerTestOutbox(t)
	originalOutbox := notificationOutbox
	notificationOutbox = store
	t.Cleanup(func() { notificationOutbox = originalOutbox })

	// An early newline in each nominal chunk also exercises the line-break
	// preference path that can otherwise create an extra small chunk.
	segment := "\n" + strings.Repeat("😀", discord.MaxContentLength/2)
	content := strings.Repeat(segment, maxDiscordNotificationChunks+3)
	if err := enqueueDiscordNotification(
		context.Background(),
		"comment",
		[]string{"NBA", "M.1720862400.A.LARGE", "revision"},
		content,
		true,
	); err != nil {
		t.Fatalf("enqueueDiscordNotification() error = %v", err)
	}

	claimed, err := store.Claim(context.Background(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("Claim() = %#v, %v", claimed, err)
	}
	if len(claimed.Chunks) == 0 || len(claimed.Chunks) > maxDiscordNotificationChunks {
		t.Fatalf("chunk count = %d, want 1..%d", len(claimed.Chunks), maxDiscordNotificationChunks)
	}
	joined := strings.Join(claimed.Chunks, "")
	if !strings.HasSuffix(joined, notificationTruncatedMarker) {
		t.Fatalf("truncated payload does not end with marker: %q", joined)
	}
	if !strings.HasPrefix(content, strings.TrimSuffix(joined, notificationTruncatedMarker)) {
		t.Fatal("truncated payload is not a prefix of the source")
	}
	for index, chunk := range claimed.Chunks {
		if !utf8.ValidString(chunk) {
			t.Fatalf("chunk %d is invalid UTF-8", index)
		}
		if units := len(utf16.Encode([]rune(chunk))); units > discord.MaxContentLength {
			t.Fatalf("chunk %d has %d UTF-16 units", index, units)
		}
	}
}

func TestWaitForDiscordOutboxEmptyHonorsContext(t *testing.T) {
	store, _, _ := newDiscordWorkerTestOutbox(t)
	originalOutbox := notificationOutbox
	notificationOutbox = store
	t.Cleanup(func() { notificationOutbox = originalOutbox })

	if err := WaitForDiscordOutboxEmpty(context.Background()); err != nil {
		t.Fatalf("empty WaitForDiscordOutboxEmpty() error = %v", err)
	}
	enqueueWorkerTestItem(t, store, []string{"pending"}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := WaitForDiscordOutboxEmpty(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForDiscordOutboxEmpty() error = %v, want deadline exceeded", err)
	}
}

func TestDiscordFailureClassificationAndBoundedBackoff(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantClass string
		permanent bool
	}{
		{name: "not configured", err: discord.ErrWebhookNotConfigured, wantClass: "discord_webhook_not_configured", permanent: true},
		{name: "bad request", err: &discord.HTTPError{StatusCode: 400}, wantClass: "discord_http_400", permanent: true},
		{name: "request timeout", err: &discord.HTTPError{StatusCode: 408}, wantClass: "discord_http_408", permanent: false},
		{name: "rate limited", err: &discord.HTTPError{StatusCode: 429}, wantClass: "discord_http_429", permanent: false},
		{name: "server error", err: &discord.HTTPError{StatusCode: 503}, wantClass: "discord_http_503", permanent: false},
		{name: "secret-bearing transport error", err: errors.New("https://discord.com/api/webhooks/1/secret"), wantClass: "discord_transport", permanent: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotClass, gotPermanent := classifyDiscordFailure(test.err)
			if gotClass != test.wantClass || gotPermanent != test.permanent {
				t.Fatalf("classifyDiscordFailure() = %q, %t; want %q, %t", gotClass, gotPermanent, test.wantClass, test.permanent)
			}
			if strings.Contains(gotClass, "secret") || strings.Contains(gotClass, "webhooks/") {
				t.Fatalf("failure classifier retained secret-bearing error: %q", gotClass)
			}
		})
	}

	if got := boundedExponentialDelay(time.Second, time.Minute, 0); got != time.Second {
		t.Errorf("first delay = %s", got)
	}
	if got := boundedExponentialDelay(time.Second, time.Minute, 5); got != 32*time.Second {
		t.Errorf("sixth delay = %s", got)
	}
	if got := boundedExponentialDelay(time.Second, time.Minute, 1_000_000); got != time.Minute {
		t.Errorf("capped delay = %s", got)
	}
}
