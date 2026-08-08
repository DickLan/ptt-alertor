package discord

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"
)

func TestClientSendSplitsContentAndDisablesMentions(t *testing.T) {
	type receivedPayload struct {
		Content         string `json:"content"`
		Username        string `json:"username"`
		AllowedMentions struct {
			Parse []string `json:"parse"`
		} `json:"allowed_mentions"`
	}

	var (
		mu       sync.Mutex
		payloads []receivedPayload
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		if got := r.URL.Query().Get("wait"); got != "true" {
			t.Errorf("wait query = %q, want true", got)
		}
		var payload receivedPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, payload)
		mu.Unlock()
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	text := strings.Repeat("文", 1998) + "\n" + strings.Repeat("😀", 1001) + " @everyone"
	client := New(Config{WebhookURL: server.URL, Username: "Ptt Alertor"})
	if err := client.Send(context.Background(), text); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) < 2 {
		t.Fatalf("request count = %d, want at least 2", len(payloads))
	}
	var joined strings.Builder
	for i, payload := range payloads {
		if got := utf16Length(payload.Content); got > MaxContentLength {
			t.Errorf("payload %d length = %d, exceeds %d", i, got, MaxContentLength)
		}
		if payload.Username != "Ptt Alertor" {
			t.Errorf("username = %q, want Ptt Alertor", payload.Username)
		}
		if payload.AllowedMentions.Parse == nil || len(payload.AllowedMentions.Parse) != 0 {
			t.Errorf("allowed_mentions.parse = %#v, want empty array", payload.AllowedMentions.Parse)
		}
		joined.WriteString(payload.Content)
	}
	if got := joined.String(); got != text {
		t.Errorf("joined message differs from input")
	}
}

func TestClientSendRetriesRateLimit(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := atomic.AddInt32(&requests, 1)
		if request == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"retry_after":0.001}`))
			return
		}
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL})
	if err := client.Send(context.Background(), "hello"); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("request count = %d, want 2", got)
	}
}

func TestClientDefersLongDiscordRateLimitWithoutEarlyRetry(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"retry_after":45}`))
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL})
	started := time.Now()
	err := client.Send(context.Background(), "hello")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTooManyRequests || httpErr.RetryAfter != 45*time.Second {
		t.Fatalf("Send() error = %#v, want HTTP 429 with 45s RetryAfter", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("request count = %d, want no early retry", got)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("long Retry-After blocked claimed delivery for %s", elapsed)
	}
}

func TestRateLimitDelayDoesNotShortenExplicitDiscordDelay(t *testing.T) {
	header := make(http.Header)
	header.Set("Retry-After", "90")
	if got := rateLimitDelay(header, nil, time.Second); got != 90*time.Second {
		t.Fatalf("rateLimitDelay() = %s, want 90s", got)
	}
}

func TestClientSendReturnsHTTPError(t *testing.T) {
	const token = "secret-token-in-response"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(r.RequestURI))
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL + "/api/webhooks/123/" + token})
	err := client.Send(context.Background(), "hello")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("Send() error = %T %v, want *HTTPError", err, err)
	}
	if httpErr.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", httpErr.StatusCode, http.StatusBadRequest)
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("HTTP error exposed webhook token: %v", err)
	}
}

func TestClientNeverFollowsWebhookRedirect(t *testing.T) {
	var redirectedRequests int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt32(&redirectedRequests, 1)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	err := New(Config{WebhookURL: source.URL + "/api/webhooks/123/secret", MaxRetries: -1}).Send(context.Background(), "do not forward")
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("Send() error = %#v, want HTTP 307", err)
	}
	if got := atomic.LoadInt32(&redirectedRequests); got != 0 {
		t.Fatalf("redirect target requests = %d, want 0", got)
	}
}

func TestClientSendRetriesServerError(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL})
	if err := client.Send(context.Background(), "hello"); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Errorf("request count = %d, want 2", got)
	}
}

func TestClientSendValidatesInput(t *testing.T) {
	if err := New(Config{}).Send(context.Background(), "hello"); !errors.Is(err, ErrWebhookNotConfigured) {
		t.Errorf("missing webhook error = %v, want ErrWebhookNotConfigured", err)
	}
	if err := New(Config{WebhookURL: "https://example.com/webhook"}).Send(context.Background(), ""); !errors.Is(err, ErrEmptyMessage) {
		t.Errorf("empty message error = %v, want ErrEmptyMessage", err)
	}
	if err := New(Config{WebhookURL: "://bad"}).Send(context.Background(), "hello"); !errors.Is(err, ErrInvalidWebhookURL) {
		t.Errorf("invalid webhook error = %v, want ErrInvalidWebhookURL", err)
	}
	if err := New(Config{WebhookURL: "http://discord.example/api/webhooks/1/token"}).Send(context.Background(), "hello"); !errors.Is(err, ErrInvalidWebhookURL) {
		t.Errorf("insecure webhook error = %v, want ErrInvalidWebhookURL", err)
	}
}

func TestClientSendRejectsTooManyChunksBeforeDelivery(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&requests, 1)
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL, MaxRetries: -1})
	content := strings.Repeat("x", MaxContentLength*MaxChunksPerSend+1)
	if err := client.Send(context.Background(), content); !errors.Is(err, ErrContentTooLong) {
		t.Fatalf("Send() error = %v, want ErrContentTooLong", err)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("request count = %d, want 0", got)
	}
}

func TestClientValidateDoesNotSend(t *testing.T) {
	if err := New(Config{}).Validate(); !errors.Is(err, ErrWebhookNotConfigured) {
		t.Fatalf("missing Validate() error = %v, want ErrWebhookNotConfigured", err)
	}
	if err := New(Config{WebhookURL: "http://discord.example/api/webhooks/1/token"}).Validate(); !errors.Is(err, ErrInvalidWebhookURL) {
		t.Fatalf("insecure Validate() error = %v, want ErrInvalidWebhookURL", err)
	}
	if err := New(Config{WebhookURL: "https://discord.com/api/webhooks/1/token"}).Validate(); err != nil {
		t.Fatalf("valid Validate() error = %v", err)
	}
}

func TestClientValidateRequiresDiscordWebhookEndpoint(t *testing.T) {
	tests := []string{
		"https://example.com/api/webhooks/1/token",
		"https://discord.com/not-a-webhook",
		"https://discord.com/api/webhooks/not-a-snowflake/token",
		"https://discord.com/api/webhooks/1/token/extra",
		"https://discord.com/api/webhooks/1/token%2Fextra",
	}
	for _, webhookURL := range tests {
		if err := New(Config{WebhookURL: webhookURL}).Validate(); !errors.Is(err, ErrInvalidWebhookURL) {
			t.Errorf("Validate(%q) error = %v, want ErrInvalidWebhookURL", webhookURL, err)
		}
	}
}

func TestClientSendRequiresConfirmedDiscordMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	err := New(Config{WebhookURL: server.URL, MaxRetries: -1}).Send(context.Background(), "hello")
	if !errors.Is(err, ErrDeliveryUnconfirmed) {
		t.Fatalf("Send() error = %v, want ErrDeliveryUnconfirmed", err)
	}
}

func TestClientSendChunkReturnsConfirmedMessageID(t *testing.T) {
	const (
		content   = "one durable chunk"
		messageID = "9876543210"
	)
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		if got := r.URL.Query().Get("wait"); got != "true" {
			t.Errorf("wait query = %q, want true", got)
		}
		var payload webhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
			return
		}
		if payload.Content != content {
			t.Errorf("content = %q, want %q", payload.Content, content)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"` + messageID + `"}`))
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL, MaxRetries: -1})
	got, err := client.SendChunk(context.Background(), content)
	if err != nil {
		t.Fatalf("SendChunk() error = %v", err)
	}
	if got != messageID {
		t.Errorf("SendChunk() message ID = %q, want %q", got, messageID)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("request count = %d, want 1", got)
	}
}

func TestClientSendChunkRejectsEmptyContent(t *testing.T) {
	client := New(Config{WebhookURL: "https://discord.example/api/webhooks/1/token"})
	messageID, err := client.SendChunk(context.Background(), "")
	if !errors.Is(err, ErrEmptyMessage) {
		t.Fatalf("SendChunk() error = %v, want ErrEmptyMessage", err)
	}
	if messageID != "" {
		t.Errorf("SendChunk() message ID = %q, want empty", messageID)
	}
}

func TestClientSendChunkRejectsContentOverUTF16Limit(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL, MaxRetries: -1})
	messageID, err := client.SendChunk(context.Background(), strings.Repeat("😀", 1001))
	if !errors.Is(err, ErrContentTooLong) {
		t.Fatalf("SendChunk() error = %v, want ErrContentTooLong", err)
	}
	if messageID != "" {
		t.Errorf("SendChunk() message ID = %q, want empty", messageID)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Errorf("request count = %d, want 0", got)
	}
}

func TestClientSendChunkAcceptsExactly2000UTF16Units(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var payload webhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
			return
		}
		if got := utf16Length(payload.Content); got != MaxContentLength {
			t.Errorf("content UTF-16 length = %d, want %d", got, MaxContentLength)
		}
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL, MaxRetries: -1})
	messageID, err := client.SendChunk(context.Background(), strings.Repeat("😀", 1000))
	if err != nil {
		t.Fatalf("SendChunk() error = %v", err)
	}
	if messageID != "1234567890" {
		t.Errorf("SendChunk() message ID = %q, want %q", messageID, "1234567890")
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("request count = %d, want 1", got)
	}
}

func TestNewFromEnv(t *testing.T) {
	t.Setenv("DISCORD_WEBHOOK_URL", " https://discord.example/api/webhooks/123/token ")
	t.Setenv("DISCORD_USERNAME", " Ptt Alertor ")
	t.Setenv("DISCORD_AVATAR_URL", " https://example.com/avatar.png ")

	client := NewFromEnv()
	if client.webhookURL != "https://discord.example/api/webhooks/123/token" {
		t.Errorf("webhookURL = %q", client.webhookURL)
	}
	if client.username != "Ptt Alertor" {
		t.Errorf("username = %q", client.username)
	}
	if client.avatarURL != "https://example.com/avatar.png" {
		t.Errorf("avatarURL = %q", client.avatarURL)
	}
}

func TestClientSendDoesNotExposeWebhookTokenInTransportError(t *testing.T) {
	const token = "super-secret-webhook-token"
	client := New(Config{
		WebhookURL: "http://127.0.0.1:1/api/webhooks/123/" + token,
		MaxRetries: -1,
	})
	err := client.Send(context.Background(), "hello")
	if err == nil {
		t.Fatal("Send() error = nil, want transport error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("transport error exposed webhook token: %v", err)
	}
}

func TestClientSendHonorsContextWhileWaitingForWebhook(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		writeConfirmedMessage(w)
	}))
	defer server.Close()

	client := New(Config{WebhookURL: server.URL})
	firstResult := make(chan error, 1)
	go func() { firstResult <- client.Send(context.Background(), "first") }()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Send(ctx, "second"); !errors.Is(err, context.Canceled) {
		t.Errorf("Send() error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-firstResult; err != nil {
		t.Errorf("first Send() error = %v", err)
	}
}

func TestSplitContentPrefersLineBreak(t *testing.T) {
	chunks := splitContent("1234\n56789", 6)
	want := []string{"1234\n", "56789"}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %#v, want %#v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("chunk %d = %q, want %q", i, chunks[i], want[i])
		}
	}
}

func TestSplitContentIsDeterministicAndUTF16Safe(t *testing.T) {
	text := strings.Repeat("文", 1999) + "\n" + strings.Repeat("😀", 1001) +
		"\nfinal @everyone"
	chunks := SplitContent(text)
	second := SplitContent(text)
	if len(chunks) < 2 {
		t.Fatalf("chunk count = %d, want at least 2", len(chunks))
	}
	if len(second) != len(chunks) {
		t.Fatalf("second chunk count = %d, want %d", len(second), len(chunks))
	}

	var joined strings.Builder
	for i, chunk := range chunks {
		if chunk == "" {
			t.Errorf("chunk %d is empty", i)
		}
		if got := utf16Length(chunk); got > MaxContentLength {
			t.Errorf("chunk %d UTF-16 length = %d, exceeds %d", i, got, MaxContentLength)
		}
		if chunk != second[i] {
			t.Errorf("chunk %d differs between calls", i)
		}
		joined.WriteString(chunk)
	}
	if got := joined.String(); got != text {
		t.Errorf("reconstructed text differs from input")
	}

	// The returned slice is independent: callers may safely retain or replace
	// entries without affecting later persisted splitting results.
	originalFirst := second[0]
	chunks[0] = "replaced"
	if second[0] != originalFirst {
		t.Errorf("replacing a chunk mutated a separate SplitContent result")
	}
}

func utf16Length(text string) int {
	return len(utf16.Encode([]rune(text)))
}

func writeConfirmedMessage(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"id":"1234567890"}`))
}
