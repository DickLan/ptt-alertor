// Package discord sends Ptt Alertor notifications through a Discord webhook.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxContentLength is Discord's maximum message content length. We count
	// conservatively in UTF-16 code units to match client-side string semantics.
	MaxContentLength = 2000
	// MaxChunksPerSend bounds one direct (non-outbox) delivery so an
	// administrative request cannot expand into an unbounded webhook burst.
	MaxChunksPerSend = 8
	defaultTimeout   = 10 * time.Second
	defaultRetries   = 3
	maxErrorBody     = 8 * 1024
	maxBackoffDelay  = 30 * time.Second
)

var (
	ErrWebhookNotConfigured = errors.New("discord webhook is not configured")
	ErrInvalidWebhookURL    = errors.New("discord webhook URL is invalid")
	ErrEmptyMessage         = errors.New("discord message is empty")
	ErrContentTooLong       = errors.New("discord message exceeds the delivery size limit")
	ErrDeliveryUnconfirmed  = errors.New("discord did not confirm that the message was saved")
)

// Config configures a Discord webhook client. WebhookURL should normally come
// from DISCORD_WEBHOOK_URL rather than a persisted user profile.
type Config struct {
	WebhookURL string
	Username   string
	AvatarURL  string
	HTTPClient *http.Client
	// MaxRetries defaults to three when zero; a negative value disables retries.
	MaxRetries int
}

// Client sends messages to one Discord webhook. Sends are serialized because
// Discord rate limits each webhook independently.
type Client struct {
	webhookURL string
	username   string
	avatarURL  string
	httpClient *http.Client
	maxRetries int
	sendLock   chan struct{}
}

// HTTPError is returned for a non-successful Discord response after retries.
type HTTPError struct {
	StatusCode int
	// RetryAfter is populated for HTTP 429 when Discord supplied a positive
	// delay. Durable callers can release their lease and schedule the next
	// attempt instead of retrying before Discord permits it.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	// Do not retain or include the response body: an intermediary could echo the
	// secret webhook URL.
	return fmt.Sprintf("discord webhook returned HTTP %d", e.StatusCode)
}

// New constructs a webhook client from an explicit configuration.
func New(config Config) *Client {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	// Never forward a secret-bearing webhook path, message body, or Referer to
	// a redirect target. Discord webhook delivery is valid only at the exact URL
	// that passed deliveryURL validation.
	httpClientCopy := *httpClient
	httpClientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	maxRetries := config.MaxRetries
	if maxRetries == 0 {
		maxRetries = defaultRetries
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	return &Client{
		webhookURL: strings.TrimSpace(config.WebhookURL),
		username:   strings.TrimSpace(config.Username),
		avatarURL:  strings.TrimSpace(config.AvatarURL),
		httpClient: &httpClientCopy,
		maxRetries: maxRetries,
		sendLock:   make(chan struct{}, 1),
	}
}

// NewFromEnv constructs a client from process environment variables.
func NewFromEnv() *Client {
	return New(Config{
		WebhookURL: os.Getenv("DISCORD_WEBHOOK_URL"),
		Username:   os.Getenv("DISCORD_USERNAME"),
		AvatarURL:  os.Getenv("DISCORD_AVATAR_URL"),
	})
}

// Configured reports whether a webhook URL was supplied. Send still validates
// its URL before making a request.
func (c *Client) Configured() bool {
	return c != nil && c.webhookURL != ""
}

// Validate checks the local webhook configuration without sending a message.
func (c *Client) Validate() error {
	if !c.Configured() {
		return ErrWebhookNotConfigured
	}
	_, err := c.deliveryURL()
	return err
}

type allowedMentions struct {
	Parse []string `json:"parse"`
}

type webhookPayload struct {
	Content         string          `json:"content"`
	Username        string          `json:"username,omitempty"`
	AvatarURL       string          `json:"avatar_url,omitempty"`
	AllowedMentions allowedMentions `json:"allowed_mentions"`
}

// Send posts text to Discord. Long messages are split without breaking UTF-8,
// and mentions are disabled so article text cannot ping @everyone or users.
func (c *Client) Send(ctx context.Context, text string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !c.Configured() {
		return ErrWebhookNotConfigured
	}
	if text == "" {
		return ErrEmptyMessage
	}
	deliveryURL, err := c.deliveryURL()
	if err != nil {
		return err
	}
	chunks := SplitContent(text)
	if len(chunks) > MaxChunksPerSend {
		return ErrContentTooLong
	}

	if err := c.acquireSendLock(ctx); err != nil {
		return err
	}
	defer c.releaseSendLock()

	for _, content := range chunks {
		if _, err := c.sendChunkLocked(ctx, deliveryURL, content); err != nil {
			return err
		}
	}
	return nil
}

// SendChunk sends exactly one non-empty Discord content chunk and returns the
// confirmed Discord message ID. It never splits oversized input; callers that
// need durable multi-part progress should persist SplitContent's result and ack
// each returned message ID independently.
func (c *Client) SendChunk(ctx context.Context, content string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !c.Configured() {
		return "", ErrWebhookNotConfigured
	}
	if content == "" {
		return "", ErrEmptyMessage
	}
	if utf16ContentLength(content) > MaxContentLength {
		return "", ErrContentTooLong
	}
	deliveryURL, err := c.deliveryURL()
	if err != nil {
		return "", err
	}
	if err = c.acquireSendLock(ctx); err != nil {
		return "", err
	}
	defer c.releaseSendLock()
	return c.sendChunkLocked(ctx, deliveryURL, content)
}

func (c *Client) acquireSendLock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case c.sendLock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("send discord webhook: %w", ctx.Err())
	}
}

func (c *Client) releaseSendLock() {
	<-c.sendLock
}

func (c *Client) sendChunkLocked(ctx context.Context, deliveryURL, content string) (string, error) {
	payload := webhookPayload{
		Content:   content,
		Username:  c.username,
		AvatarURL: c.avatarURL,
		AllowedMentions: allowedMentions{
			Parse: []string{},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode discord message: %w", err)
	}
	return c.post(ctx, deliveryURL, body)
}

func (c *Client) deliveryURL() (string, error) {
	parsedURL, err := url.ParseRequestURI(c.webhookURL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.Fragment != "" {
		return "", ErrInvalidWebhookURL
	}

	host := strings.ToLower(parsedURL.Hostname())
	ip := net.ParseIP(host)
	loopback := strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback())
	if loopback {
		if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
			return "", ErrInvalidWebhookURL
		}
	} else if parsedURL.Scheme != "https" ||
		(parsedURL.Port() != "" && parsedURL.Port() != "443") ||
		!isDiscordWebhookHost(host) || !isDiscordWebhookPath(parsedURL.EscapedPath()) {
		return "", ErrInvalidWebhookURL
	}
	query := parsedURL.Query()
	query.Set("wait", "true")
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String(), nil
}

func isDiscordWebhookHost(host string) bool {
	return host == "discord.com" || host == "discordapp.com"
}

func isDiscordWebhookPath(escapedPath string) bool {
	parts := strings.Split(strings.Trim(escapedPath, "/"), "/")
	if len(parts) != 4 || parts[0] != "api" || parts[1] != "webhooks" {
		return false
	}
	if parts[2] == "" || parts[3] == "" {
		return false
	}
	for _, character := range parts[2] {
		if character < '0' || character > '9' {
			return false
		}
	}
	for _, character := range parts[3] {
		if !(character >= 'a' && character <= 'z') &&
			!(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') &&
			character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func (c *Client) post(ctx context.Context, deliveryURL string, body []byte) (string, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, deliveryURL, bytes.NewReader(body))
		if err != nil {
			return "", fmt.Errorf("create discord webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Ptt-Alertor/discord-webhook")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if attempt < c.maxRetries {
				if err := waitForRetry(ctx, retryBackoff(attempt)); err != nil {
					return "", err
				}
				continue
			}
			return "", cleanTransportError(err, deliveryURL)
		}

		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		resp.Body.Close()
		if readErr != nil {
			return "", fmt.Errorf("read discord webhook response: %w", readErr)
		}
		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			var message struct {
				ID string `json:"id"`
			}
			if resp.StatusCode != http.StatusOK || json.Unmarshal(responseBody, &message) != nil || message.ID == "" {
				return "", ErrDeliveryUnconfirmed
			}
			return message.ID, nil
		}

		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError
		delay := retryBackoff(attempt)
		var retryAfter time.Duration
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter = rateLimitDelay(resp.Header, responseBody, delay)
			delay = retryAfter
		}
		if retryable && attempt < c.maxRetries {
			// Keep a claimed outbox lease short. A longer explicit Discord delay
			// is returned to the durable worker, which releases the lease and
			// schedules the event for exactly that delay.
			if resp.StatusCode == http.StatusTooManyRequests && delay > maxBackoffDelay {
				return "", &HTTPError{StatusCode: resp.StatusCode, RetryAfter: delay}
			}
			if err := waitForRetry(ctx, delay); err != nil {
				return "", err
			}
			continue
		}

		return "", &HTTPError{StatusCode: resp.StatusCode, RetryAfter: retryAfter}
	}
}

// cleanTransportError strips net/url's URL field before the error is logged;
// a Discord webhook URL contains the authentication token.
func cleanTransportError(err error, webhookURL string) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("send discord webhook: %w", context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("send discord webhook: %w", context.DeadlineExceeded)
	}
	for {
		var urlError *url.Error
		if !errors.As(err, &urlError) || urlError.Err == nil || urlError.Err == err {
			break
		}
		err = urlError.Err
	}
	message := strings.ReplaceAll(err.Error(), webhookURL, "[redacted]")
	if parsedURL, parseErr := url.Parse(webhookURL); parseErr == nil {
		parts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
		if len(parts) > 0 {
			token := parts[len(parts)-1]
			if token != "" {
				message = strings.ReplaceAll(message, token, "[redacted]")
			}
		}
	}
	return errors.New("send discord webhook: " + message)
}

func retryBackoff(attempt int) time.Duration {
	delay := 250 * time.Millisecond * time.Duration(1<<uint(attempt))
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}
	return delay
}

func rateLimitDelay(header http.Header, body []byte, fallback time.Duration) time.Duration {
	var response struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if json.Unmarshal(body, &response) == nil && response.RetryAfter > 0 {
		if delay := time.Duration(response.RetryAfter * float64(time.Second)); delay > 0 {
			return delay
		}
	}
	for _, name := range []string{"Retry-After", "X-RateLimit-Reset-After"} {
		seconds, err := strconv.ParseFloat(header.Get(name), 64)
		if err == nil && seconds > 0 {
			if delay := time.Duration(seconds * float64(time.Second)); delay > 0 {
				return delay
			}
		}
	}
	return capRetryDelay(fallback)
}

func capRetryDelay(delay time.Duration) time.Duration {
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}
	return delay
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("send discord webhook: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// SplitContent deterministically splits text into independent string copies
// that fit Discord's maximum UTF-16 content length. It preserves the original
// text and prefers a newline as the split point.
func SplitContent(text string) []string {
	return splitContent(text, MaxContentLength)
}

// splitContent is the limit-parameterized implementation used by focused unit
// tests. Astral Unicode characters, including many emoji, count as two UTF-16
// code units.
func splitContent(text string, limit int) []string {
	if text == "" || limit <= 0 {
		return nil
	}
	var chunks []string
	for start := 0; start < len(text); {
		units := 0
		end := start
		lastLineBreak := -1
		for offset, r := range text[start:] {
			unitsForRune := 1
			if r > 0xffff {
				unitsForRune = 2
			}
			if units+unitsForRune > limit {
				break
			}
			units += unitsForRune
			_, size := utf8.DecodeRuneInString(text[start+offset:])
			end = start + offset + size
			if r == '\n' {
				lastLineBreak = end
			}
		}
		if end == start {
			_, size := utf8.DecodeRuneInString(text[start:])
			end = start + size
		}
		if end < len(text) && lastLineBreak > start {
			end = lastLineBreak
		}
		chunks = append(chunks, strings.Clone(text[start:end]))
		start = end
	}
	return chunks
}

func utf16ContentLength(text string) int {
	units := 0
	for _, r := range text {
		units++
		if r > 0xffff {
			units++
		}
	}
	return units
}
