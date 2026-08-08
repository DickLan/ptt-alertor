package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const defaultUserAgent = "Ptt-Alertor/5.0 (+https://github.com/Ptt-Alertor/ptt-alertor)"

const policyPersistenceTimeout = 5 * time.Second

var (
	ErrUnsafeUserAgent = errors.New("PTT user agent impersonates a browser or third-party crawler")

	requestInterval      = durationFromEnv("PTT_REQUEST_INTERVAL", time.Second)
	rateRetryBase        = durationFromEnv("PTT_RETRY_BASE_DELAY", 30*time.Second)
	serverRetryBase      = durationFromEnv("PTT_SERVER_RETRY_BASE_DELAY", 2*time.Second)
	forbiddenDelay       = durationFromEnv("PTT_FORBIDDEN_COOLDOWN", 15*time.Minute)
	maxRetries           = intFromEnv("PTT_MAX_RETRIES", 3)
	requestTimeout       = durationFromEnv("PTT_REQUEST_TIMEOUT", 30*time.Second)
	breakerEnabled       = boolFromEnv("PTT_CIRCUIT_BREAKER_ENABLED", true)
	resetCooldownEnabled = boolFromEnv("PTT_TRANSPORT_RESET_COOLDOWN_ENABLED", true)
	blockThreshold       = circuitBreakerThresholdFromEnv()
	breakerDelay         = durationFromEnv("PTT_CIRCUIT_BREAKER_COOLDOWN", 24*time.Hour)
	defaultHTTPClient    = &http.Client{
		Timeout:       requestTimeout,
		CheckRedirect: stopRedirect,
	}

	limiterMu    sync.Mutex
	nextRequest  time.Time
	cooldownEnds time.Time
	inflight     = make(chan struct{}, 1)

	accessStateMu  sync.RWMutex
	accessState    AccessState
	blockMu        sync.Mutex
	blockEvents    []time.Time
	policyErrMu    sync.Mutex
	policyErr      error
	policyErrDelay time.Duration
)

// AccessState persists host-wide cooldown, request budgets, and repeated block
// signals across process restarts. The default nil state keeps the package
// usable in isolated libraries/tests; the application configures Redis before
// starting any PTT jobs.
type AccessState interface {
	LoadCooldown(context.Context) (time.Time, error)
	ExtendCooldown(context.Context, time.Time) error
	ReserveRequest(context.Context, time.Time) (time.Time, error)
	RecordBlock(context.Context, time.Time, time.Duration, int, time.Duration) (int64, time.Time, error)
}

// RequestBudgetError means no network request was made because the configured
// host-wide hourly or daily budget is exhausted.
type RequestBudgetError struct {
	RetryAt time.Time
}

func (err *RequestBudgetError) Error() string {
	return "PTT request budget exhausted until " + err.RetryAt.UTC().Format(time.RFC3339)
}

// SetAccessState installs the process-wide persistent access policy. Configure
// it once during startup, before any PTT request is admitted.
func SetAccessState(state AccessState) {
	accessStateMu.Lock()
	accessState = state
	accessStateMu.Unlock()
}

func configuredAccessState() AccessState {
	accessStateMu.RLock()
	state := accessState
	accessStateMu.RUnlock()
	return state
}

func HttpRequest(reqURL string) (*http.Request, error) {
	return HttpRequestWithContext(context.Background(), reqURL)
}

// HttpRequestWithContext builds a PTT request whose limiter wait, retries, and
// network operation are canceled with the caller.
func HttpRequestWithContext(ctx context.Context, reqURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	userAgent := strings.TrimSpace(os.Getenv("PTT_USER_AGENT"))
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	if unsafeUserAgent(userAgent) {
		return nil, ErrUnsafeUserAgent
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/atom+xml,application/xml;q=0.9,*/*;q=0.8")
	if strings.EqualFold(strings.TrimSpace(os.Getenv("PTT_OVER18")), "true") {
		req.AddCookie(&http.Cookie{
			Name:     "over18",
			Value:    "1",
			Domain:   "www.ptt.cc",
			Path:     "/",
			HttpOnly: true,
		})
	}
	return req, nil
}

func unsafeUserAgent(userAgent string) bool {
	lower := strings.ToLower(strings.TrimSpace(userAgent))
	for _, marker := range []string{
		"mozilla/", "googlebot", "bingbot", "baiduspider", "yandexbot",
		"duckduckbot", "facebookexternalhit", "twitterbot",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// Do sends a PTT request through the shared rate limiter and retry policy.
func Do(req *http.Request) (*http.Response, error) {
	return DoWithClient(defaultHTTPClient, req)
}

// DoWithClient is Do with a caller-supplied client (for redirect policy/tests).
func DoWithClient(client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = defaultHTTPClient
	}
	client = clientWithoutRedirects(client)

	for attempt := 0; ; attempt++ {
		if err := acquireInflight(req.Context()); err != nil {
			return nil, err
		}
		if err := waitForTurn(req.Context()); err != nil {
			releaseInflight()
			return nil, err
		}

		attemptReq := req.Clone(req.Context())
		resp, err := client.Do(attemptReq)
		if err != nil {
			// PTT may reset a connection instead of returning an HTTP challenge.
			// Retrying that signal immediately can turn a suspected block into a
			// larger burst, so apply the same conservative host-wide cooldown as a
			// 403 and leave the retry to a later polling cycle.
			if req.Context().Err() == nil && suspectedRemoteBlock(err) {
				releaseResponse(resp)
				if !resetCooldownEnabled {
					return nil, err
				}
				policyErr := setBlockCooldown(req.Context(), forbiddenDelay)
				if policyErr != nil {
					return nil, errors.Join(err, policyErr)
				}
				return nil, err
			}
			delay := cappedBackoff(attempt, serverRetryBase, 5*time.Minute)
			if attempt >= maxRetries || req.Context().Err() != nil {
				if req.Context().Err() == nil {
					setCooldown(req.Context(), delay)
				}
				releaseResponse(resp)
				return nil, err
			}
			setCooldown(req.Context(), delay)
			releaseResponse(resp)
			continue
		}
		if resp.Body == nil {
			releaseInflight()
		} else {
			resp.Body = &inflightBody{ReadCloser: resp.Body}
		}

		if resp.StatusCode == http.StatusForbidden {
			// A 403 may be an operator block/challenge. Cool down globally and do
			// not attempt to bypass it.
			if err := setBlockCooldown(req.Context(), forbiddenDelay); err != nil {
				releaseResponse(resp)
				return nil, err
			}
			return resp, nil
		}

		if !retryableStatus(resp.StatusCode) {
			return resp, nil
		}

		delay := retryDelay(resp, attempt)
		if resp.StatusCode == http.StatusTooManyRequests {
			if err := setBlockCooldown(req.Context(), delay); err != nil {
				releaseResponse(resp)
				return nil, err
			}
		} else {
			setCooldown(req.Context(), delay)
		}
		if attempt >= maxRetries {
			return resp, nil
		}
		drainAndClose(resp.Body)
	}
}

func suspectedRemoteBlock(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func stopRedirect(_ *http.Request, _ []*http.Request) error {
	// Redirect hops would otherwise be sent inside http.Client.Do without
	// reacquiring the process-wide PTT limiter. Return the 3xx response to the
	// caller instead of issuing an unmetered second request.
	return http.ErrUseLastResponse
}

func clientWithoutRedirects(client *http.Client) *http.Client {
	return &http.Client{
		Transport:     client.Transport,
		CheckRedirect: stopRedirect,
		Jar:           client.Jar,
		Timeout:       client.Timeout,
	}
}

type inflightBody struct {
	io.ReadCloser
	once sync.Once
}

func (body *inflightBody) Close() error {
	err := body.ReadCloser.Close()
	body.once.Do(releaseInflight)
	return err
}

func acquireInflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case inflight <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseInflight() {
	select {
	case <-inflight:
	default:
	}
}

func releaseResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	releaseInflight()
}

func waitForTurn(ctx context.Context) error {
	for {
		state := configuredAccessState()
		if err := repairPersistentPolicyFailure(ctx, state); err != nil {
			return err
		}
		if state != nil {
			persisted, err := state.LoadCooldown(ctx)
			if err != nil {
				return fmt.Errorf("load persistent PTT cooldown: %w", err)
			}
			limiterMu.Lock()
			if persisted.After(cooldownEnds) {
				cooldownEnds = persisted
			}
			limiterMu.Unlock()
		}
		limiterMu.Lock()
		now := time.Now()
		allowedAt := nextRequest
		if cooldownEnds.After(allowedAt) {
			allowedAt = cooldownEnds
		}
		if !allowedAt.After(now) {
			limiterMu.Unlock()
			if state != nil {
				retryAt, err := state.ReserveRequest(ctx, now)
				if err != nil {
					return fmt.Errorf("reserve persistent PTT request budget: %w", err)
				}
				if retryAt.After(now) {
					return &RequestBudgetError{RetryAt: retryAt}
				}
			}
			limiterMu.Lock()
			nextRequest = time.Now().Add(requestInterval)
			limiterMu.Unlock()
			return nil
		}
		wait := time.Until(allowedAt)
		limiterMu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func setCooldown(ctx context.Context, delay time.Duration) {
	if delay <= 0 {
		return
	}
	until := time.Now().Add(delay)
	limiterMu.Lock()
	if until.After(cooldownEnds) {
		cooldownEnds = until
	}
	limiterMu.Unlock()
	if state := configuredAccessState(); state != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		_ = state.ExtendCooldown(ctx, until)
	}
}

func setBlockCooldown(_ context.Context, minimum time.Duration) error {
	// Once a block signal has been observed, its safety state must survive a
	// canceled HTTP request or shutdown. Give the durable write its own short,
	// bounded context rather than inheriting caller cancellation.
	policyContext, cancel := context.WithTimeout(context.Background(), policyPersistenceTimeout)
	defer cancel()
	now := time.Now()
	delay := minimum
	if delay < forbiddenDelay {
		delay = forbiddenDelay
	}
	blockMu.Lock()
	cutoff := now.Add(-24 * time.Hour)
	kept := blockEvents[:0]
	for _, observed := range blockEvents {
		if observed.After(cutoff) {
			kept = append(kept, observed)
		}
	}
	blockEvents = append(kept, now)
	count := int64(len(blockEvents))
	blockMu.Unlock()
	if breakerEnabled && count >= int64(blockThreshold) && delay < breakerDelay {
		delay = breakerDelay
	}

	if state := configuredAccessState(); state != nil {
		// Keep recording block observations while the circuit breaker is disabled,
		// but make the Redis-side breaker delay no greater than this signal's
		// ordinary cooldown so the atomic script cannot escalate it to 24 hours.
		effectiveBreakerDelay := breakerDelay
		if !breakerEnabled {
			effectiveBreakerDelay = delay
		}
		_, until, err := state.RecordBlock(policyContext, now, delay, blockThreshold, effectiveBreakerDelay)
		if err != nil {
			failClosedDelay := breakerDelay
			if !breakerEnabled {
				failClosedDelay = delay
			}
			setMemoryCooldownUntil(now.Add(failClosedDelay))
			setPolicyFailure(err, failClosedDelay)
			return fmt.Errorf("persist PTT block and cooldown atomically: %w", err)
		}
		clearPolicyFailure()
		setMemoryCooldownUntil(until)
		return nil
	}
	setCooldown(policyContext, delay)
	return nil
}

// ReportChallenge applies the same host-wide cooldown as an HTTP 403 when a
// higher-level parser recognizes a 200 anti-bot challenge page.
func ReportChallenge() {
	_ = setBlockCooldown(context.Background(), forbiddenDelay)
}

func setMemoryCooldownUntil(until time.Time) {
	limiterMu.Lock()
	if until.After(cooldownEnds) {
		cooldownEnds = until
	}
	limiterMu.Unlock()
}

func setPolicyFailure(err error, repairDelay time.Duration) {
	policyErrMu.Lock()
	policyErr = err
	policyErrDelay = repairDelay
	policyErrMu.Unlock()
}

func clearPolicyFailure() {
	policyErrMu.Lock()
	policyErr = nil
	policyErrDelay = 0
	policyErrMu.Unlock()
}

func currentPolicyFailure() error {
	policyErrMu.Lock()
	err := policyErr
	policyErrMu.Unlock()
	return err
}

func currentPolicyFailureState() (error, time.Duration) {
	policyErrMu.Lock()
	err := policyErr
	delay := policyErrDelay
	policyErrMu.Unlock()
	return err, delay
}

func repairPersistentPolicyFailure(ctx context.Context, state AccessState) error {
	failure, repairDelay := currentPolicyFailureState()
	if failure == nil {
		return nil
	}
	if state == nil {
		return fmt.Errorf("PTT access policy remains fail-closed: %w", failure)
	}
	if repairDelay <= 0 {
		repairDelay = breakerDelay
	}
	until := time.Now().Add(repairDelay)
	if err := state.ExtendCooldown(ctx, until); err != nil {
		return fmt.Errorf("repair persistent PTT cooldown after policy failure: %w", err)
	}
	setMemoryCooldownUntil(until)
	clearPolicyFailure()
	return nil
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusInternalServerError ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
		if retryAt, err := http.ParseTime(value); err == nil {
			if delay := time.Until(retryAt); delay > 0 {
				return delay
			}
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return cappedBackoff(attempt, rateRetryBase, 15*time.Minute)
	}
	return cappedBackoff(attempt, serverRetryBase, 5*time.Minute)
}

func backoff(attempt int, base time.Duration) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	delay := base * time.Duration(1<<uint(attempt))
	if delay <= 0 {
		return 0
	}
	// Positive jitter prevents several replicas from retrying together.
	return delay + time.Duration(rand.Int63n(int64(delay/4)+1))
}

func cappedBackoff(attempt int, base, maximum time.Duration) time.Duration {
	delay := backoff(attempt, base)
	if maximum > 0 && delay > maximum {
		return maximum
	}
	return delay
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = ioutil.ReadAll(io.LimitReader(body, 4096))
	_ = body.Close()
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	minimums := map[string]time.Duration{
		"PTT_REQUEST_INTERVAL":         time.Second,
		"PTT_RETRY_BASE_DELAY":         30 * time.Second,
		"PTT_SERVER_RETRY_BASE_DELAY":  time.Second,
		"PTT_FORBIDDEN_COOLDOWN":       15 * time.Minute,
		"PTT_CIRCUIT_BREAKER_COOLDOWN": 24 * time.Hour,
		"PTT_REQUEST_TIMEOUT":          5 * time.Second,
	}
	if minimum := minimums[key]; minimum > 0 && duration < minimum {
		return fallback
	}
	return duration
}

func intFromEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func boolFromEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func positiveIntFromEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func circuitBreakerThresholdFromEnv() int {
	threshold := positiveIntFromEnv("PTT_CIRCUIT_BREAKER_THRESHOLD", 3)
	if threshold > 3 {
		return 3
	}
	return threshold
}
