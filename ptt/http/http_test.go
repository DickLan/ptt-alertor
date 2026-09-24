package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type roundTripFunc func(*stdhttp.Request) (*stdhttp.Response, error)

func (f roundTripFunc) RoundTrip(req *stdhttp.Request) (*stdhttp.Response, error) {
	return f(req)
}

type fakeAccessState struct {
	cooldown         time.Time
	retryAt          time.Time
	blocks           int64
	extendedTo       time.Time
	reserveErr       error
	recordErr        error
	recordContextErr error
}

func (state *fakeAccessState) LoadCooldown(context.Context) (time.Time, error) {
	return state.cooldown, nil
}

func (state *fakeAccessState) ExtendCooldown(_ context.Context, until time.Time) error {
	if until.After(state.extendedTo) {
		state.extendedTo = until
	}
	state.cooldown = state.extendedTo
	return nil
}

func (state *fakeAccessState) ReserveRequest(context.Context, time.Time) (time.Time, error) {
	return state.retryAt, state.reserveErr
}

func (state *fakeAccessState) RecordBlock(
	ctx context.Context,
	now time.Time,
	minimum time.Duration,
	threshold int,
	breaker time.Duration,
) (int64, time.Time, error) {
	state.recordContextErr = ctx.Err()
	if state.recordErr != nil {
		return 0, time.Time{}, state.recordErr
	}
	state.blocks++
	delay := minimum
	if state.blocks >= int64(threshold) && breaker > delay {
		delay = breaker
	}
	until := now.Add(delay)
	if until.After(state.extendedTo) {
		state.extendedTo = until
	}
	state.cooldown = state.extendedTo
	return state.blocks, until, nil
}

func TestEOFSignalsUseBlockCooldownWithoutSameCycleRetry(t *testing.T) {
	for _, transportErr := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(transportErr.Error(), func(t *testing.T) {
			isolateHTTPPolicy(t)
			resetCooldownEnabled = true
			maxRetries = 3
			forbiddenDelay = time.Minute
			state := &fakeAccessState{}
			SetAccessState(state)
			var calls int32
			client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
				atomic.AddInt32(&calls, 1)
				return nil, transportErr
			})}
			req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
			if _, err := DoWithClient(client, req); !errors.Is(err, transportErr) {
				t.Fatalf("DoWithClient() error = %v, want %v", err, transportErr)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("transport calls = %d, want no same-cycle retry", got)
			}
			if state.blocks != 1 {
				t.Fatalf("persistent block signals = %d, want 1", state.blocks)
			}
		})
	}
}

func TestTransportResetCooldownCanBeDisabledWithoutEnablingRetry(t *testing.T) {
	for _, transportErr := range []error{
		fmt.Errorf("TLS handshake: %w", syscall.ECONNRESET),
		io.EOF,
		io.ErrUnexpectedEOF,
	} {
		t.Run(transportErr.Error(), func(t *testing.T) {
			isolateHTTPPolicy(t)
			resetCooldownEnabled = false
			requestInterval = 10 * time.Minute
			maxRetries = 3
			state := &fakeAccessState{}
			SetAccessState(state)
			var calls int32
			client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
				atomic.AddInt32(&calls, 1)
				return nil, transportErr
			})}
			req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
			before := time.Now()
			if _, err := DoWithClient(client, req); !errors.Is(err, transportErr) {
				t.Fatalf("DoWithClient() error = %v, want %v", err, transportErr)
			}
			if got := atomic.LoadInt32(&calls); got != 1 {
				t.Fatalf("transport calls = %d, want no same-cycle retry", got)
			}
			if state.blocks != 0 {
				t.Fatalf("persistent block signals = %d, want 0", state.blocks)
			}
			limiterMu.Lock()
			gotCooldown := cooldownEnds
			gotNextRequest := nextRequest
			limiterMu.Unlock()
			if !gotCooldown.IsZero() {
				t.Fatalf("transport reset cooldown = %v, want none", gotCooldown)
			}
			if gotNextRequest.Before(before.Add(requestInterval)) || gotNextRequest.After(time.Now().Add(requestInterval)) {
				t.Fatalf("next request = %v, want approximately one request interval from now", gotNextRequest)
			}
		})
	}
}

func TestDisabledTransportResetCooldownDoesNotAffectExplicitBlockSignals(t *testing.T) {
	for _, status := range []int{stdhttp.StatusForbidden, stdhttp.StatusTooManyRequests} {
		t.Run(stdhttp.StatusText(status), func(t *testing.T) {
			isolateHTTPPolicy(t)
			resetCooldownEnabled = false
			forbiddenDelay = 15 * time.Minute
			maxRetries = 0
			state := &fakeAccessState{}
			SetAccessState(state)
			client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
				return testResponse(status, nil), nil
			})}
			req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
			resp, err := DoWithClient(client, req)
			if err != nil {
				t.Fatalf("DoWithClient() error = %v", err)
			}
			defer resp.Body.Close()
			if state.blocks != 1 {
				t.Fatalf("persistent block signals = %d, want 1", state.blocks)
			}
			if state.extendedTo.Before(time.Now().Add(forbiddenDelay - time.Second)) {
				t.Fatalf("explicit HTTP block cooldown ends at %v, want approximately %v", state.extendedTo, forbiddenDelay)
			}
		})
	}

	t.Run("HTML challenge", func(t *testing.T) {
		isolateHTTPPolicy(t)
		resetCooldownEnabled = false
		forbiddenDelay = 15 * time.Minute
		state := &fakeAccessState{}
		SetAccessState(state)
		ReportChallenge()
		if state.blocks != 1 {
			t.Fatalf("persistent block signals = %d, want 1", state.blocks)
		}
		if state.extendedTo.Before(time.Now().Add(forbiddenDelay - time.Second)) {
			t.Fatalf("challenge cooldown ends at %v, want approximately %v", state.extendedTo, forbiddenDelay)
		}
	})
}

type trackingBody struct {
	io.Reader
	closed int32
}

func newTrackingBody(content string) *trackingBody {
	return &trackingBody{Reader: strings.NewReader(content)}
}

func (b *trackingBody) Close() error {
	atomic.StoreInt32(&b.closed, 1)
	return nil
}

func (b *trackingBody) isClosed() bool {
	return atomic.LoadInt32(&b.closed) == 1
}

func testResponse(status int, body *trackingBody) *stdhttp.Response {
	if body == nil {
		body = newTrackingBody("")
	}
	return &stdhttp.Response{
		StatusCode: status,
		Header:     make(stdhttp.Header),
		Body:       body,
	}
}

func TestHttpRequestUsesHonestConfigurableUserAgent(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		setTestEnv(t, "PTT_USER_AGENT", "")

		req, err := HttpRequest("https://www.ptt.cc/bbs/index.html")
		if err != nil {
			t.Fatalf("HttpRequest() error = %v", err)
		}
		if got := req.UserAgent(); got != defaultUserAgent {
			t.Fatalf("User-Agent = %q, want %q", got, defaultUserAgent)
		}
		if strings.Contains(strings.ToLower(req.UserAgent()), "mozilla") {
			t.Fatalf("default User-Agent impersonates a browser: %q", req.UserAgent())
		}
	})

	t.Run("configured", func(t *testing.T) {
		const configured = "Example-PTT-Monitor/1.2 (+mailto:operator@example.test)"
		setTestEnv(t, "PTT_USER_AGENT", "  "+configured+"  ")

		req, err := HttpRequest("https://www.ptt.cc/atom/Test.xml")
		if err != nil {
			t.Fatalf("HttpRequest() error = %v", err)
		}
		if got := req.UserAgent(); got != configured {
			t.Fatalf("User-Agent = %q, want %q", got, configured)
		}
	})

	t.Run("rejects impersonation", func(t *testing.T) {
		setTestEnv(t, "PTT_USER_AGENT", "Mozilla/5.0 (compatible; Googlebot/2.1)")
		if _, err := HttpRequest("https://www.ptt.cc/atom/Test.xml"); !errors.Is(err, ErrUnsafeUserAgent) {
			t.Fatalf("HttpRequest() error = %v, want ErrUnsafeUserAgent", err)
		}
	})
}

func TestDurationFromEnvEnforcesPTTSafetyMinimums(t *testing.T) {
	for _, test := range []struct {
		name, key, value string
		fallback, want   time.Duration
	}{
		{name: "request interval cannot be zero", key: "PTT_REQUEST_INTERVAL", value: "0s", fallback: time.Second, want: time.Second},
		{name: "request interval cannot be shortened", key: "PTT_REQUEST_INTERVAL", value: "100ms", fallback: time.Second, want: time.Second},
		{name: "cooldown cannot be disabled", key: "PTT_FORBIDDEN_COOLDOWN", value: "1s", fallback: 15 * time.Minute, want: 15 * time.Minute},
		{name: "circuit breaker cannot be shorter than one hour", key: "PTT_CIRCUIT_BREAKER_COOLDOWN", value: "59m", fallback: 24 * time.Hour, want: 24 * time.Hour},
		{name: "operator may use a one hour circuit breaker", key: "PTT_CIRCUIT_BREAKER_COOLDOWN", value: "1h", fallback: 24 * time.Hour, want: time.Hour},
		{name: "operator may slow requests", key: "PTT_REQUEST_INTERVAL", value: "3s", fallback: time.Second, want: 3 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if got := durationFromEnv(test.key, test.fallback); got != test.want {
				t.Fatalf("durationFromEnv() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestCircuitBreakerEnabledDefaultsTrueAndAcceptsExplicitFalse(t *testing.T) {
	t.Setenv("PTT_CIRCUIT_BREAKER_ENABLED", "")
	if !boolFromEnv("PTT_CIRCUIT_BREAKER_ENABLED", true) {
		t.Fatal("empty PTT_CIRCUIT_BREAKER_ENABLED disabled the default circuit breaker")
	}
	t.Setenv("PTT_CIRCUIT_BREAKER_ENABLED", "false")
	if boolFromEnv("PTT_CIRCUIT_BREAKER_ENABLED", true) {
		t.Fatal("explicit false did not disable the circuit breaker")
	}
	t.Setenv("PTT_CIRCUIT_BREAKER_ENABLED", "invalid")
	if !boolFromEnv("PTT_CIRCUIT_BREAKER_ENABLED", true) {
		t.Fatal("invalid PTT_CIRCUIT_BREAKER_ENABLED did not use the safe default")
	}
}

func TestTransportResetCooldownDefaultsTrueAndAcceptsExplicitFalse(t *testing.T) {
	t.Setenv("PTT_TRANSPORT_RESET_COOLDOWN_ENABLED", "")
	if !boolFromEnv("PTT_TRANSPORT_RESET_COOLDOWN_ENABLED", true) {
		t.Fatal("empty PTT_TRANSPORT_RESET_COOLDOWN_ENABLED disabled the default cooldown")
	}
	t.Setenv("PTT_TRANSPORT_RESET_COOLDOWN_ENABLED", "false")
	if boolFromEnv("PTT_TRANSPORT_RESET_COOLDOWN_ENABLED", true) {
		t.Fatal("explicit false did not disable the transport reset cooldown")
	}
}

func TestHttpRequestWithContextPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req, err := HttpRequestWithContext(ctx, "https://www.ptt.cc/bbs/index.html")
	if err != nil {
		t.Fatalf("HttpRequestWithContext() error = %v", err)
	}
	cancel()
	if err := req.Context().Err(); err != context.Canceled {
		t.Fatalf("request context error = %v, want context.Canceled", err)
	}
}

func TestDoWithClientSerializesInflightRequestsAndSpacesBacklog(t *testing.T) {
	isolateHTTPPolicy(t)
	requestInterval = 40 * time.Millisecond

	starts := make(chan time.Time, 3)

	client := &stdhttp.Client{Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		starts <- time.Now()
		return testResponse(stdhttp.StatusOK, nil), nil
	})}

	type doResult struct {
		resp *stdhttp.Response
		err  error
	}
	do := func(req *stdhttp.Request, ready chan<- struct{}, result chan<- doResult) {
		ready <- struct{}{}
		resp, err := DoWithClient(client, req)
		result <- doResult{resp: resp, err: err}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	firstReq, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/first", nil)
	firstReq = firstReq.WithContext(ctx)
	firstResp, err := DoWithClient(client, firstReq)
	if err != nil {
		t.Fatalf("first DoWithClient() error = %v", err)
	}
	var startTimes []time.Time
	select {
	case started := <-starts:
		startTimes = append(startTimes, started)
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}

	ready := make(chan struct{}, 2)
	results := make(chan doResult, 2)
	secondReq, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/second", nil)
	thirdReq, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/third", nil)
	go do(secondReq.WithContext(ctx), ready, results)
	go do(thirdReq.WithContext(ctx), ready, results)
	<-ready
	<-ready

	// Keep the first response body open beyond the configured interval. Both
	// queued calls must remain outside the transport until the caller closes it.
	startedWhileInflight := false
	select {
	case started := <-starts:
		startedWhileInflight = true
		startTimes = append(startTimes, started)
	case <-time.After(2 * requestInterval):
	}
	_ = firstResp.Body.Close()

	var callErrors []error
	for i := 0; i < 2; i++ {
		var result doResult
		select {
		case result = <-results:
		case <-ctx.Done():
			releaseInflight()
			t.Fatalf("queued request did not finish: %v", ctx.Err())
		}
		if result.err != nil {
			callErrors = append(callErrors, result.err)
		}
		if result.resp != nil && result.resp.Body != nil {
			_ = result.resp.Body.Close()
		}
		for len(startTimes) < i+2 {
			select {
			case started := <-starts:
				startTimes = append(startTimes, started)
			case <-ctx.Done():
				t.Fatalf("transport start was not recorded: %v", ctx.Err())
			}
		}
	}

	if startedWhileInflight {
		t.Error("a queued transport started before the previous response body was closed")
	}
	for _, err := range callErrors {
		t.Errorf("queued DoWithClient() error = %v", err)
	}
	if len(startTimes) != 3 {
		t.Fatalf("transport starts = %d, want 3", len(startTimes))
	}
	if got := startTimes[2].Sub(startTimes[1]); got < requestInterval-5*time.Millisecond {
		t.Fatalf("backlogged request start interval = %v, want at least %v", got, requestInterval)
	}
}

func TestRetryDelayHonorsRetryAfter(t *testing.T) {
	resp := testResponse(stdhttp.StatusTooManyRequests, nil)
	resp.Header.Set("Retry-After", "7")

	if got := retryDelay(resp, 4); got != 7*time.Second {
		t.Fatalf("retryDelay() = %v, want %v", got, 7*time.Second)
	}
}

func TestDoWithClientRetries429(t *testing.T) {
	isolateHTTPPolicy(t)
	maxRetries = 1

	firstBody := newTrackingBody("rate limited")
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			resp := testResponse(stdhttp.StatusTooManyRequests, firstBody)
			resp.Header.Set("Retry-After", "0")
			return resp, nil
		}
		return testResponse(stdhttp.StatusOK, nil), nil
	})}

	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
	resp, err := DoWithClient(client, req)
	if err != nil {
		t.Fatalf("DoWithClient() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, stdhttp.StatusOK)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("transport calls = %d, want 2", got)
	}
	if !firstBody.isClosed() {
		t.Fatal("retry response body was not closed")
	}
}

func TestDoWithClientSetsCooldownOnFinal429(t *testing.T) {
	isolateHTTPPolicy(t)
	maxRetries = 0

	client := &stdhttp.Client{Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		resp := testResponse(stdhttp.StatusTooManyRequests, nil)
		resp.Header.Set("Retry-After", "7")
		return resp, nil
	})}
	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
	lowerBound := time.Now().Add(7 * time.Second)
	resp, err := DoWithClient(client, req)
	if err != nil {
		t.Fatalf("DoWithClient() error = %v", err)
	}
	defer resp.Body.Close()

	limiterMu.Lock()
	got := cooldownEnds
	limiterMu.Unlock()
	if got.Before(lowerBound) {
		t.Fatalf("final 429 cooldown ends at %v, want no earlier than %v", got, lowerBound)
	}
}

func TestDoWithClientSetsCooldownOnFinalTransportError(t *testing.T) {
	isolateHTTPPolicy(t)
	maxRetries = 0
	serverRetryBase = 2 * time.Second
	client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
		return nil, errors.New("connection reset")
	})}
	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
	lowerBound := time.Now().Add(serverRetryBase)
	if _, err := DoWithClient(client, req); err == nil {
		t.Fatal("DoWithClient() error = nil, want transport error")
	}
	limiterMu.Lock()
	got := cooldownEnds
	limiterMu.Unlock()
	if got.Before(lowerBound) {
		t.Fatalf("final transport cooldown ends at %v, want no earlier than %v", got, lowerBound)
	}
}

func TestDoWithClientDoesNotRetryConnectionResetAndUsesBlockCooldown(t *testing.T) {
	isolateHTTPPolicy(t)
	resetCooldownEnabled = true
	maxRetries = 3
	forbiddenDelay = 15 * time.Minute
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
		atomic.AddInt32(&calls, 1)
		return nil, fmt.Errorf("TLS handshake: %w", syscall.ECONNRESET)
	})}
	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
	lowerBound := time.Now().Add(forbiddenDelay)
	if _, err := DoWithClient(client, req); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("DoWithClient() error = %v, want ECONNRESET", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("transport calls = %d, want no immediate retry", got)
	}
	limiterMu.Lock()
	got := cooldownEnds
	limiterMu.Unlock()
	if got.Before(lowerBound) {
		t.Fatalf("reset cooldown ends at %v, want no earlier than %v", got, lowerBound)
	}
}

func TestDoWithClientDoesNotFollowRedirectOutsideLimiter(t *testing.T) {
	isolateHTTPPolicy(t)
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		atomic.AddInt32(&calls, 1)
		resp := testResponse(stdhttp.StatusFound, nil)
		resp.Header.Set("Location", "https://www.ptt.cc/redirected")
		return resp, nil
	})}
	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/start", nil)
	resp, err := DoWithClient(client, req)
	if err != nil {
		t.Fatalf("DoWithClient() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, stdhttp.StatusFound)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("transport calls = %d, want one metered request", got)
	}
}

func TestDoWithClientDoesNotRetry403AndSetsCooldown(t *testing.T) {
	isolateHTTPPolicy(t)
	forbiddenDelay = 10 * time.Minute
	maxRetries = 3

	body := newTrackingBody("forbidden")
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
		atomic.AddInt32(&calls, 1)
		return testResponse(stdhttp.StatusForbidden, body), nil
	})}

	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/bbs/index.html", nil)
	cooldownLowerBound := time.Now().Add(forbiddenDelay)
	resp, err := DoWithClient(client, req)
	if err != nil {
		t.Fatalf("DoWithClient() error = %v", err)
	}
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, stdhttp.StatusForbidden)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("transport calls = %d, want 1", got)
	}
	if body.isClosed() {
		t.Fatal("returned 403 response body was closed before the caller could read it")
	}

	limiterMu.Lock()
	gotCooldownEnd := cooldownEnds
	limiterMu.Unlock()
	if gotCooldownEnd.Before(cooldownLowerBound) {
		t.Fatalf("cooldown ends at %v, want no earlier than %v", gotCooldownEnd, cooldownLowerBound)
	}
	_ = resp.Body.Close()
}

func TestReportChallengeSetsForbiddenCooldown(t *testing.T) {
	isolateHTTPPolicy(t)
	forbiddenDelay = 10 * time.Minute
	lowerBound := time.Now().Add(forbiddenDelay)
	ReportChallenge()
	limiterMu.Lock()
	got := cooldownEnds
	limiterMu.Unlock()
	if got.Before(lowerBound) {
		t.Fatalf("challenge cooldown ends at %v, want no earlier than %v", got, lowerBound)
	}
}

func TestPersistentCooldownSurvivesFreshInMemoryLimiter(t *testing.T) {
	isolateHTTPPolicy(t)
	state := &fakeAccessState{cooldown: time.Now().Add(time.Minute)}
	SetAccessState(state)
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
		atomic.AddInt32(&calls, 1)
		return testResponse(stdhttp.StatusOK, nil), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := stdhttp.NewRequestWithContext(ctx, "GET", "https://www.ptt.cc/atom/Test.xml", nil)
	if _, err := DoWithClient(client, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DoWithClient() error = %v, want persistent cooldown wait deadline", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("transport calls = %d during persisted cooldown", got)
	}
}

func TestPersistentRequestBudgetRejectsBeforeTransport(t *testing.T) {
	isolateHTTPPolicy(t)
	retryAt := time.Now().Add(time.Hour)
	SetAccessState(&fakeAccessState{retryAt: retryAt})
	var calls int32
	client := &stdhttp.Client{Transport: roundTripFunc(func(*stdhttp.Request) (*stdhttp.Response, error) {
		atomic.AddInt32(&calls, 1)
		return testResponse(stdhttp.StatusOK, nil), nil
	})}
	req, _ := stdhttp.NewRequest("GET", "https://www.ptt.cc/atom/Test.xml", nil)
	_, err := DoWithClient(client, req)
	var budgetErr *RequestBudgetError
	if !errors.As(err, &budgetErr) || !budgetErr.RetryAt.Equal(retryAt) {
		t.Fatalf("DoWithClient() error = %#v, want budget retry at %v", err, retryAt)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("transport calls = %d after budget rejection", got)
	}
}

func TestRepeatedBlockSignalsOpenPersistentCircuitBreaker(t *testing.T) {
	isolateHTTPPolicy(t)
	breakerEnabled = true
	forbiddenDelay = 15 * time.Minute
	breakerDelay = 24 * time.Hour
	blockThreshold = 3
	state := &fakeAccessState{}
	SetAccessState(state)

	for index := 0; index < blockThreshold; index++ {
		setBlockCooldown(context.Background(), forbiddenDelay)
	}
	if state.blocks != int64(blockThreshold) {
		t.Fatalf("persistent block count = %d, want %d", state.blocks, blockThreshold)
	}
	if state.extendedTo.Before(time.Now().Add(breakerDelay - time.Second)) {
		t.Fatalf("persistent circuit cooldown ends at %v, want approximately 24h", state.extendedTo)
	}
}

func TestDisabledCircuitBreakerKeepsPerSignalPersistentCooldown(t *testing.T) {
	isolateHTTPPolicy(t)
	breakerEnabled = false
	forbiddenDelay = 15 * time.Minute
	breakerDelay = 24 * time.Hour
	blockThreshold = 3
	state := &fakeAccessState{}
	SetAccessState(state)

	for index := 0; index < blockThreshold+1; index++ {
		if err := setBlockCooldown(context.Background(), forbiddenDelay); err != nil {
			t.Fatalf("setBlockCooldown() error = %v", err)
		}
	}
	if state.blocks != int64(blockThreshold+1) {
		t.Fatalf("persistent block count = %d, want %d", state.blocks, blockThreshold+1)
	}
	remaining := time.Until(state.extendedTo)
	if remaining < forbiddenDelay-time.Second || remaining > forbiddenDelay+time.Second {
		t.Fatalf("persistent cooldown remaining = %v, want approximately %v", remaining, forbiddenDelay)
	}
}

func TestDisabledCircuitBreakerPersistenceFailureFailsClosedForSignalCooldown(t *testing.T) {
	isolateHTTPPolicy(t)
	breakerEnabled = false
	forbiddenDelay = 15 * time.Minute
	breakerDelay = 24 * time.Hour
	state := &fakeAccessState{recordErr: errors.New("Redis write failed")}
	SetAccessState(state)

	if err := setBlockCooldown(context.Background(), forbiddenDelay); err == nil {
		t.Fatal("setBlockCooldown() error = nil, want persistence failure")
	}
	remaining := time.Until(cooldownEnds)
	if remaining < forbiddenDelay-time.Second || remaining > forbiddenDelay+time.Second {
		t.Fatalf("local fail-closed cooldown remaining = %v, want approximately %v", remaining, forbiddenDelay)
	}

	state.recordErr = nil
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := waitForTurn(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForTurn() after repair error = %v, want cooldown wait deadline", err)
	}
	remaining = time.Until(state.extendedTo)
	if remaining < forbiddenDelay-time.Second || remaining > forbiddenDelay+time.Second {
		t.Fatalf("durable repaired cooldown remaining = %v, want approximately %v", remaining, forbiddenDelay)
	}
}

func TestObservedBlockPersistsAfterRequestContextCancellation(t *testing.T) {
	isolateHTTPPolicy(t)
	forbiddenDelay = 15 * time.Minute
	state := &fakeAccessState{}
	SetAccessState(state)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := setBlockCooldown(ctx, forbiddenDelay); err != nil {
		t.Fatalf("setBlockCooldown() error = %v", err)
	}
	if state.blocks != 1 || state.recordContextErr != nil {
		t.Fatalf("durable block write = %d, context error = %v", state.blocks, state.recordContextErr)
	}
}

func TestBlockPersistenceFailureFailsClosedUntilDurableRepair(t *testing.T) {
	isolateHTTPPolicy(t)
	forbiddenDelay = 15 * time.Minute
	breakerDelay = 24 * time.Hour
	state := &fakeAccessState{recordErr: errors.New("Redis write failed")}
	SetAccessState(state)

	if err := setBlockCooldown(context.Background(), forbiddenDelay); err == nil {
		t.Fatal("setBlockCooldown() error = nil, want persistence failure")
	}
	if currentPolicyFailure() == nil {
		t.Fatal("persistence failure did not latch fail-closed policy state")
	}
	limiterMu.Lock()
	localUntil := cooldownEnds
	limiterMu.Unlock()
	if localUntil.Before(time.Now().Add(breakerDelay - time.Second)) {
		t.Fatalf("local fail-closed cooldown = %v, want approximately 24h", localUntil)
	}

	state.recordErr = nil
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := waitForTurn(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForTurn() after repair error = %v, want cooldown wait deadline", err)
	}
	if currentPolicyFailure() != nil {
		t.Fatal("successful durable repair did not clear fail-closed latch")
	}
	if state.extendedTo.Before(time.Now().Add(breakerDelay - time.Second)) {
		t.Fatalf("durable repaired cooldown = %v, want approximately 24h", state.extendedTo)
	}
}

func isolateHTTPPolicy(t *testing.T) {
	oldRequestInterval := requestInterval
	oldRateRetryBase := rateRetryBase
	oldServerRetryBase := serverRetryBase
	oldForbiddenDelay := forbiddenDelay
	oldMaxRetries := maxRetries
	oldBreakerEnabled := breakerEnabled
	oldResetCooldownEnabled := resetCooldownEnabled
	oldBlockThreshold := blockThreshold
	oldBreakerDelay := breakerDelay
	oldInflight := inflight
	oldAccessState := configuredAccessState()
	blockMu.Lock()
	oldBlockEvents := append([]time.Time(nil), blockEvents...)
	blockEvents = nil
	blockMu.Unlock()
	policyErrMu.Lock()
	oldPolicyErr := policyErr
	oldPolicyErrDelay := policyErrDelay
	policyErr = nil
	policyErrDelay = 0
	policyErrMu.Unlock()
	SetAccessState(nil)
	limiterMu.Lock()
	oldNextRequest := nextRequest
	oldCooldownEnds := cooldownEnds
	nextRequest = time.Time{}
	cooldownEnds = time.Time{}
	limiterMu.Unlock()

	requestInterval = 0
	rateRetryBase = 0
	serverRetryBase = 0
	forbiddenDelay = 0
	maxRetries = 0
	breakerEnabled = true
	resetCooldownEnabled = true
	blockThreshold = 3
	breakerDelay = 24 * time.Hour
	inflight = make(chan struct{}, 1)

	t.Cleanup(func() {
		requestInterval = oldRequestInterval
		rateRetryBase = oldRateRetryBase
		serverRetryBase = oldServerRetryBase
		forbiddenDelay = oldForbiddenDelay
		maxRetries = oldMaxRetries
		breakerEnabled = oldBreakerEnabled
		resetCooldownEnabled = oldResetCooldownEnabled
		blockThreshold = oldBlockThreshold
		breakerDelay = oldBreakerDelay
		inflight = oldInflight
		SetAccessState(oldAccessState)
		blockMu.Lock()
		blockEvents = oldBlockEvents
		blockMu.Unlock()
		policyErrMu.Lock()
		policyErr = oldPolicyErr
		policyErrDelay = oldPolicyErrDelay
		policyErrMu.Unlock()
		limiterMu.Lock()
		nextRequest = oldNextRequest
		cooldownEnds = oldCooldownEnds
		limiterMu.Unlock()
	})
}

func setTestEnv(t *testing.T, key, value string) {
	oldValue, existed := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("os.Setenv(%q) error = %v", key, err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(key, oldValue)
			return
		}
		_ = os.Unsetenv(key)
	})
}
