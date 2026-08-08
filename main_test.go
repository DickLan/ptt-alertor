package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/robfig/cron"
)

func TestRequireStorageReturnsGenericUnavailableWithoutCallingHandler(t *testing.T) {
	original := storagePing
	storagePing = func() error { return errors.New("dial redis.internal:6379: secret detail") }
	defer func() { storagePing = original }()

	called := false
	handler := requireStorage(func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/users", nil), nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if called {
		t.Fatal("wrapped handler was called while storage was unavailable")
	}
	if body := recorder.Body.String(); body != "storage unavailable\n" {
		t.Fatalf("response body = %q", body)
	}
}

func TestRequireStorageCallsHandlerWhenPingSucceeds(t *testing.T) {
	original := storagePing
	storagePing = func() error { return nil }
	defer func() { storagePing = original }()

	handler := requireStorage(func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.WriteHeader(http.StatusNoContent)
	})
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/users", nil), nil)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestReadinessRequiresStorageAndDeliveryWhenJobsRequested(t *testing.T) {
	original := storagePing
	defer func() { storagePing = original }()

	for _, test := range []struct {
		name          string
		storageErr    error
		jobsRequested bool
		webhookErr    error
		wantStatus    int
		wantBody      string
	}{
		{
			name: "ready", wantStatus: http.StatusOK, wantBody: "ok\n",
		},
		{
			name: "storage unavailable", storageErr: errors.New("dial detail"),
			wantStatus: http.StatusServiceUnavailable, wantBody: "redis unavailable\n",
		},
		{
			name: "jobs require webhook", jobsRequested: true, webhookErr: errors.New("invalid webhook"),
			wantStatus: http.StatusServiceUnavailable, wantBody: "notification delivery unavailable\n",
		},
		{
			name: "API-only mode permits missing webhook", jobsRequested: false, webhookErr: errors.New("invalid webhook"),
			wantStatus: http.StatusOK, wantBody: "ok\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			storagePing = func() error { return test.storageErr }
			recorder := httptest.NewRecorder()
			readinessHandler(test.jobsRequested, test.webhookErr)(
				recorder,
				httptest.NewRequest(http.MethodGet, "/readyz", nil),
				nil,
			)
			if recorder.Code != test.wantStatus || recorder.Body.String() != test.wantBody {
				t.Fatalf("response = (%d, %q), want (%d, %q)", recorder.Code, recorder.Body.String(), test.wantStatus, test.wantBody)
			}
		})
	}
}

func TestScheduledJobTrackerStopsAdmissionAndWaitsForRunningJob(t *testing.T) {
	tracker := &scheduledJobTracker{accepting: true}
	started := make(chan struct{})
	release := make(chan struct{})
	var runs int32
	job := tracker.wrap(cron.FuncJob(func() {
		atomic.AddInt32(&runs, 1)
		close(started)
		<-release
	}))

	go job.Run()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tracked job did not start")
	}

	stopped := make(chan struct{})
	go func() {
		tracker.stopAndWait()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("stopAndWait returned while a scheduled job was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stopAndWait did not return after the job completed")
	}

	job.Run()
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("runs after admission closed = %d, want 1", got)
	}
}

func TestJobRuntimeStopIsConcurrentAndIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &jobRuntime{
		cancel: cancel,
		cron:   cron.New(),
	}
	runtime.scheduled.accepting = true
	runtime.cron.Start()
	runtime.pollers.Add(1)
	go func() {
		defer runtime.pollers.Done()
		<-ctx.Done()
	}()

	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			runtime.Stop()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("concurrent Stop did not return")
		}
	}
}

func TestDurationFromEnvironmentUsesPositiveValueOrFallback(t *testing.T) {
	const name = "TEST_SHUTDOWN_DRAIN"
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "valid", value: "17s", want: 17 * time.Second},
		{name: "unset", value: "", want: 15 * time.Second},
		{name: "invalid", value: "later", want: 15 * time.Second},
		{name: "zero", value: "0s", want: 15 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(name, test.value)
			if got := durationFromEnvironment(name, 15*time.Second); got != test.want {
				t.Fatalf("duration = %s, want %s", got, test.want)
			}
		})
	}
}

func TestBoundedPositiveInt64FromEnvironment(t *testing.T) {
	const name = "TEST_PTT_REQUEST_BUDGET"
	for _, test := range []struct {
		name, value string
		want        int64
	}{
		{name: "unset", want: 900},
		{name: "valid lower budget", value: "300", want: 300},
		{name: "hard maximum", value: "99999", want: 1800},
		{name: "zero falls back", value: "0", want: 900},
		{name: "invalid falls back", value: "many", want: 900},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(name, test.value)
			if got := boundedPositiveInt64FromEnvironment(name, 900, 1800); got != test.want {
				t.Fatalf("budget = %d, want %d", got, test.want)
			}
		})
	}
}

func TestEnvironmentExplicitlyEnabledIsSafeOptIn(t *testing.T) {
	const name = "PTT_ALERTOR_TEST_ENABLED"
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", value: "", want: false},
		{name: "false", value: "false", want: false},
		{name: "misspelled", value: "flase", want: false},
		{name: "one", value: "1", want: false},
		{name: "true", value: "true", want: true},
		{name: "case and whitespace", value: " TRUE ", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(name, test.value)
			if got := environmentExplicitlyEnabled(name); got != test.want {
				t.Fatalf("environmentExplicitlyEnabled() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestConfiguredPTTPollersKeepBoardEnabledAndOptionalPollersOptIn(t *testing.T) {
	for _, test := range []struct {
		name           string
		pushsumEnabled string
		commentEnabled string
		want           []string
	}{
		{
			name: "safe defaults only start board checker",
			want: []string{"board"},
		},
		{
			name:           "pushsum explicitly enabled",
			pushsumEnabled: "true",
			want:           []string{"board", "pushsum"},
		},
		{
			name:           "comment explicitly enabled",
			commentEnabled: "true",
			want:           []string{"board", "comment"},
		},
		{
			name:           "both explicitly enabled",
			pushsumEnabled: " TRUE ",
			commentEnabled: "true",
			want:           []string{"board", "pushsum", "comment"},
		},
		{
			name:           "ambiguous values remain disabled",
			pushsumEnabled: "1",
			commentEnabled: "yes",
			want:           []string{"board"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PTT_PUSHSUM_JOBS_ENABLED", test.pushsumEnabled)
			t.Setenv("PTT_COMMENT_JOBS_ENABLED", test.commentEnabled)
			pollers := configuredPTTPollers()
			got := make([]string, 0, len(pollers))
			for _, poller := range pollers {
				if poller.run == nil {
					t.Fatalf("poller %q has a nil runner", poller.name)
				}
				got = append(got, poller.name)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("configured pollers = %v, want %v", got, test.want)
			}
		})
	}
}
