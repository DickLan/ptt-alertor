package jobs

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

func TestPttMonitorStateCountsEveryHealthErrorAndRecovers(t *testing.T) {
	state := newPttMonitorState(3)
	transportErr := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}
	httpErr := web.HTTPStatusError{URL: "https://www.ptt.cc/bbs/index.html", StatusCode: 503}

	tests := []struct {
		name  string
		err   error
		want  pttMonitorEvent
		count int
		dead  bool
	}{
		{name: "transport failure", err: transportErr, want: pttDying, count: 1},
		{name: "HTTP failure", err: httpErr, want: pttDying, count: 2},
		{name: "challenge is third failure", err: web.ErrBotChallenge, want: pttDead, count: 3, dead: true},
		{name: "continued failure", err: errors.New("still down"), want: pttStillDead, count: 4, dead: true},
		{name: "recovered", want: pttRecovered},
		{name: "stays healthy", want: pttAlive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := state.observe(tt.err); got != tt.want {
				t.Fatalf("observe(%v) = %v, want %v", tt.err, got, tt.want)
			}
			if state.failures != tt.count {
				t.Fatalf("failures = %d, want %d", state.failures, tt.count)
			}
			if state.dead != tt.dead {
				t.Fatalf("dead = %t, want %t", state.dead, tt.dead)
			}
		})
	}
}

func TestPttMonitorRunContextCancelsInFlightHealthCheck(t *testing.T) {
	started := make(chan struct{})
	monitor := &pttMonitor{
		duration: time.Millisecond,
		retry:    3,
		healthCheck: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitor.RunContext(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("health check did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunContext did not stop after cancellation")
	}
}

func TestPttMonitorRetryMinimumIsOne(t *testing.T) {
	state := newPttMonitorState(0)
	if got := state.observe(errors.New("down")); got != pttDead {
		t.Fatalf("first failure event = %v, want %v", got, pttDead)
	}
}
