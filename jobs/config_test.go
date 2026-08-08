package jobs

import (
	"context"
	"testing"
	"time"
)

func TestJobDurationFromEnvUsesPositiveDurationOrFallback(t *testing.T) {
	const name = "PTT_TEST_CYCLE_INTERVAL"
	for _, test := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "unset", value: "", want: time.Minute},
		{name: "valid", value: "17s", want: 17 * time.Second},
		{name: "invalid", value: "soon", want: time.Minute},
		{name: "zero", value: "0s", want: time.Minute},
		{name: "negative", value: "-1s", want: time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(name, test.value)
			if got := jobDurationFromEnv(name, time.Minute); got != test.want {
				t.Fatalf("duration = %s, want %s", got, test.want)
			}
		})
	}
}

func TestPositiveInt64FromEnvUsesPositiveValueOrFallback(t *testing.T) {
	const name = "PTT_TEST_MAX_PENDING"
	for _, test := range []struct {
		name  string
		value string
		want  int64
	}{
		{name: "unset", value: "", want: 10_000},
		{name: "valid", value: " 12345 ", want: 12_345},
		{name: "invalid", value: "many", want: 10_000},
		{name: "zero", value: "0", want: 10_000},
		{name: "negative", value: "-1", want: 10_000},
		{name: "overflow", value: "9223372036854775808", want: 10_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(name, test.value)
			if got := positiveInt64FromEnv(name, 10_000); got != test.want {
				t.Fatalf("value = %d, want %d", got, test.want)
			}
		})
	}
}

func TestWaitForNextCycleEnforcesMinimumAndCancels(t *testing.T) {
	started := time.Now()
	if !waitForNextCycle(context.Background(), started, 20*time.Millisecond) {
		t.Fatal("waitForNextCycle unexpectedly canceled")
	}
	if elapsed := time.Since(started); elapsed < 18*time.Millisecond {
		t.Fatalf("elapsed = %s, want approximately 20ms or more", elapsed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started = time.Now()
	if waitForNextCycle(ctx, started, time.Minute) {
		t.Fatal("waitForNextCycle returned true for canceled context")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled wait took %s", elapsed)
	}
}
