package jobs

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHighBoardCycle = time.Minute
	defaultBoardCycle     = time.Minute
	defaultPushSumCycle   = 15 * time.Minute
	defaultCommentCycle   = 10 * time.Minute
)

func jobDurationFromEnv(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

func positiveInt64FromEnv(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

// waitForNextCycle enforces a minimum start-to-start interval without adding
// unnecessary delay when a large board set already took longer than the
// configured cadence. The shared PTT limiter remains the hard request bound.
func waitForNextCycle(ctx context.Context, started time.Time, minimum time.Duration) bool {
	remaining := minimum - time.Since(started)
	if remaining <= 0 {
		return ctx.Err() == nil
	}
	return waitForContext(ctx, remaining)
}
