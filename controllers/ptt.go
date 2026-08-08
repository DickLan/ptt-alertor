package controllers

import (
	"context"
	"os"
	"strings"
	"time"
)

var pttAPITimeout = apiTimeoutFromEnv()

func apiTimeoutFromEnv() time.Duration {
	value := strings.TrimSpace(os.Getenv("PTT_API_TIMEOUT"))
	if value == "" {
		return 45 * time.Second
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 45 * time.Second
	}
	return duration
}

func pttOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, pttAPITimeout)
}
