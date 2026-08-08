package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNoBroadcastPlatform          = errors.New("broadcast platform is required")
	ErrUnsupportedBroadcastPlatform = errors.New("unsupported broadcast platform")
)

type Broadcaster struct {
	Checker
	Msg string
}

func (bc Broadcaster) String() string {
	return bc.Msg
}

// Send delivers an administrative broadcast once to the global Discord
// webhook. Legacy per-user delivery platforms are intentionally unsupported.
func (bc Broadcaster) Send(platforms []string) error {
	return bc.SendContext(context.Background(), platforms)
}

// SendContext is Send with cancellation inherited from the HTTP request.
func (bc Broadcaster) SendContext(ctx context.Context, platforms []string) error {
	if len(platforms) == 0 {
		return ErrNoBroadcastPlatform
	}

	discordRequested := false
	for _, platform := range platforms {
		platform = strings.ToLower(strings.TrimSpace(platform))
		if platform != "discord" {
			return fmt.Errorf("%w %q; use discord", ErrUnsupportedBroadcastPlatform, platform)
		}
		discordRequested = true
	}
	if !discordRequested {
		return ErrNoBroadcastPlatform
	}

	return discordWebhook.Send(ctx, bc.Msg)
}
