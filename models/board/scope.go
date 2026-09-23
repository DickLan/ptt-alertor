package board

import (
	"errors"
	"os"
	"strings"
)

const pollingAllowlistEnvironment = "BOARD_ALLOWLIST"

// ErrSubscriptionNotAllowed is returned when this deployment has an explicit
// board boundary and a caller tries to create or expand a subscription outside
// it. The error deliberately carries no configured board names.
var ErrSubscriptionNotAllowed = errors.New("board is not available for new subscriptions")

// PollingAllowed reports whether a board may be polled by this deployment.
// An unset or blank allowlist preserves the historical unrestricted behavior.
// A configured allowlist is fail-closed: an empty/malformed member never
// grants access to a board.
func PollingAllowed(boardName string) bool {
	value := strings.TrimSpace(os.Getenv(pollingAllowlistEnvironment))
	if value == "" {
		return true
	}
	boardName = strings.ToLower(strings.TrimSpace(boardName))
	if boardName == "" {
		return false
	}
	for _, configured := range strings.Split(value, ",") {
		if strings.ToLower(strings.TrimSpace(configured)) == boardName {
			return true
		}
	}
	return false
}
