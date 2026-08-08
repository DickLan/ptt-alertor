package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

// enqueueNotification is replaceable in focused producer tests. Production
// calls always land in the Redis-backed durable Discord outbox.
var enqueueNotification = enqueueDiscordNotification

var findNotificationUser = func(account string) (user.User, error) {
	return models.User().FindE(account)
}

// loadNotificationUsers is deliberately error-aware. A corrupt or unavailable
// user record can change whether an event matches, so producers must retain
// their source cursor and retry rather than silently treating that user as
// disabled.
func loadNotificationUsers(accounts []string) ([]user.User, error) {
	unique := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		account = strings.TrimSpace(account)
		if account != "" {
			unique[account] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(unique))
	for account := range unique {
		ordered = append(ordered, account)
	}
	sort.Strings(ordered)

	users := make([]user.User, 0, len(ordered))
	for _, account := range ordered {
		current, err := findNotificationUser(account)
		if err != nil {
			return nil, err
		}
		// A stale derived index can refer to a deleted user. Reconciliation will
		// remove it; it is not a storage failure and cannot enable a notification.
		if current.Profile.Account == "" {
			continue
		}
		users = append(users, current)
	}
	return users, nil
}

func anyDiscordUserEnabled(users []user.User) bool {
	for _, current := range users {
		if discordNotificationsEnabled(current) {
			return true
		}
	}
	return false
}

// notificationCanonicalParts adds a payload revision to the stable PTT source
// identity. PTT titles or displayed push counts can be edited between retries;
// a deterministic payload revision avoids permanently wedging the source
// cursor on an immutable-outbox conflict while retaining idempotency for an
// unchanged event.
func notificationCanonicalParts(parts []string, content string) []string {
	digest := sha256.Sum256([]byte(content))
	result := append([]string(nil), parts...)
	return append(result, hex.EncodeToString(digest[:]))
}

func enqueueStableNotification(
	ctx context.Context,
	kind string,
	canonicalParts []string,
	content string,
	countAlert bool,
) error {
	return enqueueNotification(
		ctx,
		kind,
		notificationCanonicalParts(canonicalParts, content),
		content,
		countAlert,
	)
}

// notificationPollingAllowed applies producer backpressure before making more
// PTT requests. Enqueue still enforces the limit atomically; this advisory
// check avoids repeatedly crawling while an unavailable Discord webhook lets
// the durable backlog fill.
func notificationPollingAllowed(ctx context.Context) bool {
	reached, count, err := notificationOutbox.HighWatermarkReached(ctx)
	if err != nil {
		log.Warn("Pause PTT notification producers because outbox capacity is unavailable")
		return false
	}
	if reached {
		log.WithField("pending", count).Warn("Pause PTT notification producers at Discord outbox high watermark")
		return false
	}
	return true
}
