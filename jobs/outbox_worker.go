package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
)

const (
	defaultDiscordOutboxMaxPending = int64(10_000)
	maxDiscordNotificationChunks   = 4
	notificationTruncatedMarker    = "\n…（內容過長，已截斷；請開啟原文查看）"
	// One default Discord chunk can consume four 10-second HTTP attempts and
	// three capped 30-second retry waits. Keep the lease beyond that envelope.
	defaultDiscordOutboxLease            = 3 * time.Minute
	defaultDiscordOutboxIdlePoll         = 250 * time.Millisecond
	defaultDiscordOutboxRecoveryInterval = 30 * time.Second
	defaultDiscordOutboxRecoverLimit     = 100
	defaultDiscordRetryBase              = 5 * time.Second
	defaultDiscordRetryMax               = 15 * time.Minute
	defaultDiscordPermanentRetryBase     = 30 * time.Minute
	defaultDiscordPermanentRetryMax      = 6 * time.Hour
	defaultDiscordOutboxDrainPoll        = 100 * time.Millisecond
)

var (
	notificationOutbox outbox.Store = outbox.NewRedis(outbox.RedisConfig{
		HighWatermark: positiveInt64FromEnv("DISCORD_OUTBOX_MAX_PENDING", defaultDiscordOutboxMaxPending),
	})
	discordOutboxSender discordChunkSender = discord.NewFromEnv()
)

type discordChunkSender interface {
	SendChunk(context.Context, string) (string, error)
}

// enqueueDiscordNotification persists the exact Discord chunks before the
// producer advances its source cursor. canonicalParts must identify the full
// logical PTT event (for example, a complete article code or canonical URL),
// never a Unix-second article timestamp by itself.
func enqueueDiscordNotification(
	ctx context.Context,
	kind string,
	canonicalParts []string,
	content string,
	countAlert bool,
) error {
	item, err := outbox.NewItem(kind, canonicalParts, boundedDiscordNotificationChunks(content), countAlert)
	if err != nil {
		return err
	}
	_, err = notificationOutbox.Enqueue(normalizeContext(ctx), item)
	return err
}

// boundedDiscordNotificationChunks keeps a single source transition from
// monopolizing the durable queue. The marker is deterministic, so retries and
// event-payload conflict checks see exactly the same immutable payload.
func boundedDiscordNotificationChunks(content string) []string {
	maxUnits := maxDiscordNotificationChunks * discord.MaxContentLength
	bounded, truncated := utf16Prefix(content, maxUnits)
	if truncated {
		prefixLimit := maxUnits - discordUTF16Length(notificationTruncatedMarker)
		bounded, _ = utf16Prefix(content, prefixLimit)
		bounded += notificationTruncatedMarker
	}

	chunks := discord.SplitContent(bounded)
	if len(chunks) <= maxDiscordNotificationChunks {
		return chunks
	}

	// SplitContent prefers line boundaries. A deliberately placed early line
	// break can therefore produce more chunks even within the aggregate unit
	// cap. Preserve the first three chunks, then make one bounded final chunk.
	result := append([]string(nil), chunks[:maxDiscordNotificationChunks-1]...)
	tail := strings.Join(chunks[maxDiscordNotificationChunks-1:], "")
	lastLimit := discord.MaxContentLength - discordUTF16Length(notificationTruncatedMarker)
	last, _ := utf16Prefix(tail, lastLimit)
	result = append(result, last+notificationTruncatedMarker)
	return result
}

func utf16Prefix(value string, maxUnits int) (string, bool) {
	if maxUnits < 0 {
		maxUnits = 0
	}
	units := 0
	for index, r := range value {
		runeUnits := 1
		if r > 0xffff {
			runeUnits = 2
		}
		if units+runeUnits > maxUnits {
			return strings.Clone(value[:index]), true
		}
		units += runeUnits
	}
	return value, false
}

func discordUTF16Length(value string) int {
	units := 0
	for _, r := range value {
		units++
		if r > 0xffff {
			units++
		}
	}
	return units
}

type discordOutboxWorker struct {
	eligible         func(context.Context, *outbox.ClaimedItem) (bool, error)
	onDelivered      func(*outbox.ClaimedItem, string)
	onFailure        func(string)
	store            outbox.Store
	client           discordChunkSender
	leaseDuration    time.Duration
	idlePoll         time.Duration
	recoveryInterval time.Duration
	recoverLimit     int
	transientBase    time.Duration
	transientMax     time.Duration
	permanentBase    time.Duration
	permanentMax     time.Duration
}

func newDiscordOutboxWorker(store outbox.Store, client discordChunkSender) *discordOutboxWorker {
	return &discordOutboxWorker{
		store:            store,
		client:           client,
		leaseDuration:    defaultDiscordOutboxLease,
		idlePoll:         defaultDiscordOutboxIdlePoll,
		recoveryInterval: defaultDiscordOutboxRecoveryInterval,
		recoverLimit:     defaultDiscordOutboxRecoverLimit,
		transientBase:    defaultDiscordRetryBase,
		transientMax:     defaultDiscordRetryMax,
		permanentBase:    defaultDiscordPermanentRetryBase,
		permanentMax:     defaultDiscordPermanentRetryMax,
	}
}

// RunDiscordOutboxWorker delivers the global durable outbox until ctx is
// canceled. It starts no goroutine itself; the application owns its lifecycle.
func RunDiscordOutboxWorker(ctx context.Context) {
	newDiscordOutboxWorker(notificationOutbox, discordOutboxSender).Run(ctx)
}

// DiscordOutboxPending returns ready plus leased Discord notifications.
func DiscordOutboxPending(ctx context.Context) (int64, error) {
	return notificationOutbox.PendingCount(normalizeContext(ctx))
}

// WaitForDiscordOutboxEmpty waits until both ready and leased notification
// counts reach zero. Producers must be stopped before calling it.
func WaitForDiscordOutboxEmpty(ctx context.Context) error {
	ctx = normalizeContext(ctx)
	for {
		count, err := DiscordOutboxPending(ctx)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		if !waitDiscordOutbox(ctx, defaultDiscordOutboxDrainPoll) {
			return ctx.Err()
		}
	}
}

func (worker *discordOutboxWorker) Run(ctx context.Context) {
	ctx = normalizeContext(ctx)
	if worker == nil || worker.store == nil || worker.client == nil {
		log.Warn("Discord outbox worker is not configured")
		return
	}

	nextRecovery := time.Time{}
	for ctx.Err() == nil {
		now := time.Now()
		if nextRecovery.IsZero() || !now.Before(nextRecovery) {
			if _, err := worker.store.RecoverExpired(ctx, worker.recoverLimit); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warn("Discord outbox lease recovery failed")
				if !waitDiscordOutbox(ctx, worker.idlePoll) {
					return
				}
			}
			nextRecovery = time.Now().Add(worker.recoveryInterval)
		}

		claimed, err := worker.store.Claim(ctx, worker.leaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("Discord outbox claim failed")
			if !waitDiscordOutbox(ctx, worker.idlePoll) {
				return
			}
			continue
		}
		if claimed == nil {
			if !waitDiscordOutbox(ctx, worker.idlePoll) {
				return
			}
			continue
		}
		worker.deliverClaimed(ctx, claimed)
	}
}

func (worker *discordOutboxWorker) deliverClaimed(ctx context.Context, claimed *outbox.ClaimedItem) {
	for ctx.Err() == nil {
		content, err := claimed.CurrentChunk()
		if err != nil {
			log.WithField("event_id", claimed.EventID).Warn("Discord outbox item has an invalid chunk cursor")
			return
		}
		if worker.eligible != nil {
			allowed, eligibilityErr := worker.eligible(ctx, claimed)
			if eligibilityErr != nil {
				if worker.onFailure != nil {
					worker.onFailure("subscription_lookup_failed")
				}
				_, _ = worker.store.Retry(ctx, claimed.EventID, claimed.LeaseToken, 30*time.Second, "subscription_lookup_failed")
				return
			}
			if !allowed {
				if canceler, ok := worker.store.(interface {
					Cancel(context.Context, string, string) error
				}); ok {
					if err := canceler.Cancel(ctx, claimed.EventID, claimed.LeaseToken); err != nil && worker.onFailure != nil {
						worker.onFailure("subscription_cancel_failed")
					}
				} else {
					_, _ = worker.store.Retry(ctx, claimed.EventID, claimed.LeaseToken, 30*time.Second, "subscription_cancel_unavailable")
				}
				return
			}
		}
		messageID, sendErr := worker.client.SendChunk(ctx, content)
		if sendErr == nil && strings.TrimSpace(messageID) == "" {
			sendErr = discord.ErrDeliveryUnconfirmed
		}
		if sendErr != nil {
			if ctx.Err() != nil {
				// Do not mutate a canceled delivery. Its fenced lease will be
				// recovered by this or a restarted worker after expiry.
				return
			}
			failureClass, permanent := classifyDiscordFailure(sendErr)
			if worker.onFailure != nil {
				worker.onFailure(failureClass)
			}
			delay := worker.retryDelay(claimed.Attempts, permanent)
			var httpError *discord.HTTPError
			if errors.As(sendErr, &httpError) && httpError.RetryAfter > delay {
				delay = httpError.RetryAfter
			}
			result, retryErr := worker.store.Retry(ctx, claimed.EventID, claimed.LeaseToken, delay, failureClass)
			if retryErr != nil {
				if ctx.Err() == nil {
					log.WithFields(log.Fields{
						"event_id": claimed.EventID,
						"failure":  failureClass,
					}).Warn("Discord outbox retry transition failed")
				}
				return
			}
			log.WithFields(log.Fields{
				"event_id": claimed.EventID,
				"failure":  failureClass,
				"attempt":  result.Attempts,
			}).Warn("Discord notification retained for retry")
			return
		}

		ack, err := worker.store.AckChunk(
			ctx,
			claimed.EventID,
			claimed.LeaseToken,
			messageID,
			worker.leaseDuration,
		)
		if err != nil {
			if ctx.Err() == nil {
				log.WithField("event_id", claimed.EventID).Warn("Discord outbox acknowledgment failed")
			}
			return
		}
		if ack.Completed || ack.AlreadyDone {
			if ack.Completed && !ack.AlreadyDone && worker.onDelivered != nil {
				worker.onDelivered(claimed, messageID)
			}
			return
		}
		claimed.NextChunk = ack.NextChunk
	}
}

func classifyDiscordFailure(err error) (failureClass string, permanent bool) {
	switch {
	case errors.Is(err, discord.ErrWebhookNotConfigured):
		return "discord_webhook_not_configured", true
	case errors.Is(err, discord.ErrInvalidWebhookURL):
		return "discord_webhook_invalid", true
	case errors.Is(err, discord.ErrEmptyMessage):
		return "discord_content_empty", true
	case errors.Is(err, discord.ErrContentTooLong):
		return "discord_content_too_long", true
	case errors.Is(err, discord.ErrDeliveryUnconfirmed):
		return "discord_delivery_unconfirmed", false
	case errors.Is(err, context.DeadlineExceeded):
		return "discord_timeout", false
	case errors.Is(err, context.Canceled):
		return "discord_canceled", false
	}

	var httpError *discord.HTTPError
	if errors.As(err, &httpError) {
		failureClass = fmt.Sprintf("discord_http_%d", httpError.StatusCode)
		permanent = httpError.StatusCode >= 400 && httpError.StatusCode < 500 &&
			httpError.StatusCode != 408 && httpError.StatusCode != 429
		return failureClass, permanent
	}
	return "discord_transport", false
}

func (worker *discordOutboxWorker) retryDelay(attempts int64, permanent bool) time.Duration {
	base, maximum := worker.transientBase, worker.transientMax
	if permanent {
		base, maximum = worker.permanentBase, worker.permanentMax
	}
	return boundedExponentialDelay(base, maximum, attempts)
}

func boundedExponentialDelay(base, maximum time.Duration, attempts int64) time.Duration {
	if base <= 0 {
		return 0
	}
	if maximum <= 0 || base >= maximum {
		return maximum
	}
	if attempts < 0 {
		attempts = 0
	}
	delay := base
	for attempts > 0 && delay < maximum {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
		attempts--
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func waitDiscordOutbox(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
