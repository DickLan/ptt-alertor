package jobs

import (
	"context"
	"sync"
	"time"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/counter"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

// One worker preserves notification order for the single global webhook. The
// Discord client also serializes administrative broadcasts against this flow.
const workers = 1

var ckCh = make(chan check)
var discordWebhook = discord.NewFromEnv()
var incrementAlert = counter.IncrAlert
var notificationWG sync.WaitGroup

func init() {
	for i := 0; i < workers; i++ {
		go messageWorker(ckCh)
	}
}

func messageWorker(ckCh chan check) {
	for ck := range ckCh {
		func() {
			defer notificationWG.Done()
			sendMessage(ck)
		}()
	}
}

func queueCheck(ctx context.Context, value check) bool {
	notificationWG.Add(1)
	select {
	case ckCh <- value:
		return true
	case <-ctx.Done():
		notificationWG.Done()
		return false
	}
}

// WaitForNotifications waits for every alert accepted by the worker queue.
// Call it only after all polling jobs have stopped producing new alerts.
func WaitForNotifications() {
	notificationWG.Wait()
}

// WaitForNotificationsContext is the bounded shutdown variant of
// WaitForNotifications. Producers must already be stopped before it is called.
func WaitForNotificationsContext(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		notificationWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

type check interface {
	String() string
	Self() Checker
	Stop()
	Run()
}

func sendMessage(c check) {
	cr := c.Self()
	account := cr.Profile.Account
	if !cr.Profile.Discord {
		return
	}
	prepared, dedupeKeys, shouldSend := prepareDiscordEvent(c)
	if !shouldSend {
		return
	}
	c = prepared
	cr = c.Self()
	if err := sendDiscord(c); err != nil {
		discordEvents.release(dedupeKeys)
		log.WithFields(log.Fields{
			"account":  account,
			"platform": "discord",
			"board":    cr.board,
			"type":     cr.subType,
			"word":     cr.word,
		}).WithError(err).Error("Message Send Failed")
		return
	}
	_ = incrementAlert()
	log.WithFields(log.Fields{
		"account":  account,
		"platform": "discord",
		"board":    cr.board,
		"type":     cr.subType,
		"word":     cr.word,
	}).Info("Message Sent")
}

func sendDiscord(c check) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return discordWebhook.Send(ctx, c.String())
}

func discordNotificationsEnabled(u user.User) bool {
	return u.Enable && u.Profile.Discord
}
