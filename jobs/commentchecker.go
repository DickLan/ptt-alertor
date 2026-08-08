package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/commentcursor"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

var cmtcker *commentChecker
var cmtOnce sync.Once

// commentChecker embedding Checker for checking comment
type commentChecker struct {
	Checker
	Article article.Article
	ch      chan commentChecker
	cycle   time.Duration
}

// NewCommentChecker return Empty PushChecker pointer
func NewCommentChecker() *commentChecker {
	cmtOnce.Do(func() {
		cmtcker = &commentChecker{}
		cmtcker.duration = 500 * time.Millisecond
		cmtcker.cycle = jobDurationFromEnv("PTT_COMMENT_CYCLE_INTERVAL", defaultCommentCycle)
		cmtcker.done = make(chan struct{})
		cmtcker.ch = make(chan commentChecker)
	})
	return cmtcker
}

func (cc commentChecker) String() string {
	return fmt.Sprintf("推文@%s\n\n%s\n%s\n%s", cc.Article.Board, cc.Article.Title, cc.Article.Link, cc.Article.Comments.String())
}

func (cc commentChecker) Stop() {
	cc.done <- struct{}{}
	log.Info("Comment Checker Stop")
}

// Run start job
func (cc commentChecker) Run() {
	cc.RunContext(context.Background())
}

func (cc commentChecker) RunContext(parent context.Context) {
	ach := make(chan article.Article)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var workers sync.WaitGroup

	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			started := time.Now()
			select {
			case <-ctx.Done():
				return
			default:
				if !notificationPollingAllowed(ctx) {
					if !waitForNextCycle(ctx, started, cc.cycle) {
						return
					}
					continue
				}
				codes := new(article.Articles).List()
				if len(codes) == 0 {
					if !waitForContext(ctx, time.Second) {
						return
					}
					continue
				}
				for _, code := range codes {
					if !waitForContext(ctx, cc.duration) {
						return
					}
					cc.checkCommentsContext(ctx, code, ach)
				}
			}
			if !waitForNextCycle(ctx, started, cc.cycle) {
				return
			}
		}
	}()

	for {
		select {
		case a := <-ach:
			cc.Article = a
			alert := cc
			workers.Add(1)
			go func() {
				defer workers.Done()
				alert.checkSubscribersContext(ctx)
			}()
		case pc := <-cc.ch:
			queueCheck(ctx, pc)
		case <-cc.done:
			cancel()
			workers.Wait()
			return
		case <-ctx.Done():
			cancel()
			workers.Wait()
			return
		}
	}
}

var fetchCommentArticle = web.FetchArticleContext
var commentArticleSubscribers = func(item article.Article) ([]string, error) {
	return item.Subscribers()
}

type cursorStore interface {
	Load(articleCode string) (commentcursor.State, error)
	Initialize(articleCode string, state commentcursor.State, allowLegacy bool) error
	Reset(articleCode string, state commentcursor.State) error
	Advance(articleCode string, source, next commentcursor.State) error
	LoadPending(articleCode string) (commentcursor.Pending, error)
	StagePending(articleCode string, source commentcursor.State, pending commentcursor.Pending) error
	CommitPending(articleCode string, source commentcursor.State, pending commentcursor.Pending) error
}

var commentCursors cursorStore = commentcursor.NewRedisStore()

func (cc commentChecker) checkComments(code string, ach chan article.Article) {
	cc.checkCommentsContext(context.Background(), code, ach)
}

func (cc commentChecker) checkCommentsContext(ctx context.Context, code string, _ chan article.Article) {
	a, err := models.Article().FindE(code)
	if err != nil {
		log.WithField("code", code).WithError(err).Error("Load Tracked Article Failed")
		return
	}
	if a.Board == "" || a.Code == "" {
		return
	}

	// Finish a previously frozen transition before touching PTT again. This
	// preserves its event identity and payload across partial Redis failures,
	// comment deletion/reindexing, and process restarts.
	pending, err := commentCursors.LoadPending(a.Code)
	if err == nil {
		if err := cc.finishPendingCommentTransition(ctx, &a, pending); err != nil {
			log.WithFields(log.Fields{"board": a.Board, "code": a.Code}).
				WithError(err).Warn("Resume Comment Transition Failed")
		}
		return
	}
	if !errors.Is(err, commentcursor.ErrPendingNotFound) {
		log.WithError(err).Error("Load Pending Comment Transition Failed")
		return
	}

	previous, err := commentCursors.Load(a.Code)
	if missing, legacy := errors.Is(err, commentcursor.ErrStateNotFound), errors.Is(err, commentcursor.ErrStateUpgradeNeeded); missing || legacy {
		// Missing/selectively lost and legacy tail-only cursors receive a fresh
		// random epoch and a complete baseline before any new event is derived.
		// Reset also removes a stale pending plan from an older lifecycle.
		previous, err = commentcursor.NewState(a.Comments)
		if err == nil {
			err = commentCursors.Initialize(a.Code, previous, legacy)
		}
	}
	if err != nil {
		log.WithError(err).Error("Load Or Initialize Comment Cursor Failed")
		return
	}

	new, err := fetchCommentArticle(ctx, a.Board, a.Code)
	var notFound web.URLNotFoundError
	if errors.As(err, &notFound) {
		cc.destroyComments(a)
		return
	}
	if err != nil {
		log.WithFields(log.Fields{
			"board": a.Board,
			"code":  a.Code,
		}).WithError(err).Warn("Skip Comment Check After PTT Error")
		return
	}
	subs, err := commentArticleSubscribers(a)
	if err != nil {
		log.WithError(err).Error("Get Comment Subscribers Failed")
		return
	}
	if len(subs) == 0 {
		cc.destroyComments(a)
		return
	}
	users, err := loadNotificationUsers(subs)
	if err != nil {
		log.WithError(err).Error("Read Comment Notification Users Failed")
		return
	}
	notify := anyDiscordUserEnabled(users)

	// A successful parse that suddenly has zero pushes is not strong enough
	// evidence that every historical push was deleted. Keeping the prior full
	// snapshot prevents one transient markup/edge-page anomaly from causing an
	// all-history replay storm. If genuinely all old pushes were deleted, the
	// first later push still fails the old subsequence and is notified.
	if len(previous.Snapshot) > 0 && len(new.Comments) == 0 {
		log.WithFields(log.Fields{"board": a.Board, "code": a.Code}).
			Warn("Skip Empty Comment Snapshot After Previously Non-Empty State")
		return
	}
	difference, err := commentcursor.Diff(previous, new.Comments)
	if err != nil {
		log.WithError(err).Warn("Comment Cursor Could Not Reconcile; Cursor Was Not Advanced")
		return
	}
	// Freeze the whole observed suffix as one durable transition-level outbox
	// item. Discord chunk progress remains durable inside that item, while the
	// producer has no partial per-comment admission window.
	if notify && len(difference.NewComments) > 0 {
		content := fmt.Sprintf(
			"推文@%s\n\n%s\n%s\n%s",
			a.Board,
			a.Title,
			a.Link,
			difference.NewComments.String(),
		)
		pending, err := commentcursor.NewPending(
			previous,
			difference.Next,
			new.Comments,
			new.LastPushDateTime,
			commentcursor.PendingEvent{
				CanonicalParts: []string{
					strings.ToLower(a.Board),
					a.Code,
					previous.Epoch,
					previous.Revision,
					difference.Next.Revision,
				},
				Content:    content,
				CountAlert: true,
			},
		)
		if err != nil {
			log.WithError(err).Error("Build Comment Transition Failed")
			return
		}
		err = commentCursors.StagePending(a.Code, previous, pending)
		if err != nil {
			log.WithError(err).Warn("Persist Comment Transition Failed")
			return
		}
		if err := cc.finishPendingCommentTransition(ctx, &a, pending); err != nil {
			log.WithFields(log.Fields{"board": a.Board, "code": a.Code}).
				WithError(err).Warn("Commit Comment Transition Failed")
		}
		return
	}

	// With no required Discord event, the version-3 cursor is authoritative.
	// Save it before updating the article's diagnostic copy.
	if err := commentCursors.Advance(a.Code, previous, difference.Next); err != nil {
		log.WithError(err).Error("Save Updated Comment Cursor Failed")
		return
	}
	a.LastPushDateTime = new.LastPushDateTime
	a.Comments = append(article.Comments(nil), new.Comments...)
	if err := a.Save(); err != nil {
		log.WithError(err).Error("Save Updated Comments Failed")
		return
	}
	if len(difference.NewComments) == 0 {
		return
	}
	log.WithFields(log.Fields{
		"board": a.Board,
		"code":  a.Code,
		"count": len(difference.NewComments),
	}).Info("Updated Comments")
}

func (cc commentChecker) finishPendingCommentTransition(
	ctx context.Context,
	a *article.Article,
	pending commentcursor.Pending,
) error {
	current, err := commentCursors.Load(a.Code)
	if err != nil {
		return fmt.Errorf("load cursor before comment transition: %w", err)
	}
	if current.Revision != pending.SourceRevision && current.Revision != pending.Next.Revision {
		return commentcursor.ErrTransitionConflict
	}
	if err := enqueueStableNotification(
		ctx,
		"comment",
		pending.Event.CanonicalParts,
		pending.Event.Content,
		pending.Event.CountAlert,
	); err != nil {
		return fmt.Errorf("enqueue staged comment transition: %w", err)
	}
	if err := commentCursors.CommitPending(a.Code, current, pending); err != nil {
		return fmt.Errorf("commit staged comment cursor: %w", err)
	}
	a.LastPushDateTime = pending.LastPushDateTime
	a.Comments = append(article.Comments(nil), pending.Comments...)
	if err := a.Save(); err != nil {
		return fmt.Errorf("save staged comment snapshot: %w", err)
	}
	log.WithFields(log.Fields{
		"board": a.Board,
		"code":  a.Code,
		"count": len(pending.Comments),
	}).Info("Committed Comment Transition")
	return nil
}

func (cc commentChecker) destroyComments(a article.Article) {
	if err := a.Destroy(); err != nil {
		log.WithError(err).Warning("Destroy Comment Failed")
	}
	log.WithFields(log.Fields{
		"board": a.Board,
		"code":  a.Code,
	}).Info("Destroy Comments")
}

func (cc commentChecker) checkSubscribers() {
	cc.checkSubscribersContext(context.Background())
}

func (cc commentChecker) checkSubscribersContext(ctx context.Context) {
	subs, err := commentArticleSubscribers(cc.Article)
	if err != nil {
		log.WithError(err).Error("Get Subscribers Failed")
	}

	for _, account := range subs {
		cc.sendContext(ctx, account)
	}
}

func (cc commentChecker) send(account string) {
	cc.sendContext(context.Background(), account)
}

func (cc commentChecker) sendContext(ctx context.Context, account string) {
	u := models.User().Find(account)
	if !discordNotificationsEnabled(u) {
		return
	}
	cc.board = cc.Article.Board
	cc.subType = "push"
	cc.word = cc.Article.Code
	cc.Profile = u.Profile
	select {
	case cc.ch <- cc:
	case <-ctx.Done():
	}
}
