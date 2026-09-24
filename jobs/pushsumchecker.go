package jobs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"strings"

	"strconv"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	boardmodel "github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/pushsum"
	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

// NewPushSumKeyReplacer Job schedule must longer than overduehour
const overdueHour = 48 * time.Hour
const defaultMaxPushSumPages = 20

func maxPushSumPages() int {
	return int(positiveInt64FromEnv("PTT_PUSHSUM_MAX_PAGES", defaultMaxPushSumPages))
}

var psCker *pushSumChecker
var pscOnce sync.Once

type pushSumChecker struct {
	Checker
	ch       chan pushSumChecker
	duration time.Duration
	cycle    time.Duration
}

func NewPushSumChecker() *pushSumChecker {
	pscOnce.Do(func() {
		psCker = &pushSumChecker{
			duration: 500 * time.Millisecond,
			cycle:    jobDurationFromEnv("PTT_PUSHSUM_CYCLE_INTERVAL", defaultPushSumCycle),
		}
		psCker.done = make(chan struct{})
		psCker.ch = make(chan pushSumChecker)
	})
	return psCker
}

func (psc pushSumChecker) String() string {
	textMap := map[string]string{
		"pushup":   "推文數",
		"pushdown": "噓文數",
	}
	subType := textMap[psc.subType]
	return fmt.Sprintf("%s@%s\r\n看板：%s；%s：%s%s", psc.word, psc.board, psc.board, subType, psc.word, psc.articles.StringWithPushSum())
}

type BoardArticles struct {
	board    string
	articles article.Articles
	complete bool
}

var currentBoardPage = web.CurrentPageContext
var fetchPushSumArticles = web.FetchArticlesContext
var listPushSumSubscribers = pushsum.ListSubscribersE
var pendingPushSumDiff = pushsum.PendingDiff
var commitPushSumDiff = pushsum.CommitDiff

func (psc pushSumChecker) Stop() {
	psc.done <- struct{}{}
	log.Info("Pushsum Checker Stop")
}

func (psc pushSumChecker) Run() {
	psc.RunContext(context.Background())
}

func (psc pushSumChecker) RunContext(parent context.Context) {
	baCh := make(chan BoardArticles)

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
					if !waitForNextCycle(ctx, started, psc.cycle) {
						return
					}
					continue
				}
				boards := pushsum.List()
				if len(boards) == 0 {
					if !waitForContext(ctx, time.Second) {
						return
					}
					continue
				}
				for _, board := range boards {
					if !boardmodel.PollingAllowed(board) {
						continue
					}
					ba := BoardArticles{board: board}
					if !waitForContext(ctx, psc.duration) {
						return
					}
					psc.crawlArticlesContext(ctx, ba, baCh)
				}
			}
			if !waitForNextCycle(ctx, started, psc.cycle) {
				return
			}
		}
	}()

	for {
		select {
		case ba := <-baCh:
			psc.board = ba.board
			if !ba.complete {
				log.WithField("board", ba.board).Warn("Skip PushSum State After Incomplete Crawl")
				continue
			}
			checker := psc
			batch := ba
			workers.Add(1)
			go func() {
				defer workers.Done()
				checker.checkSubscribersContext(ctx, batch)
			}()
		case pscker := <-psc.ch:
			queueCheck(ctx, pscker)
		case <-psc.done:
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

func (psc pushSumChecker) crawlArticles(ba BoardArticles, baCh chan BoardArticles) {
	psc.crawlArticlesContext(context.Background(), ba, baCh)
}

func (psc pushSumChecker) crawlArticlesContext(ctx context.Context, ba BoardArticles, baCh chan BoardArticles) {
	if !boardmodel.PollingAllowed(ba.board) {
		return
	}
	currentPage, err := currentBoardPage(ctx, ba.board)
	if err != nil {
		log.WithFields(log.Fields{
			"board": ba.board,
		}).WithError(err).Error("Get CurrentPage Failed")
		select {
		case baCh <- ba:
		case <-ctx.Done():
		}
		return
	}

	pagesFetched := 0
	loc := time.FixedZone("CST", 8*60*60)
	now := time.Now().In(loc)
	nowDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	pageLimit := maxPushSumPages()
Page:
	for page := currentPage; page > 0 && pagesFetched < pageLimit; page-- {
		pagesFetched++
		articles, err := fetchPushSumArticles(ctx, ba.board, page)
		if err != nil {
			log.WithFields(log.Fields{
				"board": ba.board,
				"page":  page,
			}).WithError(err).Warn("Stop PushSum Crawl After PTT Error")
			break Page
		}
		if len(articles) == 0 {
			log.WithFields(log.Fields{
				"board": ba.board,
				"page":  page,
			}).Warn("Stop PushSum Crawl After Empty Page")
			// A successfully parsed single-page board can legitimately have no
			// articles. An empty intermediate page is ambiguous and must not
			// advance any threshold state.
			ba.complete = page == 1
			break Page
		}
		validDates := 0
		parseFailed := false
		reachedBoundary := false
		for i := len(articles) - 1; i >= 0; i-- {
			a := articles[i]
			if a.ID == 0 {
				continue
			}
			t, err := time.ParseInLocation("1/02", a.Date, loc)
			if err != nil {
				log.WithFields(log.Fields{
					"board": ba.board,
					"page":  page,
				}).WithError(err).Error("Parse DateTime Error")
				parseFailed = true
				continue
			}
			if t.Month() > now.Month() {
				t = t.AddDate(now.Year()-1, 0, 0)
			} else {
				t = t.AddDate(now.Year(), 0, 0)
			}
			validDates++
			if nowDate.After(t.Add(overdueHour)) {
				reachedBoundary = true
				break
			}
			ba.articles = append(ba.articles, a)
		}
		if parseFailed {
			log.WithFields(log.Fields{
				"board": ba.board,
				"page":  page,
			}).Warn("Stop PushSum Crawl After Partially Unparseable Page")
			break Page
		}
		if reachedBoundary {
			ba.complete = true
			break Page
		}
		if page == 1 {
			ba.complete = true
			break Page
		}
		if validDates == 0 {
			log.WithFields(log.Fields{
				"board": ba.board,
				"page":  page,
			}).Warn("Continue PushSum Crawl Past Page With Only Deleted Articles")
		}
	}
	if !ba.complete && pagesFetched >= pageLimit {
		log.WithFields(log.Fields{
			"board": ba.board,
			"pages": pagesFetched,
		}).Warn("PushSum Crawl Reached Page Limit Without 48-Hour Boundary")
	}

	log.WithFields(log.Fields{
		"board":    ba.board,
		"total":    len(ba.articles),
		"complete": ba.complete,
	}).Info("PushSum Crawl Finish")

	select {
	case baCh <- ba:
	case <-ctx.Done():
	}
}

func (psc pushSumChecker) checkSubscribers(ba BoardArticles) {
	psc.checkSubscribersContext(context.Background(), ba)
}

func (psc pushSumChecker) checkSubscribersContext(ctx context.Context, ba BoardArticles) {
	if !ba.complete {
		return
	}
	subs, err := listPushSumSubscribers(ba.board)
	if err != nil {
		log.WithField("board", ba.board).WithError(err).Error("Read PushSum Subscribers Failed")
		return
	}
	users, err := loadNotificationUsers(subs)
	if err != nil {
		log.WithField("board", ba.board).WithError(err).Error("Read PushSum Notification Users Failed")
		return
	}
	for _, current := range users {
		if !discordNotificationsEnabled(current) {
			continue
		}
		checker := psc
		checker.board = ba.board
		checker.Profile = current.Profile
		checker.checkPushSumContext(ctx, current, ba, checkUp)
		checker.checkPushSumContext(ctx, current, ba, checkDown)
	}
}

type checkPushSumFn func(*pushSumChecker, subscription.Subscription, article.Articles) (article.Articles, []int)

func checkUp(psc *pushSumChecker, sub subscription.Subscription, articles article.Articles) (upArticles article.Articles, ids []int) {
	psc.word = strconv.Itoa(sub.Up)
	psc.subType = "pushup"
	if sub.Up != 0 {
		for _, a := range articles {
			if a.PushSum >= sub.Up {
				upArticles = append(upArticles, a)
				ids = append(ids, a.ID)
			}
		}
	}
	return upArticles, ids
}

func checkDown(psc *pushSumChecker, sub subscription.Subscription, articles article.Articles) (downArticles article.Articles, ids []int) {
	down := sub.Down * -1
	psc.word = strconv.Itoa(down)
	psc.subType = "pushdown"
	if sub.Down != 0 {
		for _, a := range articles {
			if a.PushSum <= down {
				downArticles = append(downArticles, a)
				ids = append(ids, a.ID)
			}
		}
	}
	return downArticles, ids
}

func (psc pushSumChecker) checkPushSum(u user.User, ba BoardArticles, checkFn checkPushSumFn) {
	psc.checkPushSumContext(context.Background(), u, ba, checkFn)
}

func (psc pushSumChecker) checkPushSumContext(ctx context.Context, u user.User, ba BoardArticles, checkFn checkPushSumFn) {
	for _, sub := range u.Subscribes {
		if !strings.EqualFold(sub.Board, ba.board) {
			continue
		}
		articles, _ := checkFn(&psc, sub, ba.articles)
		if psc.pushSumRevision() == "0" {
			continue
		}
		if err := psc.enqueueAndCommitPushSum(ctx, articles); err != nil {
			log.WithFields(log.Fields{
				"account": psc.Profile.Account,
				"board":   ba.board,
				"kind":    psc.subType,
			}).WithError(err).Error("Enqueue Or Commit PushSum Cursor Failed")
			return
		}
	}
}

func (psc pushSumChecker) enqueueAndCommitPushSum(ctx context.Context, articles article.Articles) error {
	kindMap := map[string]string{
		"pushup":   "up",
		"pushdown": "down",
	}
	stateKind, ok := kindMap[psc.subType]
	if !ok {
		return fmt.Errorf("unsupported push-sum kind %q", psc.subType)
	}
	revision := psc.pushSumRevision()

	identities := make([]string, 0, len(articles))
	byIdentity := make(map[string]article.Article, len(articles))
	for _, item := range articles {
		identity := pushSumArticleIdentity(psc.board, item)
		if identity == "" {
			continue
		}
		if _, duplicate := byIdentity[identity]; duplicate {
			continue
		}
		identities = append(identities, identity)
		byIdentity[identity] = item
	}
	state, err := pendingPushSumDiff(
		psc.Profile.Account,
		psc.board,
		stateKind,
		revision,
		identities...,
	)
	if err != nil {
		return err
	}
	if state.Initialized {
		for _, identity := range state.New {
			item, exists := byIdentity[identity]
			if !exists {
				continue
			}
			event := psc
			event.articles = article.Articles{item}
			content := event.String()
			if err := enqueueStableNotification(
				ctx,
				"pushsum",
				[]string{strings.ToLower(psc.board), stateKind, revision, identity},
				content,
				true,
			); err != nil {
				return fmt.Errorf("enqueue push-sum article %s: %w", identity, err)
			}
		}
	}

	// A new threshold revision is baselined without replaying historical
	// crossings. Existing revisions advance only after every new logical event
	// has been admitted to the durable outbox.
	return commitPushSumDiff(
		psc.Profile.Account,
		psc.board,
		stateKind,
		revision,
		identities...,
	)
}

func (psc pushSumChecker) pushSumRevision() string {
	if psc.subType == "pushdown" {
		return strings.TrimPrefix(psc.word, "-")
	}
	return psc.word
}

func pushSumArticleIdentity(boardName string, item article.Article) string {
	if identity := item.Identity(); identity != "" {
		return strings.ToLower(boardName) + ":" + identity
	}
	return ""
}
