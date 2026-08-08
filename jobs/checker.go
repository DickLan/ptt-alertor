package jobs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/author"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/keyword"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

const checkHighBoardDuration = 250 * time.Millisecond

var boardCh = make(chan *board.Board, 700)
var highBoards, highBoardSet = parseHighBoards(os.Getenv("BOARD_HIGH"))

var keywordSubscribersForBoard = keyword.SubscribersE
var authorSubscribersForBoard = author.SubscribersE

func parseHighBoards(value string) ([]*board.Board, map[string]struct{}) {
	boards := make([]*board.Board, 0)
	boardSet := make(map[string]struct{})
	for _, name := range strings.Split(value, ",") {
		// Subscription commands and their Redis indexes use lower-case board
		// names. Keep priority boards in that same canonical form; otherwise a
		// value such as BOARD_HIGH=Stock is excluded from the normal worker but
		// looked up under keyword:Stock:subs by the priority worker.
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		if _, exists := boardSet[name]; exists {
			continue
		}
		bd := models.Board()
		bd.Name = name
		boards = append(boards, bd)
		boardSet[name] = struct{}{}
	}
	return boards, boardSet
}

var cker *Checker
var ckerOnce sync.Once

type Checker struct {
	board     string
	keyword   string
	author    string
	articles  article.Articles
	subType   string
	word      string
	Profile   user.Profile
	done      chan struct{}
	ch        chan Checker
	duration  time.Duration
	cycle     time.Duration
	highCycle time.Duration
}

// NewChecker gets a Checker instance
func NewChecker() *Checker {
	ckerOnce.Do(func() {
		cker = &Checker{
			duration:  250 * time.Millisecond,
			cycle:     jobDurationFromEnv("PTT_BOARD_CYCLE_INTERVAL", defaultBoardCycle),
			highCycle: jobDurationFromEnv("PTT_HIGH_BOARD_CYCLE_INTERVAL", defaultHighBoardCycle),
		}
		cker.done = make(chan struct{})
		cker.ch = make(chan Checker)
	})
	return cker
}

func (c Checker) String() string {
	subType := "關鍵字"
	if c.author != "" {
		subType = "作者"
	}
	return fmt.Sprintf("%s@%s\r\n看板：%s；%s：%s%s", c.word, c.board, c.board, subType, c.word, c.articles.String())
}

// Self return Checker itself
func (c Checker) Self() Checker {
	return c
}

// Run is main in Job
func (c Checker) Run() {
	c.RunContext(context.Background())
}

func (c Checker) RunContext(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var workers sync.WaitGroup
	// step 1: check boards which one has new articles
	// check high boards
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			if ctx.Err() != nil {
				return
			}
			started := time.Now()
			// BOARD_HIGH is only a priority hint, never an instruction to poll an
			// otherwise unsubscribed board forever.
			checkBoards(ctx, intersectHighBoards(highBoards, models.Board().All()), checkHighBoardDuration)
			if !waitForNextCycle(ctx, started, c.highCycle) {
				return
			}
		}
	}()

	// check off peak
	offPeakCh := make(chan bool)
	workers.Add(1)
	go func() {
		defer workers.Done()
		c.checkOffPeak(ctx, offPeakCh)
	}()

	// check normal boards, slow when off peak
	workers.Add(1)
	go func() {
		defer workers.Done()
		var offPeak bool
		duration := c.duration
		for {
			select {
			case <-ctx.Done():
				for len(offPeakCh) > 0 {
					<-offPeakCh
				}
				return
			case op := <-offPeakCh:
				if offPeak != op {
					if op {
						log.Info("Switch to Slow Mode")
						duration = c.duration * 2
					} else {
						log.Info("Switch to Normal Mode")
						duration = c.duration
					}
					offPeak = op
				}
			default:
				started := time.Now()
				checkBoards(ctx, excludeHighBoards(models.Board().All()), duration)
				if !waitForNextCycle(ctx, started, c.cycle) {
					return
				}
			}
		}
	}()

	// main
	for {
		select {
		//step 2: check user who subscribes board
		case bd := <-boardCh:
			workers.Add(2)
			go func() {
				defer workers.Done()
				checkKeywordSubscriber(ctx, bd, c)
			}()
			go func() {
				defer workers.Done()
				checkAuthorSubscriber(ctx, bd, c)
			}()
		//step 3: send notification
		case cker := <-c.ch:
			queueCheck(ctx, cker)
		case <-c.done:
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

func excludeHighBoards(boards []*board.Board) (normalBoards []*board.Board) {
	for _, bd := range boards {
		if _, high := highBoardSet[strings.ToLower(bd.Name)]; high {
			continue
		}
		normalBoards = append(normalBoards, bd)
	}
	return normalBoards
}

func intersectHighBoards(configured, subscribed []*board.Board) []*board.Board {
	active := make(map[string]struct{}, len(subscribed))
	for _, bd := range subscribed {
		if bd != nil {
			active[strings.ToLower(strings.TrimSpace(bd.Name))] = struct{}{}
		}
	}
	result := make([]*board.Board, 0, len(configured))
	for _, bd := range configured {
		if bd == nil {
			continue
		}
		if _, ok := active[strings.ToLower(strings.TrimSpace(bd.Name))]; ok {
			result = append(result, bd)
		}
	}
	return result
}

func (c Checker) checkOffPeak(ctx context.Context, offPeakCh chan<- bool) {
	loc := time.FixedZone("CST", 8*60*60)
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t := now.In(loc)
			if t.Hour() >= 3 && t.Hour() < 7 {
				select {
				case offPeakCh <- true:
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case offPeakCh <- false:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (c Checker) Stop() {
	c.done <- struct{}{}
	log.Info("Checker Stop")
}

func checkBoards(ctx context.Context, bds []*board.Board, duration time.Duration) {
	if len(bds) == 0 {
		waitForContext(ctx, time.Second)
		return
	}
	if !notificationPollingAllowed(ctx) {
		return
	}
	for _, bd := range bds {
		if !waitForContext(ctx, duration) {
			return
		}
		checkNewArticle(ctx, bd, boardCh)
	}
}

func checkNewArticle(ctx context.Context, bd *board.Board, _ chan *board.Board) {
	if err := bd.WithNewArticlesContext(ctx); err != nil {
		log.WithField("board", bd.Name).WithError(err).Warn("Skip Board Cursor After Fetch Or Storage Error")
		return
	}
	if bd.NewArticles == nil && len(bd.OnlineArticles) > 0 {
		bd.Articles = bd.OnlineArticles
		log.WithField("board", bd.Name).Info("Created Articles")
		if err := bd.Save(); err != nil {
			log.WithField("board", bd.Name).WithError(err).Error("Save Initial Board Cursor Failed")
		}
		return
	}
	if len(bd.NewArticles) != 0 {
		// Every matching logical event must exist durably before the board
		// snapshot advances. A partial enqueue is safe: deterministic event IDs
		// make the next polling pass idempotent.
		if err := enqueueBoardNotifications(ctx, bd); err != nil {
			log.WithField("board", bd.Name).WithError(err).Warn("Skip Board Cursor After Outbox Error")
			return
		}
		bd.Articles = bd.OnlineArticles
		if err := bd.Save(); err != nil {
			log.WithField("board", bd.Name).WithError(err).Error("Save Updated Board Cursor Failed")
			return
		}
		log.WithFields(log.Fields{
			"board": bd.Name,
			"count": len(bd.NewArticles),
		}).Info("Updated Articles")
		return
	}
	log.WithFields(log.Fields{
		"board":    bd.Name,
		"observed": len(bd.OnlineArticles),
	}).Info("Checked Articles")
}

func enqueueBoardNotifications(ctx context.Context, bd *board.Board) error {
	keywordAccounts, err := keywordSubscribersForBoard(bd.Name)
	if err != nil {
		return fmt.Errorf("read keyword subscribers: %w", err)
	}
	authorAccounts, err := authorSubscribersForBoard(bd.Name)
	if err != nil {
		return fmt.Errorf("read author subscribers: %w", err)
	}
	users, err := loadNotificationUsers(append(keywordAccounts, authorAccounts...))
	if err != nil {
		return fmt.Errorf("read notification user: %w", err)
	}

	matched := make(map[string]struct{}, len(bd.NewArticles))
	for _, current := range users {
		if !discordNotificationsEnabled(current) {
			continue
		}
		for _, sub := range current.Subscribes {
			if !strings.EqualFold(sub.Board, bd.Name) {
				continue
			}
			for _, candidate := range bd.NewArticles {
				identity := candidate.Identity()
				if identity == "" {
					continue
				}
				if articleMatchesSubscription(candidate, sub.Keywords, sub.Authors) {
					matched[identity] = struct{}{}
				}
			}
		}
	}

	seen := make(map[string]struct{}, len(matched))
	for _, candidate := range bd.NewArticles {
		identity := candidate.Identity()
		if _, ok := matched[identity]; !ok {
			continue
		}
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		content := fmt.Sprintf("新文章@%s\n\n%s", bd.Name, candidate.String())
		if err := enqueueStableNotification(
			ctx,
			"article",
			[]string{strings.ToLower(bd.Name), identity},
			content,
			true,
		); err != nil {
			return fmt.Errorf("enqueue article %s: %w", identity, err)
		}
	}
	return nil
}

func articleMatchesSubscription(candidate article.Article, keywords, authors []string) bool {
	for _, value := range keywords {
		if candidate.MatchKeyword(value) {
			return true
		}
	}
	for _, value := range authors {
		if candidate.MatchAuthor(value) {
			return true
		}
	}
	return false
}

func checkKeywordSubscriber(ctx context.Context, bd *board.Board, cker Checker) {
	u := models.User()
	accounts := keyword.Subscribers(bd.Name)
	for _, account := range accounts {
		user := u.Find(account)
		if discordNotificationsEnabled(user) {
			cker.Profile = user.Profile
			checkKeywordSubscription(ctx, user, bd, cker)
		}
	}
}

func checkKeywordSubscription(ctx context.Context, user user.User, bd *board.Board, cker Checker) {
	for _, sub := range user.Subscribes {
		if strings.EqualFold(bd.Name, sub.Board) {
			cker.board = sub.Board
			for _, keyword := range sub.Keywords {
				checkKeyword(ctx, keyword, bd, cker)
			}
		}
	}
}

func checkKeyword(ctx context.Context, keyword string, bd *board.Board, cker Checker) {
	keywordArticles := make(article.Articles, 0)
	for _, newAtcl := range bd.NewArticles {
		if newAtcl.MatchKeyword(keyword) {
			newAtcl.Author = ""
			keywordArticles = append(keywordArticles, newAtcl)
		}
	}
	if len(keywordArticles) != 0 {
		cker.keyword = keyword
		cker.articles = keywordArticles
		cker.subType = "keyword"
		cker.word = keyword
		select {
		case cker.ch <- cker:
		case <-ctx.Done():
		}
	}
}

func checkAuthorSubscriber(ctx context.Context, bd *board.Board, cker Checker) {
	u := models.User()
	accounts := author.Subscribers(bd.Name)
	for _, account := range accounts {
		user := u.Find(account)
		if discordNotificationsEnabled(user) {
			cker.Profile = user.Profile
			checkAuthorSubscription(ctx, user, bd, cker)
		}
	}
}

func checkAuthorSubscription(ctx context.Context, user user.User, bd *board.Board, cker Checker) {
	for _, sub := range user.Subscribes {
		if strings.EqualFold(bd.Name, sub.Board) {
			cker.board = sub.Board
			for _, author := range sub.Authors {
				checkAuthor(ctx, author, bd, cker)
			}
		}
	}
}

func checkAuthor(ctx context.Context, author string, bd *board.Board, cker Checker) {
	authorArticles := make(article.Articles, 0)
	for _, newAtcl := range bd.NewArticles {
		if newAtcl.MatchAuthor(author) {
			authorArticles = append(authorArticles, newAtcl)
		}
	}
	if len(authorArticles) != 0 {
		cker.author = author
		cker.articles = authorArticles
		cker.subType = "author"
		cker.word = author
		select {
		case cker.ch <- cker:
		case <-ctx.Done():
		}
	}
}

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
