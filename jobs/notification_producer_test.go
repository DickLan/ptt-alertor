package jobs

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/commentcursor"
	"github.com/Ptt-Alertor/ptt-alertor/models/pushsum"
	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

type recordedNotification struct {
	kind    string
	parts   []string
	content string
}

func TestDelayedArticleNotice(t *testing.T) {
	publishedAt := time.Date(2026, time.September, 16, 23, 10, 15, 0, time.Local)
	candidate := article.Article{
		ID:    int(publishedAt.Unix()),
		Code:  "M.1789571415.A.EAD",
		Title: "[賣] 4070ti",
	}

	notice := delayedArticleNotice(candidate, true)
	for _, expected := range []string{"⚠️ 延遲補發", "PTT 恢復後取得累積文章", "2026-09-16 23:10", "PTT 暫時無法存取"} {
		if !strings.Contains(notice, expected) {
			t.Errorf("notice = %q, want %q", notice, expected)
		}
	}
	if got := delayedArticleNotice(candidate, false); got != "" {
		t.Errorf("ordinary article notice = %q, want empty", got)
	}
	if got := delayedArticleNotice(article.Article{Title: "missing timestamp"}, true); !strings.Contains(got, "⚠️ 延遲補發") {
		t.Errorf("timestamp-free delayed notice = %q, want warning", got)
	}
}

func TestIsDelayedArticleBatch(t *testing.T) {
	base := time.Date(2026, time.September, 16, 21, 59, 34, 0, time.Local)
	articleAt := func(offset time.Duration) article.Article {
		return article.Article{ID: int(base.Add(offset).Unix())}
	}
	if !isDelayedArticleBatch(article.Articles{articleAt(0), articleAt(71 * time.Minute)}, 10*time.Minute) {
		t.Fatal("wide catch-up batch was not marked delayed")
	}
	if isDelayedArticleBatch(article.Articles{articleAt(0), articleAt(2 * time.Minute)}, 10*time.Minute) {
		t.Fatal("ordinary polling batch was marked delayed")
	}
	if isDelayedArticleBatch(article.Articles{articleAt(0)}, 10*time.Minute) {
		t.Fatal("single article was marked as a delayed batch")
	}
}

func TestBoardProducerEnqueuesSameSecondArticlesBeforeCursorCanAdvance(t *testing.T) {
	originalKeyword := keywordSubscribersForBoard
	originalAuthor := authorSubscribersForBoard
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	defer func() {
		keywordSubscribersForBoard = originalKeyword
		authorSubscribersForBoard = originalAuthor
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
	}()

	keywordSubscribersForBoard = func(string) ([]string, error) { return []string{"subscriber"}, nil }
	authorSubscribersForBoard = func(string) ([]string, error) { return []string{"subscriber"}, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{
			Enable:  true,
			Profile: user.Profile{Account: "subscriber", Discord: true},
			Subscribes: subscription.Subscriptions{{
				Board:    "NBA",
				Keywords: []string{"trade"},
				Authors:  []string{"alice"},
			}},
		}, nil
	}
	var notifications []recordedNotification
	enqueueNotification = func(_ context.Context, kind string, parts []string, content string, _ bool) error {
		notifications = append(notifications, recordedNotification{kind: kind, parts: append([]string(nil), parts...), content: content})
		return nil
	}

	bd := &board.Board{Name: "NBA", NewArticles: article.Articles{
		{ID: 1700000000, Code: "M.1700000000.A.AAA", Title: "trade news", Link: "https://www.ptt.cc/bbs/NBA/M.1700000000.A.AAA.html", Author: "bob"},
		{ID: 1700000000, Code: "M.1700000000.A.BBB", Title: "other", Link: "https://www.ptt.cc/bbs/NBA/M.1700000000.A.BBB.html", Author: "Alice"},
	}}
	if err := enqueueBoardNotifications(context.Background(), bd); err != nil {
		t.Fatalf("enqueueBoardNotifications() error = %v", err)
	}
	if len(notifications) != 2 {
		t.Fatalf("notifications = %d, want both same-second articles", len(notifications))
	}
	if notifications[0].parts[1] == notifications[1].parts[1] {
		t.Fatalf("full article identities collided: %#v", notifications)
	}
	for _, notification := range notifications {
		if notification.kind != "article" || !strings.Contains(notification.content, "https://www.ptt.cc/") {
			t.Fatalf("unexpected notification = %#v", notification)
		}
	}
}

func TestBoardProducerEnqueuesOnlyCompleteAuthorAndTitleMatch(t *testing.T) {
	originalKeyword := keywordSubscribersForBoard
	originalAuthor := authorSubscribersForBoard
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	defer func() {
		keywordSubscribersForBoard = originalKeyword
		authorSubscribersForBoard = originalAuthor
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
	}()

	keywordSubscribersForBoard = func(string) ([]string, error) { return []string{"subscriber"}, nil }
	// The compound rule is indexed only as a keyword; no author index lookup is
	// required for it to reach the durable producer.
	authorSubscribersForBoard = func(string) ([]string, error) { return nil, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{
			Enable:  true,
			Profile: user.Profile{Account: "subscriber", Discord: true},
			Subscribes: subscription.Subscriptions{{
				Board:    "HardwareSale",
				Keywords: []string{"author:lushin&賣"},
			}},
		}, nil
	}
	var notifications []recordedNotification
	enqueueNotification = func(_ context.Context, kind string, parts []string, content string, _ bool) error {
		notifications = append(notifications, recordedNotification{
			kind: kind, parts: append([]string(nil), parts...), content: content,
		})
		return nil
	}

	bd := &board.Board{Name: "HardwareSale", NewArticles: article.Articles{
		{Code: "M.1.A.001", Title: "[賣] DDR4", Author: "LuShIn", Link: "https://www.ptt.cc/bbs/HardwareSale/M.1.A.001.html"},
		{Code: "M.2.A.002", Title: "[徵] DDR4", Author: "lushin", Link: "https://www.ptt.cc/bbs/HardwareSale/M.2.A.002.html"},
		{Code: "M.3.A.003", Title: "[賣] DDR5", Author: "other", Link: "https://www.ptt.cc/bbs/HardwareSale/M.3.A.003.html"},
	}}
	if err := enqueueBoardNotifications(context.Background(), bd); err != nil {
		t.Fatalf("enqueueBoardNotifications() error = %v", err)
	}
	if len(notifications) != 1 {
		t.Fatalf("notifications = %#v, want exactly one complete match", notifications)
	}
	if notifications[0].kind != "article" || !strings.Contains(notifications[0].content, "M.1.A.001") {
		t.Fatalf("notification = %#v, want the both-match article", notifications[0])
	}
}

func TestBoardProducerPropagatesSubscriberAndOutboxFailures(t *testing.T) {
	originalKeyword := keywordSubscribersForBoard
	originalAuthor := authorSubscribersForBoard
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	defer func() {
		keywordSubscribersForBoard = originalKeyword
		authorSubscribersForBoard = originalAuthor
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
	}()

	wantStorageErr := errors.New("redis unavailable")
	keywordSubscribersForBoard = func(string) ([]string, error) { return nil, wantStorageErr }
	authorSubscribersForBoard = func(string) ([]string, error) { return nil, nil }
	bd := &board.Board{Name: "NBA", NewArticles: article.Articles{{Code: "M.1.A.AAA"}}}
	if err := enqueueBoardNotifications(context.Background(), bd); !errors.Is(err, wantStorageErr) {
		t.Fatalf("subscriber error = %v, want %v", err, wantStorageErr)
	}

	keywordSubscribersForBoard = func(string) ([]string, error) { return []string{"subscriber"}, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{
			Enable:     true,
			Profile:    user.Profile{Account: "subscriber", Discord: true},
			Subscribes: subscription.Subscriptions{{Board: "NBA", Keywords: []string{"regexp:.*"}}},
		}, nil
	}
	wantOutboxErr := errors.New("outbox full")
	enqueueNotification = func(context.Context, string, []string, string, bool) error { return wantOutboxErr }
	if err := enqueueBoardNotifications(context.Background(), bd); !errors.Is(err, wantOutboxErr) {
		t.Fatalf("outbox error = %v, want %v", err, wantOutboxErr)
	}
}

func TestPushSumProducerCommitsOnlyAfterEveryOutboxWrite(t *testing.T) {
	originalPending := pendingPushSumDiff
	originalCommit := commitPushSumDiff
	originalEnqueue := enqueueNotification
	defer func() {
		pendingPushSumDiff = originalPending
		commitPushSumDiff = originalCommit
		enqueueNotification = originalEnqueue
	}()

	items := article.Articles{
		{Code: "M.1.A.AAA", Title: "first", Link: "https://www.ptt.cc/bbs/NBA/M.1.A.AAA.html", PushSum: 51},
		{Code: "M.2.A.BBB", Title: "second", Link: "https://www.ptt.cc/bbs/NBA/M.2.A.BBB.html", PushSum: 55},
	}
	identities := []string{"nba:M.1.A.AAA", "nba:M.2.A.BBB"}
	pendingPushSumDiff = func(string, string, string, string, ...string) (pushsum.DiffState, error) {
		return pushsum.DiffState{Initialized: true, New: identities}, nil
	}
	commits := 0
	commitPushSumDiff = func(string, string, string, string, ...string) error {
		commits++
		return nil
	}
	psc := pushSumChecker{Checker: Checker{
		board:   "NBA",
		subType: "pushup",
		word:    "50",
		Profile: user.Profile{Account: "subscriber", Discord: true},
	}}
	wantErr := errors.New("enqueue failed")
	enqueueCalls := 0
	enqueueNotification = func(context.Context, string, []string, string, bool) error {
		enqueueCalls++
		if enqueueCalls == 2 {
			return wantErr
		}
		return nil
	}
	if err := psc.enqueueAndCommitPushSum(context.Background(), items); !errors.Is(err, wantErr) {
		t.Fatalf("enqueueAndCommitPushSum() error = %v, want %v", err, wantErr)
	}
	if commits != 0 {
		t.Fatalf("commits = %d, want none after partial enqueue", commits)
	}

	enqueueNotification = func(context.Context, string, []string, string, bool) error { return nil }
	if err := psc.enqueueAndCommitPushSum(context.Background(), items); err != nil {
		t.Fatalf("successful enqueueAndCommitPushSum() error = %v", err)
	}
	if commits != 1 {
		t.Fatalf("commits = %d, want one after all outbox writes", commits)
	}
}

func TestPushSumDownRevisionMatchesPositiveSubscriptionThreshold(t *testing.T) {
	checker := pushSumChecker{Checker: Checker{subType: "pushdown", word: "-20"}}
	if got := checker.pushSumRevision(); got != "20" {
		t.Fatalf("down revision = %q, want positive configured threshold 20", got)
	}
}

func TestPushSumProducerBaselinesEmptyThenEnqueuesFirstCrossing(t *testing.T) {
	originalPending := pendingPushSumDiff
	originalCommit := commitPushSumDiff
	originalEnqueue := enqueueNotification
	defer func() {
		pendingPushSumDiff = originalPending
		commitPushSumDiff = originalCommit
		enqueueNotification = originalEnqueue
	}()

	initialized := false
	known := make(map[string]struct{})
	pendingPushSumDiff = func(_, _, _, _ string, identities ...string) (pushsum.DiffState, error) {
		state := pushsum.DiffState{Initialized: initialized}
		if initialized {
			for _, identity := range identities {
				if _, exists := known[identity]; !exists {
					state.New = append(state.New, identity)
				}
			}
		}
		return state, nil
	}
	commits := 0
	commitPushSumDiff = func(_, _, _, _ string, identities ...string) error {
		commits++
		initialized = true
		for _, identity := range identities {
			known[identity] = struct{}{}
		}
		return nil
	}
	enqueues := 0
	enqueueNotification = func(context.Context, string, []string, string, bool) error {
		enqueues++
		return nil
	}

	currentUser := user.User{
		Enable:  true,
		Profile: user.Profile{Account: "subscriber", Discord: true},
		Subscribes: subscription.Subscriptions{{
			Board:   "NBA",
			PushSum: subscription.PushSum{Up: 50},
		}},
	}
	checker := pushSumChecker{Checker: Checker{Profile: currentUser.Profile}}
	checker.checkPushSumContext(context.Background(), currentUser, BoardArticles{board: "NBA", complete: true}, checkUp)
	if !initialized || commits != 1 || enqueues != 0 {
		t.Fatalf("empty baseline: initialized=%t commits=%d enqueues=%d", initialized, commits, enqueues)
	}

	crossing := article.Article{
		ID: 1, Code: "M.1.A.AAA", Title: "crossed",
		Link: "https://www.ptt.cc/bbs/NBA/M.1.A.AAA.html", PushSum: 50,
	}
	checker.checkPushSumContext(
		context.Background(),
		currentUser,
		BoardArticles{board: "NBA", articles: article.Articles{crossing}, complete: true},
		checkUp,
	)
	if commits != 2 || enqueues != 1 {
		t.Fatalf("first crossing: commits=%d enqueues=%d, want 2 and 1", commits, enqueues)
	}
}

type memoryArticleDriver struct {
	stored article.Article
	saves  int
}

func (driver *memoryArticleDriver) Find(_ string, target *article.Article) {
	target.ID = driver.stored.ID
	target.Code = driver.stored.Code
	target.Title = driver.stored.Title
	target.Link = driver.stored.Link
	target.Date = driver.stored.Date
	target.Author = driver.stored.Author
	target.Comments = append(article.Comments(nil), driver.stored.Comments...)
	target.LastPushDateTime = driver.stored.LastPushDateTime
	target.Board = driver.stored.Board
	target.PushSum = driver.stored.PushSum
}

func (driver *memoryArticleDriver) Save(item article.Article) error {
	driver.saves++
	driver.stored = item
	return nil
}

func (driver *memoryArticleDriver) Delete(string) error { return nil }

type memoryCommentCursor struct {
	mu      sync.Mutex
	state   commentcursor.State
	saves   int
	loadErr error
	pending *commentcursor.Pending
}

func TestCommentCheckerRejectsBoardOutsideAllowlistBeforeFetch(t *testing.T) {
	t.Setenv("BOARD_ALLOWLIST", "HardwareSale,MacShop,PC_Shopping")
	originalArticleFactory := models.Article
	originalFetch := fetchCommentArticle
	defer func() {
		models.Article = originalArticleFactory
		fetchCommentArticle = originalFetch
	}()

	driver := &memoryArticleDriver{stored: article.Article{Code: "M.1.A.AAA", Board: "Stock"}}
	models.Article = func() *article.Article { return article.NewArticle(driver) }
	fetchCalls := 0
	fetchCommentArticle = func(context.Context, string, string) (article.Article, error) {
		fetchCalls++
		return article.Article{}, nil
	}

	commentChecker{}.checkCommentsContext(context.Background(), driver.stored.Code, nil)
	if fetchCalls != 0 {
		t.Fatalf("PTT comment fetch calls = %d, want 0", fetchCalls)
	}
}

func (cursor *memoryCommentCursor) Load(string) (commentcursor.State, error) {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.loadErr != nil {
		return commentcursor.State{}, cursor.loadErr
	}
	return cursor.state, nil
}
func (cursor *memoryCommentCursor) Initialize(_ string, state commentcursor.State, allowLegacy bool) error {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.pending != nil {
		return commentcursor.ErrTransitionConflict
	}
	if cursor.loadErr == nil {
		if sameCommentCursorState(cursor.state, state) {
			return nil
		}
		return commentcursor.ErrTransitionConflict
	}
	if !errors.Is(cursor.loadErr, commentcursor.ErrStateNotFound) &&
		!(allowLegacy && errors.Is(cursor.loadErr, commentcursor.ErrStateUpgradeNeeded)) {
		return commentcursor.ErrTransitionConflict
	}
	cursor.saves++
	cursor.state = state
	cursor.loadErr = nil
	return nil
}
func (cursor *memoryCommentCursor) Reset(_ string, state commentcursor.State) error {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	cursor.pending = nil
	cursor.saves++
	cursor.state = state
	cursor.loadErr = nil
	return nil
}
func (cursor *memoryCommentCursor) Advance(_ string, source, next commentcursor.State) error {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.pending != nil {
		return commentcursor.ErrTransitionConflict
	}
	if sameCommentCursorState(cursor.state, next) {
		return nil
	}
	if cursor.loadErr != nil || !sameCommentCursorState(cursor.state, source) {
		return commentcursor.ErrTransitionConflict
	}
	cursor.saves++
	cursor.state = next
	return nil
}
func (cursor *memoryCommentCursor) LoadPending(string) (commentcursor.Pending, error) {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.pending == nil {
		return commentcursor.Pending{}, commentcursor.ErrPendingNotFound
	}
	return *cursor.pending, nil
}
func (cursor *memoryCommentCursor) StagePending(_ string, source commentcursor.State, pending commentcursor.Pending) error {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if cursor.loadErr != nil || !sameCommentCursorState(cursor.state, source) {
		return commentcursor.ErrTransitionConflict
	}
	if cursor.pending != nil {
		if reflect.DeepEqual(*cursor.pending, pending) {
			return nil
		}
		return commentcursor.ErrTransitionConflict
	}
	copy := pending
	cursor.pending = &copy
	return nil
}
func (cursor *memoryCommentCursor) CommitPending(_ string, source commentcursor.State, pending commentcursor.Pending) error {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	if sameCommentCursorState(cursor.state, pending.Next) {
		if cursor.pending == nil {
			return nil
		}
		if !reflect.DeepEqual(*cursor.pending, pending) {
			return commentcursor.ErrTransitionConflict
		}
		cursor.pending = nil
		return nil
	}
	if cursor.loadErr != nil || !sameCommentCursorState(cursor.state, source) ||
		cursor.pending == nil || !reflect.DeepEqual(*cursor.pending, pending) {
		return commentcursor.ErrTransitionConflict
	}
	cursor.saves++
	cursor.state = pending.Next
	cursor.pending = nil
	return nil
}

func sameCommentCursorState(left, right commentcursor.State) bool {
	return left.Version == right.Version && left.Epoch == right.Epoch &&
		left.Revision == right.Revision && reflect.DeepEqual(left.Snapshot, right.Snapshot)
}

func TestCommentProducerDoesNotAdvanceArticleOrCursorAfterOutboxFailure(t *testing.T) {
	originalArticleFactory := models.Article
	originalFetch := fetchCommentArticle
	originalSubscribers := commentArticleSubscribers
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	originalCursors := commentCursors
	defer func() {
		models.Article = originalArticleFactory
		fetchCommentArticle = originalFetch
		commentArticleSubscribers = originalSubscribers
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
		commentCursors = originalCursors
	}()

	minute := time.Date(2026, time.July, 13, 12, 34, 0, 0, time.FixedZone("CST", 8*60*60))
	first := article.Comment{Tag: "推", UserID: "same: ", Content: "hello", DateTime: minute}
	second := first
	driver := &memoryArticleDriver{stored: article.Article{
		Code: "M.1.A.AAA", Board: "NBA", Title: "title",
		Link: "https://www.ptt.cc/bbs/NBA/M.1.A.AAA.html", Comments: article.Comments{first},
	}}
	cursor := &memoryCommentCursor{state: newCommentCursorState(t, driver.stored.Comments)}
	models.Article = func() *article.Article { return article.NewArticle(driver) }
	fetchCommentArticle = func(context.Context, string, string) (article.Article, error) {
		current := driver.stored
		current.Comments = article.Comments{first, second}
		current.LastPushDateTime = minute
		return current, nil
	}
	commentArticleSubscribers = func(article.Article) ([]string, error) { return []string{"subscriber"}, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{Enable: true, Profile: user.Profile{Account: "subscriber", Discord: true}}, nil
	}
	commentCursors = cursor
	wantErr := errors.New("outbox unavailable")
	enqueueNotification = func(context.Context, string, []string, string, bool) error { return wantErr }

	commentChecker{}.checkCommentsContext(context.Background(), driver.stored.Code, nil)
	if driver.saves != 0 || cursor.saves != 0 {
		t.Fatalf("source advanced after enqueue failure: article saves=%d cursor saves=%d", driver.saves, cursor.saves)
	}

	enqueued := 0
	enqueueNotification = func(_ context.Context, kind string, parts []string, _ string, _ bool) error {
		enqueued++
		if kind != "comment" || len(parts) != 6 || parts[2] != cursor.state.Epoch {
			t.Fatalf("comment event identity = %#v, kind=%q", parts, kind)
		}
		return nil
	}
	commentChecker{}.checkCommentsContext(context.Background(), driver.stored.Code, nil)
	if enqueued != 1 || driver.saves != 1 || cursor.saves != 1 {
		t.Fatalf("success counts: enqueue=%d article saves=%d cursor saves=%d; want 1 each", enqueued, driver.saves, cursor.saves)
	}
}

func TestCommentProducerNamespacesRenumberedOccurrenceByCursorTransition(t *testing.T) {
	originalArticleFactory := models.Article
	originalFetch := fetchCommentArticle
	originalSubscribers := commentArticleSubscribers
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	originalCursors := commentCursors
	defer func() {
		models.Article = originalArticleFactory
		fetchCommentArticle = originalFetch
		commentArticleSubscribers = originalSubscribers
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
		commentCursors = originalCursors
	}()

	minute := time.Date(2026, time.July, 13, 13, 0, 0, 0, time.UTC)
	a := article.Comment{Tag: "推 ", UserID: "same", Content: ": repeated", DateTime: minute}
	b := article.Comment{Tag: "推 ", UserID: "anchor", Content: ": anchor", DateTime: minute.Add(time.Minute)}
	code := "M.2.A.BBB"
	driver := &memoryArticleDriver{stored: article.Article{
		Code: code, Board: "NBA", Title: "title",
		Link:     "https://www.ptt.cc/bbs/NBA/" + code + ".html",
		Comments: article.Comments{a, a, b},
	}}
	cursor := &memoryCommentCursor{state: newCommentCursorState(t, driver.stored.Comments)}
	models.Article = func() *article.Article { return article.NewArticle(driver) }
	fetchCommentArticle = func(context.Context, string, string) (article.Article, error) {
		current := driver.stored
		// The first identical A was deleted and another identical A was
		// appended, so PTT reuses occurrence ordinal A:2.
		current.Comments = article.Comments{a, b, a}
		return current, nil
	}
	commentArticleSubscribers = func(article.Article) ([]string, error) { return []string{"subscriber"}, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{Enable: true, Profile: user.Profile{Account: "subscriber", Discord: true}}, nil
	}
	commentCursors = cursor

	legacyContent := "推文@NBA\n\ntitle\nhttps://www.ptt.cc/bbs/NBA/" + code + ".html\n\n" + a.String()
	legacyDoneIdentity := notificationCanonicalParts(
		[]string{"nba", code, string(commentcursor.OccurrenceIDs(article.Comments{a})[0])},
		legacyContent,
	)
	enqueues := 0
	enqueueNotification = func(_ context.Context, kind string, parts []string, content string, _ bool) error {
		enqueues++
		if kind != "comment" || content != legacyContent {
			t.Fatalf("unexpected event kind=%q content=%q", kind, content)
		}
		if strings.Join(parts, "|") == strings.Join(legacyDoneIdentity, "|") {
			t.Fatalf("renumbered occurrence reused legacy done identity: %#v", parts)
		}
		if len(parts) != 6 || parts[2] != cursor.state.Epoch || parts[3] != cursor.state.Revision {
			t.Fatalf("transition identity = %#v", parts)
		}
		return nil
	}

	commentChecker{}.checkCommentsContext(context.Background(), code, nil)
	if enqueues != 1 || driver.saves != 1 || cursor.saves != 1 {
		t.Fatalf("success counts: enqueue=%d article saves=%d cursor saves=%d; want 1 each", enqueues, driver.saves, cursor.saves)
	}
}

func newCommentCursorState(t *testing.T, comments article.Comments) commentcursor.State {
	t.Helper()
	state, err := commentcursor.NewState(comments)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCommentProducerMissingEmptyCursorDoesNotAbsorbCurrentComments(t *testing.T) {
	originalArticleFactory := models.Article
	originalFetch := fetchCommentArticle
	originalSubscribers := commentArticleSubscribers
	originalFindUser := findNotificationUser
	originalEnqueue := enqueueNotification
	originalCursors := commentCursors
	defer func() {
		models.Article = originalArticleFactory
		fetchCommentArticle = originalFetch
		commentArticleSubscribers = originalSubscribers
		findNotificationUser = originalFindUser
		enqueueNotification = originalEnqueue
		commentCursors = originalCursors
	}()

	code := "M.3.A.CCC"
	newComment := article.Comment{
		Tag: "推 ", UserID: "new", Content: ": must notify",
		DateTime: time.Date(2026, time.July, 13, 14, 0, 0, 0, time.UTC),
	}
	driver := &memoryArticleDriver{stored: article.Article{
		Code: code, Board: "NBA", Title: "title",
		Link:     "https://www.ptt.cc/bbs/NBA/" + code + ".html",
		Comments: make(article.Comments, 0),
	}}
	cursor := &memoryCommentCursor{loadErr: commentcursor.ErrStateNotFound}
	models.Article = func() *article.Article { return article.NewArticle(driver) }
	fetchCommentArticle = func(context.Context, string, string) (article.Article, error) {
		current := driver.stored
		current.Comments = article.Comments{newComment}
		return current, nil
	}
	commentArticleSubscribers = func(article.Article) ([]string, error) { return []string{"subscriber"}, nil }
	findNotificationUser = func(string) (user.User, error) {
		return user.User{Enable: true, Profile: user.Profile{Account: "subscriber", Discord: true}}, nil
	}
	commentCursors = cursor
	enqueues := 0
	enqueueNotification = func(context.Context, string, []string, string, bool) error {
		enqueues++
		return nil
	}

	commentChecker{}.checkCommentsContext(context.Background(), code, nil)
	if enqueues != 1 || driver.saves != 1 || cursor.saves != 2 {
		t.Fatalf("missing cursor counts: enqueue=%d article saves=%d cursor saves=%d; want 1, 1, 2", enqueues, driver.saves, cursor.saves)
	}
}
