package board

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/myutil/maputil"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/rss"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

type BoardNotExistError struct {
	Suggestion string
}

func (e BoardNotExistError) Error() string {
	return "board is not exist"
}

type Driver interface {
	GetArticles(boardName string) article.Articles
	Save(boardName string, articles article.Articles) error
	Delete(boardName string) error
}

type errorArticleDriver interface {
	GetArticlesE(boardName string) (article.Articles, bool, error)
}

type Cacher interface {
	List() []string
	Create(boardName string) error
	Exist(boardName string) bool
	Remove(boardName string) error
}

const (
	defaultCatchUpMaxPages = 20
	catchUpMaxPagesEnv     = "PTT_CATCHUP_MAX_PAGES"
)

// ErrCatchUpPageLimit means that the saved board boundary could not be found
// within the configured HTML history window. Callers must not advance the
// board cursor after this error, otherwise articles beyond the Atom window can
// be silently skipped.
var ErrCatchUpPageLimit = errors.New("PTT board catch-up page limit reached")

type articleSource interface {
	FetchAtom(context.Context, string) (article.Articles, error)
	FetchHTML(context.Context, string, int) (article.Articles, error)
	CurrentPage(context.Context, string) (int, error)
}

type pttArticleSource struct{}

func (pttArticleSource) FetchAtom(ctx context.Context, boardName string) (article.Articles, error) {
	return rss.BuildArticlesContext(ctx, boardName)
}

func (pttArticleSource) FetchHTML(ctx context.Context, boardName string, page int) (article.Articles, error) {
	return web.FetchArticlesContext(ctx, boardName, page)
}

func (pttArticleSource) CurrentPage(ctx context.Context, boardName string) (int, error) {
	return web.CurrentPageContext(ctx, boardName)
}

type Board struct {
	Name            string
	Articles        article.Articles
	OnlineArticles  article.Articles
	NewArticles     article.Articles
	driver          Driver
	cacher          Cacher
	articleSource   articleSource
	catchUpMaxPages int
}

func NewBoard(drive Driver, cache Cacher) *Board {
	return &Board{
		driver:          drive,
		cacher:          cache,
		articleSource:   pttArticleSource{},
		catchUpMaxPages: configuredCatchUpMaxPages(),
	}
}

func configuredCatchUpMaxPages() int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(catchUpMaxPagesEnv)))
	if err != nil || value <= 0 {
		return defaultCatchUpMaxPages
	}
	return value
}

func (bd Board) source() articleSource {
	if bd.articleSource != nil {
		return bd.articleSource
	}
	return pttArticleSource{}
}

func (bd Board) catchUpPageLimit() int {
	if bd.catchUpMaxPages > 0 {
		return bd.catchUpMaxPages
	}
	return configuredCatchUpMaxPages()
}

func (bd Board) List() []string {
	return bd.cacher.List()
}

func (bd Board) Exist() bool {
	return bd.cacher.Exist(bd.Name)
}

func (bd Board) All() (bds []*Board) {
	boards := bd.List()
	for _, board := range boards {
		bd := NewBoard(bd.driver, bd.cacher)
		bd.Name = board
		bds = append(bds, bd)
	}
	return bds
}

func (bd Board) GetArticles() (articles article.Articles) {
	return bd.driver.GetArticles(bd.Name)
}

func (bd Board) Create() error {
	return bd.cacher.Create(bd.Name)
}

func (bd Board) Save() error {
	return bd.driver.Save(bd.Name, bd.Articles)
}

func (bd Board) Delete() error {
	if err := bd.driver.Delete(bd.Name); err != nil {
		return err
	}

	if err := bd.cacher.Remove(bd.Name); err != nil {
		return err
	}
	return nil
}

func (bd *Board) WithArticles() {
	bd.Articles = bd.GetArticles()
}

func (bd *Board) WithNewArticles() {
	_ = bd.WithNewArticlesContext(context.Background())
}

func newArticles(bd Board) (newArticles, onlineArticles article.Articles) {
	newArticles, onlineArticles, _ = newArticlesContext(context.Background(), bd)
	return newArticles, onlineArticles
}

func (bd *Board) WithNewArticlesContext(ctx context.Context) error {
	var err error
	bd.NewArticles, bd.OnlineArticles, err = newArticlesContext(ctx, *bd)
	return err
}

func newArticlesContext(ctx context.Context, bd Board) (newArticles, onlineArticles article.Articles, err error) {
	savedArticles := bd.driver.GetArticles(bd.Name)
	initialized := len(savedArticles) > 0
	if reader, ok := bd.driver.(errorArticleDriver); ok {
		savedArticles, initialized, err = reader.GetArticlesE(bd.Name)
		if err != nil {
			return nil, nil, err
		}
	}
	onlineArticles, err = bd.FetchArticlesContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !initialized {
		return nil, onlineArticles, nil
	}

	newArticles, catchUpRequired := articlesAfterSavedHighWater(savedArticles, onlineArticles)
	if !catchUpRequired {
		return newArticles, onlineArticles, nil
	}

	newArticles, err = bd.catchUpArticlesContext(ctx, savedArticles, onlineArticles)
	if err != nil {
		return nil, onlineArticles, err
	}
	return newArticles, onlineArticles, nil
}

type articleHighWater struct {
	maxID           int
	hasID           bool
	identities      map[string]struct{}
	maxIDIdentities map[string]struct{}
}

func buildArticleHighWater(savedArticles article.Articles) articleHighWater {
	highWater := articleHighWater{
		identities:      make(map[string]struct{}, len(savedArticles)),
		maxIDIdentities: make(map[string]struct{}),
	}
	for _, savedArticle := range savedArticles {
		identity := savedArticle.Identity()
		if identity != "" {
			highWater.identities[identity] = struct{}{}
		}
		id := articleTimestamp(savedArticle)
		if id <= 0 {
			continue
		}
		if !highWater.hasID || id > highWater.maxID {
			highWater.maxID = id
			highWater.hasID = true
			highWater.maxIDIdentities = make(map[string]struct{})
		}
		if id == highWater.maxID && identity != "" {
			highWater.maxIDIdentities[identity] = struct{}{}
		}
	}
	return highWater
}

func articleTimestamp(item article.Article) int {
	if item.ID > 0 {
		return item.ID
	}
	if id := item.ParseID(item.Code); id > 0 {
		return id
	}
	return item.ParseID(item.Link)
}

func containsSavedIdentity(highWater articleHighWater, articles article.Articles) bool {
	for _, item := range articles {
		if identity := item.Identity(); identity != "" {
			if _, exists := highWater.identities[identity]; exists {
				return true
			}
		}
	}
	return false
}

// articlesAfterSavedHighWater handles the common Atom path and reports whether
// the feed is entirely newer than the saved cursor. In that case the exact
// boundary may have fallen beyond Atom's small window and HTML catch-up is
// required. If the feed has already crossed to an older timestamp, a deleted
// saved article is enough to explain the missing identity and no crawl is
// needed.
func articlesAfterSavedHighWater(savedArticles, onlineArticles article.Articles) (article.Articles, bool) {
	if len(savedArticles) == 0 || len(onlineArticles) == 0 {
		return articlesBeforeSavedBoundary(savedArticles, onlineArticles), false
	}

	highWater := buildArticleHighWater(savedArticles)
	if containsSavedIdentity(highWater, onlineArticles) {
		return articlesBeforeSavedBoundary(savedArticles, onlineArticles), false
	}

	newArticles := make(article.Articles, 0, len(onlineArticles))
	seen := make(map[string]struct{}, len(onlineArticles))
	crossedHighWater := false
	ambiguousTimestamp := !highWater.hasID
	for _, item := range onlineArticles {
		identity := item.Identity()
		if identity == "" {
			continue
		}
		id := articleTimestamp(item)
		if id <= 0 {
			ambiguousTimestamp = true
			continue
		}
		if highWater.hasID && id < highWater.maxID {
			crossedHighWater = true
			continue
		}
		if highWater.hasID && id == highWater.maxID {
			if _, saved := highWater.maxIDIdentities[identity]; saved {
				crossedHighWater = true
				continue
			}
		}
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		newArticles = append(newArticles, item)
	}

	if crossedHighWater {
		return newArticles, false
	}
	// Missing or malformed timestamps cannot safely prove that the feed crossed
	// the saved cursor. Prefer a bounded crawl and an explicit error over moving
	// the cursor across an unknown gap.
	return newArticles, len(newArticles) > 0 || ambiguousTimestamp
}

func (bd Board) catchUpArticlesContext(ctx context.Context, savedArticles, onlineArticles article.Articles) (article.Articles, error) {
	highWater := buildArticleHighWater(savedArticles)
	newArticles := make(article.Articles, 0, len(onlineArticles))
	seen := make(map[string]struct{}, len(onlineArticles))
	appendNew := func(items article.Articles) (boundaryReached bool) {
		for _, item := range items {
			identity := item.Identity()
			if identity == "" {
				continue
			}
			if _, saved := highWater.identities[identity]; saved {
				boundaryReached = true
				continue
			}

			id := articleTimestamp(item)
			if highWater.hasID && id > 0 && id < highWater.maxID {
				boundaryReached = true
				continue
			}
			if highWater.hasID && id > 0 && id == highWater.maxID {
				if _, saved := highWater.maxIDIdentities[identity]; saved {
					boundaryReached = true
					continue
				}
			}
			if _, duplicate := seen[identity]; duplicate {
				continue
			}
			seen[identity] = struct{}{}
			newArticles = append(newArticles, item)
		}
		return boundaryReached
	}

	if appendNew(onlineArticles) {
		return newArticles, nil
	}

	currentPage, err := bd.source().CurrentPage(ctx, bd.Name)
	if err != nil {
		return nil, fmt.Errorf("get current page for board %s catch-up: %w", bd.Name, err)
	}
	if currentPage < 1 {
		return nil, fmt.Errorf("get current page for board %s catch-up: invalid page %d", bd.Name, currentPage)
	}

	maxPages := bd.catchUpPageLimit()
	for pagesFetched := 0; pagesFetched < maxPages; pagesFetched++ {
		page := currentPage - pagesFetched
		if page < 1 {
			return newArticles, nil
		}
		pageArticles, err := bd.source().FetchHTML(ctx, bd.Name, page)
		if err != nil {
			return nil, fmt.Errorf("fetch board %s catch-up page %d: %w", bd.Name, page, err)
		}
		if strings.EqualFold(bd.Name, "allpost") {
			fixLink(&pageArticles)
		}
		if appendNew(pageArticles) {
			return newArticles, nil
		}
		if page == 1 {
			return newArticles, nil
		}
	}

	return nil, fmt.Errorf("%w: board %s after %d pages", ErrCatchUpPageLimit, bd.Name, maxPages)
}

// articlesBeforeSavedBoundary walks PTT's newest-first list until it reaches
// any article identity present in the previous snapshot. Full article codes (or
// links for legacy cache entries) are used so same-second articles with
// different hashes remain distinct.
func articlesBeforeSavedBoundary(savedArticles, onlineArticles article.Articles) article.Articles {
	savedIdentities := make(map[string]struct{}, len(savedArticles))
	for _, savedArticle := range savedArticles {
		if identity := savedArticle.Identity(); identity != "" {
			savedIdentities[identity] = struct{}{}
		}
	}

	newArticles := make(article.Articles, 0)
	seenOnline := make(map[string]struct{}, len(onlineArticles))
	for _, onlineArticle := range onlineArticles {
		identity := onlineArticle.Identity()
		if identity == "" {
			continue
		}
		if _, isBoundary := savedIdentities[identity]; isBoundary {
			break
		}
		if _, duplicate := seenOnline[identity]; duplicate {
			continue
		}
		seenOnline[identity] = struct{}{}
		newArticles = append(newArticles, onlineArticle)
	}
	return newArticles
}

func (bd Board) FetchArticles() (articles article.Articles) {
	articles, _ = bd.FetchArticlesE()
	return articles
}

// FetchArticlesE fetches the newest board articles while preserving the
// reason for a temporary PTT failure. Background jobs intentionally use the
// compatibility wrapper above, while HTTP handlers can return an honest
// service error instead of a misleading successful null response.
func (bd Board) FetchArticlesE() (articles article.Articles, err error) {
	return bd.FetchArticlesContext(context.Background())
}

// FetchArticlesContext is FetchArticlesE with caller cancellation propagated
// through both the Atom and HTML paths.
func (bd Board) FetchArticlesContext(ctx context.Context) (articles article.Articles, err error) {
	source := bd.source()
	_, usePersistentPolicy := source.(pttArticleSource)
	return fetchArticlesWithPolicy(ctx, bd.Name, source, usePersistentPolicy)
}

func fetchArticlesWithPolicy(
	ctx context.Context,
	boardName string,
	source articleSource,
	usePersistentPolicy bool,
) (articles article.Articles, err error) {
	policy := boardFetchPolicy{}
	if usePersistentPolicy {
		policy, err = loadBoardFetchPolicy(boardName)
		if err != nil {
			return nil, err
		}
		if policy.Backoff {
			return nil, fmt.Errorf("%w: board %s", ErrBoardFetchBackoff, boardName)
		}
	}

	if policy.HTMLOnly {
		articles, err = source.FetchHTML(ctx, boardName, -1)
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if policyErr := markBoardBackoff(boardName); policyErr != nil {
				return nil, errors.Join(err, policyErr)
			}
			return nil, err
		}
		if strings.EqualFold(boardName, "allpost") {
			fixLink(&articles)
		}
		return articles, nil
	}

	articles, err = source.FetchAtom(ctx, boardName)
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, rss.ErrTooManyRequests) || errors.Is(err, rss.ErrTemporarilyUnavailable) {
			log.WithError(err).Warning("RSS Parse Failed")
			return nil, err
		}
		log.WithField("board", boardName).WithError(err).Error("RSS Parse Failed, Switch to HTML Crawler")
		articles, err = source.FetchHTML(ctx, boardName, -1)
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if usePersistentPolicy {
				if policyErr := markBoardBackoff(boardName); policyErr != nil {
					return nil, errors.Join(err, policyErr)
				}
			}
			log.WithField("board", boardName).WithError(err).Error("HTML Parse Failed")
			return nil, err
		}
		if usePersistentPolicy {
			if err := markBoardHTMLOnly(boardName); err != nil {
				return nil, err
			}
		}
	}
	if strings.EqualFold(boardName, "allpost") {
		fixLink(&articles)
	}
	return articles, nil
}

func fixLink(articles *article.Articles) {
	for i, a := range *articles {
		preParenthesesIndex := strings.LastIndex(a.Title, "(")
		backParenthesesIndex := strings.LastIndex(a.Title, ")")
		if preParenthesesIndex < 0 || backParenthesesIndex <= preParenthesesIndex+1 {
			continue
		}
		realBoard := strings.TrimSpace(a.Title[preParenthesesIndex+1 : backParenthesesIndex])
		if realBoard == "" {
			continue
		}
		a.Link = strings.Replace(a.Link, "ALLPOST", realBoard, -1)
		(*articles)[i] = a
	}
}

func (bd Board) SuggestBoardName() string {
	names := bd.List()
	boardWeight := map[string]int{}
	chars := strings.Split(strings.ToLower(bd.Name), "")
	for _, name := range names {
		count := 0
		for _, char := range chars {
			if strings.Contains(name, char) {
				count++
			}
		}
		boardWeight[name] = count / (1 + int(math.Abs(float64(len(bd.Name)-len(name)))))
	}
	return maputil.MaxIntKey(boardWeight)
}

func CheckBoardExist(boardName string) (bool, string, error) {
	return CheckBoardExistContext(context.Background(), boardName)
}

// CheckBoardExistContext is CheckBoardExist with caller cancellation.
func CheckBoardExistContext(ctx context.Context, boardName string) (bool, string, error) {
	exists, suggestion, err := VerifyBoardExistContext(ctx, boardName)
	if err != nil || !exists {
		return exists, suggestion, err
	}
	// Board existence and the board-name cache live in Redis for both supported
	// storage modes; the article driver is not used by this operation.
	redisStore := new(Redis)
	bd := NewBoard(redisStore, redisStore)
	bd.Name = boardName
	if bd.Exist() {
		return true, "", nil
	}
	if err := bd.Create(); err != nil {
		return false, "", err
	}
	return true, "", nil
}

// VerifyBoardExistContext validates a board without changing the Redis polling
// set. Command handlers use it before their single atomic subscription commit.
func VerifyBoardExistContext(ctx context.Context, boardName string) (bool, string, error) {
	redisStore := new(Redis)
	bd := NewBoard(redisStore, redisStore)
	bd.Name = boardName
	if bd.Exist() {
		return true, "", nil
	}
	exists, err := rss.CheckBoardExistContext(ctx, boardName)
	if err != nil {
		return false, "", err
	}
	if exists {
		return true, "", nil
	}
	suggestBoard := bd.SuggestBoardName()
	log.WithFields(log.Fields{
		"inputBoard":   boardName,
		"suggestBoard": suggestBoard,
	}).Warning("Board Not Exist")
	return false, suggestBoard, nil
}
