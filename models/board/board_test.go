package board

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

type catchUpTestDriver struct {
	saved       article.Articles
	initialized bool
	err         error
}

func (d catchUpTestDriver) GetArticles(string) article.Articles {
	return d.saved
}

func (d catchUpTestDriver) GetArticlesE(string) (article.Articles, bool, error) {
	return d.saved, d.initialized, d.err
}

func (catchUpTestDriver) Save(string, article.Articles) error { return nil }
func (catchUpTestDriver) Delete(string) error                 { return nil }

type catchUpTestSource struct {
	atom        article.Articles
	atomErr     error
	currentPage int
	currentErr  error
	pages       map[int]article.Articles
	pageErrors  map[int]error
	htmlCalls   []int
}

func (s *catchUpTestSource) FetchAtom(context.Context, string) (article.Articles, error) {
	return s.atom, s.atomErr
}

func (s *catchUpTestSource) FetchHTML(_ context.Context, _ string, page int) (article.Articles, error) {
	s.htmlCalls = append(s.htmlCalls, page)
	if err := s.pageErrors[page]; err != nil {
		return nil, err
	}
	return s.pages[page], nil
}

func (s *catchUpTestSource) CurrentPage(context.Context, string) (int, error) {
	return s.currentPage, s.currentErr
}

func catchUpArticle(id int, suffix string) article.Article {
	code := fmt.Sprintf("M.%d.A.%s", id, suffix)
	return article.Article{
		ID:   id,
		Code: code,
		Link: "https://www.ptt.cc/bbs/NBA/" + code + ".html",
	}
}

func catchUpArticleRange(newest, oldest int) article.Articles {
	articles := make(article.Articles, 0, newest-oldest+1)
	for id := newest; id >= oldest; id-- {
		articles = append(articles, catchUpArticle(id, fmt.Sprintf("%03d", id)))
	}
	return articles
}

func articleIDs(articles article.Articles) []int {
	ids := make([]int, 0, len(articles))
	for _, item := range articles {
		ids = append(ids, item.ID)
	}
	return ids
}

func newCatchUpTestBoard(driver catchUpTestDriver, source *catchUpTestSource, maxPages int) Board {
	bd := NewBoard(driver, nil)
	bd.Name = "NBA"
	bd.articleSource = source
	bd.catchUpMaxPages = maxPages
	return *bd
}

func TestArticlesBeforeSavedBoundaryDistinguishesSameSecondCodes(t *testing.T) {
	saved := article.Articles{
		{ID: 99, Code: "M.99.A.099", Link: "https://www.ptt.cc/bbs/NBA/M.99.A.099.html"},
		{ID: 100, Code: "M.100.A.001", Link: "https://www.ptt.cc/bbs/NBA/M.100.A.001.html"},
	}
	online := article.Articles{
		{ID: 101, Code: "M.101.A.001", Link: "https://www.ptt.cc/bbs/NBA/M.101.A.001.html"},
		{ID: 100, Code: "M.100.A.002", Link: "https://www.ptt.cc/bbs/NBA/M.100.A.002.html"},
		{ID: 100, Code: "M.100.A.001", Link: "https://www.ptt.cc/bbs/NBA/M.100.A.001.html"},
		{ID: 99, Code: "M.99.A.099", Link: "https://www.ptt.cc/bbs/NBA/M.99.A.099.html"},
	}

	got := articlesBeforeSavedBoundary(saved, online)
	want := []string{"M.101.A.001", "M.100.A.002"}
	gotIdentities := make([]string, 0, len(got))
	for _, item := range got {
		gotIdentities = append(gotIdentities, item.Identity())
	}
	if !reflect.DeepEqual(gotIdentities, want) {
		t.Fatalf("new article identities = %#v, want %#v", gotIdentities, want)
	}
}

func TestArticlesBeforeSavedBoundaryMatchesLegacyLinkToNewCode(t *testing.T) {
	saved := article.Articles{{
		ID:   100,
		Link: "https://www.ptt.cc/bbs/NBA/M.100.A.001.html",
	}}
	online := article.Articles{{
		ID:   100,
		Code: "M.100.A.001",
		Link: "https://www.ptt.cc/bbs/NBA/M.100.A.001.html",
	}}

	if got := articlesBeforeSavedBoundary(saved, online); len(got) != 0 {
		t.Fatalf("legacy link boundary returned %d new articles, want none", len(got))
	}
}

func TestArticlesBeforeSavedBoundaryDoesNotConflateTimestampOnly(t *testing.T) {
	saved := article.Articles{{ID: 100}}
	online := article.Articles{{
		ID:   100,
		Code: "M.100.A.002",
		Link: "https://www.ptt.cc/bbs/NBA/M.100.A.002.html",
	}}

	got := articlesBeforeSavedBoundary(saved, online)
	if len(got) != 1 || got[0].Identity() != "M.100.A.002" {
		t.Fatalf("same-second article was conflated with timestamp-only cache: %#v", got)
	}
}

func TestPersistentBoardFetchPolicyAvoidsRepeatedAtomFallbackTraffic(t *testing.T) {
	originalLoad := loadBoardFetchPolicy
	originalHTML := markBoardHTMLOnly
	originalBackoff := markBoardBackoff
	defer func() {
		loadBoardFetchPolicy = originalLoad
		markBoardHTMLOnly = originalHTML
		markBoardBackoff = originalBackoff
	}()

	var markedHTML, markedBackoff int
	markBoardHTMLOnly = func(string) error { markedHTML++; return nil }
	markBoardBackoff = func(string) error { markedBackoff++; return nil }

	t.Run("successful fallback caches HTML-only mode", func(t *testing.T) {
		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) { return boardFetchPolicy{}, nil }
		source := &catchUpTestSource{
			atomErr: errors.New("malformed Atom"),
			pages:   map[int]article.Articles{-1: {catchUpArticle(1, "HTML")}},
		}
		articles, err := fetchArticlesWithPolicy(context.Background(), "NBA", source, true)
		if err != nil || len(articles) != 1 || markedHTML != 1 || !reflect.DeepEqual(source.htmlCalls, []int{-1}) {
			t.Fatalf("fallback = (%#v, %v), marked=%d calls=%#v", articles, err, markedHTML, source.htmlCalls)
		}
	})

	t.Run("HTML-only mode skips Atom", func(t *testing.T) {
		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) {
			return boardFetchPolicy{HTMLOnly: true}, nil
		}
		source := &catchUpTestSource{pages: map[int]article.Articles{-1: {catchUpArticle(2, "HTML")}}}
		articles, err := fetchArticlesWithPolicy(context.Background(), "NBA", source, true)
		if err != nil || len(articles) != 1 || len(source.htmlCalls) != 1 {
			t.Fatalf("HTML-only fetch = (%#v, %v), calls=%#v", articles, err, source.htmlCalls)
		}
	})

	t.Run("dual failure enters persistent backoff", func(t *testing.T) {
		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) { return boardFetchPolicy{}, nil }
		source := &catchUpTestSource{
			atomErr:    errors.New("malformed Atom"),
			pages:      map[int]article.Articles{},
			pageErrors: map[int]error{-1: errors.New("HTML unavailable")},
		}
		if _, err := fetchArticlesWithPolicy(context.Background(), "NBA", source, true); err == nil || markedBackoff != 1 {
			t.Fatalf("dual failure error=%v markedBackoff=%d", err, markedBackoff)
		}
	})

	t.Run("backoff makes no PTT call", func(t *testing.T) {
		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) {
			return boardFetchPolicy{Backoff: true}, nil
		}
		source := &catchUpTestSource{pages: map[int]article.Articles{}}
		if _, err := fetchArticlesWithPolicy(context.Background(), "NBA", source, true); !errors.Is(err, ErrBoardFetchBackoff) {
			t.Fatalf("backoff error = %v", err)
		}
		if len(source.htmlCalls) != 0 {
			t.Fatalf("backoff made HTML calls %#v", source.htmlCalls)
		}
	})

	t.Run("caller cancellation does not poison persistent policy", func(t *testing.T) {
		before := markedBackoff
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) { return boardFetchPolicy{}, nil }
		source := &catchUpTestSource{atomErr: context.Canceled, pages: map[int]article.Articles{}}
		if _, err := fetchArticlesWithPolicy(ctx, "NBA", source, true); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Atom error = %v", err)
		}
		if len(source.htmlCalls) != 0 || markedBackoff != before {
			t.Fatalf("canceled Atom used fallback/backoff: calls=%#v marked=%d", source.htmlCalls, markedBackoff-before)
		}

		loadBoardFetchPolicy = func(string) (boardFetchPolicy, error) {
			return boardFetchPolicy{HTMLOnly: true}, nil
		}
		source = &catchUpTestSource{
			pages:      map[int]article.Articles{},
			pageErrors: map[int]error{-1: context.Canceled},
		}
		if _, err := fetchArticlesWithPolicy(ctx, "NBA", source, true); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled HTML-only error = %v", err)
		}
		if markedBackoff != before {
			t.Fatalf("canceled HTML-only marked backoff %d times", markedBackoff-before)
		}
	})

}

func TestNewArticlesContextCatchesUpBeyondAtomWindow(t *testing.T) {
	driver := catchUpTestDriver{
		saved:       article.Articles{catchUpArticle(100, "SAVED")},
		initialized: true,
	}
	source := &catchUpTestSource{
		atom:        catchUpArticleRange(125, 106),
		currentPage: 10,
		pages: map[int]article.Articles{
			10: catchUpArticleRange(125, 111),
			9: append(append(
				catchUpArticleRange(110, 101),
				catchUpArticle(100, "SAVED"),
			), catchUpArticleRange(99, 96)...),
		},
	}
	bd := newCatchUpTestBoard(driver, source, 3)

	got, online, err := newArticlesContext(context.Background(), bd)
	if err != nil {
		t.Fatal(err)
	}
	if len(online) != 20 {
		t.Fatalf("online articles = %d, want Atom snapshot of 20", len(online))
	}
	if want := articleIDs(catchUpArticleRange(125, 101)); !reflect.DeepEqual(articleIDs(got), want) {
		t.Fatalf("new article IDs = %#v, want %#v", articleIDs(got), want)
	}
	if !reflect.DeepEqual(source.htmlCalls, []int{10, 9}) {
		t.Fatalf("HTML page calls = %#v, want [10 9]", source.htmlCalls)
	}
}

func TestNewArticlesContextCatchUpKeepsSameSecondHashes(t *testing.T) {
	driver := catchUpTestDriver{
		saved:       article.Articles{catchUpArticle(100, "SAVED")},
		initialized: true,
	}
	sameSecondA := catchUpArticle(100, "NEW-A")
	sameSecondB := catchUpArticle(100, "NEW-B")
	boundary := catchUpArticle(100, "SAVED")
	source := &catchUpTestSource{
		atom:        catchUpArticleRange(121, 102),
		currentPage: 10,
		pages: map[int]article.Articles{
			10: catchUpArticleRange(121, 102),
			9: append(article.Articles{
				catchUpArticle(101, "101"),
				sameSecondA,
				sameSecondB,
				boundary,
			}, catchUpArticleRange(99, 95)...),
		},
	}
	bd := newCatchUpTestBoard(driver, source, 3)

	got, _, err := newArticlesContext(context.Background(), bd)
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[string]struct{}, len(got))
	for _, item := range got {
		identities[item.Identity()] = struct{}{}
	}
	for _, identity := range []string{sameSecondA.Identity(), sameSecondB.Identity()} {
		if _, exists := identities[identity]; !exists {
			t.Errorf("same-second article %q was lost", identity)
		}
	}
	if _, replayed := identities[boundary.Identity()]; replayed {
		t.Errorf("saved boundary %q was replayed", boundary.Identity())
	}
}

func TestNewArticlesContextUsesHighWaterWhenSavedArticleWasDeleted(t *testing.T) {
	driver := catchUpTestDriver{
		saved:       article.Articles{catchUpArticle(100, "DELETED")},
		initialized: true,
	}
	sameSecond := catchUpArticle(100, "NEW")
	source := &catchUpTestSource{
		atom: article.Articles{
			catchUpArticle(102, "102"),
			catchUpArticle(101, "101"),
			sameSecond,
			catchUpArticle(99, "99"),
			catchUpArticle(98, "98"),
		},
	}
	bd := newCatchUpTestBoard(driver, source, 2)

	got, _, err := newArticlesContext(context.Background(), bd)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		catchUpArticle(102, "102").Identity(),
		catchUpArticle(101, "101").Identity(),
		sameSecond.Identity(),
	}
	identities := make([]string, 0, len(got))
	for _, item := range got {
		identities = append(identities, item.Identity())
	}
	if !reflect.DeepEqual(identities, want) {
		t.Fatalf("new identities = %#v, want %#v", identities, want)
	}
	if len(source.htmlCalls) != 0 {
		t.Fatalf("deleted boundary triggered unnecessary HTML crawl: %#v", source.htmlCalls)
	}
}

func TestNewArticlesContextCatchUpLimitAndFetchErrors(t *testing.T) {
	driver := catchUpTestDriver{
		saved:       article.Articles{catchUpArticle(100, "SAVED")},
		initialized: true,
	}

	t.Run("page limit", func(t *testing.T) {
		source := &catchUpTestSource{
			atom:        catchUpArticleRange(150, 131),
			currentPage: 10,
			pages: map[int]article.Articles{
				10: catchUpArticleRange(150, 131),
				9:  catchUpArticleRange(130, 111),
			},
		}
		bd := newCatchUpTestBoard(driver, source, 2)

		got, online, err := newArticlesContext(context.Background(), bd)
		if !errors.Is(err, ErrCatchUpPageLimit) {
			t.Fatalf("error = %v, want ErrCatchUpPageLimit", err)
		}
		if got != nil {
			t.Fatalf("partial catch-up leaked on limit: %#v", got)
		}
		if len(online) != 20 {
			t.Fatalf("online articles = %d, want 20", len(online))
		}
		if !reflect.DeepEqual(source.htmlCalls, []int{10, 9}) {
			t.Fatalf("HTML page calls = %#v, want [10 9]", source.htmlCalls)
		}
	})

	t.Run("page fetch error", func(t *testing.T) {
		fetchErr := errors.New("PTT page unavailable")
		source := &catchUpTestSource{
			atom:        catchUpArticleRange(125, 106),
			currentPage: 10,
			pages: map[int]article.Articles{
				10: catchUpArticleRange(125, 106),
			},
			pageErrors: map[int]error{9: fetchErr},
		}
		bd := newCatchUpTestBoard(driver, source, 3)

		got, _, err := newArticlesContext(context.Background(), bd)
		if !errors.Is(err, fetchErr) {
			t.Fatalf("error = %v, want wrapped fetch error", err)
		}
		if got != nil {
			t.Fatalf("partial catch-up leaked on fetch error: %#v", got)
		}
	})
}

func TestNewArticlesContextPreservesNormalBoundaryAndBaseline(t *testing.T) {
	saved := catchUpArticle(100, "SAVED")

	t.Run("normal Atom boundary", func(t *testing.T) {
		driver := catchUpTestDriver{saved: article.Articles{saved}, initialized: true}
		source := &catchUpTestSource{atom: article.Articles{
			catchUpArticle(102, "102"),
			catchUpArticle(101, "101"),
			saved,
			catchUpArticle(99, "99"),
		}}
		bd := newCatchUpTestBoard(driver, source, 2)

		got, _, err := newArticlesContext(context.Background(), bd)
		if err != nil {
			t.Fatal(err)
		}
		if want := []int{102, 101}; !reflect.DeepEqual(articleIDs(got), want) {
			t.Fatalf("new IDs = %#v, want %#v", articleIDs(got), want)
		}
		if len(source.htmlCalls) != 0 {
			t.Fatalf("normal boundary triggered HTML crawl: %#v", source.htmlCalls)
		}
	})

	t.Run("subscription activation watermark", func(t *testing.T) {
		driver := catchUpTestDriver{
			saved:       article.Articles{{ID: 100, Code: "subscription-activation:100"}},
			initialized: true,
		}
		source := &catchUpTestSource{atom: article.Articles{
			catchUpArticle(102, "102"),
			catchUpArticle(101, "101"),
			catchUpArticle(99, "99"),
		}}
		bd := newCatchUpTestBoard(driver, source, 2)

		got, _, err := newArticlesContext(context.Background(), bd)
		if err != nil {
			t.Fatal(err)
		}
		if want := []int{102, 101}; !reflect.DeepEqual(articleIDs(got), want) {
			t.Fatalf("new IDs = %#v, want %#v", articleIDs(got), want)
		}
	})

	t.Run("first baseline", func(t *testing.T) {
		driver := catchUpTestDriver{initialized: false}
		source := &catchUpTestSource{atom: catchUpArticleRange(102, 100)}
		bd := newCatchUpTestBoard(driver, source, 2)

		got, online, err := newArticlesContext(context.Background(), bd)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil || len(online) != 3 {
			t.Fatalf("baseline = (%#v, %#v), want no new and three online", got, online)
		}
		if len(source.htmlCalls) != 0 {
			t.Fatalf("baseline triggered HTML crawl: %#v", source.htmlCalls)
		}
	})
}

func TestConfiguredCatchUpMaxPages(t *testing.T) {
	t.Setenv(catchUpMaxPagesEnv, "7")
	if got := NewBoard(catchUpTestDriver{}, nil).catchUpMaxPages; got != 7 {
		t.Fatalf("configured catch-up max pages = %d, want 7", got)
	}

	t.Setenv(catchUpMaxPagesEnv, "invalid")
	if got := NewBoard(catchUpTestDriver{}, nil).catchUpMaxPages; got != defaultCatchUpMaxPages {
		t.Fatalf("invalid configured max pages = %d, want default %d", got, defaultCatchUpMaxPages)
	}
}

func TestFixLinkSkipsMalformedAllPostTitles(t *testing.T) {
	articles := article.Articles{
		{Title: "malformed title", Link: "https://www.ptt.cc/bbs/ALLPOST/M.1.A.ONE.html"},
		{Title: "empty ()", Link: "https://www.ptt.cc/bbs/ALLPOST/M.2.A.TWO.html"},
		{Title: "valid (NBA)", Link: "https://www.ptt.cc/bbs/ALLPOST/M.3.A.THREE.html"},
	}

	fixLink(&articles)
	if got := articles[0].Link; got != "https://www.ptt.cc/bbs/ALLPOST/M.1.A.ONE.html" {
		t.Fatalf("malformed title link = %q", got)
	}
	if got := articles[1].Link; got != "https://www.ptt.cc/bbs/ALLPOST/M.2.A.TWO.html" {
		t.Fatalf("empty board link = %q", got)
	}
	if got := articles[2].Link; got != "https://www.ptt.cc/bbs/NBA/M.3.A.THREE.html" {
		t.Fatalf("valid ALLPOST link = %q", got)
	}
}
