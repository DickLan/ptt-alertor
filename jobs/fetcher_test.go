package jobs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
)

type recordingFetcherBoardDriver struct {
	cached    article.Articles
	saved     article.Articles
	saveCalls int
}

func (driver *recordingFetcherBoardDriver) GetArticles(string) article.Articles {
	return driver.cached
}

func (driver *recordingFetcherBoardDriver) Save(_ string, articles article.Articles) error {
	driver.saveCalls++
	driver.saved = articles
	return nil
}

func (*recordingFetcherBoardDriver) Delete(string) error { return nil }

func TestFetchAndCacheBoardRetainsCacheOnUpstreamError(t *testing.T) {
	originalFetch := fetchArticlesForCache
	defer func() { fetchArticlesForCache = originalFetch }()

	existing := article.Articles{{Code: "M.existing", Title: "existing"}}
	driver := &recordingFetcherBoardDriver{cached: existing}
	bd := board.NewBoard(driver, nil)
	bd.Name = "NBA"

	type contextKey string
	const requestKey contextKey = "request"
	upstreamErr := errors.New("temporary PTT failure")
	fetchArticlesForCache = func(ctx context.Context, got board.Board) (article.Articles, error) {
		if got.Name != "NBA" {
			t.Fatalf("board = %q, want NBA", got.Name)
		}
		if ctx.Value(requestKey) != "present" {
			t.Fatal("fetch context did not preserve caller value")
		}
		return nil, upstreamErr
	}

	err := fetchAndCacheBoard(context.WithValue(context.Background(), requestKey, "present"), *bd)
	if !errors.Is(err, upstreamErr) {
		t.Fatalf("error = %v, want %v", err, upstreamErr)
	}
	if driver.saveCalls != 0 {
		t.Fatalf("save calls = %d, want 0", driver.saveCalls)
	}
	if !reflect.DeepEqual(driver.cached, existing) {
		t.Fatalf("cached articles = %#v, want existing cache %#v", driver.cached, existing)
	}
}

func TestFetchAndCacheBoardSavesSuccessfulFetch(t *testing.T) {
	originalFetch := fetchArticlesForCache
	defer func() { fetchArticlesForCache = originalFetch }()

	driver := &recordingFetcherBoardDriver{}
	bd := board.NewBoard(driver, nil)
	bd.Name = "NBA"
	want := article.Articles{{Code: "M.new", Title: "new"}}
	fetchArticlesForCache = func(context.Context, board.Board) (article.Articles, error) {
		return want, nil
	}

	if err := fetchAndCacheBoard(context.Background(), *bd); err != nil {
		t.Fatalf("fetchAndCacheBoard() error = %v", err)
	}
	if driver.saveCalls != 1 {
		t.Fatalf("save calls = %d, want 1", driver.saveCalls)
	}
	if !reflect.DeepEqual(driver.saved, want) {
		t.Fatalf("saved articles = %#v, want %#v", driver.saved, want)
	}
}
