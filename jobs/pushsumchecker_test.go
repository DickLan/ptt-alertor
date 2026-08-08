package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

func TestPushSumCrawlStopsAfterFirstPageError(t *testing.T) {
	originalCurrentPage := currentBoardPage
	originalFetch := fetchPushSumArticles
	defer func() {
		currentBoardPage = originalCurrentPage
		fetchPushSumArticles = originalFetch
	}()

	currentBoardPage = func(context.Context, string) (int, error) { return 10, nil }
	calls := 0
	fetchPushSumArticles = func(context.Context, string, int) (article.Articles, error) {
		calls++
		return nil, errors.New("PTT challenge")
	}

	results := make(chan BoardArticles, 1)
	pushSumChecker{}.crawlArticles(BoardArticles{board: "NBA"}, results)
	result := <-results
	if calls != 1 {
		t.Fatalf("FetchArticles calls = %d, want 1", calls)
	}
	if len(result.articles) != 0 {
		t.Fatalf("articles = %d, want none after error", len(result.articles))
	}
	if result.complete {
		t.Fatal("failed crawl was marked complete")
	}
}

func TestPushSumCrawlStopsAfterEmptyPage(t *testing.T) {
	originalCurrentPage := currentBoardPage
	originalFetch := fetchPushSumArticles
	defer func() {
		currentBoardPage = originalCurrentPage
		fetchPushSumArticles = originalFetch
	}()

	currentBoardPage = func(context.Context, string) (int, error) { return 100, nil }
	calls := 0
	fetchPushSumArticles = func(context.Context, string, int) (article.Articles, error) {
		calls++
		return article.Articles{}, nil
	}
	results := make(chan BoardArticles, 1)
	pushSumChecker{}.crawlArticles(BoardArticles{board: "NBA"}, results)
	result := <-results
	if calls != 1 {
		t.Fatalf("FetchArticles calls = %d, want 1", calls)
	}
	if result.complete {
		t.Fatal("empty intermediate page was marked complete")
	}
}

func TestPushSumCrawlHasHardPageLimit(t *testing.T) {
	originalCurrentPage := currentBoardPage
	originalFetch := fetchPushSumArticles
	defer func() {
		currentBoardPage = originalCurrentPage
		fetchPushSumArticles = originalFetch
	}()

	currentBoardPage = func(context.Context, string) (int, error) { return 1000, nil }
	t.Setenv("PTT_PUSHSUM_MAX_PAGES", "20")
	calls := 0
	fetchPushSumArticles = func(context.Context, string, int) (article.Articles, error) {
		calls++
		return article.Articles{{ID: calls, Date: time.Now().Format("1/02")}}, nil
	}
	results := make(chan BoardArticles, 1)
	pushSumChecker{}.crawlArticles(BoardArticles{board: "NBA"}, results)
	result := <-results
	if calls != defaultMaxPushSumPages {
		t.Fatalf("FetchArticles calls = %d, want hard limit %d", calls, defaultMaxPushSumPages)
	}
	if result.complete {
		t.Fatal("page-limited crawl was marked complete")
	}
}

func TestPushSumCrawlTreatsValidEmptySinglePageAsComplete(t *testing.T) {
	originalCurrentPage := currentBoardPage
	originalFetch := fetchPushSumArticles
	defer func() {
		currentBoardPage = originalCurrentPage
		fetchPushSumArticles = originalFetch
	}()

	currentBoardPage = func(context.Context, string) (int, error) { return 1, nil }
	fetchPushSumArticles = func(context.Context, string, int) (article.Articles, error) {
		return article.Articles{}, nil
	}
	results := make(chan BoardArticles, 1)
	pushSumChecker{}.crawlArticles(BoardArticles{board: "NBA"}, results)
	if result := <-results; !result.complete || len(result.articles) != 0 {
		t.Fatalf("single-page result = %#v, want complete empty crawl", result)
	}
}
