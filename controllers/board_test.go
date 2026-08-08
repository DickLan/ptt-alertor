package controllers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/rss"
	"github.com/julienschmidt/httprouter"
)

func TestBoardArticleIndex(t *testing.T) {
	original := fetchBoardArticles
	defer func() { fetchBoardArticles = original }()

	tests := []struct {
		name       string
		fetch      func(context.Context, string) (article.Articles, error)
		wantStatus int
		wantBody   string
	}{
		{
			name: "success with empty array",
			fetch: func(_ context.Context, boardName string) (article.Articles, error) {
				if boardName != "NBA" {
					t.Fatalf("board name = %q, want NBA", boardName)
				}
				return nil, nil
			},
			wantStatus: http.StatusOK,
			wantBody:   "[]",
		},
		{
			name: "temporary failure",
			fetch: func(context.Context, string) (article.Articles, error) {
				return nil, rss.ErrTemporarilyUnavailable
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "PTT is temporarily unavailable",
		},
		{
			name: "rate limited",
			fetch: func(context.Context, string) (article.Articles, error) {
				return nil, rss.ErrTooManyRequests
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "PTT is temporarily unavailable",
		},
		{
			name: "other upstream failure",
			fetch: func(context.Context, string) (article.Articles, error) {
				return nil, errors.New("bad feed")
			},
			wantStatus: http.StatusBadGateway,
			wantBody:   "failed to fetch PTT articles",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fetchBoardArticles = test.fetch
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/boards/nba/articles", nil)
			BoardArticleIndex(recorder, request, httprouter.Params{{Key: "boardName", Value: "nba"}})
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if body := strings.TrimSpace(recorder.Body.String()); body != test.wantBody {
				t.Fatalf("body = %q, want %q", body, test.wantBody)
			}
		})
	}
}

func TestBoardArticleIndexPropagatesRequestContext(t *testing.T) {
	original := fetchBoardArticles
	defer func() { fetchBoardArticles = original }()

	type contextKey string
	const key contextKey = "request"
	fetchBoardArticles = func(ctx context.Context, _ string) (article.Articles, error) {
		if got := ctx.Value(key); got != "present" {
			t.Fatalf("request context value = %v, want present", got)
		}
		return article.Articles{}, nil
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/boards/nba/articles", nil)
	request = request.WithContext(context.WithValue(request.Context(), key, "present"))
	BoardArticleIndex(recorder, request, httprouter.Params{{Key: "boardName", Value: "nba"}})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestBoardArticle(t *testing.T) {
	original := findCachedArticle
	defer func() { findCachedArticle = original }()

	tests := []struct {
		name       string
		article    article.Article
		wantStatus int
		wantBody   string
	}{
		{
			name:       "cached article",
			article:    article.Article{Code: "M.123.A.456", Title: "cached"},
			wantStatus: http.StatusOK,
			wantBody:   `{"code":"M.123.A.456","Title":"cached","Link":"","lastPushDateTime":"0001-01-01T00:00:00Z"}`,
		},
		{
			name:       "cache miss",
			article:    article.Article{},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error":"article not found"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findCachedArticle = func(code string) article.Article {
				if code != "M.123.A.456" {
					t.Fatalf("code = %q, want M.123.A.456", code)
				}
				return test.article
			}

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/boards/NBA/articles/M.123.A.456", nil)
			BoardArticle(recorder, request, httprouter.Params{{Key: "code", Value: "M.123.A.456"}})

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q, want application/json; charset=utf-8", contentType)
			}
			if body := recorder.Body.String(); body != test.wantBody {
				t.Fatalf("body = %q, want %q", body, test.wantBody)
			}
		})
	}
}
