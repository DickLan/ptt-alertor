package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/rss"
	"github.com/julienschmidt/httprouter"
)

var fetchBoardArticles = func(ctx context.Context, boardName string) (article.Articles, error) {
	bd := models.Board()
	bd.Name = boardName
	return bd.FetchArticlesContext(ctx)
}

var findCachedArticle = func(code string) article.Article {
	return models.Article().Find(code)
}

func BoardArticleIndex(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	boardName := strings.ToUpper(params.ByName("boardName"))
	if !board.PollingAllowed(boardName) {
		http.Error(w, "board is not available on this deployment", http.StatusBadRequest)
		return
	}
	ctx, cancel := pttOperationContext(r.Context())
	defer cancel()
	articles, err := fetchBoardArticles(ctx, boardName)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "PTT request timed out", http.StatusGatewayTimeout)
			return
		}
		if errors.Is(err, rss.ErrTooManyRequests) || errors.Is(err, rss.ErrTemporarilyUnavailable) {
			http.Error(w, "PTT is temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "failed to fetch PTT articles", http.StatusBadGateway)
		return
	}
	if articles == nil {
		articles = make(article.Articles, 0)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	articlesJSON, err := json.Marshal(articles)
	if err != nil {
		myutil.LogJSONEncode(err, articles)
		http.Error(w, "failed to encode articles", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%s", articlesJSON)
}

func BoardArticle(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	code := params.ByName("code")
	a := findCachedArticle(code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if a.Code == "" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"article not found"}`))
		return
	}

	aJSON, err := json.Marshal(a)
	if err != nil {
		myutil.LogJSONEncode(err, a)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"failed to encode article"}`))
		return
	}
	_, _ = w.Write(aJSON)
}

func BoardIndex(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	bds := models.Board().All()
	fmt.Fprintf(w, "追蹤看板總數：%d", len(bds))
	for _, bd := range bds {
		fmt.Fprintf(w, "\n%s", bd.Name)
	}
}
