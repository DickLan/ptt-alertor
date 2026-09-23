package controllers

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"github.com/julienschmidt/httprouter"
)

// StockWatch is a fixed-scope internal API. It cannot select a webhook or board.
type StockWatch struct {
	Service *stockwatch.Service
	Token   string
}

func (api StockWatch) Handle(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Header().Set("Cache-Control", "no-store")
	supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(api.Token) < 32 || subtle.ConstantTimeCompare([]byte(supplied), []byte(api.Token)) != 1 || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if api.Service == nil {
		http.Error(w, "notification service unavailable", 503)
		return
	}
	var err error
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Authors []string `json:"authors"`
		}
		if decodeStrictJSON(w, r, &body) != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if len(body.Authors) == 0 || len(body.Authors) > stockwatch.MaxAuthors {
			http.Error(w, "invalid author list", 400)
			return
		}
		err = api.Service.Store.Add(body.Authors)
	case http.MethodPatch:
		var body struct {
			ArticleScope string `json:"article_scope"`
		}
		if decodeStrictJSON(w, r, &body) != nil || (body.ArticleScope != "all" && body.ArticleScope != "targets") {
			http.Error(w, "invalid article scope", 400)
			return
		}
		err = api.Service.Store.SetArticleScope(params.ByName("author"), body.ArticleScope)
	case http.MethodDelete:
		err = api.Service.Store.Remove(params.ByName("author"))
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", 405)
		return
	}
	if err != nil {
		if errors.Is(err, stockwatch.ErrAuthorNotFound) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "author_not_found"})
		} else if errors.Is(err, stockwatch.ErrInvalidAuthor) || errors.Is(err, stockwatch.ErrAuthorLimit) || errors.Is(err, stockwatch.ErrInvalidArticleScope) {
			http.Error(w, "invalid author or list exceeds 100 authors", 400)
		} else {
			http.Error(w, "notification storage unavailable", 503)
		}
		return
	}
	state, err := api.Service.Snapshot(r.Context())
	if err != nil {
		http.Error(w, "notification storage unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(state)
}
