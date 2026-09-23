package controllers

import (
	"encoding/json"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"github.com/julienschmidt/httprouter"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStockWatchScopeHTTPContract(t *testing.T) {
	r := miniredis.RunT(t)
	pool := &redis.Pool{Dial: func() (redis.Conn, error) { return redis.Dial("tcp", r.Addr()) }}
	defer pool.Close()
	token := strings.Repeat("t", 64)
	s := stockwatch.New(&stockwatch.Store{Connect: pool.Get, Prefix: "test:scope-api"}, false, "", "", token)
	if e := s.Store.Add([]string{"Alpha"}); e != nil {
		t.Fatal(e)
	}
	api := StockWatch{Service: s, Token: token}
	router := httprouter.New()
	router.PATCH("/authors/:author", api.Handle)
	for _, tc := range []struct {
		author, body, auth string
		code               int
	}{
		{"Alpha", `{"article_scope":"targets"}`, token, 200}, {"ALPHA", `{"article_scope":"all"}`, token, 200},
		{"Alpha", `{}`, token, 400}, {"Alpha", `{"article_scope":null}`, token, 400}, {"Alpha", `{"article_scope":"news"}`, token, 400},
		{"Alpha", `{"article_scope":1}`, token, 400}, {"Alpha", `{"article_scope":"all","board":"NBA"}`, token, 400},
		{"Alpha", `{"article_scope":"all"}{}`, token, 400}, {"Alpha", `{"article_scope":"` + strings.Repeat("x", (1<<20)+1) + `"}`, token, 400},
		{"Missing", `{"article_scope":"targets"}`, token, 404}, {"Alpha", `{"article_scope":"targets"}`, "wrong", 401},
	} {
		req := httptest.NewRequest("PATCH", "/authors/"+tc.author, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.auth)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != tc.code || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), token) {
			t.Fatal(w.Code, tc.code, w.Body.String())
		}
		if w.Code == 404 {
			var data map[string]string
			if json.Unmarshal(w.Body.Bytes(), &data) != nil || data["error"] != "author_not_found" {
				t.Fatal(w.Body.String())
			}
		}
	}
	if r.HGet("test:scope-api:authors", "missing") != "" {
		t.Fatal("PATCH created missing author")
	}
}
