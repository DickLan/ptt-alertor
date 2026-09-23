package controllers

import (
	"context"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"github.com/alicebob/miniredis/v2"
	"github.com/garyburd/redigo/redis"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStockWatchAPIAuthenticatedFixedScopeCRUD(t *testing.T) {
	r := miniredis.RunT(t)
	pool := &redis.Pool{Dial: func() (redis.Conn, error) { return redis.Dial("tcp", r.Addr()) }}
	defer pool.Close()
	token := strings.Repeat("secret", 8)
	s := stockwatch.New(&stockwatch.Store{Connect: pool.Get, Prefix: "test:api"}, false, "", "", token)
	api := StockWatch{Service: s, Token: token}
	router := httprouter.New()
	router.GET("/integrations/stock-quant/authors", api.Handle)
	router.POST("/integrations/stock-quant/authors", api.Handle)
	router.DELETE("/integrations/stock-quant/authors/:author", api.Handle)
	perform := func(method, path, auth, body string) int {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if strings.Contains(w.Body.String(), token) {
			t.Fatal("token exposed")
		}
		return w.Code
	}
	base := "/integrations/stock-quant/authors"
	if code := perform("GET", base, "", ""); code != 401 {
		t.Fatal(code)
	}
	if code := perform("POST", base, "Bearer wrong", `{"authors":["Alpha"]}`); code != 401 {
		t.Fatal(code)
	}
	for _, body := range []string{`{"authors":["Alpha"],"webhook":"untrusted"}`, `{"authors":["Alpha"],"board":"NBA"}`, `{"authors":["bad/name"]}`, `{"authors":["Alpha"]} {}`} {
		if code := perform("POST", base, "Bearer "+token, body); code != 400 {
			t.Fatal(code, body)
		}
	}
	if code := perform(http.MethodPost, base, "Bearer "+token, `{"authors":["Alpha","ALPHA","Beta"]}`); code != 200 {
		t.Fatal(code)
	}
	snapshot, e := s.Snapshot(context.Background())
	if e != nil || len(snapshot.Authors) != 2 {
		t.Fatal(e, snapshot.Authors)
	}
	if code := perform("DELETE", base+"/aLpHa", "Bearer "+token, ""); code != 200 {
		t.Fatal(code)
	}
	snapshot, e = s.Snapshot(context.Background())
	if e != nil || len(snapshot.Authors) != 1 || snapshot.Authors[0].Key != "beta" {
		t.Fatal(e, snapshot.Authors)
	}
	r.Del("test:api:authors")
	r.Set("test:api:authors", "broken")
	if code := perform("GET", base, "Bearer "+token, ""); code != 503 {
		t.Fatal(code)
	}
}
