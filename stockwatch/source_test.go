package stockwatch

import (
	"context"
	"fmt"
	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Exercise the actual Atom parser, HTML catch-up and the synthetic first cursor
// with a fake transport. No network request can reach PTT.
func TestRealParserFirstTickCatchesBeyondAtomWindow(t *testing.T) {
	s, r := fixture(t)
	host, port, _ := net.SplitHostPort(r.Addr())
	t.Setenv("REDIS_ENDPOINT", host)
	t.Setenv("REDIS_PORT", port)
	defer connections.Close()
	must(t, s.Store.Add([]string{"Alpha"}))
	ts := testStart.Unix()
	due(r, 1)
	atom := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title>Stock</title><id>stock</id><updated>2026-09-10T01:01:00Z</updated><entry><title>Re: discussion</title><id>https://www.ptt.cc/bbs/Stock/M.%d.A.002.html</id><link href="https://www.ptt.cc/bbs/Stock/M.%d.A.002.html"/><published>2026-09-10T01:01:00Z</published><author><name>Alpha (測試暱稱)</name></author></entry></feed>`, ts+60, ts+60)
	row := func(second int64, code string) string {
		return fmt.Sprintf(`<div class="r-ent"><div class="title"><a href="/bbs/Stock/M.%d.A.%s.html">Fw: news</a></div><div class="meta"><div class="date">9/10</div><div class="author">Alpha</div></div></div>`, second, code)
	}

	page := func(rows string) string {
		return `<html><body><div id="main-container"><div id="action-bar-container"><div class="btn-group btn-group-paging"><a href="/bbs/Stock/index9.html">上頁</a></div></div><div class="r-list-container">` + rows + `<div class="r-list-sep"></div>` + row(ts-99999, "PINNED") + `</div></div></body></html>`
	}
	html := page(row(ts+60, "002"))
	historyHTML := page(row(ts, "001") + row(ts-2, "OLD"))

	old := http.DefaultTransport
	defer func() { http.DefaultTransport = old }()
	requests := []string{}
	http.DefaultTransport = transportFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "www.ptt.cc" {
			t.Fatalf("unexpected host %s", req.URL.Host)
		}
		requests = append(requests, req.URL.Path)
		body := html
		if req.URL.Path == "/bbs/Stock/index9.html" {
			body = historyHTML
		}
		mime := "text/html; charset=utf-8"
		if req.URL.Path == "/atom/Stock.xml" {
			body = atom
			mime = "application/atom+xml"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{mime}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	must(t, s.Tick(context.Background()))
	n, e := s.Outbox.PendingCount(context.Background())
	must(t, e)
	if n != 2 {
		t.Fatalf("first poll queued %d; wanted activation-second plus later, excluding old; requests %v", n, requests)
	}
	if len(requests) != 4 || requests[0] != "/atom/Stock.xml" || requests[2] != "/bbs/Stock/index10.html" || requests[3] != "/bbs/Stock/index9.html" {
		t.Fatalf("catch-up not exercised: %v", requests)
	}
}
