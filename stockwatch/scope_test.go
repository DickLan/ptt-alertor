package stockwatch

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

func TestArticleScopePersistenceLegacyAndIndependentAuthors(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha", "Beta"}))
	authors, _ := s.Store.Authors()
	before, _ := s.Store.State()
	// Simulate the exact legacy four-field record without a scope.
	raw := map[string]interface{}{"author": authors[0].Author, "key": authors[0].Key, "generation": authors[0].Generation, "activated_at": authors[0].ActivatedAt}
	legacy, _ := json.Marshal(raw)
	r.HSet(s.Store.key("authors"), "alpha", string(legacy))
	read, _ := s.Store.Authors()
	if read[0].ArticleScope != "all" {
		t.Fatal(read)
	}
	if r.HGet(s.Store.key("authors"), "alpha") != string(legacy) {
		t.Fatal("GET rewrote legacy data")
	}
	r.Set(s.Store.key("cursor"), "unchanged cursor")
	must(t, s.Store.SetArticleScope("ALPHA", "targets"))
	must(t, s.Store.Add([]string{"alpha"}))
	after, _ := s.Store.State()
	read, _ = s.Store.Authors()
	if !reflect.DeepEqual(before, after) || read[0].ArticleScope != "targets" || read[1].ArticleScope != "all" {
		t.Fatal(after, read)
	}
	if read[0].Generation != authors[0].Generation || read[0].ActivatedAt != authors[0].ActivatedAt {
		t.Fatal("setting reset identity")
	}
	cursor, _ := r.Get(s.Store.key("cursor"))
	if cursor != "unchanged cursor" {
		t.Fatal(cursor)
	}
	restarted := &Store{Connect: s.Store.Connect, Prefix: s.Store.Prefix}
	read, _ = restarted.Authors()
	if read[0].ArticleScope != "targets" {
		t.Fatal("restart lost scope")
	}
	for _, scope := range []string{"", "news", "ALL", "Targets"} {
		if !errors.Is(s.Store.SetArticleScope("alpha", scope), ErrInvalidArticleScope) {
			t.Fatal(scope)
		}
	}
	if !errors.Is(s.Store.SetArticleScope("missing", "all"), ErrAuthorNotFound) {
		t.Fatal("missing author not rejected")
	}
	if !errors.Is(s.Store.SetArticleScope("bad/name", "targets"), ErrInvalidAuthor) {
		t.Fatal("invalid name")
	}
	for i := 0; i < 20; i++ {
		must(t, s.Store.Add([]string{"Race"}))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			e := s.Store.SetArticleScope("Race", "targets")
			if e != nil && !errors.Is(e, ErrAuthorNotFound) {
				t.Error(e)
			}
		}()
		go func() {
			defer wg.Done()
			if e := s.Store.Remove("race"); e != nil {
				t.Error(e)
			}
		}()
		wg.Wait()
		if r.HGet(s.Store.key("authors"), "race") != "" {
			t.Fatal("removed author revived")
		}
	}
}

func TestTargetTitleGrammar(t *testing.T) {
	yes := []string{"[標的] 2330 多", "[標的] 空", "Re: [標的] 續文", "Fw: Re:[標的] 轉文", " re :\t[ 標的\t] x ", "　[標的] 外圍空白　", strings.Repeat("Re:", 8) + "[標的] x", "[標的] " + strings.Repeat("多", 300)}
	no := []string{"[新聞] 標的", "[討論] [標的] x", "Re:[標的X]", "標的[標的]", "Fwd: [標的]", "Re：[標的]", "Re:　[標的]", "［標的］", "[標的\n] x", strings.Repeat("Re:", 9) + "[標的] x"}
	for _, title := range yes {
		if !IsTargetTitle(title) {
			t.Errorf("not matched %q", title)
		}
	}
	for _, title := range no {
		if IsTargetTitle(title) {
			t.Errorf("false match %q", title)
		}
	}
}

func TestCollectorArticleScopeMatrixAndCursor(t *testing.T) {
	for _, scope := range []string{"all", "targets"} {
		for _, title := range []string{"[標的] 台積電", "[新聞] 台積電"} {
			t.Run(scope+title, func(t *testing.T) {
				s, r := fixture(t)
				must(t, s.Store.Add([]string{"Alpha"}))
				must(t, s.Store.SetArticleScope("alpha", scope))
				row := good("Alpha", title, "001", testStart.Unix()+1)
				fetchRows(s, article.Articles{row})
				due(r, 1)
				must(t, s.Tick(context.Background()))
				n, e := s.Outbox.PendingCount(context.Background())
				must(t, e)
				want := int64(0)
				if scope == "all" || IsTargetTitle(title) {
					want = 1
				}
				if n != want {
					t.Fatal(n, want)
				}
				cursor, _ := r.Get(s.Store.key("cursor"))
				if !strings.Contains(cursor, row.Code) {
					t.Fatal("filter prevented cursor advance")
				}
				due(r, 2)
				must(t, s.Tick(context.Background()))
				n, _ = s.Outbox.PendingCount(context.Background())
				if n != want {
					t.Fatal("duplicate notification")
				}
			})
		}
	}
}

func TestQueuedScopeChangesAndLegacyPayload(t *testing.T) {
	for _, title := range []string{"[新聞] ignore", "Re: [標的] keep", "[標的] " + strings.Repeat("多", 300)} {
		t.Run(title[:8], func(t *testing.T) {
			s, r := fixture(t)
			ctx := context.Background()
			must(t, s.Store.Add([]string{"Alpha"}))
			fetchRows(s, article.Articles{good("Alpha", title, "001", testStart.Unix()+1)})
			due(r, 1)
			must(t, s.Tick(ctx))
			item, e := s.Outbox.Claim(ctx, time.Minute)
			must(t, e)
			must(t, s.Store.SetArticleScope("alpha", "targets"))
			allowed, e := s.Eligible(ctx, item)
			must(t, e)
			if allowed != IsTargetTitle(title) {
				t.Fatal("queued scope ignored", allowed, title)
			}
			item.Chunks = []string{"malformed"}
			allowed, e = s.Eligible(ctx, item)
			must(t, e)
			if allowed {
				t.Fatal("malformed targets payload sent")
			}
			item.Chunks = []string{"【專家追蹤・Stock 新文章】\n作者：Other\n[標的] wrong author\nend"}
			allowed, e = s.Eligible(ctx, item)
			must(t, e)
			if allowed {
				t.Fatal("incorrect envelope author")
			}
			must(t, s.Store.SetArticleScope("alpha", "all"))
			allowed, e = s.Eligible(ctx, item)
			must(t, e)
			if !allowed {
				t.Fatal("all legacy payload incompatible")
			}
			must(t, s.Store.Remove("alpha"))
			must(t, s.Store.Add([]string{"Alpha"}))
			allowed, e = s.Eligible(ctx, item)
			must(t, e)
			if allowed {
				t.Fatal("old generation after re-add")
			}
		})
	}
}

func TestNonTargetMalformedDoesNotBlockTargets(t *testing.T) {
	s, r := fixture(t)
	must(t, s.Store.Add([]string{"Alpha"}))
	must(t, s.Store.SetArticleScope("Alpha", "targets"))
	fetchRows(s, article.Articles{good("Other", "[新聞] valid", "001", testStart.Unix()+1), {Author: "Alpha", Code: "bad", Title: "[新聞] ignore"}})
	due(r, 1)
	must(t, s.Tick(context.Background()))
	state, _ := s.Store.State()
	if state["last_error"] != "" || state["last_success_at_ms"] == "" {
		t.Fatal(state)
	}
}
