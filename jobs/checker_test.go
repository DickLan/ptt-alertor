package jobs

import (
	"context"
	"reflect"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
)

func TestParseHighBoardsCanonicalizesAndDeduplicatesNames(t *testing.T) {
	boards, boardSet := parseHighBoards(" Stock, NBA,stock, ,nBa ")
	gotNames := make([]string, 0, len(boards))
	for _, item := range boards {
		gotNames = append(gotNames, item.Name)
	}
	if want := []string{"stock", "nba"}; !reflect.DeepEqual(gotNames, want) {
		t.Fatalf("priority board names = %#v, want %#v", gotNames, want)
	}
	if !reflect.DeepEqual(boardSet, map[string]struct{}{"stock": {}, "nba": {}}) {
		t.Fatalf("priority board set = %#v", boardSet)
	}
}

func TestExcludeHighBoards(t *testing.T) {
	original := highBoardSet
	highBoardSet = map[string]struct{}{"gossiping": {}}
	defer func() { highBoardSet = original }()

	boards := []*board.Board{
		{Name: "Gossiping"},
		{Name: "Stock"},
	}
	normal := excludeHighBoards(boards)
	if len(normal) != 1 || normal[0].Name != "Stock" {
		t.Fatalf("excludeHighBoards() = %#v, want only Stock", normal)
	}
}

func TestIntersectHighBoardsDoesNotPollUnsubscribedPriorityEntries(t *testing.T) {
	configured, _ := parseHighBoards("Stock,NBA,Gossiping")
	active := []*board.Board{{Name: "nba"}, {Name: "stock"}, {Name: "other"}}
	got := intersectHighBoards(configured, active)
	if len(got) != 2 || got[0].Name != "stock" || got[1].Name != "nba" {
		t.Fatalf("intersectHighBoards() = %#v, want stock and nba", got)
	}
}

func TestFilterPollingBoardsHonorsDeploymentAllowlist(t *testing.T) {
	t.Setenv("BOARD_ALLOWLIST", "HardwareSale,MacShop,PC_Shopping")
	got := filterPollingBoards([]*board.Board{
		{Name: "HardwareSale"},
		{Name: "Stock"},
		{Name: "pc_shopping"},
	})
	if len(got) != 2 || got[0].Name != "HardwareSale" || got[1].Name != "pc_shopping" {
		t.Fatalf("filterPollingBoards() = %#v, want HardwareSale and pc_shopping", got)
	}
}

func TestCheckNewArticleDoesNotFetchBoardOutsideDeploymentAllowlist(t *testing.T) {
	t.Setenv("BOARD_ALLOWLIST", "HardwareSale,MacShop,PC_Shopping")
	// A Board without drivers would panic if WithNewArticlesContext were called.
	// Returning safely therefore proves the allowlist gate runs before fetch.
	checkNewArticle(context.Background(), &board.Board{Name: "Stock"}, nil)
}

func TestArticleMatchesSubscriptionUsesTitleAndAuthorSubstrings(t *testing.T) {
	candidate := article.Article{Title: "[情報] TSMC outlook", Author: "SomeAuthor"}

	if !articleMatchesSubscription(candidate, []string{"tsmc"}, nil) {
		t.Fatal("title substring did not match")
	}
	if !articleMatchesSubscription(candidate, nil, []string{"AUTH"}) {
		t.Fatal("case-insensitive author substring did not match")
	}
	if articleMatchesSubscription(candidate, []string{"missing"}, []string{"nobody"}) {
		t.Fatal("unrelated title and author rules matched")
	}
}

func TestArticleMatchesSubscriptionSupportsAuthorAndTitleRule(t *testing.T) {
	candidate := article.Article{
		Title:  "[販售] AirPods Pro 2 耳機",
		Author: "LuShIn",
	}

	if !articleMatchesSubscription(candidate, []string{"author:lushin&pro&耳"}, nil) {
		t.Fatal("author-and-title keyword did not match")
	}
	if articleMatchesSubscription(candidate, []string{"author:other&pro&耳"}, nil) {
		t.Fatal("author-and-title keyword matched the wrong author")
	}
	if articleMatchesSubscription(candidate, []string{"author:lushin&max&耳"}, nil) {
		t.Fatal("author-and-title keyword matched without every title term")
	}
	if articleMatchesSubscription(candidate, []string{"pro&author:lushin"}, nil) {
		t.Fatal("author prefix outside the first term was reinterpreted as an author rule")
	}
	for _, malformed := range []string{"author:&耳", "author:lushin&", "author:lushin&&耳"} {
		if articleMatchesSubscription(candidate, []string{malformed}, nil) {
			t.Errorf("malformed author-and-title keyword %q matched", malformed)
		}
	}
}

func TestLegacyKeywordCheckerSupportsAuthorAndTitleRule(t *testing.T) {
	ch := make(chan Checker, 1)
	bd := &board.Board{Name: "HardwareSale", NewArticles: article.Articles{
		{Code: "M.1.A.001", Title: "[賣] DDR4 3200", Author: "LuShIn"},
		{Code: "M.2.A.002", Title: "[賣] DDR4 3600", Author: "AnotherWriter"},
	}}

	checkKeyword(context.Background(), "author:lushin&賣", bd, Checker{ch: ch})
	select {
	case got := <-ch:
		if len(got.articles) != 1 || got.articles[0].Code != "M.1.A.001" {
			t.Fatalf("matched articles = %#v, want only LuShIn's sale", got.articles)
		}
	default:
		t.Fatal("author-and-title keyword did not produce a checker event")
	}
}

func TestLegacyAuthorCheckerUsesAuthorSubstring(t *testing.T) {
	ch := make(chan Checker, 1)
	bd := &board.Board{Name: "Stock", NewArticles: article.Articles{
		{Code: "M.1.A.001", Author: "SomeAuthor"},
		{Code: "M.2.A.002", Author: "AnotherWriter"},
	}}

	checkAuthor(context.Background(), "AUTH", bd, Checker{ch: ch})
	select {
	case got := <-ch:
		if len(got.articles) != 1 || got.articles[0].Code != "M.1.A.001" {
			t.Fatalf("matched articles = %#v, want only SomeAuthor", got.articles)
		}
	default:
		t.Fatal("author substring did not produce a checker event")
	}
}
