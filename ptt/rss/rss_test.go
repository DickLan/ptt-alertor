package rss

import (
	"context"
	"errors"
	"strings"
	"testing"

	gock "gopkg.in/h2non/gock.v1"
)

func TestReadAtomBodyEnforcesByteLimitWithoutTruncation(t *testing.T) {
	exact, err := readAtomBody(strings.NewReader(strings.Repeat("x", maxAtomResponseSize)))
	if err != nil || len(exact) != maxAtomResponseSize {
		t.Fatalf("exact-limit body length = %d, error = %v", len(exact), err)
	}

	if _, err := readAtomBody(strings.NewReader(strings.Repeat("x", maxAtomResponseSize+1))); !errors.Is(err, ErrAtomResponseTooLarge) {
		t.Fatalf("oversized body error = %v, want ErrAtomResponseTooLarge", err)
	}
}

func TestBuildArticlesRejectsOversizedAtomBeforeParsing(t *testing.T) {
	defer gock.Off()
	originalReport := reportChallenge
	reported := false
	reportChallenge = func() { reported = true }
	defer func() { reportChallenge = originalReport }()

	gock.New("https://www.ptt.cc").Get("/atom/oversized.xml").Reply(200).
		SetHeader("Content-Type", "text/html").
		BodyString(strings.Repeat("x", maxAtomResponseSize+1))

	if _, err := BuildArticles("oversized"); !errors.Is(err, ErrAtomResponseTooLarge) {
		t.Fatalf("BuildArticles() error = %v, want ErrAtomResponseTooLarge", err)
	}
	if reported {
		t.Fatal("oversized Atom response was challenge-sniffed before enforcing the byte cap")
	}
}

func TestBuildArticlesContextPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BuildArticlesContext(ctx, "movie")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildArticlesContext() error = %v, want context.Canceled", err)
	}
	if !errors.Is(err, ErrTemporarilyUnavailable) {
		t.Fatalf("BuildArticlesContext() error = %v, want ErrTemporarilyUnavailable", err)
	}
}

func TestBuildArticlesTreatsHTMLAsChallengeAndReportsCooldown(t *testing.T) {
	defer gock.Off()
	originalReport := reportChallenge
	reported := false
	reportChallenge = func() { reported = true }
	defer func() { reportChallenge = originalReport }()

	gock.New("https://www.ptt.cc").Get("/atom/challenge.xml").Reply(200).
		SetHeader("Content-Type", "text/html").BodyString("<html><title>challenge</title></html>")
	_, err := BuildArticles("challenge")
	if !errors.Is(err, ErrTemporarilyUnavailable) {
		t.Fatalf("BuildArticles() error = %v, want ErrTemporarilyUnavailable", err)
	}
	if !reported {
		t.Fatal("HTML Atom challenge did not report a shared cooldown")
	}
}

func TestBuildArticlesDetectsHTMLChallengeWithoutContentType(t *testing.T) {
	defer gock.Off()
	originalReport := reportChallenge
	reported := false
	reportChallenge = func() { reported = true }
	defer func() { reportChallenge = originalReport }()

	gock.New("https://www.ptt.cc").Get("/atom/no-header.xml").Reply(200).
		BodyString("<!DOCTYPE html><html><title>challenge</title></html>")
	_, err := BuildArticles("no-header")
	if !errors.Is(err, ErrTemporarilyUnavailable) {
		t.Fatalf("BuildArticles() error = %v, want ErrTemporarilyUnavailable", err)
	}
	if !reported {
		t.Fatal("headerless HTML challenge did not report a shared cooldown")
	}
}

func TestBuildArticlesDoesNotFollowRedirectOrFallback(t *testing.T) {
	defer gock.Off()
	gock.New("https://www.ptt.cc").Get("/atom/redirect.xml").Reply(302).
		SetHeader("Location", "https://www.ptt.cc/atom/other.xml")
	_, err := BuildArticles("redirect")
	if !errors.Is(err, ErrTemporarilyUnavailable) {
		t.Fatalf("BuildArticles() error = %v, want ErrTemporarilyUnavailable", err)
	}
	if pending := gock.Pending(); len(pending) != 0 {
		t.Fatalf("unmatched HTTP mocks after redirect: %d", len(pending))
	}
}

func TestBuildArticlesReturnsTypedAtomNotFound(t *testing.T) {
	defer gock.Off()
	gock.New("https://www.ptt.cc").Get("/atom/Missing.xml").Reply(404)
	if _, err := BuildArticles("Missing"); !errors.Is(err, ErrFeedNotFound) {
		t.Fatalf("BuildArticles() error = %v, want ErrFeedNotFound", err)
	}
}

func TestBuildArticlesPopulatesDistinctFullCodesForSameSecond(t *testing.T) {
	defer gock.Off()
	gock.New("https://www.ptt.cc").Get("/atom/same-second.xml").Reply(200).BodyString(`
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>same-second</title><id>https://www.ptt.cc/atom/same-second.xml</id>
  <updated>2026-07-13T00:00:01+08:00</updated>
  <entry><title>second</title><id>https://www.ptt.cc/bbs/Test/M.1700000000.A.002.html</id>
    <published>2026-07-13T00:00:01+08:00</published><updated>2026-07-13T00:00:01+08:00</updated>
    <author><name>two</name></author></entry>
  <entry><title>first</title><id>https://www.ptt.cc/bbs/Test/M.1700000000.A.001.html</id>
    <published>2026-07-13T00:00:00+08:00</published><updated>2026-07-13T00:00:00+08:00</updated>
    <author><name>one</name></author></entry>
</feed>`)

	articles, err := BuildArticles("same-second")
	if err != nil {
		t.Fatalf("BuildArticles() error = %v", err)
	}
	if len(articles) != 2 {
		t.Fatalf("BuildArticles() returned %d articles, want 2", len(articles))
	}
	if articles[0].ID != articles[1].ID {
		t.Fatalf("article IDs = %d and %d, want the same compatibility ID", articles[0].ID, articles[1].ID)
	}
	if articles[0].Code != "M.1700000000.A.002" || articles[1].Code != "M.1700000000.A.001" {
		t.Fatalf("article codes = %q and %q, want distinct full codes", articles[0].Code, articles[1].Code)
	}
	if articles[0].Identity() == articles[1].Identity() {
		t.Fatalf("same-second article identities collided: %q", articles[0].Identity())
	}
}

func TestBuildArticlesAllowsEntryWithoutAuthor(t *testing.T) {
	defer gock.Off()
	gock.New("https://www.ptt.cc").Get("/atom/no-author.xml").Reply(200).BodyString(`
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>no-author</title><id>https://www.ptt.cc/atom/no-author.xml</id>
  <updated>2026-07-13T00:00:00+08:00</updated>
  <entry><title>test</title><id>https://www.ptt.cc/bbs/Test/M.1.A.001.html</id>
    <published>2026-07-13T00:00:00+08:00</published>
    <updated>2026-07-13T00:00:00+08:00</updated></entry>
</feed>`)

	articles, err := BuildArticles("no-author")
	if err != nil {
		t.Fatalf("BuildArticles() error = %v", err)
	}
	if len(articles) != 1 || articles[0].Author != "" {
		t.Fatalf("articles = %#v, want one article with empty author", articles)
	}
}

func TestCheckBoardExist(t *testing.T) {
	defer gock.Off()
	gock.New("https://www.ptt.cc").Get("/atom/movie.xml").Reply(200).BodyString(`
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>movie</title><id>https://www.ptt.cc/atom/movie.xml</id>
  <updated>2026-07-13T00:00:00+08:00</updated>
  <entry><title>test</title><id>https://www.ptt.cc/bbs/movie/M.1.A.001.html</id>
    <updated>2026-07-13T00:00:00+08:00</updated></entry>
</feed>`)
	gock.New("https://www.ptt.cc").Get("/atom/movies.xml").Reply(404)

	type args struct {
		board string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"exist", args{"movie"}, true},
		{"not exist", args{"movies"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckBoardExist(tt.args.board); got != tt.want {
				t.Errorf("CheckBoardExist() = %v, want %v", got, tt.want)
			}
		})
	}
}
