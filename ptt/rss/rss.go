package rss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	pttHttp "github.com/Ptt-Alertor/ptt-alertor/ptt/http"
	"github.com/mmcdole/gofeed"
)

const maxAtomResponseSize = 2 << 20

var ErrTooManyRequests = errors.New("Too Many Requests")
var ErrTemporarilyUnavailable = errors.New("PTT Temporarily Unavailable")
var ErrFeedNotFound = errors.New("PTT Atom feed not found")
var ErrAtomResponseTooLarge = errors.New("PTT Atom response exceeds the safe size limit")
var reportChallenge = pttHttp.ReportChallenge

// CheckBoardExist use for checking board exist or not
func CheckBoardExist(board string) bool {
	exists, _ := CheckBoardExistE(board)
	return exists
}

// CheckBoardExistE distinguishes a missing board from a temporary PTT error.
func CheckBoardExistE(board string) (bool, error) {
	return CheckBoardExistContext(context.Background(), board)
}

// CheckBoardExistContext distinguishes a missing board from a temporary PTT
// failure and propagates caller cancellation.
func CheckBoardExistContext(ctx context.Context, board string) (bool, error) {
	feed, err := parseURLContext(ctx, "https://www.ptt.cc/atom/"+board+".xml")
	if err != nil {
		if herr, ok := err.(gofeed.HTTPError); ok {
			switch herr.StatusCode {
			case http.StatusNotFound:
				return false, nil
			case http.StatusTooManyRequests:
				return false, ErrTooManyRequests
			}
		}
		return false, err
	}
	// category didn't have any items but has rss link as well
	if len(feed.Items) == 0 {
		return false, nil
	}
	return true, nil
}

func BuildArticles(board string) (articles article.Articles, err error) {
	return BuildArticlesContext(context.Background(), board)
}

// BuildArticlesContext is BuildArticles with caller cancellation propagated to
// the shared PTT limiter and HTTP request.
func BuildArticlesContext(ctx context.Context, board string) (articles article.Articles, err error) {
	feed, err := parseURLContext(ctx, "https://www.ptt.cc/atom/"+board+".xml")
	if err != nil {
		if herr, ok := err.(gofeed.HTTPError); ok {
			switch herr.StatusCode {
			case http.StatusNotFound:
				return nil, ErrFeedNotFound
			case http.StatusTooManyRequests:
				return nil, ErrTooManyRequests
			}
		}
		return nil, err
	}
	for _, item := range feed.Items {
		if item == nil {
			continue
		}
		authorName := ""
		if item.Author != nil {
			authorName = item.Author.Name
		}
		article := article.Article{
			Title:  item.Title,
			Link:   item.GUID,
			Date:   item.Published,
			Author: authorName,
		}
		article.Code = article.ParseCode(item.GUID)
		article.ID = article.ParseID(item.GUID)
		articles = append(articles, article)
	}
	return articles, nil
}

var fp = gofeed.NewParser()

func parseURL(feedURL string) (feed *gofeed.Feed, err error) {
	return parseURLContext(context.Background(), feedURL)
}

func parseURLContext(ctx context.Context, feedURL string) (feed *gofeed.Feed, err error) {
	req, err := pttHttp.HttpRequestWithContext(ctx, feedURL)
	if err != nil {
		return nil, err
	}
	resp, err := pttHttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTemporarilyUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode >= http.StatusInternalServerError {
		return nil, ErrTemporarilyUnavailable
	}
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
		return nil, ErrTemporarilyUnavailable
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, gofeed.HTTPError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
		}
	}
	content, err := readAtomBody(resp.Body)
	if err != nil {
		return nil, err
	}
	prefix := content
	if len(prefix) > 512 {
		prefix = prefix[:512]
	}
	trimmedPrefix := bytes.ToLower(bytes.TrimSpace(prefix))
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType == "text/html" || bytes.HasPrefix(trimmedPrefix, []byte("<!doctype html")) ||
		bytes.HasPrefix(trimmedPrefix, []byte("<html")) {
		// A successful HTML response at an Atom URL is commonly a proxy or
		// anti-bot challenge. Do not double traffic by falling back to HTML.
		reportChallenge()
		return nil, ErrTemporarilyUnavailable
	}

	return fp.Parse(bytes.NewReader(content))
}

func readAtomBody(reader io.Reader) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(reader, maxAtomResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read PTT Atom response: %w", err)
	}
	if len(content) > maxAtomResponseSize {
		return nil, ErrAtomResponseTooLarge
	}
	return content, nil
}
