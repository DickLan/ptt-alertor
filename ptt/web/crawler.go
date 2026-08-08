package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/Ptt-Alertor/logrus"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	pttHttp "github.com/Ptt-Alertor/ptt-alertor/ptt/http"

	"github.com/Ptt-Alertor/ptt-alertor/models/pushsum"
	"golang.org/x/net/html"
)

const (
	pttHostURL          = "https://www.ptt.cc"
	maxHTMLResponseSize = 8 << 20
)

var boardIndexPattern = regexp.MustCompile(`^index([0-9]+)\.html$`)

// CheckSiteContext checks PTT through the same rate limiter, retry policy, HTTP
// status validation, and anti-bot challenge detection used by every crawler.
func CheckSiteContext(ctx context.Context) error {
	_, err := fetchHTMLContext(ctx, pttHostURL+"/bbs/index.html")
	return err
}

// CurrentPage find Board Last Page Number
func CurrentPage(board string) (int, error) {
	return CurrentPageContext(context.Background(), board)
}

// CurrentPageContext is CurrentPage with caller cancellation.
func CurrentPageContext(ctx context.Context, board string) (int, error) {
	url := makeBoardURL(board, -1)
	htmlNodes, err := fetchHTMLContext(ctx, url)
	if err != nil {
		return 0, err
	}
	return currentPageFromDocument(htmlNodes, board)
}

// FetchArticles makes board's index articles to a article slice
func FetchArticles(board string, page int) (articles article.Articles, err error) {
	return FetchArticlesContext(context.Background(), board, page)
}

// FetchArticlesContext is FetchArticles with caller cancellation propagated to
// the shared PTT limiter and HTTP request.
func FetchArticlesContext(ctx context.Context, board string, page int) (articles article.Articles, err error) {
	reqURL := makeBoardURL(board, page)
	htmlNodes, err := fetchHTMLContext(ctx, reqURL)
	if err != nil {
		return nil, err
	}
	return articlesFromBoardDocument(htmlNodes, board)
}

func currentPageFromDocument(document *html.Node, board string) (int, error) {
	_, paging, err := boardPageStructure(document)
	if err != nil {
		return 0, err
	}
	for _, anchor := range findNodes(paging, findAnchor) {
		if !strings.Contains(strings.TrimSpace(nodeText(anchor)), "上頁") {
			continue
		}
		link := strings.TrimSpace(getAnchorLink(anchor))
		if link == "" {
			return 1, nil
		}
		parsed, err := url.Parse(link)
		if err != nil || !isLocalOrPTTURL(parsed) || parsed.RawQuery != "" || parsed.Fragment != "" {
			return 0, fmt.Errorf("%w: malformed previous-page link", ErrIncompleteBoardPage)
		}
		parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "bbs" || !strings.EqualFold(parts[1], board) {
			return 0, fmt.Errorf("%w: previous-page link targets another board", ErrIncompleteBoardPage)
		}
		match := boardIndexPattern.FindStringSubmatch(parts[2])
		if len(match) != 2 {
			return 0, fmt.Errorf("%w: malformed previous-page number", ErrIncompleteBoardPage)
		}
		previousPage, err := strconv.Atoi(match[1])
		if err != nil || previousPage < 1 {
			return 0, fmt.Errorf("%w: invalid previous-page number", ErrIncompleteBoardPage)
		}
		return previousPage + 1, nil
	}
	return 0, fmt.Errorf("%w: paging block has no previous-page control", ErrIncompleteBoardPage)
}

func articlesFromBoardDocument(document *html.Node, board string) (article.Articles, error) {
	list, _, err := boardPageStructure(document)
	if err != nil {
		return nil, err
	}
	separatorCount := 0
	for child := list.FirstChild; child != nil; child = child.NextSibling {
		if findDividerDiv(child) != nil {
			separatorCount++
		}
	}
	if separatorCount > 1 {
		return nil, fmt.Errorf("%w: expected at most one list separator, got %d", ErrIncompleteBoardPage, separatorCount)
	}

	articles := make(article.Articles, 0)
	for child := list.FirstChild; child != nil; child = child.NextSibling {
		if findDividerDiv(child) != nil {
			break
		}
		if findArticleBlocks(child) == nil {
			continue
		}
		item, err := parseBoardArticle(child, board)
		if err != nil {
			return nil, err
		}
		articles = append(articles, item)
	}
	// PTT's current board pages can omit the decorative r-list-sep entirely.
	// Still require at least one fully parsed entry in that form, so an empty
	// or incomplete 200 response cannot be accepted as a successful poll.
	if separatorCount == 0 && len(articles) == 0 {
		return nil, fmt.Errorf("%w: list has neither a separator nor article entries", ErrIncompleteBoardPage)
	}
	return articles, nil
}

func boardPageStructure(document *html.Node) (*html.Node, *html.Node, error) {
	mains := findNodes(document, findMainBoardContainer)
	if len(mains) != 1 {
		return nil, nil, fmt.Errorf("%w: expected one main container, got %d", ErrIncompleteBoardPage, len(mains))
	}
	actions := findNodes(mains[0], findBoardActionContainer)
	lists := findNodes(mains[0], findBoardListContainer)
	if len(actions) != 1 || len(lists) != 1 {
		return nil, nil, fmt.Errorf("%w: expected one action and list container", ErrIncompleteBoardPage)
	}
	paging := findNodes(actions[0], findPagingBlock)
	if len(paging) != 1 {
		return nil, nil, fmt.Errorf("%w: expected one paging block, got %d", ErrIncompleteBoardPage, len(paging))
	}
	return lists[0], paging[0], nil
}

func parseBoardArticle(block *html.Node, board string) (article.Article, error) {
	item := article.Article{}
	pushCounts := findNodes(block, findPushCountDiv)
	if len(pushCounts) > 1 {
		return item, fmt.Errorf("%w: article has duplicate push counts", ErrIncompleteBoardPage)
	}
	if len(pushCounts) == 1 {
		item.PushSum = pushsum.ConvertPushCount(strings.TrimSpace(nodeText(pushCounts[0])))
	}

	titles := findNodes(block, findTitleDiv)
	if len(titles) != 1 {
		return item, fmt.Errorf("%w: article has %d title blocks", ErrIncompleteBoardPage, len(titles))
	}
	anchors := findNodes(titles[0], findAnchor)
	switch len(anchors) {
	case 0:
		item.Title = strings.TrimSpace(nodeText(titles[0]))
		if item.Title == "" {
			return item, fmt.Errorf("%w: deleted article title is empty", ErrIncompleteBoardPage)
		}
	case 1:
		item.Title = strings.TrimSpace(nodeText(anchors[0]))
		link, code, id, err := canonicalBoardArticleLink(board, getAnchorLink(anchors[0]))
		if err != nil || item.Title == "" {
			return item, fmt.Errorf("%w: article title or link is malformed", ErrIncompleteBoardPage)
		}
		item.Link, item.Code, item.ID = link, code, id
	default:
		return item, fmt.Errorf("%w: article has duplicate title links", ErrIncompleteBoardPage)
	}

	metas := findNodes(block, findMetaDiv)
	if len(metas) != 1 {
		return item, fmt.Errorf("%w: article has %d metadata blocks", ErrIncompleteBoardPage, len(metas))
	}
	dates := findNodes(metas[0], findDateDiv)
	authors := findNodes(metas[0], findAuthorDiv)
	if len(dates) != 1 || len(authors) != 1 {
		return item, fmt.Errorf("%w: article metadata is incomplete", ErrIncompleteBoardPage)
	}
	item.Date = strings.TrimSpace(nodeText(dates[0]))
	item.Author = strings.TrimSpace(nodeText(authors[0]))
	if item.Date == "" || item.Author == "" {
		return item, fmt.Errorf("%w: article metadata is empty", ErrIncompleteBoardPage)
	}
	return item, nil
}

func canonicalBoardArticleLink(board, rawLink string) (string, string, int, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawLink))
	if err != nil || !isLocalOrPTTURL(parsed) || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", 0, errors.New("malformed article link")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "bbs" || !strings.EqualFold(parts[1], board) {
		return "", "", 0, errors.New("article link targets another board")
	}
	canonical := pttHostURL + parsed.Path
	item := article.Article{}
	code := item.ParseCode(canonical)
	id := item.ParseID(canonical)
	if code == "" || id <= 0 {
		return "", "", 0, errors.New("article code is malformed")
	}
	return canonical, code, id, nil
}

func isLocalOrPTTURL(parsed *url.URL) bool {
	if parsed == nil || parsed.User != nil {
		return false
	}
	if parsed.Scheme == "" && parsed.Host == "" {
		return true
	}
	return parsed.Scheme == "https" && strings.EqualFold(parsed.Host, "www.ptt.cc")
}

func isLastArticleBlock(articleBlock *html.Node) bool {
	for next := articleBlock.NextSibling; ; next = next.NextSibling {
		if next == nil {
			break
		}
		if next.Type == html.ElementNode {
			for _, attr := range next.Attr {
				if attr.Val == "r-list-sep" {
					return true
				}
				return false
			}
		}
	}
	return false
}

// FetchArticle build article object from html
func FetchArticle(board, articleCode string) (article.Article, error) {
	return FetchArticleContext(context.Background(), board, articleCode)
}

// FetchArticleContext is FetchArticle with caller cancellation.
func FetchArticleContext(ctx context.Context, board, articleCode string) (article.Article, error) {
	reqURL := makeArticleURL(board, articleCode)
	htmlNodes, err := fetchHTMLContext(ctx, reqURL)
	if err != nil {
		return article.Article{}, err
	}
	if mainContent := findNodes(htmlNodes, findMainArticleContent); len(mainContent) != 1 {
		return article.Article{}, fmt.Errorf("%w: expected one main-content node, got %d", ErrIncompleteArticlePage, len(mainContent))
	}
	atcl := article.Article{
		Link:  reqURL,
		Code:  articleCode,
		Board: board,
	}
	nodes := findNodes(htmlNodes, findOgTitleMeta)
	if len(nodes) > 0 {
		atcl.Title = getMetaContent(nodes[0])
	} else {
		atcl.Title = "[內文標題已被刪除]"
	}
	atcl.ID = atcl.ParseID(reqURL)
	pushBlocks := findNodes(htmlNodes, findPushBlocks)
	pushes := []article.Comment{}

	for _, pushBlock := range pushBlocks {
		push := article.Comment{}
		dateNodes := findNodes(pushBlock, findPushIPDateTime)
		if len(dateNodes) != 1 || dateNodes[0].FirstChild == nil {
			return article.Article{}, fmt.Errorf("%w: push has no unique timestamp", ErrIncompleteArticlePage)
		}
		ipdatetime := strings.TrimSpace(dateNodes[0].FirstChild.Data)
		dateTime, err := parseDateTime(ipdatetime)
		if err != nil {
			return article.Article{}, fmt.Errorf("%w: parse push timestamp: %v", ErrIncompleteArticlePage, err)
		}
		push.DateTime = dateTime

		tagNodes := findNodes(pushBlock, findPushTag)
		userNodes := findNodes(pushBlock, findPushUserID)
		contentNodes := findNodes(pushBlock, findPushContent)
		if len(tagNodes) != 1 || tagNodes[0].FirstChild == nil ||
			len(userNodes) != 1 || userNodes[0].FirstChild == nil ||
			len(contentNodes) != 1 || contentNodes[0].FirstChild == nil {
			return article.Article{}, fmt.Errorf("%w: push fields are incomplete", ErrIncompleteArticlePage)
		}
		push.Tag = tagNodes[0].FirstChild.Data
		push.UserID = userNodes[0].FirstChild.Data
		pushContent := contentNodes[0]
		content := pushContent.FirstChild.Data
		for n := pushContent.FirstChild.NextSibling; n != nil; n = n.NextSibling {
			if findEmailProtected(n) != nil {
				break
			}
			if n.FirstChild != nil {
				content += n.FirstChild.Data
			}
			if n.NextSibling != nil {
				content += n.NextSibling.Data
			}
		}
		push.Content = content
		pushes = append(pushes, push)
	}
	if len(pushes) > 0 {
		atcl.LastPushDateTime = pushes[len(pushes)-1].DateTime
	}
	atcl.Comments = pushes
	return atcl, nil
}

func parseDateTime(ipdatetime string) (time.Time, error) {
	re, _ := regexp.Compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)?\\s*(.*)")
	subMatches := re.FindStringSubmatch(ipdatetime)
	dateTime := strings.TrimSpace(subMatches[len(subMatches)-1])
	loc := time.FixedZone("CST", 8*60*60)
	t, err := time.ParseInLocation("01/02 15:04", dateTime, loc)
	if err != nil {
		return time.Time{}, err
	}
	t = t.AddDate(getYear(t), 0, 0)
	return t, nil
}

func getYear(commentTime time.Time) int {
	t := time.Now()
	commentTime = commentTime.AddDate(t.Year(), 0, 0)
	if commentTime.After(t) {
		return t.Year() - 1
	}
	return t.Year()
}

// CheckBoardExist use for checking board exist or not
func CheckBoardExist(board string) bool {
	return checkURLExist(makeBoardURL(board, -1))
}

// CheckArticleExist user for checking article exist or not
func CheckArticleExist(board, articleCode string) bool {
	return checkURLExist(makeArticleURL(board, articleCode))
}

func checkURLExist(url string) bool {
	req, err := pttHttp.HttpRequest(url)
	if err != nil {
		return false
	}
	resp, err := pttHttp.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

func makeBoardURL(board string, page int) string {
	var pageStr string
	if page < 0 {
		pageStr = ""
	} else {
		pageStr = strconv.Itoa(page)
	}
	return pttHostURL + "/bbs/" + board + "/index" + pageStr + ".html"
}

func makeArticleURL(board, articleCode string) string {
	return pttHostURL + "/bbs/" + board + "/" + articleCode + ".html"
}

// URLNotFoundError is an error type present 404 Not Found
type URLNotFoundError struct {
	URL string
}

func (u URLNotFoundError) Error() string {
	return "Fetched URL Not Found"
}

// HTTPStatusError reports a non-success response from PTT.
type HTTPStatusError struct {
	URL        string
	StatusCode int
}

func (e HTTPStatusError) Error() string {
	return fmt.Sprintf("PTT returned HTTP %d for %s", e.StatusCode, e.URL)
}

var ErrOver18ConsentRequired = errors.New("PTT_OVER18=true is required for an age-restricted board")
var ErrBotChallenge = errors.New("PTT returned an anti-bot challenge page")
var ErrUnexpectedRedirect = errors.New("PTT returned an unexpected redirect")
var ErrIncompleteArticlePage = errors.New("PTT article page structure is incomplete")
var ErrIncompleteBoardPage = errors.New("PTT board page structure is incomplete")
var ErrHTMLResponseTooLarge = errors.New("PTT HTML response exceeds the safe size limit")
var reportBotChallenge = pttHttp.ReportChallenge

func fetchHTML(reqURL string) (doc *html.Node, err error) {
	return fetchHTMLContext(context.Background(), reqURL)
}

func fetchHTMLContext(ctx context.Context, reqURL string) (doc *html.Node, err error) {
	req, err := pttHttp.HttpRequestWithContext(ctx, reqURL)
	if err != nil {
		return nil, err
	}
	resp, err := pttHttp.Do(req)
	if err != nil {
		log.WithField("url", reqURL).WithError(err).Error("Fetch URL Failed")
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		err = URLNotFoundError{reqURL}
		return nil, err
	}
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
		if location := resp.Header.Get("Location"); strings.Contains(location, "/ask/over18") {
			return nil, ErrOver18ConsentRequired
		}
		return nil, ErrUnexpectedRedirect
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, HTTPStatusError{URL: reqURL, StatusCode: resp.StatusCode}
	}

	if resp.Body == nil {
		return nil, errors.New("PTT returned an empty response body")
	}
	content, err := readHTMLBody(resp.Body)
	if err != nil {
		return nil, err
	}
	if looksLikeBotChallenge(content) {
		reportBotChallenge()
		return nil, ErrBotChallenge
	}
	if bytes.Contains(content, []byte("document.cookie.indexOf('over18=1')")) &&
		!strings.EqualFold(os.Getenv("PTT_OVER18"), "true") {
		return nil, ErrOver18ConsentRequired
	}

	doc, err = html.Parse(bytes.NewReader(content))
	if err != nil {
		log.WithError(err).Error("Crawler Fetch HTML Failed")
		return nil, err
	}
	return doc, nil
}

func readHTMLBody(reader io.Reader) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(reader, maxHTMLResponseSize+1))
	if err != nil {
		return nil, err
	}
	if len(content) > maxHTMLResponseSize {
		return nil, ErrHTMLResponseTooLarge
	}
	return content, nil
}

func looksLikeBotChallenge(content []byte) bool {
	lower := bytes.ToLower(content)
	head := lower
	if end := bytes.Index(lower, []byte("</head>")); end >= 0 {
		head = lower[:end]
	} else if len(head) > 64<<10 {
		head = head[:64<<10]
	}
	if bytes.Contains(head, []byte("<title>just a moment")) {
		return true
	}
	return bytes.Contains(lower, []byte("cdn-cgi/challenge-platform")) ||
		bytes.Contains(lower, []byte("cf-chl-")) ||
		bytes.Contains(lower, []byte("cf-browser-verification"))
}
