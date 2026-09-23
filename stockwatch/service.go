package stockwatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
	"github.com/garyburd/redigo/redis"
)

var codePattern = regexp.MustCompile(`^[MG]\.([0-9]{9,12})\.A\.[A-Za-z0-9_-]{1,16}$`)

// PTT Atom can display "ID (nickname)"; HTML index uses the bare ID.
// Parse that envelope, then match the complete ID (never a substring).
var displayedAuthorPattern = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9_]{0,31})(?:[ \t]*\([^\r\n]*\))?$`)

var targetTitlePattern = regexp.MustCompile(`(?i)^(?:(?:re|fw)[ \t]*:[ \t]*){0,8}\[[ \t]*標的[ \t]*\]`)

// IsTargetTitle recognizes the PTT title category, including reply/forward
// prefixes. It does not classify the recommendation's direction or AI status.
func IsTargetTitle(title string) bool {
	return targetTitlePattern.MatchString(strings.TrimSpace(title))
}

func scopeAllows(scope, title string) bool {
	return scope == "all" || scope == "targets" && IsTargetTitle(title)
}

// Keep the existing outbox kind, IDs and immutable payload compatible. Both old
// and new messages store the bounded source title in this exact first envelope.
func queuedTitle(item *outbox.ClaimedItem, author Author) (string, bool) {
	if len(item.Chunks) == 0 {
		return "", false
	}
	prefix := "【專家追蹤・Stock 新文章】\n作者：" + author.Author + "\n"
	if !strings.HasPrefix(item.Chunks[0], prefix) {
		return "", false
	}
	title, _, ok := strings.Cut(strings.TrimPrefix(item.Chunks[0], prefix), "\n")
	return title, ok
}

func authorKey(raw string) string {
	m := displayedAuthorPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(m) != 2 {
		return ""
	}
	return strings.ToLower(m[1])
}

type Service struct {
	Store       *Store
	Outbox      *outbox.Redis
	Client      *discord.Client
	Enabled     bool
	ConfigError string
	// Fetch is injectable for isolated tests; production uses the existing paced PTT source.
	Fetch func(context.Context, *board.Board) error
}

func New(store *Store, enabled bool, webhook, oldWebhook, token string) *Service {
	s := &Service{Store: store, Enabled: enabled, Client: discord.New(discord.Config{WebhookURL: webhook})}
	s.Outbox = outbox.NewRedis(outbox.RedisConfig{Connect: store.Connect, Prefix: store.key("outbox"), HighWatermark: 1000, CounterKey: store.key("sent_count"), CounterChannel: store.key("sent_counter")})
	s.Fetch = func(ctx context.Context, b *board.Board) error { return b.WithNewArticlesContext(ctx) }
	switch {
	case !enabled:
		s.ConfigError = "disabled"
	case len(token) < 32:
		s.ConfigError = "api_token_missing"
	case s.Client.Validate() != nil:
		s.ConfigError = "webhook_invalid"
	case strings.TrimSpace(webhook) == strings.TrimSpace(oldWebhook):
		s.ConfigError = "webhook_matches_original"
	}
	return s
}

func (s *Service) Ready() bool { return s.Enabled && s.ConfigError == "" }

type Snapshot struct {
	Configured      bool                `json:"configured"`
	ConfigError     string              `json:"config_error"`
	IntervalSeconds int64               `json:"interval_seconds"`
	Authors         []Author            `json:"authors"`
	State           map[string]string   `json:"state"`
	Pending         int64               `json:"pending"`
	SentCount       int64               `json:"sent_count"`
	Recent          []map[string]string `json:"recent"`
	Invalid         map[string]string   `json:"invalid"`
}

func (s *Service) Snapshot(ctx context.Context) (Snapshot, error) {
	r := Snapshot{Configured: s.Ready(), ConfigError: s.ConfigError, IntervalSeconds: IntervalSeconds}
	var err error
	if r.Authors, err = s.Store.Authors(); err != nil {
		return r, err
	}
	if r.State, err = s.Store.State(); err != nil {
		return r, err
	}
	if r.Pending, err = s.Outbox.PendingCount(ctx); err != nil {
		return r, err
	}
	if r.Recent, err = s.Store.Recent(); err != nil {
		return r, err
	}
	c := s.Store.Connect()
	defer c.Close()
	r.SentCount, err = redis.Int64(c.Do("GET", s.Store.key("sent_count")))
	if err == redis.ErrNil {
		err = nil
	}
	if err != nil {
		return r, err
	}
	r.Invalid, err = redis.StringMap(c.Do("HGETALL", s.Store.key("invalid")))
	return r, err
}

func codeOf(a article.Article) string {
	if a.Code != "" {
		return a.Code
	}
	return a.ParseCode(a.Link)
}
func codeTimestamp(a article.Article) (int64, bool) {
	m := codePattern.FindStringSubmatch(codeOf(a))
	if len(m) != 2 {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	return n, err == nil && n > 0
}
func articleURL(code string) string { return "https://www.ptt.cc/bbs/Stock/" + code + ".html" }

func (s *Service) Eligible(_ context.Context, item *outbox.ClaimedItem) (bool, error) {
	parts := strings.Split(item.Kind, ":")
	if len(parts) != 4 || parts[0] != "stockwatch" || !codePattern.MatchString(parts[3]) {
		return false, errors.New("invalid stock watch notification")
	}
	authors, err := s.Store.Authors()
	if err != nil {
		return false, err
	}
	for _, a := range authors {
		if a.Key == parts[1] && a.Generation == parts[2] {
			if a.ArticleScope == "all" {
				return true, nil
			}
			if a.ArticleScope != "targets" {
				return false, nil
			}
			title, ok := queuedTitle(item, a)
			if !ok {
				log.Warn("Stock notification cancelled: scope_payload_unparsable")
				return false, nil
			}
			return IsTargetTitle(title), nil
		}
	}
	return false, nil
}

func (s *Service) Tick(parent context.Context) error {
	if !s.Ready() {
		return nil
	}
	a, err := s.Store.Begin()
	if err != nil || a == nil {
		return err
	}
	defer s.Store.Release(a)
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	fail := func(code string) error {
		e := s.Store.Finish(a, nil, code, 0, 0)
		if e != nil {
			return e
		}
		return errors.New(code)
	}
	high, _, err := s.Outbox.HighWatermarkReached(ctx)
	if err != nil {
		return fail("outbox_unavailable")
	}
	if high {
		return fail("delivery_backlog")
	}
	authors, err := s.Store.Authors()
	if err != nil {
		return fail("author_store_unavailable")
	}
	if len(authors) == 0 {
		return nil
	}
	boundary := authors[0].ActivatedAt
	for _, author := range authors {
		if author.ActivatedAt < boundary {
			boundary = author.ActivatedAt
		}
	}
	driver := cursorDriver{store: s.Store, attempt: a, boundary: boundary}
	b := board.NewBoard(driver, new(board.Redis))
	b.Name = "Stock"
	if err = s.Fetch(ctx, b); err != nil {
		if errors.Is(err, board.ErrCatchUpPageLimit) {
			return fail("ptt_catchup_limit")
		}
		return fail("ptt_fetch_failed")
	}
	if len(b.OnlineArticles) == 0 {
		return fail("ptt_empty_snapshot")
	}
	// Refresh configuration after network I/O. Removal and empty->new epoch are authoritative.
	if err = s.Store.Fence(a); err != nil {
		return err
	}
	authors, err = s.Store.Authors()
	if err != nil {
		return fail("author_store_unavailable")
	}
	byKey := make(map[string]Author, len(authors))
	for _, author := range authors {
		byKey[author.Key] = author
	}
	validOnline := make(article.Articles, 0, len(b.OnlineArticles))
	for _, item := range b.OnlineArticles {
		if ts, ok := codeTimestamp(item); ok {
			if ts > a.StartedMS/1000+180 {
				return fail("ptt_future_article")
			}
			item.ID = int(ts)
			validOnline = append(validOnline, item)
		}
	}
	if len(validOnline) == 0 {
		return fail("ptt_invalid_snapshot")
	}
	items := append(article.Articles(nil), b.NewArticles...)
	sort.SliceStable(items, func(i, j int) bool { x, _ := codeTimestamp(items[i]); y, _ := codeTimestamp(items[j]); return x < y })
	matched := 0
	hold := false
	seen := map[string]bool{}
	seenInvalid := map[string]bool{}
	for _, item := range items {
		author, watched := byKey[authorKey(item.Author)]
		if !watched || !scopeAllows(author.ArticleScope, item.Title) {
			continue
		}
		ts, ok := codeTimestamp(item)
		if !ok {
			raw := item.Identity()
			if raw == "" {
				raw = item.Author + "|" + item.Title
			}
			digest := sha256.Sum256([]byte(raw))
			identity := hex.EncodeToString(digest[:8]) + " " + stringPrefix(raw, 100)
			if seenInvalid[identity] {
				continue
			}
			seenInvalid[identity] = true
			skipped, e := s.Store.MarkMalformed(a, identity)
			if e != nil {
				return fail("malformed_registry_unavailable")
			}
			hold = hold || !skipped
			continue
		}
		// The synthetic boundary controls crawl depth only. This is the authorization gate.
		if ts < author.ActivatedAt {
			continue
		}
		// A future filename is not evidence of a currently published article.
		if ts > a.StartedMS/1000+180 {
			return fail("ptt_future_article")
		}
		code := codeOf(item)
		if seen[code] {
			continue
		}
		seen[code] = true
		if err = s.Store.Fence(a); err != nil {
			return err
		}
		content := fmt.Sprintf("【專家追蹤・Stock 新文章】\n作者：%s\n%s\n發文：%s（台灣時間）\n原文：%s\n作者歷史：https://stock.littlesheng.com/experts?author=%s\n通知依作者新發文觸發，尚未經 AI 判讀。", author.Author, stringPrefix(item.Title, 250), time.Unix(ts, 0).In(time.FixedZone("Taipei", 8*60*60)).Format("2006-01-02 15:04:05"), articleURL(code), url.QueryEscape(author.Key))
		kind := "stockwatch:" + author.Key + ":" + author.Generation + ":" + code
		notification, e := outbox.NewItem(kind, []string{"stock", author.Key, author.Generation, code}, discord.SplitContent(content), true)
		if e != nil {
			return fail("notification_invalid")
		}
		if _, e = s.Outbox.Enqueue(ctx, notification); e != nil && !errors.Is(e, outbox.ErrEventConflict) {
			return fail("outbox_enqueue_failed")
		}
		matched++
	}
	if hold {
		return fail("article_code_unresolved")
	}
	return s.Store.Finish(a, validOnline, "", len(items), matched)
}

func stringPrefix(value string, n int) string {
	r := []rune(value)
	if len(r) > n {
		return string(r[:n]) + "…"
	}
	return value
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		_ = s.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
