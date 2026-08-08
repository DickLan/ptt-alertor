package jobs

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
)

const discordEventDedupeTTL = 10 * time.Minute

type eventDeduper struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]time.Time
}

var discordEvents = newEventDeduper(discordEventDedupeTTL)

func newEventDeduper(ttl time.Duration) *eventDeduper {
	return &eventDeduper{ttl: ttl, seen: make(map[string]time.Time)}
}

func (d *eventDeduper) claim(key string) bool {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for existing, expires := range d.seen {
		if !expires.After(now) {
			delete(d.seen, existing)
		}
	}
	if expires, exists := d.seen[key]; exists && expires.After(now) {
		return false
	}
	d.seen[key] = now.Add(d.ttl)
	return true
}

func (d *eventDeduper) release(keys []string) {
	d.mu.Lock()
	for _, key := range keys {
		delete(d.seen, key)
	}
	d.mu.Unlock()
}

// prepareDiscordEvent removes articles already queued for the single global
// webhook. This prevents duplicate messages when several users, keywords, or
// author rules match the same new article.
func prepareDiscordEvent(value check) (check, []string, bool) {
	switch event := value.(type) {
	case Checker:
		filtered, keys := claimArticles(event.articles, "article:"+strings.ToLower(event.board)+":")
		if len(filtered) == 0 {
			return event, keys, false
		}
		event.articles = filtered
		return event, keys, true
	case pushSumChecker:
		prefix := fmt.Sprintf("pushsum:%s:%s:%s:", strings.ToLower(event.board), event.subType, event.word)
		filtered, keys := claimArticles(event.articles, prefix)
		if len(filtered) == 0 {
			return event, keys, false
		}
		event.articles = filtered
		return event, keys, true
	case commentChecker:
		revision := event.Article.LastPushDateTime.UTC().Format(time.RFC3339Nano)
		if revision == "0001-01-01T00:00:00Z" {
			revision = event.Article.Comments.String()
		}
		key := fmt.Sprintf("comment:%s:%s:%s", strings.ToLower(event.Article.Board), event.Article.Code, revision)
		if !discordEvents.claim(key) {
			return event, nil, false
		}
		return event, []string{key}, true
	default:
		return value, nil, true
	}
}

func claimArticles(articles article.Articles, prefix string) (article.Articles, []string) {
	filtered := make(article.Articles, 0, len(articles))
	keys := make([]string, 0, len(articles))
	for _, item := range articles {
		identity := item.Identity()
		key := prefix + identity
		if identity == "" || !discordEvents.claim(key) {
			continue
		}
		filtered = append(filtered, item)
		keys = append(keys, key)
	}
	return filtered, keys
}
