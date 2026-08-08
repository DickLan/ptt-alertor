package jobs

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

func TestCheckerAlertIsDeliveredToDiscord(t *testing.T) {
	var (
		requests   int32
		increments int32
		content    string
		mentions   []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var payload struct {
			Content         string `json:"content"`
			AllowedMentions struct {
				Parse []string `json:"parse"`
			} `json:"allowed_mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		content = payload.Content
		mentions = payload.AllowedMentions.Parse
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1234567890"}`))
	}))
	defer server.Close()

	originalWebhook := discordWebhook
	originalIncrement := incrementAlert
	originalDedupe := discordEvents
	discordWebhook = discord.New(discord.Config{WebhookURL: server.URL})
	discordEvents = newEventDeduper(discordEventDedupeTTL)
	incrementAlert = func() error {
		atomic.AddInt32(&increments, 1)
		return nil
	}
	defer func() {
		discordWebhook = originalWebhook
		incrementAlert = originalIncrement
		discordEvents = originalDedupe
	}()

	alert := Checker{
		board:   "NBA",
		keyword: "trade",
		word:    "trade",
		Profile: user.Profile{Account: "discord-main", Discord: true},
		articles: article.Articles{{
			Title: "[新聞] trade @everyone",
			Link:  "https://www.ptt.cc/bbs/NBA/M.1.A.001.html",
		}},
	}
	sendMessage(alert)

	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("Discord requests = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&increments); got != 1 {
		t.Fatalf("alert counter increments = %d, want 1", got)
	}
	if !strings.Contains(content, alert.articles[0].Title) || !strings.Contains(content, alert.articles[0].Link) {
		t.Fatalf("Discord content = %q, want title and link", content)
	}
	if mentions == nil || len(mentions) != 0 {
		t.Fatalf("allowed_mentions.parse = %#v, want empty", mentions)
	}
}

func TestDiscordAlertDeduplicatesOverlappingGlobalWebhookEvents(t *testing.T) {
	var (
		requests int32
		mu       sync.Mutex
		contents []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		mu.Lock()
		contents = append(contents, payload.Content)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1234567890"}`))
	}))
	defer server.Close()

	originalWebhook := discordWebhook
	originalIncrement := incrementAlert
	originalDedupe := discordEvents
	discordWebhook = discord.New(discord.Config{WebhookURL: server.URL})
	incrementAlert = func() error { return nil }
	discordEvents = newEventDeduper(discordEventDedupeTTL)
	defer func() {
		discordWebhook = originalWebhook
		incrementAlert = originalIncrement
		discordEvents = originalDedupe
	}()

	articleA := article.Article{ID: 1, Title: "article A", Link: "https://www.ptt.cc/bbs/NBA/M.1.A.001.html"}
	articleB := article.Article{ID: 2, Title: "article B", Link: "https://www.ptt.cc/bbs/NBA/M.2.A.002.html"}
	articleC := article.Article{ID: 3, Title: "article C", Link: "https://www.ptt.cc/bbs/NBA/M.3.A.003.html"}
	first := Checker{
		board: "NBA", word: "trade", subType: "keyword",
		Profile:  user.Profile{Account: "first", Discord: true},
		articles: article.Articles{articleA, articleB},
	}
	second := Checker{
		board: "NBA", word: "author", subType: "author",
		Profile:  user.Profile{Account: "second", Discord: true},
		articles: article.Articles{articleB, articleC},
	}
	sendMessage(first)
	sendMessage(second)
	sendMessage(first)

	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("Discord requests = %d, want 2", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(contents) != 2 {
		t.Fatalf("payloads = %d, want 2", len(contents))
	}
	if strings.Contains(contents[1], "article B") || !strings.Contains(contents[1], "article C") {
		t.Fatalf("second payload = %q, want only previously unseen article C", contents[1])
	}
}

func TestDiscordAlertDoesNotMergeDifferentArticlesFromSameSecond(t *testing.T) {
	var requests int32
	var content string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		content = payload.Content
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1234567890"}`))
	}))
	defer server.Close()

	originalWebhook := discordWebhook
	originalIncrement := incrementAlert
	originalDedupe := discordEvents
	discordWebhook = discord.New(discord.Config{WebhookURL: server.URL})
	incrementAlert = func() error { return nil }
	discordEvents = newEventDeduper(discordEventDedupeTTL)
	defer func() {
		discordWebhook = originalWebhook
		incrementAlert = originalIncrement
		discordEvents = originalDedupe
	}()

	alert := Checker{
		board: "allpost", word: "regexp:.*", subType: "keyword",
		Profile: user.Profile{Account: "discord-main", Discord: true},
		articles: article.Articles{
			{ID: 1700000000, Code: "M.1700000000.A.AAA", Title: "first"},
			{ID: 1700000000, Code: "M.1700000000.A.BBB", Title: "second"},
		},
	}
	sendMessage(alert)
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("Discord requests = %d, want one batch", got)
	}
	if !strings.Contains(content, "first") || !strings.Contains(content, "second") {
		t.Fatalf("payload = %q, want both same-second articles", content)
	}
}

func TestDiscordAlertReleasesDedupeClaimAfterDeliveryFailure(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1234567890"}`))
	}))
	defer server.Close()

	originalWebhook := discordWebhook
	originalIncrement := incrementAlert
	originalDedupe := discordEvents
	discordWebhook = discord.New(discord.Config{WebhookURL: server.URL, MaxRetries: -1})
	incrementAlert = func() error { return nil }
	discordEvents = newEventDeduper(discordEventDedupeTTL)
	defer func() {
		discordWebhook = originalWebhook
		incrementAlert = originalIncrement
		discordEvents = originalDedupe
	}()

	alert := Checker{
		board: "NBA", word: "trade", subType: "keyword",
		Profile: user.Profile{Account: "discord-main", Discord: true},
		articles: article.Articles{{
			ID: 1, Title: "article", Link: "https://www.ptt.cc/bbs/NBA/M.1.A.001.html",
		}},
	}
	sendMessage(alert)
	sendMessage(alert)
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("Discord requests = %d, want retry via next matching event", got)
	}
}

func TestBroadcasterSendDiscordOnce(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1234567890"}`))
	}))
	defer server.Close()

	original := discordWebhook
	discordWebhook = discord.New(discord.Config{WebhookURL: server.URL})
	defer func() { discordWebhook = original }()

	broadcaster := Broadcaster{Msg: "maintenance"}
	if err := broadcaster.Send([]string{"discord", "DISCORD"}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("request count = %d, want 1", got)
	}
}

func TestBroadcasterRejectsLegacyPlatform(t *testing.T) {
	broadcaster := Broadcaster{Msg: "maintenance"}
	err := broadcaster.Send([]string{"telegram"})
	if !errors.Is(err, ErrUnsupportedBroadcastPlatform) {
		t.Errorf("Send() error = %v, want ErrUnsupportedBroadcastPlatform", err)
	}
}

func TestBroadcasterRequiresPlatform(t *testing.T) {
	broadcaster := Broadcaster{Msg: "maintenance"}
	err := broadcaster.Send(nil)
	if !errors.Is(err, ErrNoBroadcastPlatform) {
		t.Errorf("Send() error = %v, want ErrNoBroadcastPlatform", err)
	}
}

func TestBroadcasterReturnsWebhookFailure(t *testing.T) {
	original := discordWebhook
	discordWebhook = discord.New(discord.Config{})
	defer func() { discordWebhook = original }()

	broadcaster := Broadcaster{Msg: "maintenance"}
	err := broadcaster.Send([]string{"discord"})
	if !errors.Is(err, discord.ErrWebhookNotConfigured) {
		t.Errorf("Send() error = %v, want ErrWebhookNotConfigured", err)
	}
}

func TestDiscordNotificationsRequireEnabledOptIn(t *testing.T) {
	tests := []struct {
		name string
		user user.User
		want bool
	}{
		{name: "enabled and opted in", user: user.User{Enable: true, Profile: user.Profile{Discord: true}}, want: true},
		{name: "disabled", user: user.User{Enable: false, Profile: user.Profile{Discord: true}}, want: false},
		{name: "not opted in", user: user.User{Enable: true, Profile: user.Profile{Discord: false}}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := discordNotificationsEnabled(test.user); got != test.want {
				t.Errorf("discordNotificationsEnabled() = %t, want %t", got, test.want)
			}
		})
	}
}
