package user

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
)

func saveAndReloadUser(t *testing.T, account string) User {
	t.Helper()
	u := NewUser(new(Redis))
	u.Enable = true
	u.Profile = Profile{Account: account, Discord: true}
	if err := u.Save(); err != nil {
		t.Fatalf("save user: %v", err)
	}
	return NewUser(new(Redis)).Find(account)
}

func requireSetMember(t *testing.T, key, member string, want bool) {
	t.Helper()
	if !s.Exists(key) {
		if want {
			t.Fatalf("set %s does not exist", key)
		}
		return
	}
	got, err := s.IsMember(key, member)
	if err != nil {
		t.Fatalf("check %s membership: %v", key, err)
	}
	if got != want {
		t.Fatalf("SISMEMBER %s %s = %t, want %t", key, member, got, want)
	}
}

func requireSubscriptionIndexMembership(t *testing.T, account, board, article string, want bool) {
	t.Helper()
	for _, check := range []struct{ key, member string }{
		{"keyword:" + board + ":subs", account},
		{"author:" + board + ":subs", account},
		{"pushsum:" + board + ":subs", account},
		{"pushsum:boards", board},
		{"article:" + article + ":subs", account},
		{"boards", board},
	} {
		requireSetMember(t, check.key, check.member, want)
	}
}

func requireBoardActivationCursor(t *testing.T, board string, activatedAt int64) {
	t.Helper()
	raw, err := s.Get("board:" + board)
	if err != nil {
		t.Fatalf("read board activation cursor: %v", err)
	}
	var cursor []boardActivationArticle
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
		t.Fatalf("decode board activation cursor %q: %v", raw, err)
	}
	if len(cursor) != 1 || int64(cursor[0].ID) != activatedAt ||
		!strings.HasPrefix(cursor[0].Code, activationCode) {
		t.Fatalf("board activation cursor = %#v, want timestamp %d", cursor, activatedAt)
	}
}

func TestUpdateSubscriptionsCommitsUserAndEveryIndex(t *testing.T) {
	s.FlushAll()
	before := saveAndReloadUser(t, "discord-main")
	after := before.Clone()
	after.Subscribes = subscription.Subscriptions{{
		Board:    "stock",
		Keywords: []string{"台積電"},
		Authors:  []string{"author"},
		Articles: []string{"M.1.A.001"},
		PushSum:  subscription.PushSum{Up: 50, Down: 10},
	}}
	if err := after.UpdateSubscriptions(before); err != nil {
		t.Fatalf("UpdateSubscriptions() error = %v", err)
	}

	stored := NewUser(new(Redis)).Find("discord-main")
	if !reflect.DeepEqual(stored.Subscribes, after.Subscribes) {
		t.Fatalf("stored subscriptions = %#v, want %#v", stored.Subscribes, after.Subscribes)
	}
	for _, check := range []struct{ key, member string }{
		{"keyword:stock:subs", "discord-main"},
		{"author:stock:subs", "discord-main"},
		{"pushsum:stock:subs", "discord-main"},
		{"pushsum:boards", "stock"},
		{"article:M.1.A.001:subs", "discord-main"},
	} {
		requireSetMember(t, check.key, check.member, true)
	}

	for _, key := range []string{
		"pushsum:discord-main:stock:up:now",
		"pushsum:discord-main:stock:up:base",
		"pushsum:discord-main:stock:up:bench",
		"pushsum:discord-main:stock:down:now",
		"pushsum:discord-main:stock:down:base",
		"pushsum:discord-main:stock:down:bench",
	} {
		s.Set(key, "old")
	}

	removeBefore := NewUser(new(Redis)).Find("discord-main")
	removeAfter := removeBefore.Clone()
	removeAfter.Subscribes = nil
	if err := removeAfter.UpdateSubscriptions(removeBefore); err != nil {
		t.Fatalf("remove UpdateSubscriptions() error = %v", err)
	}
	for _, check := range []struct{ key, member string }{
		{"keyword:stock:subs", "discord-main"},
		{"author:stock:subs", "discord-main"},
		{"pushsum:stock:subs", "discord-main"},
		{"pushsum:boards", "stock"},
		{"article:M.1.A.001:subs", "discord-main"},
	} {
		requireSetMember(t, check.key, check.member, false)
	}
	for _, key := range []string{
		"pushsum:discord-main:stock:up:now",
		"pushsum:discord-main:stock:up:base",
		"pushsum:discord-main:stock:up:bench",
		"pushsum:discord-main:stock:down:now",
		"pushsum:discord-main:stock:down:base",
		"pushsum:discord-main:stock:down:bench",
	} {
		if s.Exists(key) {
			t.Fatalf("diff key %s survived threshold removal", key)
		}
	}
}

func TestUpdateSubscriptionsRejectsStaleSnapshotWithoutIndexWrites(t *testing.T) {
	s.FlushAll()
	firstBefore := saveAndReloadUser(t, "discord-main")
	staleBefore := firstBefore.Clone()

	firstAfter := firstBefore.Clone()
	firstAfter.Subscribes = subscription.Subscriptions{{Board: "stock", Keywords: []string{"first"}}}
	if err := firstAfter.UpdateSubscriptions(firstBefore); err != nil {
		t.Fatalf("first update: %v", err)
	}

	staleAfter := staleBefore.Clone()
	staleAfter.Subscribes = subscription.Subscriptions{{Board: "nba", Authors: []string{"stale"}}}
	if err := staleAfter.UpdateSubscriptions(staleBefore); !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("stale update error = %v, want ErrConcurrentUpdate", err)
	}
	requireSetMember(t, "keyword:stock:subs", "discord-main", true)
	requireSetMember(t, "author:nba:subs", "discord-main", false)
	stored := NewUser(new(Redis)).Find("discord-main")
	if len(stored.Subscribes) != 1 || stored.Subscribes[0].Board != "stock" {
		t.Fatalf("stale update replaced user: %#v", stored.Subscribes)
	}
}

func TestUpdateSubscriptionsAcceptsEquivalentNonCanonicalJSON(t *testing.T) {
	s.FlushAll()
	s.Set("user:discord-main", `{ "Subscribes" : null, "Profile" : { "discord" : true, "account" : "discord-main" }, "enable" : true, "createTime" : "0001-01-01T00:00:00Z", "updateTime" : "0001-01-01T00:00:00Z" }`)
	before := NewUser(new(Redis)).Find("discord-main")
	after := before.Clone()
	after.Subscribes = subscription.Subscriptions{{Board: "stock", Keywords: []string{"canonical"}}}
	if err := after.UpdateSubscriptions(before); err != nil {
		t.Fatalf("UpdateSubscriptions() with equivalent non-canonical JSON: %v", err)
	}
	requireSetMember(t, "keyword:stock:subs", "discord-main", true)
}

func TestSubscriptionBoardSetsOnlyTrackRelevantActiveSubscribers(t *testing.T) {
	s.FlushAll()
	firstBefore := saveAndReloadUser(t, "first")
	firstAfter := firstBefore.Clone()
	firstAfter.Subscribes = subscription.Subscriptions{{
		Board: "stock", Keywords: []string{"first"}, PushSum: subscription.PushSum{Up: 10},
	}}
	if err := firstAfter.UpdateSubscriptions(firstBefore); err != nil {
		t.Fatalf("add first subscriptions: %v", err)
	}
	secondBefore := saveAndReloadUser(t, "second")
	secondAfter := secondBefore.Clone()
	secondAfter.Subscribes = subscription.Subscriptions{{
		Board: "stock", Authors: []string{"second"}, PushSum: subscription.PushSum{Down: 10},
	}}
	if err := secondAfter.UpdateSubscriptions(secondBefore); err != nil {
		t.Fatalf("add second subscriptions: %v", err)
	}

	removeFirstBefore := NewUser(new(Redis)).Find("first")
	removeFirstAfter := removeFirstBefore.Clone()
	removeFirstAfter.Subscribes = nil
	if err := removeFirstAfter.UpdateSubscriptions(removeFirstBefore); err != nil {
		t.Fatalf("remove first subscriptions: %v", err)
	}
	requireSetMember(t, "boards", "stock", true)
	requireSetMember(t, "pushsum:boards", "stock", true)
	requireSetMember(t, "keyword:stock:subs", "first", false)
	requireSetMember(t, "author:stock:subs", "second", true)
	requireSetMember(t, "pushsum:stock:subs", "first", false)
	requireSetMember(t, "pushsum:stock:subs", "second", true)

	pushOnlyBefore := saveAndReloadUser(t, "push-only")
	pushOnlyAfter := pushOnlyBefore.Clone()
	pushOnlyAfter.Subscribes = subscription.Subscriptions{{
		Board: "nba", PushSum: subscription.PushSum{Up: 20},
	}}
	if err := pushOnlyAfter.UpdateSubscriptions(pushOnlyBefore); err != nil {
		t.Fatalf("add push-only subscription: %v", err)
	}
	requireSetMember(t, "pushsum:boards", "nba", true)
	requireSetMember(t, "boards", "nba", false)
}

func TestPollingBoardCursorResetsOnlyAcrossInactiveTransition(t *testing.T) {
	s.FlushAll()
	s.Set("board:stock", "stale-before-first-subscriber")

	firstBefore := saveAndReloadUser(t, "first")
	firstAfter := firstBefore.Clone()
	firstAfter.Subscribes = subscription.Subscriptions{{Board: "stock", Keywords: []string{"first"}}}
	if err := firstAfter.UpdateSubscriptions(firstBefore); err != nil {
		t.Fatalf("add first subscriber: %v", err)
	}
	requireBoardActivationCursor(t, "stock", firstAfter.UpdateTime.Unix())

	s.Set("board:stock", "current-active-cursor")
	secondBefore := saveAndReloadUser(t, "second")
	secondAfter := secondBefore.Clone()
	secondAfter.Subscribes = subscription.Subscriptions{{Board: "stock", Authors: []string{"second"}}}
	if err := secondAfter.UpdateSubscriptions(secondBefore); err != nil {
		t.Fatalf("add second subscriber: %v", err)
	}
	if !s.Exists("board:stock") {
		t.Fatal("adding another active subscriber reset the current cursor")
	}

	removeFirstBefore := NewUser(new(Redis)).Find("first")
	removeFirstAfter := removeFirstBefore.Clone()
	removeFirstAfter.Subscribes = nil
	if err := removeFirstAfter.UpdateSubscriptions(removeFirstBefore); err != nil {
		t.Fatalf("remove first subscriber: %v", err)
	}
	if !s.Exists("board:stock") {
		t.Fatal("removing a non-final subscriber reset the current cursor")
	}

	removeSecondBefore := NewUser(new(Redis)).Find("second")
	removeSecondAfter := removeSecondBefore.Clone()
	removeSecondAfter.Subscribes = nil
	if err := removeSecondAfter.UpdateSubscriptions(removeSecondBefore); err != nil {
		t.Fatalf("remove final subscriber: %v", err)
	}
	if s.Exists("board:stock") {
		t.Fatal("cursor survived active-to-inactive transition")
	}
}

func TestUpdateSubscriptionsWrongTypeLeavesEverythingUnchanged(t *testing.T) {
	s.FlushAll()
	before := saveAndReloadUser(t, "discord-main")
	after := before.Clone()
	after.Subscribes = subscription.Subscriptions{
		{Board: "stock", Keywords: []string{"ok"}},
		{Board: "nba", Keywords: []string{"wrong-type"}},
	}
	s.Set("keyword:nba:subs", "not-a-set")

	err := after.UpdateSubscriptions(before)
	if !errors.Is(err, ErrInvalidSubscriptionIndex) {
		t.Fatalf("error = %v, want ErrInvalidSubscriptionIndex", err)
	}
	requireSetMember(t, "keyword:stock:subs", "discord-main", false)
	stored := NewUser(new(Redis)).Find("discord-main")
	if len(stored.Subscribes) != 0 {
		t.Fatalf("user changed despite failed preflight: %#v", stored.Subscribes)
	}
}

func TestUpdateIfUnchangedProtectsProfileFromStaleOverwrite(t *testing.T) {
	s.FlushAll()
	firstBefore := saveAndReloadUser(t, "discord-main")
	staleBefore := firstBefore.Clone()
	firstAfter := firstBefore.Clone()
	firstAfter.Enable = false
	if err := firstAfter.UpdateIfUnchanged(firstBefore); err != nil {
		t.Fatalf("first profile update: %v", err)
	}
	staleAfter := staleBefore.Clone()
	staleAfter.Profile.Type = "room"
	if err := staleAfter.UpdateIfUnchanged(staleBefore); !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("stale profile error = %v, want ErrConcurrentUpdate", err)
	}
	stored := NewUser(new(Redis)).Find("discord-main")
	if stored.Enable || stored.Profile.Type != "" {
		t.Fatalf("stale profile overwrote current state: %#v", stored)
	}
}

func TestProfileUpdateReconcilesIndexesWhenNotificationStateChanges(t *testing.T) {
	s.FlushAll()
	before := saveAndReloadUser(t, "discord-main")
	active := before.Clone()
	active.Subscribes = subscription.Subscriptions{{
		Board:    "stock",
		Keywords: []string{"keyword"},
		Authors:  []string{"author"},
		Articles: []string{"M.1.A.001"},
		PushSum:  subscription.PushSum{Up: 50, Down: 10},
	}}
	if err := active.UpdateSubscriptions(before); err != nil {
		t.Fatalf("add subscriptions: %v", err)
	}
	requireSubscriptionIndexMembership(t, "discord-main", "stock", "M.1.A.001", true)

	s.Set("board:stock", "current-active-cursor")
	s.Set("pushsum:discord-main:stock:up:base", "current-push-state")
	disableBefore := NewUser(new(Redis)).Find("discord-main")
	disabled := disableBefore.Clone()
	disabled.Enable = false
	if err := disabled.UpdateIfUnchanged(disableBefore); err != nil {
		t.Fatalf("disable profile: %v", err)
	}
	requireSubscriptionIndexMembership(t, "discord-main", "stock", "M.1.A.001", false)
	if s.Exists("board:stock") {
		t.Fatal("board cursor survived disabling the final active subscriber")
	}
	if s.Exists("pushsum:discord-main:stock:up:base") {
		t.Fatal("push-sum state survived disabling its subscriber")
	}
	storedDisabled := NewUser(new(Redis)).Find("discord-main")
	if !reflect.DeepEqual(storedDisabled.Subscribes, active.Subscribes) {
		t.Fatalf("disabling profile discarded subscriptions: %#v", storedDisabled.Subscribes)
	}

	s.Set("board:stock", "stale-while-disabled")
	enabled := storedDisabled.Clone()
	enabled.Enable = true
	if err := enabled.UpdateIfUnchanged(storedDisabled); err != nil {
		t.Fatalf("re-enable profile: %v", err)
	}
	requireSubscriptionIndexMembership(t, "discord-main", "stock", "M.1.A.001", true)
	requireBoardActivationCursor(t, "stock", enabled.UpdateTime.Unix())

	s.Set("board:stock", "current-after-re-enable")
	discordBefore := NewUser(new(Redis)).Find("discord-main")
	withoutDiscord := discordBefore.Clone()
	withoutDiscord.Profile.Discord = false
	if err := withoutDiscord.UpdateIfUnchanged(discordBefore); err != nil {
		t.Fatalf("disable Discord profile: %v", err)
	}
	requireSubscriptionIndexMembership(t, "discord-main", "stock", "M.1.A.001", false)
	if s.Exists("board:stock") {
		t.Fatal("board cursor survived removing Discord from the final active subscriber")
	}

	s.Set("board:stock", "stale-while-discord-disabled")
	discordDisabled := NewUser(new(Redis)).Find("discord-main")
	withDiscord := discordDisabled.Clone()
	withDiscord.Profile.Discord = true
	if err := withDiscord.UpdateIfUnchanged(discordDisabled); err != nil {
		t.Fatalf("re-enable Discord profile: %v", err)
	}
	requireSubscriptionIndexMembership(t, "discord-main", "stock", "M.1.A.001", true)
	requireBoardActivationCursor(t, "stock", withDiscord.UpdateTime.Unix())
}

func TestRebuildSubscriptionIndexesRemovesStaleAndPreservesDetails(t *testing.T) {
	s.FlushAll()
	u := saveAndReloadUser(t, "discord-main")
	u.Subscribes = subscription.Subscriptions{{
		Board:    "stock",
		Keywords: []string{"台積電"},
		Authors:  []string{"author"},
		Articles: []string{"M.1.A.001"},
		PushSum:  subscription.PushSum{Up: 50},
	}}
	data, err := json.Marshal(u)
	if err != nil {
		t.Fatalf("marshal source user: %v", err)
	}
	s.Set("user:discord-main", string(data))
	conn := connectRedis()
	for _, command := range [][]interface{}{
		{"SADD", "keyword:stale:subs", "ghost"},
		{"SADD", "author:stale:subs", "ghost"},
		{"SADD", "pushsum:stale:subs", "ghost"},
		{"SADD", "pushsum:boards", "stale"},
		{"SADD", "article:M.stale:subs", "ghost"},
	} {
		if _, err := conn.Do(command[0].(string), command[1:]...); err != nil {
			_ = conn.Close()
			t.Fatalf("seed stale index: %v", err)
		}
	}
	_ = conn.Close()
	s.Set("article:M.1.A.001:detail", "must-survive")
	s.Set("pushsum:discord-main:stock:up:base", "must-survive")

	for run := 0; run < 2; run++ {
		if err := RebuildSubscriptionIndexes(); err != nil {
			t.Fatalf("RebuildSubscriptionIndexes() run %d: %v", run+1, err)
		}
	}
	for _, check := range []struct{ key, member string }{
		{"keyword:stock:subs", "discord-main"},
		{"author:stock:subs", "discord-main"},
		{"pushsum:stock:subs", "discord-main"},
		{"pushsum:boards", "stock"},
		{"article:M.1.A.001:subs", "discord-main"},
	} {
		requireSetMember(t, check.key, check.member, true)
	}
	for _, key := range []string{
		"keyword:stale:subs",
		"author:stale:subs",
		"pushsum:stale:subs",
		"article:M.stale:subs",
	} {
		if s.Exists(key) {
			t.Fatalf("stale index %s survived rebuild", key)
		}
	}
	requireSetMember(t, "pushsum:boards", "stale", false)
	for _, key := range []string{"article:M.1.A.001:detail", "pushsum:discord-main:stock:up:base"} {
		if !s.Exists(key) {
			t.Fatalf("non-membership key %s was deleted", key)
		}
	}
	requireSetMember(t, "boards", "stock", true)
}

func TestRebuildSubscriptionIndexesIgnoresInactiveUsers(t *testing.T) {
	s.FlushAll()
	users := []User{
		{
			Enable:  true,
			Profile: Profile{Account: "active", Discord: true},
			Subscribes: subscription.Subscriptions{{
				Board: "stock", Keywords: []string{"active"}, Authors: []string{"active"},
				Articles: []string{"M.active"}, PushSum: subscription.PushSum{Up: 10},
			}},
		},
		{
			Enable:  false,
			Profile: Profile{Account: "disabled", Discord: true},
			Subscribes: subscription.Subscriptions{{
				Board: "nba", Keywords: []string{"disabled"}, Authors: []string{"disabled"},
				Articles: []string{"M.disabled"}, PushSum: subscription.PushSum{Up: 20},
			}},
		},
		{
			Enable:  true,
			Profile: Profile{Account: "legacy", Discord: false},
			Subscribes: subscription.Subscriptions{{
				Board: "movie", Keywords: []string{"legacy"}, Authors: []string{"legacy"},
				Articles: []string{"M.legacy"}, PushSum: subscription.PushSum{Down: 30},
			}},
		},
	}
	for _, u := range users {
		data, err := json.Marshal(u)
		if err != nil {
			t.Fatalf("marshal %s: %v", u.Profile.Account, err)
		}
		s.Set("user:"+u.Profile.Account, string(data))
	}

	conn := connectRedis()
	for _, command := range [][]interface{}{
		{"SADD", "keyword:nba:subs", "disabled"},
		{"SADD", "author:nba:subs", "disabled"},
		{"SADD", "pushsum:nba:subs", "disabled"},
		{"SADD", "article:M.disabled:subs", "disabled"},
		{"SADD", "keyword:movie:subs", "legacy"},
		{"SADD", "author:movie:subs", "legacy"},
		{"SADD", "pushsum:movie:subs", "legacy"},
		{"SADD", "article:M.legacy:subs", "legacy"},
		{"SADD", "pushsum:boards", "nba", "movie"},
		{"SADD", "boards", "nba", "movie"},
	} {
		if _, err := conn.Do(command[0].(string), command[1:]...); err != nil {
			_ = conn.Close()
			t.Fatalf("seed inactive index: %v", err)
		}
	}
	_ = conn.Close()

	if err := RebuildSubscriptionIndexes(); err != nil {
		t.Fatalf("RebuildSubscriptionIndexes(): %v", err)
	}
	requireSubscriptionIndexMembership(t, "active", "stock", "M.active", true)
	requireSubscriptionIndexMembership(t, "disabled", "nba", "M.disabled", false)
	requireSubscriptionIndexMembership(t, "legacy", "movie", "M.legacy", false)
	for _, account := range []string{"disabled", "legacy"} {
		stored := NewUser(new(Redis)).Find(account)
		if len(stored.Subscribes) != 1 {
			t.Fatalf("rebuild changed %s's stored subscriptions: %#v", account, stored.Subscribes)
		}
	}
}

func TestRebuildSubscriptionIndexesAbortsBeforeDeletingOnInvalidUser(t *testing.T) {
	s.FlushAll()
	s.Set("user:broken", "not-json")
	conn := connectRedis()
	_, err := conn.Do("SADD", "keyword:stale:subs", "ghost")
	_ = conn.Close()
	if err != nil {
		t.Fatalf("seed stale index: %v", err)
	}
	if err := RebuildSubscriptionIndexes(); err == nil {
		t.Fatal("RebuildSubscriptionIndexes() succeeded with invalid source user")
	}
	requireSetMember(t, "keyword:stale:subs", "ghost", true)
}
