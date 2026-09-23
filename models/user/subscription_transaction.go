package user

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	boardmodel "github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/garyburd/redigo/redis"
)

const (
	keywordIndexPrefix = "keyword:"
	authorIndexPrefix  = "author:"
	pushSumIndexPrefix = "pushsum:"
	articleIndexPrefix = "article:"
	boardCursorPrefix  = "board:"
	activationCode     = "subscription-activation:"
)

type boardActivationArticle struct {
	ID   int    `json:"ID"`
	Code string `json:"code"`
}

// boardActivationCursor creates a synthetic high-water mark for the instant a
// board first becomes active. The first poll can then notify articles posted
// after subscription commit without replaying older Atom entries or swallowing
// posts created while the rate-limited poller is waiting for its first turn.
func boardActivationCursor(activatedAt time.Time) (string, error) {
	seconds := activatedAt.Unix()
	if activatedAt.IsZero() || seconds <= 0 || int64(int(seconds)) != seconds {
		return "", fmt.Errorf("invalid board subscription activation time %q", activatedAt)
	}
	cursor, err := json.Marshal([]boardActivationArticle{{
		ID: int(seconds),
		Code: activationCode + strconv.FormatInt(seconds, 10) + "-" +
			strconv.Itoa(activatedAt.Nanosecond()),
	}})
	if err != nil {
		return "", fmt.Errorf("encode board subscription activation cursor: %w", err)
	}
	return string(cursor), nil
}

type subscriptionIndexState struct {
	keywords bool
	authors  bool
	pushUp   int
	pushDown int
	articles map[string]struct{}
}

func (state subscriptionIndexState) hasPushSum() bool {
	return state.pushUp != 0 || state.pushDown != 0
}

type redisIndexOperation struct {
	command string
	key     string
	member  string
}

type subscriptionIndexPlan struct {
	operations       []redisIndexOperation
	setKeys          map[string]struct{}
	pushBoardsToDrop map[string]string
	pollBoardsToDrop map[string][2]string
	pollBoardsToAdd  map[string][2]string
}

func (Redis) CompareAndSwap(account string, previous, next User) error {
	return compareAndSwapUser(account, previous, next, buildSubscriptionIndexPlan(previous, next, account))
}

func (Redis) UpdateSubscriptions(account string, previous, next User) error {
	return compareAndSwapUser(account, previous, next, buildSubscriptionIndexPlan(previous, next, account))
}

// RebuildSubscriptionIndexes treats user JSON documents as the source of truth
// and exactly replaces all derived notification membership sets. Run it before
// accepting API writes, such as during single-instance startup or maintenance.
func RebuildSubscriptionIndexes() error {
	conn := connectRedis()
	defer conn.Close()

	userKeys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
	if err != nil {
		return fmt.Errorf("list users for subscription reconciliation: %w", err)
	}
	sort.Strings(userKeys)
	if len(userKeys) > 0 {
		if _, err := conn.Do("WATCH", redis.Args{}.AddFlat(userKeys)...); err != nil {
			return err
		}
		defer func() { _, _ = conn.Do("UNWATCH") }()
	}

	expectedSets := make(map[string]map[string]struct{})
	boardNames := make(map[string]struct{})
	for _, userKey := range userKeys {
		data, err := redis.Bytes(conn.Do("GET", userKey))
		if err != nil {
			return fmt.Errorf("read %s for subscription reconciliation: %w", userKey, err)
		}
		var u User
		if err := json.Unmarshal(data, &u); err != nil {
			return fmt.Errorf("decode %s for subscription reconciliation: %w", userKey, err)
		}
		account := strings.TrimPrefix(userKey, prefix)
		if account == "" || u.Profile.Account != account {
			return fmt.Errorf("%s account does not match its Redis key", userKey)
		}
		for board, state := range subscriptionStates(u) {
			if state.keywords {
				addExpectedSetMember(expectedSets, keywordIndexPrefix+board+":subs", account)
			}
			if state.authors {
				addExpectedSetMember(expectedSets, authorIndexPrefix+board+":subs", account)
			}
			if (state.keywords || state.authors) && boardmodel.PollingAllowed(board) {
				boardNames[board] = struct{}{}
			}
			if state.hasPushSum() {
				addExpectedSetMember(expectedSets, pushSumIndexPrefix+"boards", board)
				addExpectedSetMember(expectedSets, pushSumIndexPrefix+board+":subs", account)
			}
			for code := range state.articles {
				addExpectedSetMember(expectedSets, articleIndexPrefix+code+":subs", account)
			}
		}
	}

	existingDerived := make(map[string]struct{})
	for _, pattern := range []string{
		keywordIndexPrefix + "*:subs",
		authorIndexPrefix + "*:subs",
		pushSumIndexPrefix + "*:subs",
		articleIndexPrefix + "*:subs",
	} {
		keys, err := redis.Strings(conn.Do("KEYS", pattern))
		if err != nil {
			return fmt.Errorf("list derived indexes matching %s: %w", pattern, err)
		}
		for _, key := range keys {
			existingDerived[key] = struct{}{}
		}
	}
	existingDerived[pushSumIndexPrefix+"boards"] = struct{}{}
	existingDerived["boards"] = struct{}{}
	derivedKeys := sortedKeys(existingDerived)
	if len(derivedKeys) > 0 {
		if _, err := conn.Do("WATCH", redis.Args{}.AddFlat(derivedKeys)...); err != nil {
			return err
		}
	}

	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	discard := true
	defer func() {
		if discard {
			_, _ = conn.Do("DISCARD")
		}
	}()
	if len(derivedKeys) > 0 {
		if err := conn.Send("DEL", redis.Args{}.AddFlat(derivedKeys)...); err != nil {
			return err
		}
	}
	for _, key := range sortedKeys(expectedSets) {
		members := sortedKeys(expectedSets[key])
		if err := conn.Send("SADD", redis.Args{}.Add(key).AddFlat(members)...); err != nil {
			return err
		}
	}
	if len(boardNames) > 0 {
		if err := conn.Send("SADD", redis.Args{}.Add("boards").AddFlat(sortedKeys(boardNames))...); err != nil {
			return err
		}
	}
	reply, err := conn.Do("EXEC")
	discard = false
	if err == redis.ErrNil || reply == nil {
		return ErrConcurrentUpdate
	}
	if err != nil {
		return err
	}
	results, err := redis.Values(reply, nil)
	if err != nil {
		return err
	}
	for _, result := range results {
		if transactionErr, ok := result.(redis.Error); ok {
			return transactionErr
		}
	}
	return nil
}

func addExpectedSetMember(sets map[string]map[string]struct{}, key, member string) {
	members := sets[key]
	if members == nil {
		members = make(map[string]struct{})
		sets[key] = members
	}
	members[member] = struct{}{}
}

func compareAndSwapUser(account string, previous, next User, plan subscriptionIndexPlan) error {
	expectedJSON, err := json.Marshal(previous)
	if err != nil {
		return fmt.Errorf("encode previous user: %w", err)
	}
	nextJSON, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode updated user: %w", err)
	}

	userKey := prefix + account
	watchSet := map[string]struct{}{userKey: {}}
	for _, operation := range plan.operations {
		watchSet[operation.key] = struct{}{}
	}
	for key := range plan.setKeys {
		watchSet[key] = struct{}{}
	}
	for key := range plan.pushBoardsToDrop {
		watchSet[key] = struct{}{}
	}
	for _, keys := range plan.pollBoardsToDrop {
		watchSet[keys[0]] = struct{}{}
		watchSet[keys[1]] = struct{}{}
	}
	for board, keys := range plan.pollBoardsToAdd {
		watchSet[keys[0]] = struct{}{}
		watchSet[keys[1]] = struct{}{}
		watchSet[boardCursorPrefix+board] = struct{}{}
	}
	for board := range plan.pollBoardsToDrop {
		watchSet[boardCursorPrefix+board] = struct{}{}
	}
	watchKeys := sortedKeys(watchSet)

	conn := connectRedis()
	defer conn.Close()
	if _, err := conn.Do("WATCH", redis.Args{}.AddFlat(watchKeys)...); err != nil {
		return err
	}
	defer func() { _, _ = conn.Do("UNWATCH") }()

	currentJSON, err := redis.Bytes(conn.Do("GET", userKey))
	if err == redis.ErrNil {
		return ErrUserNotExist
	}
	if err != nil {
		return err
	}
	var current User
	if err := json.Unmarshal(currentJSON, &current); err != nil {
		return fmt.Errorf("decode current user before update: %w", err)
	}
	currentCanonicalJSON, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("encode current user before update: %w", err)
	}
	if !bytes.Equal(currentCanonicalJSON, expectedJSON) {
		return ErrConcurrentUpdate
	}
	for key := range plan.setKeys {
		keyType, err := redis.String(conn.Do("TYPE", key))
		if err != nil {
			return err
		}
		if keyType != "none" && keyType != "set" {
			return fmt.Errorf("%w: %s is %s", ErrInvalidSubscriptionIndex, key, keyType)
		}
	}

	for subscriberKey, board := range plan.pushBoardsToDrop {
		count, err := redis.Int(conn.Do("SCARD", subscriberKey))
		if err != nil {
			return err
		}
		isMember, err := redis.Bool(conn.Do("SISMEMBER", subscriberKey, account))
		if err != nil {
			return err
		}
		if count == 0 || (count == 1 && isMember) {
			plan.operations = append(plan.operations, redisIndexOperation{
				command: "SREM",
				key:     pushSumIndexPrefix + "boards",
				member:  board,
			})
		}
	}
	for _, board := range sortedKeys(plan.pollBoardsToDrop) {
		keys := plan.pollBoardsToDrop[board]
		remaining := 0
		for _, subscriberKey := range keys {
			count, err := redis.Int(conn.Do("SCARD", subscriberKey))
			if err != nil {
				return err
			}
			isMember, err := redis.Bool(conn.Do("SISMEMBER", subscriberKey, account))
			if err != nil {
				return err
			}
			if isMember {
				count--
			}
			remaining += count
		}
		if remaining == 0 {
			plan.operations = append(plan.operations, redisIndexOperation{
				command: "SREM",
				key:     "boards",
				member:  board,
			})
			plan.operations = append(plan.operations, redisIndexOperation{
				command: "DEL",
				key:     boardCursorPrefix + board,
			})
		}
	}
	for _, board := range sortedKeys(plan.pollBoardsToAdd) {
		keys := plan.pollBoardsToAdd[board]
		active := 0
		for _, subscriberKey := range keys {
			count, err := redis.Int(conn.Do("SCARD", subscriberKey))
			if err != nil {
				return err
			}
			active += count
		}
		if active == 0 {
			// Start from the atomic subscription activation time. Unlike deleting
			// the cursor and taking a silent baseline, this preserves posts made
			// while the globally rate-limited checker waits for its first turn.
			activationCursor, err := boardActivationCursor(next.UpdateTime)
			if err != nil {
				return err
			}
			plan.operations = append(plan.operations, redisIndexOperation{
				command: "SET",
				key:     boardCursorPrefix + board,
				member:  activationCursor,
			})
		}
	}

	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	discard := true
	defer func() {
		if discard {
			_, _ = conn.Do("DISCARD")
		}
	}()
	if err := conn.Send("SET", userKey, nextJSON); err != nil {
		return err
	}
	for _, operation := range plan.operations {
		var err error
		if operation.command == "DEL" {
			err = conn.Send(operation.command, operation.key)
		} else {
			err = conn.Send(operation.command, operation.key, operation.member)
		}
		if err != nil {
			return err
		}
	}
	reply, err := conn.Do("EXEC")
	discard = false
	if err == redis.ErrNil || reply == nil {
		return ErrConcurrentUpdate
	}
	if err != nil {
		return err
	}
	results, err := redis.Values(reply, nil)
	if err != nil {
		return err
	}
	for _, result := range results {
		if transactionErr, ok := result.(redis.Error); ok {
			return transactionErr
		}
	}
	return nil
}

func buildSubscriptionIndexPlan(previous, next User, account string) subscriptionIndexPlan {
	before := subscriptionStates(previous)
	after := subscriptionStates(next)
	plan := subscriptionIndexPlan{
		setKeys:          make(map[string]struct{}),
		pushBoardsToDrop: make(map[string]string),
		pollBoardsToDrop: make(map[string][2]string),
		pollBoardsToAdd:  make(map[string][2]string),
	}
	boardSet := make(map[string]struct{})
	for board := range before {
		boardSet[board] = struct{}{}
	}
	for board := range after {
		boardSet[board] = struct{}{}
	}
	for _, board := range sortedKeys(boardSet) {
		oldState := before[board]
		newState := after[board]
		keywordKey := keywordIndexPrefix + board + ":subs"
		authorKey := authorIndexPrefix + board + ":subs"
		plan.reconcileSet(keywordKey, account, newState.keywords)
		plan.reconcileSet(authorKey, account, newState.authors)
		pollingSubscription := newState.keywords || newState.authors
		pollingAllowed := boardmodel.PollingAllowed(board)
		if pollingSubscription && pollingAllowed {
			plan.addSet("SADD", "boards", board)
			plan.pollBoardsToAdd[board] = [2]string{keywordKey, authorKey}
		} else if !pollingSubscription && pollingAllowed {
			plan.pollBoardsToDrop[board] = [2]string{keywordKey, authorKey}
			plan.setKeys["boards"] = struct{}{}
		} else {
			// Reconcile the user and subscriber indexes above, while ensuring this
			// deployment cannot schedule the board. Preserve its cursor across both
			// migration and deletion so enabling the board later cannot replay old
			// articles.
			plan.addSet("SREM", "boards", board)
		}

		pushSubscriberKey := pushSumIndexPrefix + board + ":subs"
		if newState.hasPushSum() {
			plan.addSet("SADD", pushSumIndexPrefix+"boards", board)
			plan.addSet("SADD", pushSubscriberKey, account)
		} else {
			plan.addSet("SREM", pushSubscriberKey, account)
			plan.pushBoardsToDrop[pushSubscriberKey] = board
			plan.setKeys[pushSumIndexPrefix+"boards"] = struct{}{}
		}
		if oldState.pushUp != newState.pushUp && oldState.pushUp != 0 {
			plan.deletePushSumDiffKeys(account, board, "up", oldState.pushUp)
		}
		if oldState.pushDown != newState.pushDown && oldState.pushDown != 0 {
			plan.deletePushSumDiffKeys(account, board, "down", oldState.pushDown)
		}
	}

	oldArticles := articleCodes(before)
	newArticles := articleCodes(after)
	articleSet := make(map[string]struct{})
	for code := range oldArticles {
		articleSet[code] = struct{}{}
	}
	for code := range newArticles {
		articleSet[code] = struct{}{}
	}
	for _, code := range sortedKeys(articleSet) {
		plan.reconcileSet(articleIndexPrefix+code+":subs", account, newArticles[code])
	}
	return plan
}

func (plan *subscriptionIndexPlan) reconcileSet(key, member string, after bool) {
	if after {
		plan.addSet("SADD", key, member)
	} else {
		plan.addSet("SREM", key, member)
	}
}

func (plan *subscriptionIndexPlan) addSet(command, key, member string) {
	plan.operations = append(plan.operations, redisIndexOperation{command: command, key: key, member: member})
	plan.setKeys[key] = struct{}{}
}

func (plan *subscriptionIndexPlan) deletePushSumDiffKeys(account, board, kind string, threshold int) {
	legacyBase := pushSumIndexPrefix + account + ":" + board + ":" + kind + ":"
	revisionBase := legacyBase + strconv.Itoa(threshold) + ":"
	for _, key := range []string{
		legacyBase + "now", legacyBase + "base", legacyBase + "bench",
		revisionBase + "base", revisionBase + "bench", revisionBase + "initialized",
	} {
		plan.operations = append(plan.operations, redisIndexOperation{command: "DEL", key: key})
	}
}

func subscriptionStates(u User) map[string]subscriptionIndexState {
	states := make(map[string]subscriptionIndexState)
	// Disabled users and legacy/non-Discord profiles retain their configured
	// subscriptions, but must not keep PTT polling or notification indexes active.
	if !u.Enable || !u.Profile.Discord {
		return states
	}
	for _, sub := range u.Subscribes {
		board := strings.ToLower(strings.TrimSpace(sub.Board))
		if board == "" {
			continue
		}
		state := states[board]
		if state.articles == nil {
			state.articles = make(map[string]struct{})
		}
		state.keywords = state.keywords || len(sub.Keywords) > 0
		state.authors = state.authors || len(sub.Authors) > 0
		if sub.PushSum.Up != 0 {
			state.pushUp = sub.PushSum.Up
		}
		if sub.PushSum.Down != 0 {
			state.pushDown = sub.PushSum.Down
		}
		for _, code := range sub.Articles {
			code = strings.TrimSpace(code)
			if code != "" {
				state.articles[code] = struct{}{}
			}
		}
		states[board] = state
	}
	return states
}

func articleCodes(states map[string]subscriptionIndexState) map[string]bool {
	codes := make(map[string]bool)
	for _, state := range states {
		for code := range state.articles {
			codes[code] = true
		}
	}
	return codes
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
