package command

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
)

type updateAction func(ctx context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error

var errArticleSubscriptionLimit = errors.New("推文追蹤最多 50 篇，輸入「推文清單」，整理追蹤列表。")
var errWildcardAdd = errors.New("新增不可使用 *；若要追蹤全部文章，請使用 regexp:.*")
var errInvalidAuthorTitleKeyword = errors.New("作者與標題複合條件格式錯誤；請使用 author:lushin&賣，作者與每個標題詞皆不可空白")
var verifyBoardExist = board.VerifyBoardExistContext

type boardValidationCacheContextKey struct{}
type boardValidationCache map[string]error

func addKeywords(ctx context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	if containsWildcard(inputs) {
		return errWildcardAdd
	}
	if err := validateAuthorTitleKeywords(inputs); err != nil {
		return err
	}
	sub.Keywords = inputs
	if err := verifyBoardForMutation(ctx, sub.Board); err != nil {
		return err
	}
	u.Subscribes.AddVerified(sub)
	return nil
}

// validateAuthorTitleKeywords is deliberately called by the shared mutation
// action so both the Chinese command and add -k path enforce the same grammar.
// Deletion does not call it: malformed or legacy values must remain removable.
func validateAuthorTitleKeywords(inputs []string) error {
	for _, input := range inputs {
		if !strings.HasPrefix(input, "author:") || !strings.Contains(input, "&") {
			continue
		}
		terms := strings.Split(input, "&")
		if strings.TrimSpace(strings.TrimPrefix(terms[0], "author:")) == "" {
			return errInvalidAuthorTitleKeyword
		}
		for _, term := range terms[1:] {
			if strings.TrimSpace(term) == "" {
				return errInvalidAuthorTitleKeyword
			}
		}
	}
	return nil
}

func removeKeywords(_ context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	sub.Keywords = inputs
	if inputs[0] == "*" {
		for _, uSub := range u.Subscribes {
			if strings.EqualFold(uSub.Board, sub.Board) {
				sub.Keywords = make(myutil.StringSlice, len(uSub.Keywords))
				copy(sub.Keywords, uSub.Keywords)
			}
		}
	}
	return u.Subscribes.Remove(sub)
}

func addAuthors(ctx context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	if containsWildcard(inputs) {
		return errWildcardAdd
	}
	sub.Authors = inputs
	if err := verifyBoardForMutation(ctx, sub.Board); err != nil {
		return err
	}
	u.Subscribes.AddVerified(sub)
	return nil
}

func containsWildcard(inputs []string) bool {
	for _, input := range inputs {
		input = strings.Trim(strings.TrimSpace(input), `"'`)
		if input == "*" {
			return true
		}
	}
	return false
}

func removeAuthors(_ context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	sub.Authors = inputs
	if inputs[0] == "*" {
		for _, uSub := range u.Subscribes {
			if strings.EqualFold(uSub.Board, sub.Board) {
				sub.Authors = make(myutil.StringSlice, len(uSub.Authors))
				copy(sub.Authors, uSub.Authors)
			}
		}
	}
	return u.Subscribes.Remove(sub)
}

func updatePushUp(ctx context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	up, err := strconv.Atoi(inputs[0])
	if err != nil {
		return err
	}
	for _, s := range u.Subscribes {
		if strings.EqualFold(s.Board, sub.Board) {
			sub.PushSum.Down = s.PushSum.Down
		}
	}
	sub.PushSum.Up = up
	if err := verifyBoardForMutation(ctx, sub.Board); err != nil {
		return err
	}
	u.Subscribes.UpdateVerified(sub)
	return nil
}

func updatePushDown(ctx context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	down, err := strconv.Atoi(inputs[0])
	if err != nil {
		return err
	}
	for _, s := range u.Subscribes {
		if strings.EqualFold(s.Board, sub.Board) {
			sub.PushSum.Up = s.PushSum.Up
		}
	}
	sub.PushSum.Down = down
	if err := verifyBoardForMutation(ctx, sub.Board); err != nil {
		return err
	}
	u.Subscribes.UpdateVerified(sub)
	return nil
}

func verifyBoardForMutation(ctx context.Context, boardName string) error {
	cache, _ := ctx.Value(boardValidationCacheContextKey{}).(boardValidationCache)
	cacheKey := strings.ToLower(strings.TrimSpace(boardName))
	if cached, ok := cache[cacheKey]; ok {
		return cached
	}
	exists, suggestion, err := verifyBoardExist(ctx, boardName)
	if err != nil {
		if cache != nil {
			cache[cacheKey] = err
		}
		return err
	}
	if !exists {
		err = board.BoardNotExistError{Suggestion: suggestion}
	}
	if cache != nil {
		cache[cacheKey] = err
	}
	return err
}

func addArticles(_ context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	sub.Articles = inputs
	articleCode := inputs[0]
	count := 0
	for _, current := range u.Subscribes {
		count += len(current.Articles)
		for _, existingCode := range current.Articles {
			if existingCode == articleCode {
				u.Subscribes.AddVerified(sub)
				return nil
			}
		}
	}
	if count >= subArticlesLimit {
		return errArticleSubscriptionLimit
	}
	u.Subscribes.AddVerified(sub)
	return nil
}

func removeArticles(_ context.Context, u *user.User, sub subscription.Subscription, inputs ...string) error {
	sub.Articles = inputs
	return u.Subscribes.Remove(sub)
}
