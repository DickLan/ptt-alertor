package subscription

import (
	"context"
	"sort"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/models/board"
)

type Subscriptions []Subscription

func (ss Subscriptions) String() string {

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Board < ss[j].Board
	})

	str := "關鍵字\n"
	for _, sub := range ss {
		if sub.String() != "" {
			str += sub.String() + "\n"
		}
	}
	str += "----\n作者\n"
	for _, sub := range ss {
		if sub.StringAuthor() != "" {
			str += sub.StringAuthor() + "\n"
		}
	}
	str += "----\n推文數\n"
	for _, sub := range ss {
		if sub.StringPushSum() != "" {
			str += sub.StringPushSum() + "\n"
		}
	}
	str += "----\n推文\n請輸入「推文清單」查看推文追蹤列表。"

	return str
}

func (ss Subscriptions) StringCommentList() string {
	var str string
	for _, sub := range ss {
		if sub.StringArticle() != "" {
			str += sub.StringArticle() + "\n"
		}
	}
	return str
}

func (ss *Subscriptions) Add(sub Subscription) error {
	return ss.AddContext(context.Background(), sub)
}

func (ss *Subscriptions) AddContext(ctx context.Context, sub Subscription) error {
	ok, suggestion, err := board.CheckBoardExistContext(ctx, sub.Board)
	if err != nil {
		return err
	}
	if !ok {
		return board.BoardNotExistError{Suggestion: suggestion}
	}
	ss.AddVerified(sub)
	return nil
}

// AddVerified updates the in-memory subscription after the caller has already
// verified the board through a successful article fetch.
func (ss *Subscriptions) AddVerified(sub Subscription) {
	sub.CleanUp()
	if isSubEmpty(sub) {
		return
	}
	for i, s := range *ss {
		if strings.EqualFold(s.Board, sub.Board) {
			s.Keywords.AppendNonRepeat(sub.Keywords, false)
			s.Authors.AppendNonRepeat(sub.Authors, false)
			s.Articles.AppendNonRepeat(sub.Articles, false)
			(*ss)[i] = s
			return
		}
	}
	*ss = append(*ss, sub)
}

func (ss *Subscriptions) Remove(sub Subscription) error {
	sub.CleanUp()
	for i := 0; i < len(*ss); i++ {
		s := (*ss)[i]
		if strings.EqualFold(s.Board, sub.Board) {
			s.DeleteKeywords(sub.Keywords)
			s.DeleteAuthors(sub.Authors)
			s.DeleteArticles(sub.Articles)
			(*ss)[i] = s
			if isSubEmpty((*ss)[i]) {
				*ss = append((*ss)[:i], (*ss)[i+1:]...)
				i--
				return nil
			}
		}
	}
	return nil
}

func (ss *Subscriptions) Update(sub Subscription) error {
	return ss.UpdateContext(context.Background(), sub)
}

func (ss *Subscriptions) UpdateContext(ctx context.Context, sub Subscription) error {
	ok, suggestion, err := board.CheckBoardExistContext(ctx, sub.Board)
	if err != nil {
		return err
	}
	if !ok {
		return board.BoardNotExistError{Suggestion: suggestion}
	}
	ss.UpdateVerified(sub)
	return nil
}

// UpdateVerified updates a push-sum subscription after board validation has
// already succeeded and performs no external writes.
func (ss *Subscriptions) UpdateVerified(sub Subscription) {
	for i := 0; i < len(*ss); i++ {
		s := (*ss)[i]
		if strings.EqualFold(s.Board, sub.Board) {
			s.PushSum = sub.PushSum
			(*ss)[i] = s
			if isSubEmpty((*ss)[i]) {
				*ss = append((*ss)[:i], (*ss)[i+1:]...)
				i--
			}
			return
		}
	}
	if isSubEmpty(sub) {
		return
	}
	*ss = append(*ss, sub)
}

func (ss *Subscriptions) Delete(sub Subscription) error {
	for i := 0; i < len(*ss); i++ {
		s := (*ss)[i]
		if strings.EqualFold(s.Board, sub.Board) {
			*ss = append((*ss)[:i], (*ss)[i+1:]...)
			i--
			return nil
		}
	}
	return nil
}

func isSubEmpty(sub Subscription) bool {
	return len(sub.Keywords) == 0 && len(sub.Authors) == 0 && len(sub.Articles) == 0 && sub.PushSum == PushSum{}
}
