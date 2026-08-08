package jobs

import (
	"context"
	"sort"
	"strconv"

	log "github.com/Ptt-Alertor/logrus"

	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/top"
)

type Top struct{}

func NewTop() *Top {
	return &Top{}
}

func (t Top) Run() {
	t.RunContext(context.Background())
}

func (t Top) RunContext(ctx context.Context) {
	log.Info("Top List Generated")
	keywordMap := make(map[top.BoardWord]int)
	authorMap := make(map[top.BoardWord]int)
	pushSumMap := make(map[top.BoardWord]int)
	for _, u := range models.User().AllContext(ctx) {
		if ctx.Err() != nil {
			return
		}
		for _, sub := range u.Subscribes {
			for _, keyword := range sub.Keywords {
				keyword = strings.ToLower(keyword)
				keywordMap[top.BoardWord{Board: sub.Board, Word: keyword}]++
			}
			for _, author := range sub.Authors {
				author = strings.ToLower(author)
				authorMap[top.BoardWord{Board: sub.Board, Word: author}]++
			}
			if sub.PushSum.Up != 0 {
				pushSumMap[top.BoardWord{Board: sub.Board, Word: strconv.Itoa(sub.PushSum.Up)}]++
			}
			if sub.PushSum.Down != 0 {
				pushSumMap[top.BoardWord{Board: sub.Board, Word: strconv.Itoa(sub.PushSum.Down * -1)}]++
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	topKeywords := rank(keywordMap)
	if err := topKeywords.SaveKeywordsContext(ctx); err != nil {
		return
	}
	topAuthors := rank(authorMap)
	if err := topAuthors.SaveAuthorsContext(ctx); err != nil {
		return
	}
	topPushSum := rank(pushSumMap)
	_ = topPushSum.SavePushSumContext(ctx)
}

func rank(m map[top.BoardWord]int) (orderSlice top.WordOrders) {
	for key, count := range m {
		k := top.WordOrder{BoardWord: key, Count: count}
		orderSlice = append(orderSlice, k)
	}
	sort.Slice(orderSlice, func(i, j int) bool {
		return orderSlice[i].Count > orderSlice[j].Count
	})
	return orderSlice
}
