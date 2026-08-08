package jobs

import (
	"context"
	"sync"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
)

var fetchArticlesForCache = func(ctx context.Context, bd board.Board) (article.Articles, error) {
	return bd.FetchArticlesContext(ctx)
}

type Fetcher struct {
}

func NewFetcher() *Fetcher {
	f := new(Fetcher)
	return f
}

func (f Fetcher) Run() {
	f.RunContext(context.Background())
}

func (f Fetcher) RunContext(ctx context.Context) {
	boards := models.Board().All()

	var wg sync.WaitGroup
	for index, bd := range boards {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(bd board.Board) {
			defer wg.Done()
			if err := fetchAndCacheBoard(ctx, bd); err != nil {
				log.WithField("board", bd.Name).WithError(err).Warning("Fetch failed; existing cache retained")
				return
			}
			log.WithField("board", bd.Name).Info("Fetched")
		}(*bd)

		if index == len(boards)-1 {
			continue
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
	wg.Wait()
	log.Info("All fetcher done")
}

func fetchAndCacheBoard(ctx context.Context, bd board.Board) error {
	articles, err := fetchArticlesForCache(ctx, bd)
	if err != nil {
		return err
	}
	bd.Articles = articles
	return bd.Save()
}
