package jobs

import (
	"context"
	"github.com/Ptt-Alertor/ptt-alertor/models/outbox"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
	"sync"
	"time"
)

// StartStockWatch owns both independent lifecycles; stopping is idempotent and
// finishes before the caller closes Redis or the shared PTT access policy.
func StartStockWatch(service *stockwatch.Service) func() {
	if service == nil || !service.Ready() {
		return func() {}
	}
	watchCtx, stopWatch := context.WithCancel(context.Background())
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	watchDone, deliveryDone := make(chan struct{}), make(chan struct{})
	worker := newDiscordOutboxWorker(service.Outbox, service.Client)
	worker.eligible = service.Eligible
	worker.onDelivered = func(item *outbox.ClaimedItem, id string) {
		if err := service.Store.RecordDelivery(item.Kind, id); err != nil {
			service.Store.DeliveryFailure("delivery_history_unavailable")
		}
	}
	worker.onFailure = service.Store.DeliveryFailure
	go func() { defer close(watchDone); service.Run(watchCtx) }()
	go func() { defer close(deliveryDone); worker.Run(deliveryCtx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			stopWatch()
			<-watchDone
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for ctx.Err() == nil {
				n, err := service.Outbox.PendingCount(ctx)
				if err != nil || n == 0 || !waitDiscordOutbox(ctx, 100*time.Millisecond) {
					break
				}
			}
			stopDelivery()
			<-deliveryDone
		})
	}
}
