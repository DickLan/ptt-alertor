package jobs

import (
	"context"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/models/pushsum"
)

type PushSumKeyReplacer struct{}

func NewPushSumKeyReplacer() *PushSumKeyReplacer {
	return &PushSumKeyReplacer{}
}

func (r PushSumKeyReplacer) Run() {
	r.RunContext(context.Background())
}

func (r PushSumKeyReplacer) RunContext(ctx context.Context) {
	if err := pushsum.ReplaceBenchKeysContext(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		log.WithError(err).Error("Replace Pushsum Key Failed")
	}
	log.Info("Replace Pushsum Key Done")
}
