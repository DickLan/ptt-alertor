package jobs

import (
	"context"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

type pttHealthCheck func(context.Context) error

type pttMonitor struct {
	duration    time.Duration
	retry       int
	healthCheck pttHealthCheck
}

func NewPttMonitor() *pttMonitor {
	return &pttMonitor{
		duration:    1 * time.Minute,
		retry:       3,
		healthCheck: web.CheckSiteContext,
	}
}

func (pm *pttMonitor) Run() {
	pm.RunContext(context.Background())
}

// RunContext monitors PTT until ctx is canceled. It only reports health: the
// shared PTT limiter and cooldown policy are responsible for reducing load, so
// the monitor must not stop or restart the singleton checker jobs.
func (pm *pttMonitor) RunContext(ctx context.Context) {
	log.Info("Start Ptt Monitor")

	ticker := time.NewTicker(pm.duration)
	defer ticker.Stop()
	state := newPttMonitorState(pm.retry)
	healthCheck := pm.healthCheck
	if healthCheck == nil {
		healthCheck = web.CheckSiteContext
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := healthCheck(ctx)
			if ctx.Err() != nil {
				return
			}
			logPttHealth(state.observe(err), err)
		}
	}
}

type pttMonitorEvent uint8

const (
	pttAlive pttMonitorEvent = iota
	pttDying
	pttDead
	pttStillDead
	pttRecovered
)

type pttMonitorState struct {
	failures int
	retry    int
	dead     bool
}

func newPttMonitorState(retry int) *pttMonitorState {
	if retry < 1 {
		retry = 1
	}
	return &pttMonitorState{retry: retry}
}

func (state *pttMonitorState) observe(err error) pttMonitorEvent {
	if err == nil {
		wasDead := state.dead
		state.failures = 0
		state.dead = false
		if wasDead {
			return pttRecovered
		}
		return pttAlive
	}

	state.failures++
	if state.failures < state.retry {
		return pttDying
	}
	if !state.dead {
		state.dead = true
		return pttDead
	}
	return pttStillDead
}

func logPttHealth(event pttMonitorEvent, err error) {
	switch event {
	case pttAlive:
		log.Info("Ptt is alive")
	case pttDying:
		log.WithError(err).Warn("Ptt is dying")
	case pttDead:
		log.WithError(err).Error("Ptt is dead")
	case pttStillDead:
		log.WithError(err).Warn("Ptt is still unavailable")
	case pttRecovered:
		log.Info("Ptt is back to life")
	}
}
