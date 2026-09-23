package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/google/gops/agent"
	"github.com/julienschmidt/httprouter"
	"github.com/robfig/cron"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/connections"
	ctrlr "github.com/Ptt-Alertor/ptt-alertor/controllers"
	"github.com/Ptt-Alertor/ptt-alertor/jobs"
	"github.com/Ptt-Alertor/ptt-alertor/models/pttaccess"
	modelUser "github.com/Ptt-Alertor/ptt-alertor/models/user"
	pttHTTP "github.com/Ptt-Alertor/ptt-alertor/ptt/http"
	"github.com/Ptt-Alertor/ptt-alertor/stockwatch"
)

var (
	authUser     = os.Getenv("AUTH_USER")
	authPassword = os.Getenv("AUTH_PW")
	storagePing  = connections.Ping
)

type myRouter struct {
	httprouter.Router
}

func (mr myRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.WithFields(log.Fields{
		"method": r.Method,
		"IP":     r.RemoteAddr,
		"URI":    r.URL.Path,
	}).Info("visit")
	mr.Router.ServeHTTP(w, r)
}

func newRouter() *myRouter {
	r := &myRouter{
		Router: *httprouter.New(),
	}
	r.NotFound = http.FileServer(http.Dir("public"))
	return r
}

func basicAuth(handle httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		if authUser == "" || authPassword == "" {
			http.Error(w, "authenticated API is disabled until AUTH_USER and AUTH_PW are configured", http.StatusServiceUnavailable)
			return
		}
		user, password, hasAuth := r.BasicAuth()
		if hasAuth && user == authUser && password == authPassword {
			handle(w, r, params)
		} else {
			w.Header().Set("WWW-Authenticate", "Basic realm=Restricted")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		}
	}
}

func requireStorage(handle httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		if err := storagePing(); err != nil {
			// Do not turn a Redis outage into fake 404/empty data, and do not leak
			// internal DNS or dial details to API clients.
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		handle(w, r, params)
	}
}

func readinessHandler(jobsRequested bool, webhookErr error) httprouter.Handle {
	return func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := storagePing(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		if jobsRequested && webhookErr != nil {
			http.Error(w, "notification delivery unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

func main() {
	if err := run(); err != nil {
		log.WithError(err).Error("Ptt Alertor stopped with an error")
		os.Exit(1)
	}
}

func run() error {
	accessStore, err := pttaccess.NewRedis(pttaccess.RedisConfig{
		HourlyLimit: boundedPositiveInt64FromEnvironment("PTT_MAX_REQUESTS_PER_HOUR", 900, 1800),
		DailyLimit:  boundedPositiveInt64FromEnvironment("PTT_MAX_REQUESTS_PER_DAY", 10_000, 20_000),
	})
	if err != nil {
		return fmt.Errorf("configure persistent PTT access policy: %w", err)
	}
	pttHTTP.SetAccessState(accessStore)
	defer pttHTTP.SetAccessState(nil)

	if !strings.EqualFold(os.Getenv("RECONCILE_SUBSCRIPTION_INDEXES"), "false") {
		log.Info("Reconcile subscription indexes from user records")
		if err := modelUser.RebuildSubscriptionIndexes(); err != nil {
			return fmt.Errorf("reconcile subscription indexes: %w", err)
		}
	}

	stockService := stockwatch.New(stockwatch.NewStore(), environmentExplicitlyEnabled("STOCK_QUANT_NOTIFY_ENABLE"), os.Getenv("STOCK_QUANT_DISCORD_WEBHOOK_URL"), os.Getenv("DISCORD_WEBHOOK_URL"), os.Getenv("STOCK_QUANT_API_TOKEN"))
	stopStockWatch := jobs.StartStockWatch(stockService)
	defer stopStockWatch()
	webhookErr := discord.NewFromEnv().Validate()
	// Polling PTT is opt-in. Empty, misspelled, and otherwise ambiguous values
	// must remain safe instead of silently starting background traffic.
	jobsRequested := environmentExplicitlyEnabled("JOBS_ENABLED")
	var delivery *discordOutboxRuntime
	if webhookErr != nil {
		log.WithError(webhookErr).Warn("Discord outbox delivery disabled until a valid webhook is configured")
	} else {
		delivery = startDiscordOutbox(context.Background())
		log.Info("Start durable Discord outbox worker")
	}

	var background *jobRuntime
	if jobsRequested {
		if webhookErr != nil {
			log.Warn("Background jobs disabled until a valid Discord webhook is configured")
		} else {
			log.Info("Start Jobs")
			var err error
			background, err = startJobs(context.Background())
			if err != nil {
				if delivery != nil {
					delivery.Stop()
				}
				return err
			}
		}
	} else {
		log.Info("Background jobs are disabled")
	}

	router := newRouter()
	stockAPI := ctrlr.StockWatch{Service: stockService, Token: os.Getenv("STOCK_QUANT_API_TOKEN")}
	router.GET("/integrations/stock-quant/authors", stockAPI.Handle)
	router.POST("/integrations/stock-quant/authors", stockAPI.Handle)
	router.PATCH("/integrations/stock-quant/authors/:author", stockAPI.Handle)
	router.DELETE("/integrations/stock-quant/authors/:author", stockAPI.Handle)

	router.GET("/", requireStorage(ctrlr.Index))
	router.GET("/redirect/:checksum", requireStorage(ctrlr.Redirect))
	router.GET("/top", requireStorage(ctrlr.Top))
	router.GET("/docs", ctrlr.Docs)
	router.GET("/healthz", func(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	router.GET("/readyz", readinessHandler(jobsRequested, webhookErr))

	// websocket
	router.GET("/ws", requireStorage(ctrlr.WebSocket))

	router.POST("/broadcast", basicAuth(ctrlr.Broadcast))

	// boards apis
	router.GET("/boards/:boardName/articles/:code", requireStorage(ctrlr.BoardArticle))
	// This endpoint performs a live PTT fetch. Keep it behind authentication so
	// anonymous callers cannot monopolize the process-wide PTT request queue.
	router.GET("/boards/:boardName/articles", basicAuth(requireStorage(ctrlr.BoardArticleIndex)))
	router.GET("/boards", requireStorage(ctrlr.BoardIndex))

	// keyword apis
	router.GET("/keyword/boards", requireStorage(ctrlr.KeywordBoards))

	// author apis
	router.GET("/author/boards", requireStorage(ctrlr.AuthorBoards))

	// pushsum apis
	router.GET("/pushsum/boards", requireStorage(ctrlr.PushSumBoards))

	// articles apis
	router.GET("/articles", requireStorage(ctrlr.ArticleIndex))

	// users apis
	router.GET("/users/:account", basicAuth(requireStorage(ctrlr.UserFind)))
	router.GET("/users", basicAuth(requireStorage(ctrlr.UserAll)))
	router.POST("/users", basicAuth(requireStorage(ctrlr.UserCreate)))
	router.PUT("/users/:account", basicAuth(requireStorage(ctrlr.UserModify)))
	router.POST("/users/:account/commands", basicAuth(requireStorage(ctrlr.UserCommand)))

	// gops is intended for diagnostics and is disabled unless an address is set.
	if addr := os.Getenv("GOPS_ADDR"); addr != "" {
		if err := agent.Listen(agent.Options{Addr: addr, ShutdownCleanup: true}); err != nil {
			if background != nil {
				background.Stop()
			}
			if delivery != nil {
				delivery.Stop()
			}
			return fmt.Errorf("start gops agent: %w", err)
		}
	}

	// Web Server
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}
	log.Info("Web Server Start on Port " + port)
	srv := http.Server{
		Addr:              ":" + port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)
	var runErr error
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			runErr = fmt.Errorf("web server failed: %w", err)
		}
	case sig := <-quit:
		log.WithField("signal", sig.String()).Info("Shutdown signal received")
	}

	jobsStopped := make(chan struct{})
	if background != nil {
		go func() {
			background.Stop()
			close(jobsStopped)
		}()
	} else {
		close(jobsStopped)
	}

	log.Info("Shutdown Web Server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Shutdown(ctx); err != nil {
		log.WithError(err).Error("Web Server Shutdown Failed")
		_ = srv.Close()
		if runErr == nil {
			runErr = fmt.Errorf("shut down web server: %w", err)
		}
	}
	cancel()
	log.Info("Web Server Was Been Shutdown")

	<-jobsStopped
	if delivery != nil {
		// Keep delivery alive after all PTT producers stop. A bounded best-effort
		// drain improves shutdown latency without risking loss: anything left is
		// durable in Redis and its lease is recovered on the next start.
		drainDuration := durationFromEnvironment("DISCORD_OUTBOX_SHUTDOWN_DRAIN", 15*time.Second)
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), drainDuration)
		if err := jobs.WaitForDiscordOutboxEmpty(drainCtx); err != nil {
			pending, countErr := jobs.DiscordOutboxPending(context.Background())
			fields := log.Fields{"pending": pending}
			if countErr != nil {
				fields["pending"] = "unknown"
			}
			log.WithFields(fields).Warn("Discord outbox drain ended; pending events remain durable")
		}
		cancelDrain()
		delivery.Stop()
	}
	stopStockWatch()
	if err := connections.Close(); err != nil {
		log.WithError(err).Warn("Close Redis Pool Failed")
	}
	return runErr
}

func durationFromEnvironment(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

func environmentExplicitlyEnabled(name string) bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(name)), "true")
}

func boundedPositiveInt64FromEnvironment(name string, fallback, maximum int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	if maximum > 0 && parsed > maximum {
		return maximum
	}
	return parsed
}

type discordOutboxRuntime struct {
	cancel   context.CancelFunc
	worker   sync.WaitGroup
	stopOnce sync.Once
}

func startDiscordOutbox(parent context.Context) *discordOutboxRuntime {
	ctx, cancel := context.WithCancel(parent)
	runtime := &discordOutboxRuntime{cancel: cancel}
	runtime.worker.Add(1)
	go func() {
		defer runtime.worker.Done()
		jobs.RunDiscordOutboxWorker(ctx)
	}()
	return runtime
}

func (runtime *discordOutboxRuntime) Stop() {
	if runtime == nil {
		return
	}
	runtime.stopOnce.Do(func() {
		runtime.cancel()
		runtime.worker.Wait()
	})
}

type jobRuntime struct {
	cancel       context.CancelFunc
	cron         *cron.Cron
	pollers      sync.WaitGroup
	scheduled    scheduledJobTracker
	shutdownOnce sync.Once
}

type scheduledJobTracker struct {
	mu        sync.Mutex
	accepting bool
	wg        sync.WaitGroup
}

type pttPoller struct {
	name string
	run  func(context.Context)
}

// configuredPTTPollers keeps the primary board/article checker enabled whenever
// JOBS_ENABLED starts the job runtime. Comment and push-sum tracking are
// independent opt-ins because both can add PTT page fetches that are unnecessary
// for title/author subscriptions.
func configuredPTTPollers() []pttPoller {
	pollers := []pttPoller{
		{
			name: "board",
			run: func(ctx context.Context) {
				jobs.NewChecker().RunContext(ctx)
			},
		},
	}
	if environmentExplicitlyEnabled("PTT_PUSHSUM_JOBS_ENABLED") {
		pollers = append(pollers, pttPoller{
			name: "pushsum",
			run: func(ctx context.Context) {
				jobs.NewPushSumChecker().RunContext(ctx)
			},
		})
	}
	if environmentExplicitlyEnabled("PTT_COMMENT_JOBS_ENABLED") {
		pollers = append(pollers, pttPoller{
			name: "comment",
			run: func(ctx context.Context) {
				jobs.NewCommentChecker().RunContext(ctx)
			},
		})
	}
	return pollers
}

func (tracker *scheduledJobTracker) wrap(job cron.Job) cron.Job {
	return cron.FuncJob(func() {
		tracker.mu.Lock()
		if !tracker.accepting {
			tracker.mu.Unlock()
			return
		}
		tracker.wg.Add(1)
		tracker.mu.Unlock()
		defer tracker.wg.Done()
		job.Run()
	})
}

func (tracker *scheduledJobTracker) stopAndWait() {
	tracker.mu.Lock()
	tracker.accepting = false
	tracker.mu.Unlock()
	tracker.wg.Wait()
}

func startJobs(parent context.Context) (*jobRuntime, error) {
	ctx, cancel := context.WithCancel(parent)
	runtime := &jobRuntime{
		cancel: cancel,
		cron:   cron.New(),
	}
	runtime.scheduled.accepting = true
	if err := runtime.cron.AddJob("@hourly", runtime.scheduled.wrap(cron.FuncJob(func() {
		jobs.NewTop().RunContext(ctx)
	}))); err != nil {
		cancel()
		return nil, fmt.Errorf("schedule top job: %w", err)
	}
	if err := runtime.cron.AddJob("@every 48h", runtime.scheduled.wrap(cron.FuncJob(func() {
		jobs.NewPushSumKeyReplacer().RunContext(ctx)
	}))); err != nil {
		cancel()
		return nil, fmt.Errorf("schedule push-sum cleanup: %w", err)
	}

	run := func(fn func(context.Context)) {
		runtime.pollers.Add(1)
		go func() {
			defer runtime.pollers.Done()
			fn(ctx)
		}()
	}
	for _, poller := range configuredPTTPollers() {
		log.WithField("poller", poller.name).Info("Start PTT poller")
		run(poller.run)
	}
	runtime.cron.Start()
	return runtime, nil
}

func (runtime *jobRuntime) Stop() {
	runtime.shutdownOnce.Do(func() {
		runtime.cancel()
		runtime.cron.Stop()
		runtime.scheduled.stopAndWait()
		runtime.pollers.Wait()
	})
}

func init() {
	// for initial app
	// jobs.NewPushSumKeyReplacer().Run()
	// jobs.NewMigrateBoard(map[string]string{}).Run()
	// jobs.NewTop().Run()
	// jobs.NewCacheCleaner().Run()
	// jobs.NewGenerator().Run()
	// jobs.NewFetcher().Run()
	// jobs.NewMigrateDB().Run()
	// jobs.NewCategoryCleaner().Run()
}
