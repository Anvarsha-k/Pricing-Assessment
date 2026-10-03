// Command pricing-engine consumes the upstream market data feed, computes
// rolling 24h statistics and serves them to WebSocket clients.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"pricing-engine/internal/config"
	"pricing-engine/internal/engine"
	"pricing-engine/internal/feed"
	"pricing-engine/internal/model"
	"pricing-engine/internal/server"
	"pricing-engine/internal/store"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local /healthz endpoint and exit (for Docker HEALTHCHECK)")
	flag.Parse()
	if *healthcheck {
		os.Exit(probe())
	}
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	basis, err := engine.ParseBasis(cfg.PriceBasis)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Persistence.
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}
	st, err := store.Open(filepath.Join(cfg.DataDir, "state.db"), cfg.Retention, log.With("component", "store"))
	if err != nil {
		return err
	}
	defer st.Close()

	// Core pipeline: feed -> engine -> hub -> clients.
	reg := model.NewRegistry(cfg.Symbols)
	hub := server.NewHub(reg, cfg.MaxFrameBytes, cfg.FanoutInterval)
	go hub.Run(ctx)
	flushCh := make(chan store.Batch, 1)
	eng := engine.New(engine.Config{
		Window:            cfg.Window,
		Resolution:        cfg.Resolution,
		Basis:             basis,
		SpikeThreshold:    cfg.SpikePct / 100,
		SpikeConfirm:      cfg.SpikeConfirm,
		SpikeConfirmAfter: cfg.SpikeHold,
		MaxClockSkew:      cfg.MaxClockSkew,
		FlushInterval:     cfg.FlushInterval,
		QueueSize:         cfg.EngineQueue,
	}, reg, hub, flushCh, log.With("component", "engine"))

	t0 := time.Now()
	buckets, quotes, err := st.Load(time.Now().Add(-cfg.Window - cfg.Resolution).UnixMilli())
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	restored := eng.Restore(buckets, quotes)
	log.Info("state restored", "symbols", restored, "took", time.Since(t0).String())

	srv := server.New(server.Config{
		MaxFrameBytes: cfg.MaxFrameBytes,
		WriteTimeout:  cfg.ClientWriteTTL,
		PingInterval:  20 * time.Second,
		PongWait:      60 * time.Second,
		MaxClients:    cfg.MaxClients,
		MaxConnsPerIP: cfg.MaxConnsPerIP,
		ControlRate:   cfg.ControlRate,
		ControlBurst:  cfg.ControlBurst,
		Resolution:    cfg.Resolution,
		Info: map[string]any{
			"window": cfg.Window.String(),
			"basis":  cfg.PriceBasis,
		},
	}, hub, eng, log.With("component", "server"))
	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	storeDone := make(chan struct{})
	go func() { st.Run(flushCh); close(storeDone) }()
	engCtx, stopEngine := context.WithCancel(context.Background())
	engDone := make(chan struct{})
	go func() { eng.Run(engCtx); close(engDone) }()

	httpErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	feedCtx, stopFeed := context.WithCancel(ctx)
	feedDone := make(chan struct{})
	fc := feed.New(feed.Config{
		URL:             cfg.FeedURL,
		APIKey:          cfg.APIKey,
		Secret:          cfg.SecretKey,
		Symbols:         cfg.Symbols,
		DialTimeout:     10 * time.Second,
		AuthTimeout:     10 * time.Second,
		IdleTimeout:     cfg.FeedIdle,
		PingInterval:    cfg.FeedIdle / 3,
		BackoffMin:      cfg.BackoffMin,
		BackoffMax:      cfg.BackoffMax,
		MaxMessageBytes: cfg.MaxFeedBytes,
	}, eng.Input(), hub.SetUpstream, log.With("component", "feed"))
	go func() { fc.Run(feedCtx); close(feedDone) }()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err = <-httpErr:
		log.Error("http server failed", "err", err)
	}

	// Ordered shutdown: stop ingest, let the engine drain and write its final
	// batch, wait for the store to commit it, then stop serving.
	stopFeed()
	<-feedDone
	stopEngine()
	<-engDone
	<-storeDone
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutCtx)
	log.Info("shutdown complete")
	return err
}

func probe() int {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
