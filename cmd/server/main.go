package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	rdb "github.com/redis/go-redis/v9"
	"likeflow/internal/config"
	"likeflow/internal/handler"
	"likeflow/internal/middleware"
	"likeflow/internal/queue"
	"likeflow/internal/repository/mysql"
	"likeflow/internal/repository/redis"
	"likeflow/internal/routes"
	"likeflow/internal/service"
	"likeflow/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	recoverID := flag.String("recover", "", "recover target ID (operators must first verify log continuity)")
	recoverType := flag.Int("type", 1, "target type for recovery")
	resume := flag.String("resume-token", "", "replace a failed recovery token and fence the old task")
	verified := flag.Bool("verified-log", false, "operator confirms surviving log and MQ continuity after fencing failed primary")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err = db.PingContext(startup); err != nil {
		return err
	}
	client := rdb.NewClient(&rdb.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, MaxRetries: -1, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	defer client.Close()
	cache := &redis.Redis{Client: client, MaxBacklog: cfg.MaxBacklog}
	store := &mysql.MySQL{DB: db}
	identity, err := cache.Identity(startup)
	if err != nil {
		return err
	}
	targets, err := store.Targets(startup)
	if err != nil {
		return err
	}
	w := &worker.Worker{Redis: cache, DB: store, ReplayAfter: cfg.ReplayAfter}
	if *recoverID != "" {
		if !*verified {
			return fmt.Errorf("recovery requires --verified-log after operator verifies no log rollback/loss and MQ in-flight/retry/dead-letter state")
		}
		for _, t := range targets {
			if t.ID == *recoverID && t.Type == *recoverType {
				return w.Recover(ctx, t, *resume)
			}
		}
		return fmt.Errorf("unknown recovery target")
	}
	for _, t := range targets {
		if !t.Initialized {
			relations, count, err := store.Snapshot(startup, t)
			if err != nil {
				return err
			}
			if len(relations) != 0 || count != 0 {
				return fmt.Errorf("historical target %s requires recovery", t.ID)
			}
			// Reserve initialization durably before Redis writes. A crash requires explicit recovery.
			result, err := db.ExecContext(startup, `UPDATE targets SET initialized=TRUE,redis_run_id=? WHERE target_type=? AND target_id=? AND initialized=FALSE`, identity, t.Type, t.ID)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if n == 1 {
				if err = cache.Initialize(startup, t, identity); err != nil {
					return err
				}
			}
		}
	}
	q, err := queue.New(cfg.MQNameserver, cfg.MQTopic, w.Apply)
	if err != nil {
		return err
	}
	if err = q.Start(); err != nil {
		return err
	}
	defer q.Close()
	w.MQ = q
	workCtx, workCancel := context.WithCancel(ctx)
	defer workCancel()
	done := make(chan struct{})
	go func() { defer close(done); w.Run(workCtx) }()
	h := &handler.Handler{Service: &service.Service{DB: store, Redis: cache}, Auth: middleware.Auth{Secret: cfg.SessionSecret}, Demo: cfg.Demo}
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: routes.New(h), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	failures := make(chan error, 1)
	go func() {
		slog.Info("Likeflow listening", "address", cfg.HTTPAddr, "demo_mode", cfg.Demo)
		failures <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
	case err = <-failures:
		if !errors.Is(err, http.ErrServerClosed) {
			workCancel()
			<-done
			return err
		}
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	err = server.Shutdown(shutdown)
	workCancel()
	<-done
	return err
}
