package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"gin-template/internal/model"
	mysqlrepo "gin-template/internal/repository/mysql"
	redisrepo "gin-template/internal/repository/redis"
	rdb "github.com/redis/go-redis/v9"
)

type Sender interface {
	Send(context.Context, model.Event) error
}
type Worker struct {
	Redis       *redisrepo.Repository
	DB          *mysqlrepo.Repository
	MQ          Sender
	ReplayAfter time.Duration
	Consumer    string
	cursors     map[string]string
}

func (w *Worker) Apply(ctx context.Context, e model.Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	t := model.Target{Type: e.TargetType, ID: e.TargetID}
	// A quarantined conflict is retried until an operator resolves the inconsistent versions.
	reason, err := w.Redis.Client.HGet(ctx, redisrepo.Keys(t)[4], "error").Result()
	if err != nil && err != rdb.Nil {
		return err
	}
	if reason != "" {
		return fmt.Errorf("quarantined target: %s", reason)
	}
	err = w.DB.Apply(ctx, e)
	if errors.Is(err, model.ErrConflict) {
		if isolateErr := w.Redis.Quarantine(ctx, t, err.Error()); isolateErr != nil {
			slog.Error("quarantine failed", "error", isolateErr)
		}
	}
	return err
}

// Run forwards pending/new entries and independently reconciles retained logs with MySQL.
func (w *Worker) Run(ctx context.Context) {
	w.Consumer = redisrepo.Token()
	w.cursors = map[string]string{}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var ticks int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			targets, err := w.DB.Targets(ctx)
			if err != nil {
				slog.Error("target discovery", "error", err)
				continue
			}
			for _, t := range targets {
				if ctx.Err() != nil {
					return
				}
				if err := w.process(ctx, t); err != nil {
					slog.Error("relay/reconcile", "type", t.Type, "target", t.ID, "error", err)
				}
			}
			ticks++
			if ticks%15 == 0 {
				w.scan(ctx, targets)
			}
			if ticks%30 == 0 {
				for _, t := range targets {
					if _, _, err := w.DB.Snapshot(ctx, t); err != nil {
						slog.Error("MySQL reconciliation", "target", t.ID, "error", err)
					}
				}
			}
		}
	}
}

func (w *Worker) send(ctx context.Context, t model.Target, m rdb.XMessage) error {
	e, err := redisrepo.Event(t, m)
	if err != nil {
		w.Redis.Quarantine(ctx, t, err.Error())
		return err
	}
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = w.MQ.Send(sendCtx, e); err != nil {
		return err
	}
	return w.Redis.Client.XAck(ctx, redisrepo.Keys(t)[3], redisrepo.Group, m.ID).Err()
}

func (w *Worker) process(ctx context.Context, t model.Target) error {
	k := redisrepo.Keys(t)
	identity, err := w.Redis.Identity(ctx)
	if err != nil {
		return err
	}
	if identity != t.RedisRunID {
		if err := w.Redis.Client.HSet(ctx, k[4], "state", "RECOVERING").Err(); err != nil {
			return err
		}
	}
	locked, err := w.Redis.Client.HExists(ctx, k[4], "recovery_token").Result()
	if err != nil {
		return err
	}
	if locked {
		return nil
	}
	// Never recreate a disappeared Stream. Missing logs require explicit operator verification.
	typ, err := w.Redis.Client.Type(ctx, k[3]).Result()
	if err != nil {
		return err
	}
	if typ != "stream" {
		return model.ErrUnavailable
	}
	err = w.Redis.Client.XGroupCreateMkStream(ctx, k[3], redisrepo.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	messages, _, err := w.Redis.Client.XAutoClaim(ctx, &rdb.XAutoClaimArgs{Stream: k[3], Group: redisrepo.Group, Consumer: w.Consumer, MinIdle: 30 * time.Second, Start: "0-0", Count: 32}).Result()
	if err != nil {
		return err
	}
	for _, m := range messages {
		if err := w.send(ctx, t, m); err != nil {
			return err
		}
	}
	streams, err := w.Redis.Client.XReadGroup(ctx, &rdb.XReadGroupArgs{Group: redisrepo.Group, Consumer: w.Consumer, Streams: []string{k[3], ">"}, Count: 32, Block: -1}).Result()
	if err != nil && err != rdb.Nil {
		return err
	}
	for _, stream := range streams {
		for _, m := range stream.Messages {
			if err := w.send(ctx, t, m); err != nil {
				return err
			}
		}
	}
	pending, err := w.Redis.Client.XPending(ctx, k[3], redisrepo.Group).Result()
	if err != nil {
		return err
	}
	if pending.Count > 0 {
		slog.Info("stream pending", "target", t.ID, "count", pending.Count, "oldest", pending.Lower)
	}
	// Walk all retained entries, including ACKed messages and dead-lettered deliveries.
	cursor := w.cursors[k[3]]
	if cursor == "" {
		cursor = "-"
	}
	entries, err := w.Redis.Client.XRangeN(ctx, k[3], cursor, "+", 100).Result()
	if err != nil {
		return err
	}
	for _, m := range entries {
		e, err := redisrepo.Event(t, m)
		if err != nil {
			w.Redis.Quarantine(ctx, t, err.Error())
			return err
		}
		relation, err := w.DB.Relation(ctx, t, e.UserID)
		if err != nil {
			return err
		}
		version, _ := strconv.ParseUint(e.Version, 10, 64)
		if relation.Version > version || (relation.Version == version && relation.Liked == (e.Action == "LIKE")) {
			if err = w.Redis.DeleteCommitted(ctx, t, m.ID); err != nil {
				return err
			}
		} else if relation.Version == version {
			w.Redis.Quarantine(ctx, t, model.ErrConflict.Error())
			return model.ErrConflict
		} else {
			ms, _, _ := strings.Cut(m.ID, "-")
			stamp, _ := strconv.ParseInt(ms, 10, 64)
			if time.Since(time.UnixMilli(stamp)) > w.ReplayAfter {
				if err = w.send(ctx, t, m); err != nil {
					return err
				}
			}
		}
		w.cursors[k[3]] = "(" + m.ID
	}
	if len(entries) < 100 {
		w.cursors[k[3]] = "-"
	}
	return nil
}

func (w *Worker) scan(ctx context.Context, targets []model.Target) {
	known := map[string]bool{}
	for _, t := range targets {
		known[redisrepo.Keys(t)[3]] = true
	}
	var cursor uint64
	for {
		keys, next, err := w.Redis.Client.Scan(ctx, cursor, "like:*:events", 100).Result()
		if err != nil {
			slog.Error("stream discovery scan", "error", err)
			return
		}
		for _, key := range keys {
			if !known[key] {
				slog.Error("orphan stream requires catalog repair", "key", key)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
