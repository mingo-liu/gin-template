package worker

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	rdb "github.com/redis/go-redis/v9"
	"likeflow/internal/model"
	mysqlrepo "likeflow/internal/repository/mysql"
	redisrepo "likeflow/internal/repository/redis"
)

type testSender struct {
	fail   bool
	events []model.Event
}

func (s *testSender) Send(_ context.Context, e model.Event) error {
	if s.fail {
		return errors.New("broker unavailable")
	}
	s.events = append(s.events, e)
	return nil
}

func TestRelayRetentionAndRecoveryIntegration(t *testing.T) {
	dsn, addr := os.Getenv("MYSQL_TEST_DSN"), os.Getenv("REDIS_TEST_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set MYSQL_TEST_DSN and REDIS_TEST_ADDR")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	client := rdb.NewClient(&rdb.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	redis := &redisrepo.Repository{Client: client, MaxBacklog: 100}
	mysql := &mysqlrepo.Repository{DB: db}
	identity, err := redis.Identity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := model.Target{Type: 1, ID: strconv.FormatInt(time.Now().UnixNano(), 10), RedisRunID: identity, Initialized: true}
	_, err = db.Exec(`INSERT INTO targets(target_type,target_id,title,summary,category,initialized,redis_run_id) VALUES(1,?,'test','','',TRUE,?)`, target.ID, identity)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("INSERT INTO like_count(target_type,target_id) VALUES(1,?)", target.ID)
	t.Cleanup(func() {
		client.Del(ctx, redisrepo.Keys(target)...)
		db.Exec("DELETE FROM user_like WHERE target_id=?", target.ID)
		db.Exec("DELETE FROM like_count WHERE target_id=?", target.ID)
		db.Exec("DELETE FROM targets WHERE target_id=?", target.ID)
	})
	if err = redis.Initialize(ctx, target, identity); err != nil {
		t.Fatal(err)
	}
	if _, err = redis.State(ctx, target, "1001", "LIKE"); err != nil {
		t.Fatal(err)
	}
	sender := &testSender{fail: true}
	w := &Worker{Redis: redis, DB: mysql, MQ: sender, Consumer: "test", ReplayAfter: -time.Second, cursors: map[string]string{}}
	if err = w.process(ctx, target); err == nil {
		t.Fatal("expected MQ failure")
	}
	key := redisrepo.Keys(target)[3]
	if pending := client.XPending(ctx, key, redisrepo.Group).Val(); pending.Count != 1 {
		t.Fatalf("pending=%+v", pending)
	}
	sender.fail = false
	if err = w.process(ctx, target); err != nil {
		t.Fatal(err)
	}
	if len(sender.events) != 1 {
		t.Fatalf("replay events=%d", len(sender.events))
	}
	if n := client.XLen(ctx, key).Val(); n != 1 {
		t.Fatal("deleted event before persistence")
	}
	if err = w.Apply(ctx, sender.events[0]); err != nil {
		t.Fatal(err)
	}
	if err = w.process(ctx, target); err != nil {
		t.Fatal(err)
	}
	if n := client.XLen(ctx, key).Val(); n != 0 {
		t.Fatal("committed log not cleaned")
	}
	// A partial write can leave the stream ahead of the versions hash.
	if _, err = redis.State(ctx, target, "1001", "UNLIKE"); err != nil {
		t.Fatal(err)
	}
	k := redisrepo.Keys(target)
	client.HSet(ctx, k[2], "1001", "1")
	client.SAdd(ctx, k[0], "1001")
	client.Set(ctx, k[1], "1", 0)
	client.HSet(ctx, k[4], "state", "RECOVERING", "recovery_token", "old-task")
	if err = w.Recover(ctx, target, ""); err == nil {
		t.Fatal("stole a recovery task")
	}
	if err = w.Recover(ctx, target, "old-task"); err != nil {
		t.Fatal(err)
	}
	state, err := redis.State(ctx, target, "1001", "QUERY")
	if err != nil || state.Liked || state.Count != "0" || state.Version != "2" {
		t.Fatalf("recovery: %+v %v", state, err)
	}
	client.HSet(ctx, k[4], "state", "RECOVERING", "recovery_token", "another-task")
	entries := client.XRange(ctx, key, "-", "+").Val()
	if len(entries) != 1 {
		t.Fatal("recovery destroyed retained log")
	}
	if err = redis.DeleteCommitted(ctx, target, entries[0].ID); err != nil {
		t.Fatal(err)
	}
	if client.XLen(ctx, key).Val() != 1 {
		t.Fatal("cleanup crossed recovery fence")
	}
}
