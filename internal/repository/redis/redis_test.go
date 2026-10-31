package redis

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	rdb "github.com/redis/go-redis/v9"
	"likeflow/internal/model"
)

func testStore(t *testing.T) (*Store, model.Target) {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	cmd := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--dir", t.TempDir(), "--appendonly", "yes", "--maxmemory-policy", "noeviction", "--save", "")
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	client := rdb.NewClient(&rdb.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
	t.Cleanup(func() { client.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for client.Ping(context.Background()).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("Redis startup timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
	store := &Store{Client: client, MaxBacklog: 1000}
	id, err := store.Identity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := model.Target{Type: 1, ID: "2001", RedisRunID: id}
	if err = store.Initialize(context.Background(), target, id); err != nil {
		t.Fatal(err)
	}
	return store, target
}

func TestConcurrentIdempotency(t *testing.T) {
	store, target := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			s, err := store.State(ctx, target, "1001", "LIKE")
			if err != nil {
				t.Error(err)
				return
			}
			if !s.Liked || s.Count != "1" || s.Version != "1" {
				t.Errorf("bad state %+v", s)
			}
		})
	}
	wg.Wait()
	if n := store.Client.XLen(ctx, Keys(target)[3]).Val(); n != 1 {
		t.Fatalf("events=%d", n)
	}
	s, err := store.State(ctx, target, "1001", "UNLIKE")
	if err != nil || s.Liked || s.Count != "0" || s.Version != "2" || !s.Changed {
		t.Fatalf("unlike: %+v %v", s, err)
	}
	s, err = store.State(ctx, target, "1001", "UNLIKE")
	if err != nil || s.Changed || s.Version != "2" {
		t.Fatalf("repeat unlike: %+v %v", s, err)
	}
	s, err = store.State(ctx, target, "1001", "LIKE")
	if err != nil || s.Version != "3" {
		t.Fatalf("late LIKE: %+v %v", s, err)
	}
	if err = store.Initialize(ctx, target, target.RedisRunID); err == nil {
		t.Fatal("reinitialized live target")
	}
}

func TestCorruptionAndBackpressure(t *testing.T) {
	store, target := testStore(t)
	ctx := context.Background()
	k := Keys(target)
	store.Client.Set(ctx, k[1], "01", 0)
	if _, err := store.State(ctx, target, "1001", "LIKE"); err == nil {
		t.Fatal("accepted corrupt count")
	}
	if n := store.Client.XLen(ctx, k[3]).Val(); n != 0 {
		t.Fatal("wrote event before validation")
	}
	store.Client.Set(ctx, k[1], "0", 0)
	store.MaxBacklog = 1
	if _, err := store.State(ctx, target, "1001", "LIKE"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.State(ctx, target, "1002", "LIKE"); err == nil {
		t.Fatal("missing backpressure")
	}
	store.Client.Del(ctx, k[1])
	if _, err := store.State(ctx, target, "1001", "QUERY"); err == nil {
		t.Fatal("missing count served")
	}
}

func TestPartialWriteLeavesRecoveryFence(t *testing.T) {
	store, target := testStore(t)
	ctx := context.Background()
	k := Keys(target)
	// Deny SET only: XADD and SADD succeed, then the script fails before the version write.
	err := store.Client.Do(ctx, "ACL", "SETUSER", "partial", "on", ">test-secret", "~*", "+@all", "-set").Err()
	if err != nil {
		t.Fatal(err)
	}
	restricted := rdb.NewClient(&rdb.Options{Addr: store.Client.Options().Addr, Username: "partial", Password: "test-secret"})
	defer restricted.Close()
	broken := &Store{Client: restricted, MaxBacklog: 100}
	if _, err = broken.State(ctx, target, "1001", "LIKE"); err == nil {
		t.Fatal("expected injected SET failure")
	}
	if state := store.Client.HGet(ctx, k[4], "state").Val(); state != "RECOVERING" {
		t.Fatalf("fence=%s", state)
	}
	if n := store.Client.XLen(ctx, k[3]).Val(); n != 1 {
		t.Fatalf("lost recovery event, length=%d", n)
	}
	if _, err = store.State(ctx, target, "1002", "LIKE"); err == nil {
		t.Fatal("served partially written state")
	}
}

func TestVersionLimitAndServerFence(t *testing.T) {
	store, target := testStore(t)
	ctx := context.Background()
	k := Keys(target)
	store.Client.HSet(ctx, k[2], "1001", strconv.FormatUint(model.MaxNumber, 10))
	if _, err := store.State(ctx, target, "1001", "LIKE"); err == nil {
		t.Fatal("accepted version overflow")
	}
	if store.Client.XLen(ctx, k[3]).Val() != 0 {
		t.Fatal("overflow emitted event")
	}
	s, err := store.State(ctx, target, "18446744073709551615", "LIKE")
	if err != nil || !s.Liked {
		t.Fatalf("large user ID: %+v %v", s, err)
	}
	store.Client.HSet(ctx, k[4], "run_id", "old-server")
	if err = stateScript.Run(ctx, store.Client, k, "1002", 1, target.ID, "LIKE", "old-server", 1000).Err(); err == nil {
		t.Fatal("atomic script accepted stale server identity")
	}
}
