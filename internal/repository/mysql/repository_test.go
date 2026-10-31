package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"likeflow/internal/model"
)

func TestApplyIntegration(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_TEST_DSN to a migrated test database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(8)
	r := &Repository{DB: db}
	ctx := context.Background()
	id := strconv.FormatInt(time.Now().UnixNano(), 10)
	target := model.Target{Type: 1, ID: id}
	_, err = db.ExecContext(ctx, "INSERT INTO like_count(target_type,target_id) VALUES(1,?)", id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec("DELETE FROM user_like WHERE target_type=1 AND target_id=?", id)
		db.Exec("DELETE FROM like_count WHERE target_type=1 AND target_id=?", id)
	})
	event := func(user, action, version string) model.Event {
		return model.Event{SchemaVersion: 1, EventID: fmt.Sprintf("1:%s:g1:123-0", id), Generation: "g1", StreamID: "123-0", TargetType: 1, TargetID: id, UserID: user, Action: action, Version: version}
	}
	for _, e := range []model.Event{event("1001", "UNLIKE", "2"), event("1001", "LIKE", "1"), event("1001", "LIKE", "3")} {
		if err = r.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := r.Apply(ctx, event("1001", "LIKE", "3")); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	relations, count, err := r.Snapshot(ctx, target)
	if err != nil || count != 1 || len(relations) != 1 || relations[0].Version != 3 {
		t.Fatalf("snapshot %+v %d %v", relations, count, err)
	}
	if err = r.Apply(ctx, event("1001", "UNLIKE", "3")); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	// Missing count must roll back both the inserted relation and its version.
	db.Exec("DELETE FROM like_count WHERE target_type=1 AND target_id=?", id)
	if err = r.Apply(ctx, event("1002", "LIKE", "1")); err == nil {
		t.Fatal("accepted missing count")
	}
	relation, err := r.Relation(ctx, target, "1002")
	if err != nil || relation.Version != 0 {
		t.Fatalf("partial commit %+v %v", relation, err)
	}
	db.Exec("INSERT INTO like_count(target_type,target_id,like_count) VALUES(1,?,0)", id)
	if err = r.Apply(ctx, event("1001", "UNLIKE", "4")); err == nil {
		t.Fatal("accepted count underflow")
	}
	relation, err = r.Relation(ctx, target, "1001")
	if err != nil || relation.Version != 3 || !relation.Liked {
		t.Fatalf("failed rollback %+v %v", relation, err)
	}
}
