package redis

import (
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"

	rdb "github.com/redis/go-redis/v9"
	"likeflow/internal/model"
)

//go:embed state.lua
var stateLua string
var stateScript = rdb.NewScript(stateLua)

const Group = "like-relay"

// Redis maintains online like state and its event log in Redis.
type Redis struct {
	Client     *rdb.Client
	MaxBacklog int64
}

func Keys(t model.Target) []string {
	prefix := fmt.Sprintf("like:{%d:%s}:", t.Type, t.ID)
	return []string{prefix + "users", prefix + "count", prefix + "versions", prefix + "events", prefix + "meta"}
}

func Token() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b)
}

// Identity rejects replicas and detects restarts against the durable target catalog.
func (r *Redis) Identity(ctx context.Context) (string, error) {
	info, err := r.Client.Info(ctx, "server", "replication", "persistence", "memory").Result()
	if err != nil {
		return "", err
	}
	fields := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok {
			fields[k] = v
		}
	}
	if fields["role"] != "master" || fields["aof_enabled"] != "1" || fields["aof_last_write_status"] != "ok" || fields["maxmemory_policy"] != "noeviction" {
		return "", fmt.Errorf("Redis requires writable primary, healthy AOF and noeviction")
	}
	if fields["run_id"] == "" {
		return "", errors.New("missing Redis identity")
	}
	return fields["run_id"], nil
}

func (r *Redis) State(ctx context.Context, t model.Target, user, action string) (model.State, error) {
	id, err := r.Identity(ctx)
	if err != nil {
		return model.State{}, err
	}
	if id != t.RedisRunID {
		return model.State{}, model.ErrUnavailable
	}
	values, err := stateScript.Run(ctx, r.Client, Keys(t), user, t.Type, t.ID, action, id, r.MaxBacklog).Slice()
	if err != nil {
		return model.State{}, err
	}
	return model.State{Liked: values[0].(int64) == 1, Count: values[1].(string), Version: values[2].(string), Changed: values[3].(int64) == 1, Code: values[4].(string)}, nil
}

// Initialize only accepts completely absent keys. Existing READY state is never reset.
func (r *Redis) Initialize(ctx context.Context, t model.Target, id string) error {
	script := rdb.NewScript(`
 for i=1,5 do if redis.call('EXISTS',KEYS[i])~=0 then return redis.error_reply('existing keys require recovery') end end
 redis.call('HSET',KEYS[5],'state','RECOVERING','generation',ARGV[1],'run_id',ARGV[2])
 redis.call('SET',KEYS[2],'0')
 redis.call('XGROUP','CREATE',KEYS[4],ARGV[3],'0','MKSTREAM')
 redis.call('HSET',KEYS[5],'state','READY')
 return 1`)
	return script.Run(ctx, r.Client, Keys(t), Token(), id, Group).Err()
}

func (r *Redis) Quarantine(ctx context.Context, t model.Target, reason string) error {
	return r.Client.HSet(ctx, Keys(t)[4], "state", "RECOVERING", "error", reason).Err()
}

func Event(t model.Target, m rdb.XMessage) (model.Event, error) {
	value := func(k string) string { v, _ := m.Values[k].(string); return v }
	typ, err := strconv.Atoi(value("target_type"))
	if err != nil {
		return model.Event{}, err
	}
	e := model.Event{SchemaVersion: 1, StreamID: m.ID, Generation: value("generation"), UserID: value("user_id"), TargetID: value("target_id"), TargetType: typ, Action: value("action"), Version: value("version")}
	e.EventID = fmt.Sprintf("%d:%s:%s:%s", typ, e.TargetID, e.Generation, m.ID)
	if typ != t.Type || e.TargetID != t.ID {
		return e, errors.New("stream target mismatch")
	}
	return e, e.Validate()
}

// DeleteCommitted is fenced against recovery and leaves uncommitted entries untouched.
func (r *Redis) DeleteCommitted(ctx context.Context, t model.Target, id string) error {
	script := rdb.NewScript(`if redis.call('HGET',KEYS[2],'state')~='READY' or redis.call('HEXISTS',KEYS[2],'recovery_token')==1 then return 0 end
 redis.call('XACK',KEYS[1],ARGV[1],ARGV[2]);return redis.call('XDEL',KEYS[1],ARGV[2])`)
	k := Keys(t)
	return script.Run(ctx, r.Client, []string{k[3], k[4]}, Group, id).Err()
}
