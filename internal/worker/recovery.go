package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"gin-template/internal/model"
	redisrepo "gin-template/internal/repository/redis"
	rdb "github.com/redis/go-redis/v9"
)

// Recover fences writers and cleanup, drains surviving log watermarks, then publishes a snapshot.
// resume replaces the previous non-expiring token, preventing an interrupted task from publishing.
func (w *Worker) Recover(ctx context.Context, t model.Target, resume string) error {
	k := redisrepo.Keys(t)
	token := redisrepo.Token()
	acquire := rdb.NewScript(`
 if redis.call('TYPE',KEYS[4]).ok~='stream' then return redis.error_reply('missing log: verify MQ and restore log before recovery') end
 local old=redis.call('HGET',KEYS[5],'recovery_token') or ''
 if old~=ARGV[1] then return redis.error_reply('recovery busy: explicit resume token required') end
 redis.call('HSET',KEYS[5],'state','RECOVERING','recovery_token',ARGV[2]);return 1`)
	if err := acquire.Run(ctx, w.Redis.Client, k, resume, token).Err(); err != nil {
		return err
	}
	slog.Info("recovery fenced; retain token for interrupted task", "target", t.ID, "resume_token", token)
	// A frozen log plus all trustworthy hash versions defines the minimum durable watermark.
	cursor := "-"
	for {
		entries, err := w.Redis.Client.XRangeN(ctx, k[3], cursor, "+", 100).Result()
		if err != nil {
			return err
		}
		for _, m := range entries {
			e, err := redisrepo.Event(t, m)
			if err != nil {
				return err
			}
			if err = w.DB.Apply(ctx, e); err != nil {
				return err
			}
			cursor = "(" + m.ID
		}
		if len(entries) < 100 {
			break
		}
	}
	versions, err := w.Redis.Client.HGetAll(ctx, k[2]).Result()
	if err != nil {
		return err
	}
	for user, v := range versions {
		n, err := model.Decimal(v, model.MaxNumber, false)
		if err != nil {
			return err
		}
		relation, err := w.DB.Relation(ctx, t, user)
		if err != nil {
			return err
		}
		if relation.Version < n {
			return fmt.Errorf("uncovered Redis version for user %s; retain frozen state", user)
		}
		if relation.Version == n {
			liked, err := w.Redis.Client.SIsMember(ctx, k[0], user).Result()
			if err != nil {
				return err
			}
			if liked != relation.Liked {
				return model.ErrConflict
			}
		}
	}
	relations, count, err := w.DB.Snapshot(ctx, t)
	if err != nil {
		return err
	}
	temp := []string{k[0] + ":" + token, k[1] + ":" + token, k[2] + ":" + token}
	// Every batch checks the same token; a superseded task cannot mutate published state.
	stage := rdb.NewScript(`if redis.call('HGET',KEYS[1],'recovery_token')~=ARGV[1] then return redis.error_reply('stale recovery') end
 if ARGV[2]=='1' then redis.call('SADD',KEYS[2],ARGV[3]) end
 redis.call('HSET',KEYS[3],ARGV[3],ARGV[4]);return 1`)
	for _, v := range relations {
		liked := "0"
		if v.Liked {
			liked = "1"
		}
		if v.Version > model.MaxNumber {
			return fmt.Errorf("invalid snapshot version")
		}
		if err = stage.Run(ctx, w.Redis.Client, []string{k[4], temp[0], temp[2]}, token, liked, v.UserID, strconv.FormatUint(v.Version, 10)).Err(); err != nil {
			return err
		}
	}
	id, err := w.Redis.Identity(ctx)
	if err != nil {
		return err
	}
	publish := rdb.NewScript(`
 if redis.call('HGET',KEYS[5],'recovery_token')~=ARGV[1] then return redis.error_reply('stale recovery') end
 local actual=string.match(redis.call('INFO','server'),'run_id:([^\r\n]+)')
 if actual~=ARGV[5] then return redis.error_reply('server changed during recovery') end
 local expected={'set','string','hash','stream','hash','set','string','hash'}
 for i=1,8 do
  local kind=redis.call('TYPE',KEYS[i]).ok
  if kind~=expected[i] and kind~='none' then return redis.error_reply('snapshot key type mismatch') end
 end
 if redis.call('TYPE',KEYS[4]).ok~='stream' then return redis.error_reply('log disappeared') end
 local card=redis.call('SCARD',KEYS[6]);local size=redis.call('HLEN',KEYS[8])
 if tostring(card)~=ARGV[2] or tostring(size)~=ARGV[3] then return redis.error_reply('incomplete snapshot') end
 redis.call('SET',KEYS[7],ARGV[2])
 redis.call('DEL',KEYS[1],KEYS[3])
 if card>0 then redis.call('RENAME',KEYS[6],KEYS[1]) end
 if size>0 then redis.call('RENAME',KEYS[8],KEYS[3]) end
 redis.call('RENAME',KEYS[7],KEYS[2])
 redis.call('HSET',KEYS[5],'generation',ARGV[4],'run_id',ARGV[5],'state','READY')
 redis.call('HDEL',KEYS[5],'recovery_token','error');return 1`)
	if err = publish.Run(ctx, w.Redis.Client, append(k, temp...), token, strconv.FormatUint(count, 10), strconv.Itoa(len(relations)), redisrepo.Token(), id).Err(); err != nil {
		return err
	}
	// Publication precedes catalog identity update, so any failure remains unavailable at the API.
	_, err = w.DB.DB.ExecContext(ctx, `UPDATE targets SET initialized=TRUE,redis_run_id=? WHERE target_type=? AND target_id=?`, id, t.Type, t.ID)
	return err
}
