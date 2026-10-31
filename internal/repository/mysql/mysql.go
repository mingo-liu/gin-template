package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"likeflow/internal/model"
)

// MySQL persists like relations and counts in MySQL.
type MySQL struct{ DB *sql.DB }

func (m *MySQL) Targets(ctx context.Context) ([]model.Target, error) {
	rows, err := m.DB.QueryContext(ctx, "SELECT target_type,target_id,title,summary,category,initialized,redis_run_id FROM targets ORDER BY target_type,target_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []model.Target{}
	for rows.Next() {
		var t model.Target
		if err := rows.Scan(&t.Type, &t.ID, &t.Title, &t.Summary, &t.Category, &t.Initialized, &t.RedisRunID); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (m *MySQL) Relation(ctx context.Context, t model.Target, user string) (model.Relation, error) {
	v := model.Relation{UserID: user}
	err := m.DB.QueryRowContext(ctx, "SELECT liked,version FROM user_like WHERE user_id=? AND target_type=? AND target_id=?", user, t.Type, t.ID).Scan(&v.Liked, &v.Version)
	if err == sql.ErrNoRows {
		err = nil
	}
	return v, err
}

// Apply atomically persists the newest relation and derives count changes from stored state.
func (m *MySQL) Apply(ctx context.Context, e model.Event) (err error) {
	if err = e.Validate(); err != nil {
		return err
	}
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO user_like(user_id,target_type,target_id) VALUES(?,?,?) ON DUPLICATE KEY UPDATE id=id`, e.UserID, e.TargetType, e.TargetID)
	if err != nil {
		return err
	}
	var liked bool
	var version uint64
	err = tx.QueryRowContext(ctx, `SELECT liked,version FROM user_like WHERE user_id=? AND target_type=? AND target_id=? FOR UPDATE`, e.UserID, e.TargetType, e.TargetID).Scan(&liked, &version)
	if err != nil {
		return err
	}
	if version > model.MaxNumber || (liked && version == 0) {
		return fmt.Errorf("invalid stored relation version")
	}
	next, _ := strconv.ParseUint(e.Version, 10, 64)
	newLiked := e.Action == "LIKE"
	if next < version {
		return tx.Commit()
	}
	if next == version {
		if liked != newLiked {
			return model.ErrConflict
		}
		return tx.Commit()
	}
	// Lock count after the relation, consistently across all consumers and repair jobs.
	var count uint64
	err = tx.QueryRowContext(ctx, `SELECT like_count FROM like_count WHERE target_type=? AND target_id=? FOR UPDATE`, e.TargetType, e.TargetID).Scan(&count)
	if err != nil {
		return fmt.Errorf("count row: %w", err)
	}
	if count > model.MaxNumber {
		return fmt.Errorf("count out of range")
	}
	_, err = tx.ExecContext(ctx, `UPDATE user_like SET liked=?,version=? WHERE user_id=? AND target_type=? AND target_id=?`, newLiked, next, e.UserID, e.TargetType, e.TargetID)
	if err != nil {
		return err
	}
	if liked != newLiked {
		query := `UPDATE like_count SET like_count=like_count+1 WHERE target_type=? AND target_id=? AND like_count<9007199254740991`
		if !newLiked {
			query = `UPDATE like_count SET like_count=like_count-1 WHERE target_type=? AND target_id=? AND like_count>0`
		}
		result, err := tx.ExecContext(ctx, query, e.TargetType, e.TargetID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("unsafe count change")
		}
	}
	return tx.Commit()
}

// Snapshot reads relationships and the aggregate from a single consistent MySQL snapshot.
func (m *MySQL) Snapshot(ctx context.Context, t model.Target) ([]model.Relation, uint64, error) {
	tx, err := m.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT user_id,liked,version FROM user_like WHERE target_type=? AND target_id=? ORDER BY user_id`, t.Type, t.ID)
	if err != nil {
		return nil, 0, err
	}
	var result []model.Relation
	var count uint64
	for rows.Next() {
		var v model.Relation
		if err = rows.Scan(&v.UserID, &v.Liked, &v.Version); err != nil {
			rows.Close()
			return nil, 0, err
		}
		if v.Version > model.MaxNumber || (v.Liked && v.Version == 0) {
			rows.Close()
			return nil, 0, fmt.Errorf("invalid snapshot relation")
		}
		result = append(result, v)
		if v.Liked {
			count++
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, 0, err
	}
	var stored uint64
	err = tx.QueryRowContext(ctx, `SELECT like_count FROM like_count WHERE target_type=? AND target_id=?`, t.Type, t.ID).Scan(&stored)
	if err != nil {
		return nil, 0, err
	}
	if stored != count {
		return nil, 0, fmt.Errorf("count mismatch: stored=%d actual=%d; pause consumers and repair", stored, count)
	}
	return result, count, tx.Commit()
}
