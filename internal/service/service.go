package service

import (
	"context"
	"likeflow/internal/model"
	"likeflow/internal/repository/mysql"
	"likeflow/internal/repository/redis"
)

type Service struct {
	DB    *mysql.MySQL
	Redis *redis.Redis
}

func (s *Service) Target(ctx context.Context, typ int, id string) (model.Target, error) {
	targets, err := s.DB.Targets(ctx)
	if err != nil {
		return model.Target{}, err
	}
	for _, t := range targets {
		if t.Type == typ && t.ID == id {
			return t, nil
		}
	}
	return model.Target{}, model.ErrNotFound
}

func (s *Service) State(ctx context.Context, typ int, id, user, action string) (model.State, error) {
	t, err := s.Target(ctx, typ, id)
	if err != nil {
		return model.State{}, err
	}
	if !t.Initialized {
		return model.State{}, model.ErrUnavailable
	}
	return s.Redis.State(ctx, t, user, action)
}
