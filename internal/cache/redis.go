package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	rc *redis.Client
}

func NewRedis(ctx context.Context, address string, password string) (*Redis, error) {

	rc := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})

	err := rc.Ping(ctx).Err()

	if err != nil {
		return nil, err
	}

	return &Redis{rc: rc}, nil
}

func (r *Redis) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return r.rc.Set(ctx, key, value, ttl).Err()

}

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	result, err := r.rc.Get(ctx, key).Result()
	return result, err
}

func (r *Redis) Exists(ctx context.Context, key string) (bool, error) {
	result := r.rc.Exists(ctx, key)
	return result.Val() > 0, result.Err()
}

func (r *Redis) Close() error {
	return r.rc.Close()
}
