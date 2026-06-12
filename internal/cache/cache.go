package cache

import (
	"context"
	"time"
)

type Cache interface {
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Exists(ctx context.Context, key string) (bool, error)
	Close() error
}
