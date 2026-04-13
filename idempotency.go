package mqredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// IdempotencyStore prevents duplicate business execution.
type IdempotencyStore interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key string) error
}

// RedisIdempotencyStore uses SETNX and DEL to guard message IDs.
type RedisIdempotencyStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisIdempotencyStore(client redis.UniversalClient, prefix string) *RedisIdempotencyStore {
	if prefix == "" {
		prefix = "mq:idempotent:"
	}
	return &RedisIdempotencyStore{client: client, prefix: prefix}
}

func (s *RedisIdempotencyStore) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return s.client.SetNX(ctx, s.prefix+key, 1, ttl).Result()
}

func (s *RedisIdempotencyStore) Release(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.prefix+key).Err()
}
