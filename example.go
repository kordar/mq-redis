package mqredis

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

func Logger(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			logger.Info("message received", "topic", msg.Topic, "id", msg.ID, "retry", msg.Retry)
			return next(ctx, msg)
		}
	}
}

func Recover() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (err error) {
			defer func() {
				if recover() != nil {
					err = errors.New("mqredis: panic recovered")
				}
			}()
			return next(ctx, msg)
		}
	}
}

func ExampleUsage() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq := NewClient(Options{
		Addr:        "localhost:6379",
		Group:       "order-group",
		Consumer:    "consumer-1",
		Concurrency: 8,
		MaxRetry:    3,
		Block:       3 * time.Second,
	})
	defer mq.Shutdown(context.Background())

	mq.Use(Logger(nil), Recover())

	go func() {
		_ = mq.SubscribeMany(ctx, map[string]Handler{
			"order.created": func(ctx context.Context, msg *Message) error {
				var payload struct {
					OrderID string `json:"order_id"`
				}
				if err := msg.Bind(&payload); err != nil {
					return err
				}
				return nil
			},
			"user.created": func(ctx context.Context, msg *Message) error {
				return nil
			},
		})
	}()

	_, _ = mq.Publish(ctx, "order.created", map[string]any{"order_id": "A1001"})
	_, _ = mq.PublishDelay(ctx, "order.created", map[string]any{"order_id": "A1002"}, 10*time.Second)
}

func ExampleUsageWithUniversalClient() {
	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	defer redisClient.Close()

	mq := NewClientWithUniversalClient(redisClient, Options{
		Group:       "order-group",
		Consumer:    "consumer-1",
		Concurrency: 4,
	})
	defer mq.Shutdown(context.Background())
}

func ExampleIndependentSubscriptions() {
	ctx := context.Background()

	mq := NewClient(Options{
		Addr:        "localhost:6379",
		Group:       "order-group",
		Consumer:    "consumer-1",
		Concurrency: 4,
	})
	defer mq.Shutdown(context.Background())

	orderSub, _ := mq.StartSubscribe(ctx, "order.created", func(ctx context.Context, msg *Message) error {
		return nil
	})
	userSub, _ := mq.StartSubscribe(ctx, "user.created", func(ctx context.Context, msg *Message) error {
		return nil
	})

	defer orderSub.Close()
	defer userSub.Close()

	_, _ = mq.Publish(ctx, "order.created", map[string]any{"order_id": "A1001"})
	_, _ = mq.Publish(ctx, "user.created", map[string]any{"user_id": "U1001"})
}
