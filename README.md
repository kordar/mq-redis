# mqredis

`mqredis` 是一个基于 Redis Stream + Consumer Group 的 Go MQ SDK，面向内部基础组件场景，提供并发消费、重试、死信、Pending 恢复、延迟消息、幂等和中间件扩展能力。

当前模块路径：

```go
github.com/kordar/mqredis
```

## 特性

- 基于 `Redis Stream` 和 `Consumer Group`
- 支持 `Worker Pool` 并发消费
- 支持失败重试和 `DLQ`
- 支持 Pending 消息自动恢复
- 支持延迟消息
- 支持 Middleware 链式扩展
- 支持消费幂等控制
- 支持优雅关闭
- 支持多 Topic 订阅
- 底层使用 `redis.UniversalClient`
- 支持直接注入外部 `redis.UniversalClient`

## 安装

```bash
go get github.com/kordar/mqredis
```

## 快速开始

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/kordar/mqredis"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq := mqredis.NewClient(mqredis.Options{
		Addr:        "localhost:6379",
		Group:       "order-group",
		Consumer:    "consumer-1",
		Concurrency: 8,
		MaxRetry:    3,
		Block:       3 * time.Second,
	})
	defer mq.Shutdown(context.Background())

	mq.Use(
		mqredis.Logger(slog.Default()),
		mqredis.Recover(),
	)

	go func() {
		_ = mq.Subscribe(ctx, "order.created", func(ctx context.Context, msg *mqredis.Message) error {
			var payload struct {
				OrderID string `json:"order_id"`
			}
			if err := msg.Bind(&payload); err != nil {
				return err
			}

			slog.Info("consume order", "order_id", payload.OrderID, "message_id", msg.ID)
			return nil
		})
	}()

	_, _ = mq.Publish(ctx, "order.created", map[string]any{
		"order_id": "A1001",
	})

	time.Sleep(2 * time.Second)
}
```

## 使用外部 Redis Client

SDK 内部使用 `redis.UniversalClient`，因此同时支持单机、Sentinel 和 Cluster。

### 方式一：由 SDK 自动创建

```go
mq := mqredis.NewClient(mqredis.Options{
	Addrs:      []string{"localhost:6379"},
	Group:      "order-group",
	Consumer:   "consumer-1",
	MasterName: "",
})
```

说明：

- 单机 Redis：设置 `Addr` 或 `Addrs`
- Redis Sentinel：设置 `Addrs` + `MasterName`
- Redis Cluster：设置 `Addrs`

### 方式二：直接注入 `redis.UniversalClient`

```go
redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
	Addrs: []string{"localhost:6379"},
})
defer redisClient.Close()

mq := mqredis.NewClientWithUniversalClient(redisClient, mqredis.Options{
	Group:       "order-group",
	Consumer:    "consumer-1",
	Concurrency: 4,
})
defer mq.Shutdown(context.Background())
```

### 方式三：通过 `Options.RedisClient` 注入

```go
redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
	Addrs: []string{"localhost:6379"},
})
defer redisClient.Close()

mq := mqredis.NewClient(mqredis.Options{
	RedisClient: redisClient,
	Group:       "order-group",
	Consumer:    "consumer-1",
	Concurrency: 4,
})
defer mq.Shutdown(context.Background())
```

说明：

- 当你注入外部 `redis.UniversalClient` 时，`mq.Shutdown()` 不会主动关闭这个 Redis 连接
- 当 SDK 自己创建 Redis 连接时，`Shutdown()` 会在退出时关闭连接

## 消息模型

```go
type Message struct {
	ID          string
	Topic       string
	Payload     json.RawMessage
	Headers     map[string]string
	Timestamp   int64
	Retry       int
	AvailableAt int64
}
```

字段说明：

- `ID`：业务消息 ID，同时用于幂等控制
- `Topic`：Stream 名称
- `Payload`：消息体
- `Headers`：扩展头信息
- `Timestamp`：消息创建时间，毫秒
- `Retry`：当前重试次数
- `AvailableAt`：延迟消息投递时间，毫秒时间戳

## 发布消息

### 发布普通消息

```go
_, err := mq.Publish(ctx, "order.created", map[string]any{
	"order_id": "A1001",
})
```

### 发布延迟消息

```go
_, err := mq.PublishDelay(ctx, "order.created", map[string]any{
	"order_id": "A1002",
}, 10*time.Second)
```

### 发布自定义 Message

```go
msg, err := mqredis.NewMessage(map[string]any{
	"order_id": "A1003",
})
if err != nil {
	return err
}

msg.Headers = map[string]string{
	"trace_id": "trace-123",
}

_, err = mq.PublishMessage(ctx, "order.created", msg)
```

## 订阅消息

### 订阅单个 Topic

```go
err := mq.Subscribe(ctx, "order.created", func(ctx context.Context, msg *mqredis.Message) error {
	var payload struct {
		OrderID string `json:"order_id"`
	}
	if err := msg.Bind(&payload); err != nil {
		return err
	}
	return nil
})
```

### 订阅多个 Topic

```go
err := mq.SubscribeMany(ctx, map[string]mqredis.Handler{
	"order.created": func(ctx context.Context, msg *mqredis.Message) error {
		return nil
	},
	"user.created": func(ctx context.Context, msg *mqredis.Message) error {
		return nil
	},
})
```

### 多次独立订阅

如果你希望在同一个 `Client` 下启动多个互不影响的订阅实例，使用：

- `StartSubscribe()`
- `StartSubscribeMany()`

示例：

```go
orderSub, err := mq.StartSubscribe(ctx, "order.created", func(ctx context.Context, msg *mqredis.Message) error {
	return nil
})
if err != nil {
	return err
}
defer orderSub.Close()

userSub, err := mq.StartSubscribe(ctx, "user.created", func(ctx context.Context, msg *mqredis.Message) error {
	return nil
})
if err != nil {
	return err
}
defer userSub.Close()
```

说明：

- 每次 `StartSubscribe` 都会创建独立的订阅生命周期
- 每个订阅可通过自己的 `context` 或 `sub.Close()` 独立停止
- `mq.Shutdown()` 会统一关闭并等待所有仍在运行的订阅结束
- 如果你只是订阅多个 topic，但生命周期一致，仍然优先推荐一次 `SubscribeMany()`

## Middleware

`mqredis` 支持类似 HTTP 中间件的链式封装：

```go
mq.Use(
	mqredis.Logger(slog.Default()),
	mqredis.Recover(),
)
```

接口定义：

```go
type Handler func(ctx context.Context, msg *Message) error
type Middleware func(next Handler) Handler
```

当前内置中间件：

- `Logger(logger *slog.Logger)`
- `Recover()`

你也可以自行扩展，例如 tracing、metrics、租户透传、审计日志等。

## 重试与死信

当消费函数返回错误时：

- `Retry` 自增
- 若 `Retry <= MaxRetry`，消息会重新进入重试流程
- 若 `Retry > MaxRetry`，消息进入 `topic:DLQ`

默认重试策略：

- 第 1 次重试延迟 1 秒
- 第 2 次重试延迟 2 秒
- 第 3 次重试延迟 3 秒
- 最大退避 30 秒

自定义重试策略：

```go
mq := mqredis.NewClient(mqredis.Options{
	Addr:      "localhost:6379",
	Group:     "order-group",
	Consumer:  "consumer-1",
	MaxRetry:  5,
	RetryPolicy: func(msg *mqredis.Message) time.Duration {
		return 5 * time.Second
	},
})
```

## Pending 恢复

SDK 会定期扫描 Consumer Group 中的 Pending 消息：

- 通过 `XPENDING` 查询 Pending
- 对超过 `ClaimIdle` 的消息执行 `XCLAIM`
- 将被接管的消息重新投递给当前消费者的 Worker Pool 处理

这意味着：

- 某个消费者异常退出后，未确认消息可被其他消费者接管
- 消息不会长期卡死在 Pending 队列中

## 幂等

SDK 默认使用 Redis `SETNX` 做消息幂等控制：

- 成功消费前，先对 `msg.ID` 加锁
- 如果同一个消息 ID 已处理过，则直接 ACK，避免重复执行业务
- 当处理失败时，会释放幂等锁，允许后续重试再次执行

如需自定义幂等策略，可实现以下接口：

```go
type IdempotencyStore interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key string) error
}
```

然后注入到 `Options.IdempotencyStore`。

## Metrics

可通过实现 `Metrics` 接口对接 Prometheus 或其他监控系统：

```go
type Metrics interface {
	IncPublished(topic string)
	IncConsumed(topic string)
	IncRetried(topic string)
	IncDLQ(topic string)
	ObservePending(topic string, count int64)
}
```

建议关注的指标：

- 发布量
- 消费量
- 重试次数
- DLQ 数量
- Pending 数量

## 配置项

`Options` 主要字段说明：

| 字段 | 说明 |
| --- | --- |
| `Addr` | 单个 Redis 地址 |
| `Addrs` | 多个 Redis 地址，适用于 UniversalClient |
| `Password` | Redis 密码 |
| `DB` | Redis DB |
| `MasterName` | Sentinel 主节点名称 |
| `Group` | Consumer Group 名称 |
| `Consumer` | 当前消费者名称 |
| `Concurrency` | Worker 并发数 |
| `MaxRetry` | 最大重试次数 |
| `Block` | `XREADGROUP` 阻塞时长 |
| `MaxLen` | Stream 最大长度 |
| `ClaimIdle` | Pending 多久后允许接管 |
| `ClaimBatch` | 每次恢复 Pending 的扫描数量 |
| `RecoverInterval` | Pending 恢复扫描间隔 |
| `DelayCheckInterval` | 延迟消息扫描间隔 |
| `DelayBatch` | 每次搬运延迟消息的数量 |
| `IdempotencyTTL` | 幂等锁 TTL |
| `RetryPolicy` | 自定义重试策略 |
| `Metrics` | 指标实现 |
| `IdempotencyStore` | 自定义幂等存储 |
| `RedisClient` | 外部注入的 `redis.UniversalClient` |

## 优雅关闭

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := mq.Shutdown(ctx); err != nil {
	return err
}
```

`Shutdown()` 会：

- 通知消费协程停止拉取消息
- 等待 Worker 和后台恢复协程退出
- 若 Redis 连接由 SDK 创建，则关闭连接

## Redis Key 约定

- 正常 Topic：`topic`
- 死信队列：`topic:DLQ`
- 延迟队列：`topic:DELAY`
- 幂等 Key 前缀：`mq:idempotent:`

## 适用场景

- 内部业务异步解耦
- 订单、用户、库存等事件驱动场景
- 轻量级任务分发
- 需要 Redis 单栈部署的中小型消息系统

## 注意事项

- Redis Stream 消费天然可能出现重复投递，业务侧仍应具备最终幂等能力
- 当前延迟消息依赖 ZSET + 后台搬运协程实现，不等同于专用延迟消息中间件
- 多实例消费时，请确保同一 `Group` 下的 `Consumer` 名称唯一
- 若使用外部注入的 Redis 连接，连接生命周期由调用方负责

## 后续可扩展方向

- Prometheus 官方适配实现
- Trace 上下文透传
- 消费限流
- 批量消费
- 消息编解码器抽象
- 更完善的管理与观测接口
