package mqredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultBlock                 = 5 * time.Second
	defaultRecoverInterval       = 5 * time.Second
	defaultClaimIdle             = 10 * time.Second
	defaultClaimBatch      int64 = 10
	defaultDelayBatch      int64 = 100
	defaultDelayTick             = time.Second
	defaultMaxRetry              = 3
	defaultMaxLen          int64 = 100000
	defaultConcurrency           = 1
	defaultIdempotencyTTL        = 24 * time.Hour
)

var ErrClientClosed = errors.New("mqredis: client is shutting down")

type RetryPolicy func(msg *Message) time.Duration

// Options controls producer and consumer behaviour.
type Options struct {
	Addr               string
	Addrs              []string
	Password           string
	DB                 int
	MasterName         string
	Group              string
	Consumer           string
	Concurrency        int
	MaxRetry           int
	Block              time.Duration
	MaxLen             int64
	ClaimIdle          time.Duration
	ClaimBatch         int64
	RecoverInterval    time.Duration
	DelayCheckInterval time.Duration
	DelayBatch         int64
	IdempotencyTTL     time.Duration
	RetryPolicy        RetryPolicy
	Metrics            Metrics
	IdempotencyStore   IdempotencyStore
	RedisClient        redis.UniversalClient
}

type job struct {
	topic   string
	msg     redis.XMessage
	handler Handler
}

// Client is a Redis Stream based MQ SDK.
type Client struct {
	client redis.UniversalClient
	opt    Options

	mu    sync.RWMutex
	mw    []Middleware
	subMu sync.Mutex
	subs  map[*Subscription]struct{}

	closeOnce     sync.Once
	connCloseOnce sync.Once
	connCloseErr  error
	stopCh        chan struct{}
	ownedConn     bool
}

// Subscription represents one independent subscribe session.
type Subscription struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	wg     sync.WaitGroup
}

func NewClient(opt Options) *Client {
	opt = opt.withDefaults()
	if opt.RedisClient != nil {
		return NewClientWithUniversalClient(opt.RedisClient, opt)
	}

	redisClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      opt.redisAddrs(),
		MasterName: opt.MasterName,
		Password:   opt.Password,
		DB:         opt.DB,
	})

	return newClientWithRedis(redisClient, opt, true)
}

func NewClientWithUniversalClient(redisClient redis.UniversalClient, opt Options) *Client {
	opt = opt.withDefaults()
	return newClientWithRedis(redisClient, opt, false)
}

func newClientWithRedis(redisClient redis.UniversalClient, opt Options, owned bool) *Client {
	if opt.IdempotencyStore == nil && redisClient != nil {
		opt.IdempotencyStore = NewRedisIdempotencyStore(redisClient, "")
	}

	return &Client{
		client:    redisClient,
		opt:       opt,
		stopCh:    make(chan struct{}),
		subs:      make(map[*Subscription]struct{}),
		ownedConn: owned,
	}
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = defaultConcurrency
	}
	if o.MaxRetry <= 0 {
		o.MaxRetry = defaultMaxRetry
	}
	if o.Block <= 0 {
		o.Block = defaultBlock
	}
	if o.MaxLen <= 0 {
		o.MaxLen = defaultMaxLen
	}
	if o.ClaimIdle <= 0 {
		o.ClaimIdle = defaultClaimIdle
	}
	if o.ClaimBatch <= 0 {
		o.ClaimBatch = defaultClaimBatch
	}
	if o.RecoverInterval <= 0 {
		o.RecoverInterval = defaultRecoverInterval
	}
	if o.DelayCheckInterval <= 0 {
		o.DelayCheckInterval = defaultDelayTick
	}
	if o.DelayBatch <= 0 {
		o.DelayBatch = defaultDelayBatch
	}
	if o.IdempotencyTTL <= 0 {
		o.IdempotencyTTL = defaultIdempotencyTTL
	}
	if o.Metrics == nil {
		o.Metrics = nopMetrics{}
	}
	if o.RetryPolicy == nil {
		o.RetryPolicy = func(msg *Message) time.Duration {
			if msg == nil || msg.Retry <= 0 {
				return 0
			}
			backoff := time.Duration(msg.Retry) * time.Second
			if backoff > 30*time.Second {
				return 30 * time.Second
			}
			return backoff
		}
	}
	return o
}

func (o Options) redisAddrs() []string {
	if len(o.Addrs) > 0 {
		return append([]string(nil), o.Addrs...)
	}
	if o.Addr != "" {
		return []string{o.Addr}
	}
	return []string{"localhost:6379"}
}

func (c *Client) Redis() redis.UniversalClient {
	return c.client
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.Shutdown(context.Background())
}

func (c *Client) Shutdown(ctx context.Context) error {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})

	subs := c.snapshotSubscriptions()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, sub := range subs {
			<-sub.done
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return c.closeRedis()
	}
}

func (c *Client) Use(middlewares ...Middleware) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mw = append(c.mw, middlewares...)
}

func (c *Client) Publish(ctx context.Context, topic string, payload any) (string, error) {
	msg, err := NewMessage(payload)
	if err != nil {
		return "", err
	}
	return c.PublishMessage(ctx, topic, msg)
}

func (c *Client) PublishMessage(ctx context.Context, topic string, msg *Message) (string, error) {
	if msg == nil {
		return "", errors.New("mqredis: message is nil")
	}

	cloned := msg.Clone()
	cloned.ensureDefaults(topic)
	if cloned.AvailableAt > time.Now().UnixMilli() {
		return cloned.ID, c.enqueueDelay(ctx, topic, cloned)
	}

	id, err := c.publishNow(ctx, topic, cloned)
	if err == nil {
		c.opt.Metrics.IncPublished(topic)
	}
	return id, err
}

func (c *Client) PublishDelay(ctx context.Context, topic string, payload any, delay time.Duration) (string, error) {
	msg, err := NewMessage(payload)
	if err != nil {
		return "", err
	}
	msg.AvailableAt = time.Now().Add(delay).UnixMilli()
	return c.PublishMessage(ctx, topic, msg)
}

func (c *Client) Subscribe(ctx context.Context, topic string, handler Handler) error {
	sub, err := c.StartSubscribe(ctx, topic, handler)
	if err != nil {
		return err
	}
	return sub.Wait()
}

func (c *Client) SubscribeMany(ctx context.Context, handlers map[string]Handler) error {
	sub, err := c.StartSubscribeMany(ctx, handlers)
	if err != nil {
		return err
	}
	return sub.Wait()
}

func (c *Client) StartSubscribe(ctx context.Context, topic string, handler Handler) (*Subscription, error) {
	return c.StartSubscribeMany(ctx, map[string]Handler{topic: handler})
}

func (c *Client) StartSubscribeMany(ctx context.Context, handlers map[string]Handler) (*Subscription, error) {
	if len(handlers) == 0 {
		return nil, errors.New("mqredis: handlers are empty")
	}

	runCtx, cancel := c.withStop(ctx)
	sub := &Subscription{
		client: c,
		ctx:    runCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	if err := c.registerSubscription(sub); err != nil {
		cancel()
		return nil, err
	}

	go sub.run(handlers)
	return sub, nil
}

func (s *Subscription) run(handlers map[string]Handler) {
	defer close(s.done)
	defer s.client.unregisterSubscription(s)
	defer s.cancel()

	c := s.client
	topics := make([]string, 0, len(handlers))
	wrapped := make(map[string]Handler, len(handlers))
	streams := make([]string, 0, len(handlers)*2)
	jobs := make(chan job, c.opt.Concurrency*4)

	c.mu.RLock()
	middlewares := append([]Middleware(nil), c.mw...)
	c.mu.RUnlock()

	for topic, handler := range handlers {
		if handler == nil {
			s.err = fmt.Errorf("mqredis: nil handler for topic %s", topic)
			return
		}
		if err := c.ensureGroup(s.ctx, topic); err != nil {
			s.err = err
			return
		}
		wrapped[topic] = chain(handler, middlewares...)
		topics = append(topics, topic)
		streams = append(streams, topic, ">")
	}

	workerCount := c.opt.Concurrency
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		go s.worker(jobs)
	}

	for _, topic := range topics {
		s.wg.Add(1)
		go s.recoverPending(topic, wrapped[topic], jobs)

		s.wg.Add(1)
		go s.promoteDelayed(topic)
	}

	readLoopErr := s.consumeLoop(streams, wrapped, jobs)
	s.cancel()
	s.wg.Wait()

	if errors.Is(readLoopErr, context.Canceled) {
		s.err = nil
		return
	}
	s.err = readLoopErr
}

func (s *Subscription) Wait() error {
	<-s.done
	return s.err
}

func (s *Subscription) Close() {
	s.cancel()
}

func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

func (s *Subscription) consumeLoop(streams []string, handlers map[string]Handler, jobs chan<- job) error {
	c := s.client
	ctx := s.ctx

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		streamsResult, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.opt.Group,
			Consumer: c.opt.Consumer,
			Streams:  streams,
			Block:    c.opt.Block,
			Count:    int64(c.opt.Concurrency),
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		for _, stream := range streamsResult {
			handler := handlers[stream.Stream]
			for _, message := range stream.Messages {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case jobs <- job{topic: stream.Stream, msg: message, handler: handler}:
				}
			}
		}
	}
}

func (s *Subscription) worker(jobs <-chan job) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case current, ok := <-jobs:
			if !ok {
				return
			}
			s.client.handle(s.ctx, current)
		}
	}
}

func (c *Client) handle(ctx context.Context, current job) {
	msg, err := decodeStreamMessage(current.topic, current.msg)
	if err != nil {
		_ = c.ack(ctx, current.topic, current.msg.ID)
		return
	}

	if c.opt.IdempotencyStore != nil {
		acquired, acquireErr := c.opt.IdempotencyStore.Acquire(ctx, msg.ID, c.opt.IdempotencyTTL)
		if acquireErr != nil {
			_ = c.requeueFailure(ctx, current.topic, current.msg.ID, msg)
			return
		}
		if !acquired {
			_ = c.ack(ctx, current.topic, current.msg.ID)
			return
		}
	}

	if err := current.handler(ctx, msg); err != nil {
		if c.opt.IdempotencyStore != nil {
			_ = c.opt.IdempotencyStore.Release(ctx, msg.ID)
		}
		_ = c.requeueFailure(ctx, current.topic, current.msg.ID, msg)
		return
	}

	c.opt.Metrics.IncConsumed(current.topic)
	_ = c.ack(ctx, current.topic, current.msg.ID)
}

func (c *Client) requeueFailure(ctx context.Context, topic, streamID string, msg *Message) error {
	msg.Retry++
	defer func() {
		_ = c.ack(ctx, topic, streamID)
	}()

	if msg.Retry > c.opt.MaxRetry {
		c.opt.Metrics.IncDLQ(topic)
		return c.dlq(ctx, topic, msg)
	}

	c.opt.Metrics.IncRetried(topic)
	delay := c.opt.RetryPolicy(msg)
	if delay > 0 {
		msg.AvailableAt = time.Now().Add(delay).UnixMilli()
		return c.enqueueDelay(ctx, topic, msg)
	}

	msg.AvailableAt = 0
	return c.retry(ctx, topic, msg)
}

func (s *Subscription) recoverPending(topic string, handler Handler, jobs chan<- job) {
	defer s.wg.Done()
	c := s.client

	ticker := time.NewTicker(c.opt.RecoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}

		pending, err := c.client.XPendingExt(s.ctx, &redis.XPendingExtArgs{
			Stream: topic,
			Group:  c.opt.Group,
			Start:  "-",
			End:    "+",
			Count:  c.opt.ClaimBatch,
		}).Result()
		if err != nil {
			continue
		}

		c.opt.Metrics.ObservePending(topic, int64(len(pending)))

		for _, item := range pending {
			if item.Idle < c.opt.ClaimIdle {
				continue
			}

			claimed, err := c.client.XClaim(s.ctx, &redis.XClaimArgs{
				Stream:   topic,
				Group:    c.opt.Group,
				Consumer: c.opt.Consumer,
				MinIdle:  c.opt.ClaimIdle,
				Messages: []string{item.ID},
			}).Result()
			if err != nil {
				continue
			}

			for _, streamMsg := range claimed {
				select {
				case <-s.ctx.Done():
					return
				case jobs <- job{topic: topic, msg: streamMsg, handler: handler}:
				}
			}
		}
	}
}

func (s *Subscription) promoteDelayed(topic string) {
	defer s.wg.Done()
	c := s.client

	ticker := time.NewTicker(c.opt.DelayCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}

		values, err := c.client.ZRangeByScore(s.ctx, delayedKey(topic), &redis.ZRangeBy{
			Min:    "-inf",
			Max:    fmt.Sprintf("%d", time.Now().UnixMilli()),
			Offset: 0,
			Count:  c.opt.DelayBatch,
		}).Result()
		if err != nil {
			continue
		}

		for _, value := range values {
			removed, err := c.client.ZRem(s.ctx, delayedKey(topic), value).Result()
			if err != nil || removed == 0 {
				continue
			}

			var msg Message
			if err := json.Unmarshal([]byte(value), &msg); err != nil {
				continue
			}

			msg.AvailableAt = 0
			if _, err := c.publishNow(s.ctx, topic, &msg); err == nil {
				c.opt.Metrics.IncPublished(topic)
			}
		}
	}
}

func (c *Client) retry(ctx context.Context, topic string, msg *Message) error {
	_, err := c.publishNow(ctx, topic, msg)
	return err
}

func (c *Client) dlq(ctx context.Context, topic string, msg *Message) error {
	_, err := c.publishNow(ctx, topic+":DLQ", msg)
	return err
}

func (c *Client) ack(ctx context.Context, topic, id string) error {
	return c.client.XAck(ctx, topic, c.opt.Group, id).Err()
}

func (c *Client) ensureGroup(ctx context.Context, topic string) error {
	err := c.client.XGroupCreateMkStream(ctx, topic, c.opt.Group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (c *Client) publishNow(ctx context.Context, topic string, msg *Message) (string, error) {
	msg.ensureDefaults(topic)

	body, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	return c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		MaxLen: c.opt.MaxLen,
		Approx: true,
		Values: map[string]any{"data": string(body)},
	}).Result()
}

func (c *Client) enqueueDelay(ctx context.Context, topic string, msg *Message) error {
	msg.ensureDefaults(topic)

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.client.ZAdd(ctx, delayedKey(topic), redis.Z{
		Score:  float64(msg.AvailableAt),
		Member: string(body),
	}).Err()
}

func (c *Client) withStop(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-ctx.Done():
		case <-c.stopCh:
			cancel()
		}
	}()
	return ctx, cancel
}

func (c *Client) isStopping() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

func (c *Client) registerSubscription(sub *Subscription) error {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	select {
	case <-c.stopCh:
		return ErrClientClosed
	default:
	}

	c.subs[sub] = struct{}{}
	return nil
}

func (c *Client) unregisterSubscription(sub *Subscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	delete(c.subs, sub)
}

func (c *Client) snapshotSubscriptions() []*Subscription {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	subs := make([]*Subscription, 0, len(c.subs))
	for sub := range c.subs {
		subs = append(subs, sub)
	}
	return subs
}

func (c *Client) closeRedis() error {
	if !c.ownedConn || c.client == nil {
		return nil
	}

	c.connCloseOnce.Do(func() {
		c.connCloseErr = c.client.Close()
	})
	return c.connCloseErr
}

func decodeStreamMessage(topic string, raw redis.XMessage) (*Message, error) {
	data, ok := raw.Values["data"].(string)
	if !ok {
		return nil, errors.New("mqredis: invalid stream payload")
	}

	var msg Message
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return nil, err
	}
	if msg.ID == "" {
		msg.ID = raw.ID
	}
	if msg.Topic == "" {
		msg.Topic = topic
	}
	return &msg, nil
}

func delayedKey(topic string) string {
	return topic + ":DELAY"
}
