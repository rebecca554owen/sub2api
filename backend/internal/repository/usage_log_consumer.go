package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/redis/go-redis/v9"
)

type usageLogConsumerConfig struct {
	Stream         string
	Group          string
	Consumer       string
	BatchSize      int
	FlushInterval  time.Duration
	ReadBlock      time.Duration
	ClaimIdle      time.Duration
	ReplayInterval time.Duration
}

type usageLogConsumer struct {
	rdb       *redis.Client
	billingDB *sql.DB
	store     *clickHouseUsageLogStore
	queue     *usageLogDurableQueue
	metrics   *usageLogQueueMetrics
	cfg       usageLogConsumerConfig

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
}

const usageLogUncommittedMaxAge = 24 * time.Hour

func newUsageLogConsumer(
	rdb *redis.Client,
	billingDB *sql.DB,
	store *clickHouseUsageLogStore,
	queue *usageLogDurableQueue,
	cfg usageLogConsumerConfig,
) (*usageLogConsumer, error) {
	if rdb == nil {
		return nil, errors.New("usage log consumer Redis client is nil")
	}
	if billingDB == nil {
		return nil, errors.New("usage log consumer billing database is nil")
	}
	if store == nil {
		return nil, errors.New("usage log consumer ClickHouse store is nil")
	}
	if queue == nil {
		return nil, errors.New("usage log consumer queue is nil")
	}
	cfg.Stream = strings.TrimSpace(cfg.Stream)
	cfg.Group = strings.TrimSpace(cfg.Group)
	cfg.Consumer = strings.TrimSpace(cfg.Consumer)
	if cfg.Stream == "" || cfg.Group == "" {
		return nil, errors.New("usage log consumer stream and group are required")
	}
	if cfg.Consumer == "" {
		host, _ := os.Hostname()
		cfg.Consumer = fmt.Sprintf("%s-%d", strings.TrimSpace(host), os.Getpid())
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.ReadBlock <= 0 {
		cfg.ReadBlock = time.Second
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	if cfg.ClaimIdle <= 0 {
		cfg.ClaimIdle = time.Minute
	}
	if cfg.ReplayInterval <= 0 {
		cfg.ReplayInterval = 5 * time.Second
	}
	return &usageLogConsumer{
		rdb: rdb, billingDB: billingDB, store: store, queue: queue,
		metrics: queue.metrics, cfg: cfg, done: make(chan struct{}),
	}, nil
}

func (c *usageLogConsumer) Start(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.startOnce.Do(func() {
		if err := c.ensureGroup(ctx); err != nil {
			logger.LegacyPrintf("repository.usage_log_consumer", "usage log Redis unavailable at startup; WAL fallback active: %v", err)
		}
		workerCtx, cancel := context.WithCancel(context.Background())
		c.cancel = cancel
		go c.run(workerCtx)
	})
	return nil
}

func (c *usageLogConsumer) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		if c.cancel == nil {
			return
		}
		c.cancel()
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
			logger.LegacyPrintf("repository.usage_log_consumer", "usage log consumer stop timed out")
		}
	})
}

func (c *usageLogConsumer) ensureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.cfg.Stream, c.cfg.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create usage log Redis consumer group: %w", err)
	}
	return nil
}

func (c *usageLogConsumer) run(ctx context.Context) {
	defer close(c.done)
	replayTicker := time.NewTicker(c.cfg.ReplayInterval)
	defer replayTicker.Stop()
	groupReady := false
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if !groupReady {
			if err := c.ensureGroup(ctx); err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}
			groupReady = true
			if err := c.queue.ReplayWAL(ctx); err != nil && ctx.Err() == nil {
				logger.LegacyPrintf("repository.usage_log_consumer", "replay usage log WAL after Redis recovery failed: %v", err)
			}
		}
		if messages, err := c.claimStale(ctx); err == nil && len(messages) > 0 {
			c.metrics.retries.Add(uint64(len(messages)))
			c.processMessages(ctx, messages)
		} else if err != nil && !errors.Is(err, redis.Nil) {
			groupReady = false
			continue
		}
		messages, err := c.readBatch(ctx)
		if err == nil {
			c.processMessages(ctx, messages)
		} else if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
			logger.LegacyPrintf("repository.usage_log_consumer", "read usage log Redis stream failed: %v", err)
			groupReady = false
		}
		select {
		case <-ctx.Done():
			return
		case <-replayTicker.C:
			if err := c.queue.ReplayWAL(ctx); err != nil && ctx.Err() == nil {
				logger.LegacyPrintf("repository.usage_log_consumer", "replay usage log WAL failed: %v", err)
			}
		default:
		}
	}
}

func (c *usageLogConsumer) readBatch(ctx context.Context) ([]redis.XMessage, error) {
	read := func(block time.Duration, count int64) ([]redis.XMessage, error) {
		streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: c.cfg.Group, Consumer: c.cfg.Consumer,
			Streams: []string{c.cfg.Stream, ">"}, Count: count, Block: block,
		}).Result()
		if err != nil {
			return nil, err
		}
		var messages []redis.XMessage
		for i := range streams {
			messages = append(messages, streams[i].Messages...)
		}
		return messages, nil
	}
	messages, err := read(c.cfg.ReadBlock, int64(c.cfg.BatchSize))
	if err != nil || len(messages) == 0 || len(messages) >= c.cfg.BatchSize {
		return messages, err
	}
	deadline := time.Now().Add(c.cfg.FlushInterval)
	for len(messages) < c.cfg.BatchSize {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		more, readErr := read(remaining, int64(c.cfg.BatchSize-len(messages)))
		if errors.Is(readErr, redis.Nil) {
			break
		}
		if readErr != nil {
			return messages, readErr
		}
		messages = append(messages, more...)
	}
	return messages, nil
}

func (c *usageLogConsumer) claimStale(ctx context.Context) ([]redis.XMessage, error) {
	messages, _, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: c.cfg.Stream, Group: c.cfg.Group, Consumer: c.cfg.Consumer,
		MinIdle: c.cfg.ClaimIdle, Start: "0-0", Count: int64(c.cfg.BatchSize),
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return messages, err
}

func (c *usageLogConsumer) processMessages(ctx context.Context, messages []redis.XMessage) {
	if len(messages) == 0 {
		return
	}
	type decodedMessage struct {
		message redis.XMessage
		event   usageLogQueueEvent
	}
	decoded := make([]decodedMessage, 0, len(messages))
	poisonIDs := make([]string, 0)
	for i := range messages {
		payload, ok := redisStreamString(messages[i].Values["payload"])
		if !ok {
			if c.moveToDeadLetter(ctx, messages[i], errors.New("missing payload")) {
				poisonIDs = append(poisonIDs, messages[i].ID)
			}
			continue
		}
		var event usageLogQueueEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil || event.Version != usageLogEventVersion || event.Log.EventID == "" {
			if err == nil {
				err = fmt.Errorf("unsupported event version %d or missing event_id", event.Version)
			}
			if c.moveToDeadLetter(ctx, messages[i], err) {
				poisonIDs = append(poisonIDs, messages[i].ID)
			}
			continue
		}
		decoded = append(decoded, decodedMessage{message: messages[i], event: event})
	}
	c.ackAndDelete(ctx, poisonIDs)
	if len(decoded) == 0 {
		return
	}
	events := make([]usageLogQueueEvent, len(decoded))
	for i := range decoded {
		events[i] = decoded[i].event
	}
	ready, err := c.billingReady(ctx, events)
	if err != nil {
		c.metrics.writeFailures.Add(1)
		logger.LegacyPrintf("repository.usage_log_consumer", "check usage billing commit state failed: %v", err)
		return
	}
	rows := make([]usageLogEvent, 0, len(decoded))
	ids := make([]string, 0, len(decoded))
	staleIDs := make([]string, 0)
	for i := range decoded {
		if !ready[i] {
			if redisStreamMessageOlderThan(decoded[i].message.ID, usageLogUncommittedMaxAge) {
				if c.moveToDeadLetter(ctx, decoded[i].message, errors.New("billing commit missing or fingerprint mismatch after 24h")) {
					staleIDs = append(staleIDs, decoded[i].message.ID)
				}
			}
			continue
		}
		rows = append(rows, decoded[i].event.Log)
		ids = append(ids, decoded[i].message.ID)
	}
	c.ackAndDelete(ctx, staleIDs)
	if len(rows) == 0 {
		return
	}
	if err := c.store.InsertBatch(ctx, rows); err != nil {
		c.metrics.writeFailures.Add(1)
		logger.LegacyPrintf("repository.usage_log_consumer", "write usage logs to ClickHouse failed: %v", err)
		return
	}
	c.metrics.recordWrite(len(rows))
	c.ackAndDelete(ctx, ids)
}

func (c *usageLogConsumer) billingReady(ctx context.Context, events []usageLogQueueEvent) ([]bool, error) {
	ready := make([]bool, len(events))
	type key struct {
		requestID string
		apiKeyID  int64
	}
	indexes := make(map[key][]int)
	for i := range events {
		if !events[i].BillingRequired {
			ready[i] = true
			continue
		}
		k := key{requestID: strings.TrimSpace(events[i].Log.RequestID), apiKeyID: events[i].Log.APIKeyID}
		if k.requestID == "" {
			continue
		}
		indexes[k] = append(indexes[k], i)
	}
	if len(indexes) == 0 {
		return ready, nil
	}
	conditions := make([]string, 0, len(indexes))
	args := make([]any, 0, len(indexes)*2)
	for k := range indexes {
		conditions = append(conditions, fmt.Sprintf("($%d, $%d)", len(args)+1, len(args)+2))
		args = append(args, k.requestID, k.apiKeyID)
	}
	tuples := strings.Join(conditions, ",")
	query := fmt.Sprintf(`
		SELECT request_id, api_key_id, request_fingerprint FROM usage_billing_dedup
		WHERE (request_id, api_key_id) IN (%s)
		UNION
		SELECT request_id, api_key_id, request_fingerprint FROM usage_billing_dedup_archive
		WHERE (request_id, api_key_id) IN (%s)
	`, tuples, tuples)
	// The same placeholders are intentionally reused in both UNION branches.
	rows, err := c.billingDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var requestID string
		var apiKeyID int64
		var fingerprint string
		if err := rows.Scan(&requestID, &apiKeyID, &fingerprint); err != nil {
			return nil, err
		}
		for _, index := range indexes[key{requestID: requestID, apiKeyID: apiKeyID}] {
			ready[index] = strings.TrimSpace(events[index].BillingFingerprint) != "" &&
				strings.TrimSpace(events[index].BillingFingerprint) == strings.TrimSpace(fingerprint)
		}
	}
	return ready, rows.Err()
}

func (c *usageLogConsumer) ackAndDelete(ctx context.Context, ids []string) {
	if len(ids) == 0 {
		return
	}
	pipe := c.rdb.Pipeline()
	pipe.XAck(ctx, c.cfg.Stream, c.cfg.Group, ids...)
	pipe.XDel(ctx, c.cfg.Stream, ids...)
	if _, err := pipe.Exec(ctx); err != nil && ctx.Err() == nil {
		logger.LegacyPrintf("repository.usage_log_consumer", "ack usage log Redis stream messages failed: %v", err)
	}
}

func (c *usageLogConsumer) moveToDeadLetter(ctx context.Context, message redis.XMessage, cause error) bool {
	values := map[string]any{
		"source_id": message.ID,
		"error":     cause.Error(),
		"failed_at": strconv.FormatInt(time.Now().UTC().Unix(), 10),
	}
	if payload, ok := redisStreamString(message.Values["payload"]); ok {
		values["payload"] = payload
	}
	if err := c.rdb.XAdd(ctx, &redis.XAddArgs{Stream: c.cfg.Stream + ":dead", Values: values}).Err(); err != nil {
		logger.LegacyPrintf("repository.usage_log_consumer", "write usage log dead-letter message failed: %v", err)
		return false
	}
	return true
}

func redisStreamString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func redisStreamMessageOlderThan(id string, age time.Duration) bool {
	millisText, _, ok := strings.Cut(strings.TrimSpace(id), "-")
	if !ok {
		return false
	}
	millis, err := strconv.ParseInt(millisText, 10, 64)
	if err != nil || millis <= 0 {
		return false
	}
	return time.Since(time.UnixMilli(millis)) >= age
}
