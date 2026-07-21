package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type usageLogQueueMetrics struct {
	enqueuedRedis atomic.Uint64
	enqueuedWAL   atomic.Uint64
	enqueueFailed atomic.Uint64
	replayedWAL   atomic.Uint64
	writeBatches  atomic.Uint64
	writeRows     atomic.Uint64
	writeFailures atomic.Uint64
	retries       atomic.Uint64
	lastWriteUnix atomic.Int64
}

type UsageLogQueueStats struct {
	EnqueuedRedis uint64 `json:"enqueued_redis"`
	EnqueuedWAL   uint64 `json:"enqueued_wal"`
	EnqueueFailed uint64 `json:"enqueue_failed"`
	ReplayedWAL   uint64 `json:"replayed_wal"`
	WriteBatches  uint64 `json:"write_batches"`
	WriteRows     uint64 `json:"write_rows"`
	WriteFailures uint64 `json:"write_failures"`
	Retries       uint64 `json:"retries"`
	LastWriteUnix int64  `json:"last_write_unix"`
	WALBytes      int64  `json:"wal_bytes"`
}

type usageLogDurableQueue struct {
	rdb     *redis.Client
	wal     *usageLogWAL
	stream  string
	metrics *usageLogQueueMetrics
}

func newUsageLogDurableQueue(rdb *redis.Client, wal *usageLogWAL, stream string, metrics *usageLogQueueMetrics) (*usageLogDurableQueue, error) {
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return nil, errors.New("usage log Redis stream is empty")
	}
	if rdb == nil && wal == nil {
		return nil, errors.New("usage log queue requires Redis or WAL")
	}
	if metrics == nil {
		metrics = &usageLogQueueMetrics{}
	}
	return &usageLogDurableQueue{rdb: rdb, wal: wal, stream: stream, metrics: metrics}, nil
}

func (q *usageLogDurableQueue) Enqueue(ctx context.Context, event usageLogQueueEvent) error {
	if q == nil {
		return errors.New("usage log durable queue is nil")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		q.metrics.enqueueFailed.Add(1)
		return fmt.Errorf("marshal usage log queue event: %w", err)
	}
	var redisErr error
	if q.rdb != nil {
		redisErr = q.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			Values: map[string]any{"payload": string(payload)},
		}).Err()
		if redisErr == nil {
			q.metrics.enqueuedRedis.Add(1)
			return nil
		}
	}
	if q.wal != nil {
		if walErr := q.wal.Append(payload); walErr == nil {
			q.metrics.enqueuedWAL.Add(1)
			return nil
		} else {
			q.metrics.enqueueFailed.Add(1)
			return fmt.Errorf("enqueue usage log failed: redis=%v; wal=%w", redisErr, walErr)
		}
	}
	q.metrics.enqueueFailed.Add(1)
	return fmt.Errorf("enqueue usage log to Redis: %w", redisErr)
}

func (q *usageLogDurableQueue) ReplayWAL(ctx context.Context) error {
	if q == nil || q.wal == nil {
		return nil
	}
	if q.rdb == nil {
		return errors.New("cannot replay usage log WAL without Redis")
	}
	return q.wal.Replay(ctx, func(ctx context.Context, payload []byte) error {
		if err := q.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			Values: map[string]any{"payload": string(payload)},
		}).Err(); err != nil {
			return fmt.Errorf("replay usage log WAL to Redis: %w", err)
		}
		q.metrics.replayedWAL.Add(1)
		return nil
	})
}

func (q *usageLogDurableQueue) Stats() UsageLogQueueStats {
	if q == nil || q.metrics == nil {
		return UsageLogQueueStats{}
	}
	stats := UsageLogQueueStats{
		EnqueuedRedis: q.metrics.enqueuedRedis.Load(),
		EnqueuedWAL:   q.metrics.enqueuedWAL.Load(),
		EnqueueFailed: q.metrics.enqueueFailed.Load(),
		ReplayedWAL:   q.metrics.replayedWAL.Load(),
		WriteBatches:  q.metrics.writeBatches.Load(),
		WriteRows:     q.metrics.writeRows.Load(),
		WriteFailures: q.metrics.writeFailures.Load(),
		Retries:       q.metrics.retries.Load(),
		LastWriteUnix: q.metrics.lastWriteUnix.Load(),
	}
	if q.wal != nil {
		stats.WALBytes, _ = q.wal.SizeBytes()
	}
	return stats
}

func (m *usageLogQueueMetrics) recordWrite(rows int) {
	if m == nil {
		return
	}
	m.writeBatches.Add(1)
	if rows > 0 {
		m.writeRows.Add(uint64(rows))
	}
	m.lastWriteUnix.Store(time.Now().UTC().Unix())
}
