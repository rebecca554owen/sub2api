package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestUsageLogQueueUsesRedisThenFallsBackToWALAndReplays(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: time.Second, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	wal, err := openUsageLogWAL(t.TempDir(), 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	queue, err := newUsageLogDurableQueue(client, wal, "usage:test", nil)
	require.NoError(t, err)
	event := usageLogQueueEvent{Version: usageLogEventVersion, Log: usageLogEvent{EventID: "0bf2cbe3-b1fc-5c12-b368-77c64ce48b13"}}

	require.NoError(t, queue.Enqueue(context.Background(), event))
	entries, err := mr.Stream("usage:test")
	require.NoError(t, err)
	require.Len(t, entries, 1)

	mr.Close()
	require.NoError(t, queue.Enqueue(context.Background(), event))
	stats := queue.Stats()
	require.Equal(t, uint64(1), stats.EnqueuedRedis)
	require.Equal(t, uint64(1), stats.EnqueuedWAL)
	require.Positive(t, stats.WALBytes)

	require.NoError(t, mr.Restart())
	require.NoError(t, queue.ReplayWAL(context.Background()))
	entries, err = mr.Stream("usage:test")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Zero(t, queue.Stats().WALBytes)
}

func TestUsageLogConsumerBillingReadyRequiresMatchingFingerprint(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT request_id, api_key_id, request_fingerprint FROM usage_billing_dedup").
		WithArgs("req-1", int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"request_id", "api_key_id", "request_fingerprint"}).AddRow("req-1", int64(5), "fingerprint-a"))
	consumer := &usageLogConsumer{billingDB: db}
	ready, err := consumer.billingReady(context.Background(), []usageLogQueueEvent{
		{BillingRequired: true, BillingFingerprint: "fingerprint-a", Log: usageLogEvent{RequestID: "req-1", APIKeyID: 5}},
		{BillingRequired: false, Log: usageLogEvent{RequestID: "free", APIKeyID: 6}},
	})
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, ready)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUsageLogConsumerBillingReadyRejectsFingerprintConflict(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT request_id, api_key_id, request_fingerprint FROM usage_billing_dedup").
		WithArgs("req-1", int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"request_id", "api_key_id", "request_fingerprint"}).AddRow("req-1", int64(5), "original"))
	consumer := &usageLogConsumer{billingDB: db}
	ready, err := consumer.billingReady(context.Background(), []usageLogQueueEvent{
		{BillingRequired: true, BillingFingerprint: "conflict", Log: usageLogEvent{RequestID: "req-1", APIKeyID: 5}},
	})
	require.NoError(t, err)
	require.Equal(t, []bool{false}, ready)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUsageLogConsumerWritesAndAcknowledgesReadyMessage(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	clickHouseDB, clickHouseMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = clickHouseDB.Close() })
	billingDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = billingDB.Close() })
	wal, err := openUsageLogWAL(t.TempDir(), 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	queue, err := newUsageLogDurableQueue(client, wal, "usage:consume", nil)
	require.NoError(t, err)
	consumer, err := newUsageLogConsumer(client, billingDB, &clickHouseUsageLogStore{db: clickHouseDB}, queue, usageLogConsumerConfig{
		Stream: "usage:consume", Group: "writers", Consumer: "test", BatchSize: 10,
	})
	require.NoError(t, err)
	require.NoError(t, consumer.ensureGroup(context.Background()))
	event := newUsageLogQueueEvent(&service.UsageLog{
		UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: "req", EventID: "0bf2cbe3-b1fc-5c12-b368-77c64ce48b13",
		Model: "model", CreatedAt: time.Now().UTC(),
	}, false)
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{Stream: "usage:consume", Values: map[string]any{"payload": string(payload)}}).Result()
	require.NoError(t, err)
	_, err = client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: "writers", Consumer: "test", Streams: []string{"usage:consume", ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)

	clickHouseMock.ExpectBegin()
	clickHouseMock.ExpectPrepare("INSERT INTO usage_logs").ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	clickHouseMock.ExpectCommit()
	consumer.processMessages(context.Background(), []redis.XMessage{{ID: id, Values: map[string]any{"payload": string(payload)}}})
	require.NoError(t, clickHouseMock.ExpectationsWereMet())
	length, err := client.XLen(context.Background(), "usage:consume").Result()
	require.NoError(t, err)
	require.Zero(t, length)
}

func TestUsageLogConsumerReportsDeadLetterWriteFailure(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	consumer := &usageLogConsumer{rdb: client, cfg: usageLogConsumerConfig{Stream: "usage:dead"}}
	mr.Close()
	require.False(t, consumer.moveToDeadLetter(context.Background(), redis.XMessage{ID: "1-0"}, context.DeadlineExceeded))
}
