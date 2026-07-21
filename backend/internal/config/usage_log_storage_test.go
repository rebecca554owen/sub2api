package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageLogStorageEnvironment(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("LOG_SQL_DSN", "clickhouse://logger:secret@127.0.0.1:9000/sub2api_logs")
	t.Setenv("OLD_LOG_SQL_DSN", "postgres://legacy:secret@127.0.0.1:5432/sub2api")
	t.Setenv("AUTO_MIGRATE_OLD_LOGS_TO_LOG_DB", "true")
	t.Setenv("LOG_MIGRATION_BATCH_SIZE", "2000")
	t.Setenv("ALLOW_LOG_MIGRATION_TO_NON_EMPTY_TARGET", "true")
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "31")
	t.Setenv("LOG_QUEUE_BATCH_SIZE", "500")
	t.Setenv("LOG_QUEUE_STREAM", "usage:custom")
	t.Setenv("LOG_QUEUE_CONSUMER_GROUP", "writers-custom")
	t.Setenv("LOG_QUEUE_CONSUMER_NAME", "writer-1")
	t.Setenv("LOG_QUEUE_FLUSH_INTERVAL_MS", "250")
	t.Setenv("LOG_QUEUE_CLAIM_IDLE_SECONDS", "75")
	t.Setenv("LOG_QUEUE_READ_BLOCK_MS", "1250")
	t.Setenv("LOG_WAL_DIR", t.TempDir())
	t.Setenv("LOG_WAL_MAX_BYTES", "1048576")

	cfg, err := Load()
	require.NoError(t, err)
	require.True(t, cfg.UsageLogStorage.Enabled())
	require.Equal(t, "postgres://legacy:secret@127.0.0.1:5432/sub2api", cfg.UsageLogStorage.OldLogDSN)
	require.True(t, cfg.UsageLogStorage.AutoMigrateOldLogs)
	require.Equal(t, 2000, cfg.UsageLogStorage.MigrationBatchSize)
	require.True(t, cfg.UsageLogStorage.AllowNonEmptyTarget)
	require.Equal(t, 31, cfg.UsageLogStorage.ClickHouseTTLDays)
	require.Equal(t, 500, cfg.UsageLogStorage.BatchSize)
	require.Equal(t, "usage:custom", cfg.UsageLogStorage.Stream)
	require.Equal(t, "writers-custom", cfg.UsageLogStorage.ConsumerGroup)
	require.Equal(t, "writer-1", cfg.UsageLogStorage.ConsumerName)
	require.Equal(t, 250, cfg.UsageLogStorage.FlushIntervalMS)
	require.Equal(t, 75, cfg.UsageLogStorage.ClaimIdleSeconds)
	require.Equal(t, 1250, cfg.UsageLogStorage.ReadBlockMilliseconds)
	require.Equal(t, int64(1048576), cfg.UsageLogStorage.WALMaxBytes)
}

func TestUsageLogStorageDefaultsToPostgreSQL(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("LOG_SQL_DSN", "")
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.UsageLogStorage.Enabled())
	require.Equal(t, 90, cfg.UsageLogStorage.ClickHouseTTLDays)
}

func TestUsageLogStorageRejectsInvalidTTL(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("LOG_SQL_DSN", "clickhouse://127.0.0.1:9000/sub2api_logs")
	t.Setenv("LOG_SQL_CLICKHOUSE_TTL_DAYS", "0")

	_, err := Load()
	require.ErrorContains(t, err, "clickhouse_ttl_days")
}
