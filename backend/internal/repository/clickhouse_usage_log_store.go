package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const clickHouseUsageLogTable = "usage_logs"

type clickHouseUsageLogStore struct {
	db *sql.DB
}

func openClickHouseUsageLogStore(ctx context.Context, dsn string, ttlDays int) (*clickHouseUsageLogStore, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, errors.New("clickhouse usage log DSN is empty")
	}
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse usage log DSN: %w", err)
	}
	db := clickhouse.OpenDB(options)
	store := &clickHouseUsageLogStore{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping clickhouse usage log database: %w", err)
	}
	if err := store.ensureSchema(ctx, ttlDays); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *clickHouseUsageLogStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *clickHouseUsageLogStore) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("clickhouse usage log database is nil")
	}
	return s.db.PingContext(ctx)
}

func (s *clickHouseUsageLogStore) ensureSchema(ctx context.Context, ttlDays int) error {
	if s == nil || s.db == nil {
		return errors.New("clickhouse usage log database is nil")
	}
	if ttlDays <= 0 || ttlDays > 3650 {
		return fmt.Errorf("clickhouse usage log TTL days must be between 1 and 3650, got %d", ttlDays)
	}
	if _, err := s.db.ExecContext(ctx, clickHouseUsageLogSchema(ttlDays)); err != nil {
		return fmt.Errorf("create clickhouse usage_logs table: %w", err)
	}
	// CREATE TABLE IF NOT EXISTS does not update an existing TTL. Keep the
	// configured retention authoritative across restarts.
	alterTTL := fmt.Sprintf(
		"ALTER TABLE %s MODIFY TTL created_at + INTERVAL %d DAY DELETE",
		clickHouseUsageLogTable,
		ttlDays,
	)
	if _, err := s.db.ExecContext(ctx, alterTTL); err != nil {
		return fmt.Errorf("update clickhouse usage_logs TTL: %w", err)
	}
	return nil
}

func clickHouseUsageLogSchema(ttlDays int) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id Int64,
    event_id UUID,
    user_id Int64,
    api_key_id Int64,
    account_id Int64,
    request_id String,
    user_email String,
    username String,
    api_key_name String,
    account_name String,
    account_platform LowCardinality(String),
    group_id Nullable(Int64),
    group_name String,
    group_platform LowCardinality(String),
    subscription_id Nullable(Int64),
    model LowCardinality(String),
    requested_model LowCardinality(String),
    upstream_model Nullable(String),
    channel_id Nullable(Int64),
    model_mapping_chain Nullable(String),
    billing_tier Nullable(String),
    billing_mode LowCardinality(Nullable(String)),
    service_tier LowCardinality(Nullable(String)),
    reasoning_effort LowCardinality(Nullable(String)),
    inbound_endpoint Nullable(String),
    upstream_endpoint Nullable(String),
    input_tokens Int64,
    output_tokens Int64,
    cache_creation_tokens Int64,
    cache_read_tokens Int64,
    cache_creation_5m_tokens Int64,
    cache_creation_1h_tokens Int64,
    image_input_tokens Int64,
    image_output_tokens Int64,
    image_count Int64,
    video_count Int64,
    video_duration_seconds Nullable(Int64),
    duration_ms Nullable(Int64),
    first_token_ms Nullable(Int64),
    image_input_cost Float64,
    image_output_cost Float64,
    input_cost Float64,
    output_cost Float64,
    cache_creation_cost Float64,
    cache_read_cost Float64,
    total_cost Float64,
    actual_cost Float64,
    rate_multiplier Float64,
    account_rate_multiplier Nullable(Float64),
    account_stats_cost Nullable(Float64),
    long_context_billing_applied Bool,
    billing_type Int8,
    request_type Int16,
    stream Bool,
    openai_ws_mode Bool,
    user_agent Nullable(String),
    ip_address Nullable(String),
    cache_ttl_overridden Bool,
    image_size Nullable(String),
    image_input_size Nullable(String),
    image_output_size Nullable(String),
    image_size_source Nullable(String),
    image_size_breakdown String,
    media_type LowCardinality(Nullable(String)),
    video_resolution Nullable(String),
    created_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 1,
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_api_key_id api_key_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_account_id account_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY event_id
TTL created_at + INTERVAL %d DAY DELETE
SETTINGS index_granularity = 8192`, clickHouseUsageLogTable, ttlDays)
}

const clickHouseUsageLogInsert = `INSERT INTO usage_logs (
    id, event_id, user_id, api_key_id, account_id, request_id,
    user_email, username, api_key_name, account_name, account_platform,
    group_id, group_name, group_platform, subscription_id,
    model, requested_model, upstream_model, channel_id, model_mapping_chain,
    billing_tier, billing_mode, service_tier, reasoning_effort,
    inbound_endpoint, upstream_endpoint,
    input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens,
    cache_creation_5m_tokens, cache_creation_1h_tokens,
    image_input_tokens, image_output_tokens, image_count, video_count,
    video_duration_seconds, duration_ms, first_token_ms,
    image_input_cost, image_output_cost, input_cost, output_cost,
    cache_creation_cost, cache_read_cost, total_cost, actual_cost,
    rate_multiplier, account_rate_multiplier, account_stats_cost,
    long_context_billing_applied, billing_type, request_type, stream,
    openai_ws_mode, user_agent, ip_address, cache_ttl_overridden,
    image_size, image_input_size, image_output_size, image_size_source,
    image_size_breakdown, media_type, video_resolution, created_at
)`

func (s *clickHouseUsageLogStore) InsertBatch(ctx context.Context, events []usageLogEvent) (err error) {
	if len(events) == 0 {
		return nil
	}
	if s == nil || s.db == nil {
		return errors.New("clickhouse usage log database is nil")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin clickhouse usage log batch: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx, clickHouseUsageLogInsert)
	if err != nil {
		return fmt.Errorf("prepare clickhouse usage log batch: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for i := range events {
		if err := appendClickHouseUsageLog(ctx, stmt, events[i]); err != nil {
			return fmt.Errorf("append clickhouse usage log event %s: %w", events[i].EventID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit clickhouse usage log batch: %w", err)
	}
	tx = nil
	return nil
}

func appendClickHouseUsageLog(ctx context.Context, stmt *sql.Stmt, event usageLogEvent) error {
	breakdown, err := json.Marshal(event.ImageSizeBreakdown)
	if err != nil {
		return fmt.Errorf("marshal image size breakdown: %w", err)
	}
	_, err = stmt.ExecContext(ctx,
		event.ID, event.EventID, event.UserID, event.APIKeyID, event.AccountID, event.RequestID,
		event.UserEmail, event.Username, event.APIKeyName, event.AccountName, event.AccountPlatform,
		nullableInt64(event.GroupID), event.GroupName, event.GroupPlatform, nullableInt64(event.SubscriptionID),
		event.Model, event.RequestedModel, nullableString(event.UpstreamModel), nullableInt64(event.ChannelID), nullableString(event.ModelMappingChain),
		nullableString(event.BillingTier), nullableString(event.BillingMode), nullableString(event.ServiceTier), nullableString(event.ReasoningEffort),
		nullableString(event.InboundEndpoint), nullableString(event.UpstreamEndpoint),
		int64(event.InputTokens), int64(event.OutputTokens), int64(event.CacheCreationTokens), int64(event.CacheReadTokens),
		int64(event.CacheCreation5mTokens), int64(event.CacheCreation1hTokens),
		int64(event.ImageInputTokens), int64(event.ImageOutputTokens), int64(event.ImageCount), int64(event.VideoCount),
		nullableInt(event.VideoDurationSeconds), nullableInt(event.DurationMs), nullableInt(event.FirstTokenMs),
		event.ImageInputCost, event.ImageOutputCost, event.InputCost, event.OutputCost,
		event.CacheCreationCost, event.CacheReadCost, event.TotalCost, event.ActualCost,
		event.RateMultiplier, nullableFloat64(event.AccountRateMultiplier), nullableFloat64(event.AccountStatsCost),
		event.LongContextBillingApplied, event.BillingType, int16(event.RequestType), event.Stream,
		event.OpenAIWSMode, nullableString(event.UserAgent), nullableString(event.IPAddress), event.CacheTTLOverridden,
		nullableString(event.ImageSize), nullableString(event.ImageInputSize), nullableString(event.ImageOutputSize), nullableString(event.ImageSizeSource),
		string(breakdown), nullableString(event.MediaType), nullableString(event.VideoResolution), event.CreatedAt.UTC(),
	)
	return err
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return int64(*value)
}

func nullableFloat64(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func (s *clickHouseUsageLogStore) LastWriteDelay(ctx context.Context) (time.Duration, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("clickhouse usage log database is nil")
	}
	var latest sql.NullTime
	if err := s.db.QueryRowContext(ctx, "SELECT maxOrNull(created_at) FROM usage_logs FINAL").Scan(&latest); err != nil {
		return 0, err
	}
	if !latest.Valid || latest.Time.IsZero() {
		return 0, nil
	}
	delay := time.Since(latest.Time)
	if delay < 0 {
		return 0, nil
	}
	return delay, nil
}
