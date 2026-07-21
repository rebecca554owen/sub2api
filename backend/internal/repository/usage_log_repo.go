package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	gocache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
)

const rawUsageLogModelColumn = "model"

// rawUsageLogModelColumn preserves the exact stored usage_logs.model semantics for direct filters.
// Historical rows may contain upstream/billing model values, while newer rows store requested_model.
// Requested/upstream/mapping analytics must use resolveModelDimensionExpression instead.

// usageLogSuccessFilterUL 用于把"失败请求 usage log"（tokens=0、cost=0、不计费的占位记录）
// 从统计性聚合中排除，避免污染 Dashboard / 用量拆分等指标。
//
// schema 中没有 success bool 列；新增列要做迁移，风险大；这里用 actual_cost > 0 作为代理：
// 任何成功落账的请求都会产生 actual_cost（包括 token 计费、纯图片 token 计费、按次/按图计费），
// 反之 failed-request usage log 的 actual_cost 为 0。
// 早期版本用 4 项 token 和 > 0 判定会把"按次/按图计费"与"image_output_tokens 独立计费"的纯图片
// 请求误判为失败，导致这部分请求从用量统计里消失，故改用 actual_cost。
// 配合 `FROM usage_logs ul` JOIN 查询使用。
const usageLogSuccessFilterUL = "ul.actual_cost > 0"

// usageLogEffectivePlatformExpr 用于按"有效平台"维度聚合 usage_logs：
// 优先取请求实际走的分组 platform，若分组未设置 platform 再 fallback 到 account.platform。
// Composite groups are a routing layer, so platform analytics must use the
// resolved concrete account platform instead of grouping spend under "composite".
// 配套要求查询里 LEFT JOIN groups g ON g.id = ul.group_id 与 LEFT JOIN accounts a ON a.id = ul.account_id。
const usageLogEffectivePlatformExpr = "CASE WHEN g.platform = 'composite' THEN a.platform ELSE COALESCE(NULLIF(g.platform,''), a.platform) END"

// dateFormatWhitelist 将 granularity 参数映射为 PostgreSQL TO_CHAR 格式字符串，防止外部输入直接拼入 SQL
var dateFormatWhitelist = map[string]string{
	"hour":  "YYYY-MM-DD HH24:00",
	"day":   "YYYY-MM-DD",
	"week":  "IYYY-IW",
	"month": "YYYY-MM",
}

// safeDateFormat 根据白名单获取 dateFormat，未匹配时返回默认值
func safeDateFormat(granularity string) string {
	if f, ok := dateFormatWhitelist[granularity]; ok {
		return f
	}
	return "YYYY-MM-DD"
}

// appendRawUsageLogModelWhereCondition keeps direct model filters on the raw model column for backward
// compatibility with historical rows. Requested/upstream analytics must use
// resolveModelDimensionExpression instead.
func appendRawUsageLogModelWhereCondition(conditions []string, args []any, model string) ([]string, []any) {
	if strings.TrimSpace(model) == "" {
		return conditions, args
	}
	conditions = append(conditions, fmt.Sprintf("%s = $%d", rawUsageLogModelColumn, len(args)+1))
	args = append(args, model)
	return conditions, args
}

func appendUsageLogBillingModeWhereCondition(conditions []string, args []any, billingMode string) ([]string, []any) {
	return appendUsageLogBillingModeWhereConditionWithAlias(conditions, args, billingMode, "")
}

func appendUsageLogBillingModeWhereConditionWithAlias(conditions []string, args []any, billingMode string, alias string) ([]string, []any) {
	mode := strings.TrimSpace(billingMode)
	if mode == "" {
		return conditions, args
	}
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}
	placeholder := fmt.Sprintf("$%d", len(args)+1)
	switch service.BillingMode(mode) {
	case service.BillingModeImage:
		conditions = append(conditions, fmt.Sprintf("(%s = %s OR ((%s IS NULL OR %s = '') AND COALESCE(%s, 0) > 0))", column("billing_mode"), placeholder, column("billing_mode"), column("billing_mode"), column("image_count")))
	case service.BillingModeVideo:
		conditions = append(conditions, fmt.Sprintf("%s = %s", column("billing_mode"), placeholder))
	case service.BillingModeToken:
		conditions = append(conditions, fmt.Sprintf("(%s = %s OR ((%s IS NULL OR %s = '') AND COALESCE(%s, 0) <= 0))", column("billing_mode"), placeholder, column("billing_mode"), column("billing_mode"), column("image_count")))
	default:
		conditions = append(conditions, fmt.Sprintf("%s = %s", column("billing_mode"), placeholder))
	}
	args = append(args, mode)
	return conditions, args
}

func appendUsageLogBillingModeQueryFilter(query string, args []any, billingMode string, alias string) (string, []any) {
	conditions, args := appendUsageLogBillingModeWhereConditionWithAlias(nil, args, billingMode, alias)
	if len(conditions) == 0 {
		return query, args
	}
	return query + " AND " + conditions[0], args
}

func appendUsageLogModelWhereCondition(conditions []string, args []any, model string, source string) ([]string, []any) {
	if strings.TrimSpace(source) == "" {
		return appendRawUsageLogModelWhereCondition(conditions, args, model)
	}
	if strings.TrimSpace(model) == "" {
		return conditions, args
	}
	conditions = append(conditions, fmt.Sprintf("%s = $%d", resolveModelDimensionExpression(source), len(args)+1))
	args = append(args, model)
	return conditions, args
}

// appendRawUsageLogModelQueryFilter keeps direct model filters on the raw model column for backward
// compatibility with historical rows. Requested/upstream analytics must use
// resolveModelDimensionExpression instead.
func appendRawUsageLogModelQueryFilter(query string, args []any, model string) (string, []any) {
	if strings.TrimSpace(model) == "" {
		return query, args
	}
	query += fmt.Sprintf(" AND %s = $%d", rawUsageLogModelColumn, len(args)+1)
	args = append(args, model)
	return query, args
}

func appendUsageLogModelQueryFilter(query string, args []any, model string, source string) (string, []any) {
	if strings.TrimSpace(source) == "" {
		return appendRawUsageLogModelQueryFilter(query, args, model)
	}
	if strings.TrimSpace(model) == "" {
		return query, args
	}
	query += fmt.Sprintf(" AND %s = $%d", resolveModelDimensionExpression(source), len(args)+1)
	args = append(args, model)
	return query, args
}

type usageLogRepository struct {
	client *dbent.Client
	sql    sqlExecutor
	db     *sql.DB

	createBatchOnce     sync.Once
	createBatchCh       chan usageLogCreateRequest
	bestEffortBatchOnce sync.Once
	bestEffortBatchCh   chan usageLogBestEffortRequest
	bestEffortRecent    *gocache.Cache
}

func NewUsageLogRepository(client *dbent.Client, sqlDB *sql.DB) service.UsageLogRepository {
	return newUsageLogRepositoryWithSQL(client, sqlDB)
}

type UsageLogRepositoryBundle struct {
	Repository service.UsageLogRepository
	Runtime    *UsageLogRuntime
}

type UsageLogRuntime struct {
	external *clickHouseUsageLogRepository
}

type UsageLogHealth struct {
	Enabled                     bool               `json:"enabled"`
	ClickHouseOK                bool               `json:"clickhouse_ok"`
	ClickHouseError             string             `json:"clickhouse_error,omitempty"`
	ClickHouseWriteDelaySeconds float64            `json:"clickhouse_write_delay_seconds"`
	RedisStreamLength           int64              `json:"redis_stream_length"`
	RedisPending                int64              `json:"redis_pending"`
	RedisError                  string             `json:"redis_error,omitempty"`
	Queue                       UsageLogQueueStats `json:"queue"`
}

func (r *UsageLogRuntime) Start(ctx context.Context) error {
	if r == nil || r.external == nil {
		return nil
	}
	return r.external.Start(ctx)
}

func (r *UsageLogRuntime) Stop() {
	if r == nil || r.external == nil {
		return
	}
	r.external.Stop()
}

func (r *UsageLogRuntime) Stats() UsageLogQueueStats {
	if r == nil || r.external == nil {
		return UsageLogQueueStats{}
	}
	return r.external.QueueStats()
}

func (r *UsageLogRuntime) Health(ctx context.Context) UsageLogHealth {
	health := UsageLogHealth{Enabled: r != nil && r.external != nil}
	if !health.Enabled {
		return health
	}
	health.Queue = r.external.QueueStats()
	if err := r.external.store.Ping(ctx); err != nil {
		health.ClickHouseError = err.Error()
	} else {
		health.ClickHouseOK = true
		if delay, err := r.external.store.LastWriteDelay(ctx); err == nil {
			health.ClickHouseWriteDelaySeconds = delay.Seconds()
		}
	}
	if r.external.queue != nil && r.external.queue.rdb != nil {
		length, err := r.external.queue.rdb.XLen(ctx, r.external.queue.stream).Result()
		if err != nil {
			health.RedisError = err.Error()
		} else {
			health.RedisStreamLength = length
			if r.external.consumer != nil {
				pending, pendingErr := r.external.queue.rdb.XPending(ctx, r.external.queue.stream, r.external.consumer.cfg.Group).Result()
				if pendingErr != nil && !errors.Is(pendingErr, redis.Nil) {
					health.RedisError = pendingErr.Error()
				} else if pending != nil {
					health.RedisPending = pending.Count
				}
			}
		}
	}
	return health
}

func ProvideUsageLogRepositoryBundle(
	client *dbent.Client,
	sqlDB *sql.DB,
	rdb *redis.Client,
	cfg *config.Config,
) (*UsageLogRepositoryBundle, error) {
	postgresRepository := NewUsageLogRepository(client, sqlDB)
	if cfg == nil || !cfg.UsageLogStorage.Enabled() {
		return &UsageLogRepositoryBundle{
			Repository: postgresRepository,
			Runtime:    &UsageLogRuntime{},
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := openClickHouseUsageLogStore(ctx, cfg.UsageLogStorage.DSN, cfg.UsageLogStorage.ClickHouseTTLDays)
	if err != nil {
		return nil, err
	}
	if cfg.UsageLogStorage.AutoMigrateOldLogs {
		sourceDB := sqlDB
		closeSource := false
		if oldDSN := strings.TrimSpace(cfg.UsageLogStorage.OldLogDSN); oldDSN != "" {
			sourceDB, err = sql.Open("postgres", oldDSN)
			if err != nil {
				_ = store.Close()
				return nil, fmt.Errorf("open old usage log PostgreSQL database: %w", err)
			}
			closeSource = true
		}
		if closeSource {
			defer func() { _ = sourceDB.Close() }()
		}
		report, migrationErr := MigrateAndClearUsageLogsToClickHouse(context.Background(), sourceDB, UsageLogMigrationOptions{
			ClickHouseDSN:       cfg.UsageLogStorage.DSN,
			TTLDays:             cfg.UsageLogStorage.ClickHouseTTLDays,
			BatchSize:           cfg.UsageLogStorage.MigrationBatchSize,
			AllowNonEmptyTarget: cfg.UsageLogStorage.AllowNonEmptyTarget,
		})
		if migrationErr != nil {
			_ = store.Close()
			return nil, fmt.Errorf("auto migrate old usage logs: %w", migrationErr)
		}
		logger.LegacyPrintf(
			"repository.usage_log_migration",
			"usage log migration completed: source_rows=%d expired_rows=%d migrated_rows=%d destination_rows=%d max_source_id=%d",
			report.SourceRows,
			report.ExpiredRows,
			report.MigratedRows,
			report.DestinationRows,
			report.MaxSourceID,
		)
	}
	wal, err := openUsageLogWAL(cfg.UsageLogStorage.WALDir, cfg.UsageLogStorage.WALMaxBytes)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	metrics := &usageLogQueueMetrics{}
	queue, err := newUsageLogDurableQueue(rdb, wal, cfg.UsageLogStorage.Stream, metrics)
	if err != nil {
		_ = wal.Close()
		_ = store.Close()
		return nil, err
	}
	consumer, err := newUsageLogConsumer(rdb, sqlDB, store, queue, usageLogConsumerConfig{
		Stream: cfg.UsageLogStorage.Stream, Group: cfg.UsageLogStorage.ConsumerGroup,
		Consumer: cfg.UsageLogStorage.ConsumerName, BatchSize: cfg.UsageLogStorage.BatchSize,
		FlushInterval:  time.Duration(cfg.UsageLogStorage.FlushIntervalMS) * time.Millisecond,
		ReadBlock:      time.Duration(cfg.UsageLogStorage.ReadBlockMilliseconds) * time.Millisecond,
		ClaimIdle:      time.Duration(cfg.UsageLogStorage.ClaimIdleSeconds) * time.Second,
		ReplayInterval: 5 * time.Second,
	})
	if err != nil {
		_ = wal.Close()
		_ = store.Close()
		return nil, err
	}
	external := &clickHouseUsageLogRepository{
		store: store, queue: queue, consumer: consumer, billingDB: sqlDB,
	}
	return &UsageLogRepositoryBundle{
		Repository: external,
		Runtime:    &UsageLogRuntime{external: external},
	}, nil
}

func ProvideUsageLogRepositoryFromBundle(bundle *UsageLogRepositoryBundle) service.UsageLogRepository {
	if bundle == nil {
		return nil
	}
	return bundle.Repository
}

func ProvideUsageLogRuntimeFromBundle(bundle *UsageLogRepositoryBundle) *UsageLogRuntime {
	if bundle == nil || bundle.Runtime == nil {
		return &UsageLogRuntime{}
	}
	return bundle.Runtime
}

func newUsageLogRepositoryWithSQL(client *dbent.Client, sqlq sqlExecutor) *usageLogRepository {
	// 使用 scanSingleRow 替代 QueryRowContext，保证 ent.Tx 作为 sqlExecutor 可用。
	repo := &usageLogRepository{client: client, sql: sqlq}
	if db, ok := sqlq.(*sql.DB); ok {
		repo.db = db
	}
	repo.bestEffortRecent = gocache.New(usageLogBestEffortRecentTTL, time.Minute)
	return repo
}

func buildWhere(conditions []string) string {
	if len(conditions) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(conditions, " AND ")
}

func appendRequestTypeOrStreamWhereCondition(conditions []string, args []any, requestType *int16, stream *bool) ([]string, []any) {
	if requestType != nil {
		condition, conditionArgs := buildRequestTypeFilterCondition(len(args)+1, *requestType)
		conditions = append(conditions, condition)
		args = append(args, conditionArgs...)
		return conditions, args
	}
	if stream != nil {
		conditions = append(conditions, fmt.Sprintf("stream = $%d", len(args)+1))
		args = append(args, *stream)
	}
	return conditions, args
}

func appendRequestTypeOrStreamQueryFilter(query string, args []any, requestType *int16, stream *bool) (string, []any) {
	if requestType != nil {
		condition, conditionArgs := buildRequestTypeFilterCondition(len(args)+1, *requestType)
		query += " AND " + condition
		args = append(args, conditionArgs...)
		return query, args
	}
	if stream != nil {
		query += fmt.Sprintf(" AND stream = $%d", len(args)+1)
		args = append(args, *stream)
	}
	return query, args
}

// buildRequestTypeFilterCondition 在 request_type 过滤时兼容 legacy 字段，避免历史数据漏查。
func buildRequestTypeFilterCondition(startArgIndex int, requestType int16) (string, []any) {
	return buildRequestTypeFilterConditionWithAlias(startArgIndex, requestType, "")
}

func buildRequestTypeFilterConditionWithAlias(startArgIndex int, requestType int16, alias string) (string, []any) {
	normalized := service.RequestTypeFromInt16(requestType)
	requestTypeArg := int16(normalized)
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	switch normalized {
	case service.RequestTypeSync:
		return fmt.Sprintf("(%srequest_type = $%d OR (%srequest_type = %d AND %sstream = FALSE AND %sopenai_ws_mode = FALSE))", prefix, startArgIndex, prefix, int16(service.RequestTypeUnknown), prefix, prefix), []any{requestTypeArg}
	case service.RequestTypeStream:
		return fmt.Sprintf("(%srequest_type = $%d OR (%srequest_type = %d AND %sstream = TRUE AND %sopenai_ws_mode = FALSE))", prefix, startArgIndex, prefix, int16(service.RequestTypeUnknown), prefix, prefix), []any{requestTypeArg}
	case service.RequestTypeWSV2:
		return fmt.Sprintf("(%srequest_type = $%d OR (%srequest_type = %d AND %sopenai_ws_mode = TRUE))", prefix, startArgIndex, prefix, int16(service.RequestTypeUnknown), prefix), []any{requestTypeArg}
	default:
		return fmt.Sprintf("%srequest_type = $%d", prefix, startArgIndex), []any{requestTypeArg}
	}
}
