package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clickHouseDashboardAggregationRepository struct {
	*dashboardAggregationRepository
	postgres *sql.DB
	store    *clickHouseUsageLogStore
}

type clickHouseDashboardAggregate struct {
	bucket              any
	totalRequests       int64
	inputTokens         int64
	outputTokens        int64
	cacheCreationTokens int64
	cacheReadTokens     int64
	totalCost           float64
	actualCost          float64
	accountCost         float64
	totalDurationMS     int64
	activeUsers         int64
}

func ProvideDashboardAggregationRepository(
	sqlDB *sql.DB,
	bundle *UsageLogRepositoryBundle,
	cfg *config.Config,
) service.DashboardAggregationRepository {
	postgres := NewDashboardAggregationRepository(sqlDB)
	if cfg == nil || !cfg.UsageLogStorage.Enabled() || bundle == nil || bundle.Runtime == nil || bundle.Runtime.external == nil {
		return postgres
	}
	base, ok := postgres.(*dashboardAggregationRepository)
	if !ok || base == nil {
		return postgres
	}
	return &clickHouseDashboardAggregationRepository{
		dashboardAggregationRepository: base,
		postgres:                       sqlDB,
		store:                          bundle.Runtime.external.store,
	}
}

func (r *clickHouseDashboardAggregationRepository) AggregateRange(ctx context.Context, start, end time.Time) error {
	return r.aggregateRange(ctx, start, end, false)
}

func (r *clickHouseDashboardAggregationRepository) RecomputeRange(ctx context.Context, start, end time.Time) error {
	return r.aggregateRange(ctx, start, end, true)
}

func (r *clickHouseDashboardAggregationRepository) aggregateRange(ctx context.Context, start, end time.Time, recompute bool) error {
	if r == nil || r.postgres == nil || r.store == nil || r.store.db == nil || !end.After(start) {
		return nil
	}
	loc := timezone.Location()
	startLocal := start.In(loc)
	endLocal := end.In(loc)
	hourStart := startLocal.Truncate(time.Hour)
	hourEnd := endLocal.Truncate(time.Hour)
	if endLocal.After(hourEnd) {
		hourEnd = hourEnd.Add(time.Hour)
	}
	dayStart := truncateToDay(startLocal)
	dayEnd := truncateToDay(endLocal)
	if endLocal.After(dayEnd) {
		dayEnd = dayEnd.Add(24 * time.Hour)
	}

	hourly, err := r.loadClickHouseDashboardAggregates(ctx, hourStart, hourEnd, "hour")
	if err != nil {
		return err
	}
	daily, err := r.loadClickHouseDashboardAggregates(ctx, dayStart, dayEnd, "day")
	if err != nil {
		return err
	}

	tx, err := r.postgres.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if recompute {
		if _, err := tx.ExecContext(ctx, "DELETE FROM usage_dashboard_hourly WHERE bucket_start >= $1 AND bucket_start < $2", hourStart, hourEnd); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM usage_dashboard_hourly_users WHERE bucket_start >= $1 AND bucket_start < $2", hourStart, hourEnd); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM usage_dashboard_daily WHERE bucket_date >= $1::date AND bucket_date < $2::date", dayStart, dayEnd); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM usage_dashboard_daily_users WHERE bucket_date >= $1::date AND bucket_date < $2::date", dayStart, dayEnd); err != nil {
			return err
		}
	}
	if err := upsertClickHouseDashboardAggregates(ctx, tx, "usage_dashboard_hourly", "bucket_start", hourly); err != nil {
		return err
	}
	if err := upsertClickHouseDashboardAggregates(ctx, tx, "usage_dashboard_daily", "bucket_date", daily); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *clickHouseDashboardAggregationRepository) loadClickHouseDashboardAggregates(ctx context.Context, start, end time.Time, granularity string) ([]clickHouseDashboardAggregate, error) {
	tzName := timezone.Name()
	bucketExpr := "toStartOfHour(toTimeZone(created_at, ?))"
	if granularity == "day" {
		bucketExpr = "formatDateTime(toStartOfDay(toTimeZone(created_at, ?)), '%F')"
	}
	query := fmt.Sprintf(`SELECT %s AS bucket,
    count(), sum(input_tokens), sum(output_tokens),
    sum(cache_creation_tokens), sum(cache_read_tokens),
    sum(total_cost), sum(actual_cost), sum(%s),
    sum(ifNull(duration_ms, 0)), uniqExact(user_id)
FROM usage_logs FINAL
WHERE created_at >= ? AND created_at < ?
GROUP BY bucket ORDER BY bucket`, bucketExpr, clickHouseAccountCostExpression)
	rows, err := r.store.db.QueryContext(ctx, query, tzName, start.UTC(), end.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]clickHouseDashboardAggregate, 0)
	for rows.Next() {
		var item clickHouseDashboardAggregate
		if granularity == "day" {
			var bucket string
			if err := rows.Scan(&bucket, &item.totalRequests, &item.inputTokens, &item.outputTokens,
				&item.cacheCreationTokens, &item.cacheReadTokens, &item.totalCost, &item.actualCost,
				&item.accountCost, &item.totalDurationMS, &item.activeUsers); err != nil {
				return nil, err
			}
			item.bucket = bucket
		} else {
			var bucket time.Time
			if err := rows.Scan(&bucket, &item.totalRequests, &item.inputTokens, &item.outputTokens,
				&item.cacheCreationTokens, &item.cacheReadTokens, &item.totalCost, &item.actualCost,
				&item.accountCost, &item.totalDurationMS, &item.activeUsers); err != nil {
				return nil, err
			}
			item.bucket = bucket
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func upsertClickHouseDashboardAggregates(ctx context.Context, tx *sql.Tx, table, bucketColumn string, values []clickHouseDashboardAggregate) error {
	if tx == nil || len(values) == 0 {
		return nil
	}
	if table != "usage_dashboard_hourly" && table != "usage_dashboard_daily" {
		return fmt.Errorf("unsupported dashboard aggregate table %q", table)
	}
	if bucketColumn != "bucket_start" && bucketColumn != "bucket_date" {
		return fmt.Errorf("unsupported dashboard aggregate bucket %q", bucketColumn)
	}
	query := fmt.Sprintf(`INSERT INTO %s (
    %s, total_requests, input_tokens, output_tokens,
    cache_creation_tokens, cache_read_tokens, total_cost, actual_cost,
    account_cost, total_duration_ms, active_users, computed_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
ON CONFLICT (%s) DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    cache_creation_tokens = EXCLUDED.cache_creation_tokens,
    cache_read_tokens = EXCLUDED.cache_read_tokens,
    total_cost = EXCLUDED.total_cost,
    actual_cost = EXCLUDED.actual_cost,
    account_cost = EXCLUDED.account_cost,
    total_duration_ms = EXCLUDED.total_duration_ms,
    active_users = EXCLUDED.active_users,
    computed_at = EXCLUDED.computed_at`, table, bucketColumn, bucketColumn)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for i := range values {
		item := values[i]
		if _, err := stmt.ExecContext(ctx, item.bucket, item.totalRequests, item.inputTokens, item.outputTokens,
			item.cacheCreationTokens, item.cacheReadTokens, item.totalCost, item.actualCost,
			item.accountCost, item.totalDurationMS, item.activeUsers); err != nil {
			return err
		}
	}
	return nil
}

func (r *clickHouseDashboardAggregationRepository) EnsureUsageLogsPartitions(context.Context, time.Time) error {
	return nil
}
