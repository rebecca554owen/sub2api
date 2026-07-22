package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *clickHouseUsageLogRepository) GetDashboardStats(ctx context.Context) (*usagestats.DashboardStats, error) {
	return r.GetDashboardStatsWithRange(ctx, time.Unix(0, 0).UTC(), time.Now().UTC())
}

func (r *clickHouseUsageLogRepository) GetDashboardStatsWithRange(ctx context.Context, start, end time.Time) (*usagestats.DashboardStats, error) {
	if !end.After(start) {
		return nil, errors.New("统计时间范围无效")
	}
	stats := &usagestats.DashboardStats{}
	if err := r.fillDashboardEntityStats(ctx, stats); err != nil {
		return nil, err
	}
	today := timezone.Today().UTC()
	hour := time.Now().UTC().Truncate(time.Hour)
	query := fmt.Sprintf(`SELECT
    count(), sum(input_tokens), sum(output_tokens), sum(cache_creation_tokens), sum(cache_read_tokens),
    sum(total_cost), sum(actual_cost), sum(%s), if(count() = 0, 0, avg(ifNull(duration_ms, 0))),
    countIf(created_at >= ?), sumIf(input_tokens, created_at >= ?), sumIf(output_tokens, created_at >= ?),
    sumIf(cache_creation_tokens, created_at >= ?), sumIf(cache_read_tokens, created_at >= ?),
    sumIf(total_cost, created_at >= ?), sumIf(actual_cost, created_at >= ?), sumIf(%s, created_at >= ?),
    uniqExactIf(user_id, created_at >= ?), uniqExactIf(user_id, created_at >= ?)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?`, clickHouseAccountCostExpression, clickHouseAccountCostExpression)
	if err := r.store.db.QueryRowContext(ctx, query,
		today, today, today, today, today, today, today, today, today, hour,
		start.UTC(), end.UTC(),
	).Scan(
		&stats.TotalRequests, &stats.TotalInputTokens, &stats.TotalOutputTokens,
		&stats.TotalCacheCreationTokens, &stats.TotalCacheReadTokens,
		&stats.TotalCost, &stats.TotalActualCost, &stats.TotalAccountCost, &stats.AverageDurationMs,
		&stats.TodayRequests, &stats.TodayInputTokens, &stats.TodayOutputTokens,
		&stats.TodayCacheCreationTokens, &stats.TodayCacheReadTokens,
		&stats.TodayCost, &stats.TodayActualCost, &stats.TodayAccountCost,
		&stats.ActiveUsers, &stats.HourlyActiveUsers,
	); err != nil {
		return nil, err
	}
	stats.TotalTokens = stats.TotalInputTokens + stats.TotalOutputTokens + stats.TotalCacheCreationTokens + stats.TotalCacheReadTokens
	stats.TodayTokens = stats.TodayInputTokens + stats.TodayOutputTokens + stats.TodayCacheCreationTokens + stats.TodayCacheReadTokens
	stats.Rpm, stats.Tpm, _ = r.performanceStats(ctx, "1 = 1")
	return stats, nil
}

func (r *clickHouseUsageLogRepository) fillDashboardEntityStats(ctx context.Context, stats *usagestats.DashboardStats) error {
	if r == nil || r.billingDB == nil {
		return errors.New("dashboard PostgreSQL database is nil")
	}
	today := timezone.Today()
	if err := r.billingDB.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE created_at >= $1)
FROM users WHERE deleted_at IS NULL`, today).Scan(&stats.TotalUsers, &stats.TodayNewUsers); err != nil {
		return err
	}
	if err := r.billingDB.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE status = $1)
FROM api_keys WHERE deleted_at IS NULL`, service.StatusActive).Scan(&stats.TotalAPIKeys, &stats.ActiveAPIKeys); err != nil {
		return err
	}
	now := time.Now()
	return r.billingDB.QueryRowContext(ctx, `SELECT COUNT(*),
    COUNT(*) FILTER (WHERE status = $1 AND schedulable = true),
    COUNT(*) FILTER (WHERE status = $2),
    COUNT(*) FILTER (WHERE rate_limited_at IS NOT NULL AND rate_limit_reset_at > $3),
    COUNT(*) FILTER (WHERE overload_until IS NOT NULL AND overload_until > $3)
FROM accounts WHERE deleted_at IS NULL`, service.StatusActive, service.StatusError, now).Scan(
		&stats.TotalAccounts, &stats.NormalAccounts, &stats.ErrorAccounts,
		&stats.RateLimitAccounts, &stats.OverloadAccounts,
	)
}

func (r *clickHouseUsageLogRepository) GetUserDashboardStats(ctx context.Context, userID int64) (*usagestats.UserDashboardStats, error) {
	stats := &usagestats.UserDashboardStats{}
	if r.billingDB != nil {
		if err := r.billingDB.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(*) FILTER (WHERE status = $2)
FROM api_keys WHERE user_id = $1 AND deleted_at IS NULL`, userID, service.StatusActive).Scan(&stats.TotalAPIKeys, &stats.ActiveAPIKeys); err != nil {
			return nil, err
		}
	}
	if err := r.fillUserDashboardUsage(ctx, stats, "user_id = ?", userID); err != nil {
		return nil, err
	}
	stats.Rpm, stats.Tpm, _ = r.performanceStats(ctx, "user_id = ?", userID)
	return stats, nil
}

func (r *clickHouseUsageLogRepository) GetAPIKeyDashboardStats(ctx context.Context, apiKeyID int64) (*usagestats.UserDashboardStats, error) {
	stats := &usagestats.UserDashboardStats{TotalAPIKeys: 1, ActiveAPIKeys: 1}
	if err := r.fillUserDashboardUsage(ctx, stats, "api_key_id = ?", apiKeyID); err != nil {
		return nil, err
	}
	stats.Rpm, stats.Tpm, _ = r.performanceStats(ctx, "api_key_id = ?", apiKeyID)
	return stats, nil
}

func (r *clickHouseUsageLogRepository) fillUserDashboardUsage(ctx context.Context, stats *usagestats.UserDashboardStats, where string, args ...any) error {
	today := timezone.Today().UTC()
	query := fmt.Sprintf(`SELECT count(), sum(input_tokens), sum(output_tokens), sum(cache_creation_tokens),
    sum(cache_read_tokens), sum(total_cost), sum(actual_cost), if(count() = 0, 0, avg(ifNull(duration_ms, 0))),
    countIf(created_at >= ?), sumIf(input_tokens, created_at >= ?), sumIf(output_tokens, created_at >= ?),
    sumIf(cache_creation_tokens, created_at >= ?), sumIf(cache_read_tokens, created_at >= ?),
    sumIf(total_cost, created_at >= ?), sumIf(actual_cost, created_at >= ?)
FROM usage_logs FINAL WHERE %s`, where)
	// ClickHouse placeholders are positional in textual order, so the today
	// values precede WHERE arguments even though WHERE is logically evaluated first.
	queryArgs := append([]any{today, today, today, today, today, today, today}, args...)
	if err := r.store.db.QueryRowContext(ctx, query, queryArgs...).Scan(
		&stats.TotalRequests, &stats.TotalInputTokens, &stats.TotalOutputTokens,
		&stats.TotalCacheCreationTokens, &stats.TotalCacheReadTokens,
		&stats.TotalCost, &stats.TotalActualCost, &stats.AverageDurationMs,
		&stats.TodayRequests, &stats.TodayInputTokens, &stats.TodayOutputTokens,
		&stats.TodayCacheCreationTokens, &stats.TodayCacheReadTokens,
		&stats.TodayCost, &stats.TodayActualCost,
	); err != nil {
		return err
	}
	stats.TotalTokens = stats.TotalInputTokens + stats.TotalOutputTokens + stats.TotalCacheCreationTokens + stats.TotalCacheReadTokens
	stats.TodayTokens = stats.TodayInputTokens + stats.TodayOutputTokens + stats.TodayCacheCreationTokens + stats.TodayCacheReadTokens
	platformQuery := fmt.Sprintf(`SELECT if(empty(group_platform), account_platform, group_platform) AS platform,
    count(), sum(%s), sum(actual_cost), countIf(created_at >= ?),
    sumIf(%s, created_at >= ?), sumIf(actual_cost, created_at >= ?)
FROM usage_logs FINAL WHERE %s AND actual_cost > 0 AND NOT empty(platform)
GROUP BY platform ORDER BY sum(actual_cost) DESC`, clickHouseTokenExpression, clickHouseTokenExpression, where)
	platformArgs := append([]any{today, today, today}, args...)
	rows, err := r.store.db.QueryContext(ctx, platformQuery, platformArgs...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item usagestats.PlatformDashboardStats
		if err := rows.Scan(&item.Platform, &item.TotalRequests, &item.TotalTokens, &item.TotalActualCost, &item.TodayRequests, &item.TodayTokens, &item.TodayActualCost); err != nil {
			return err
		}
		stats.ByPlatform = append(stats.ByPlatform, item)
	}
	return rows.Err()
}

func (r *clickHouseUsageLogRepository) performanceStats(ctx context.Context, where string, args ...any) (int64, int64, error) {
	start := time.Now().UTC().Add(-5 * time.Minute)
	queryArgs := append([]any{start}, args...)
	query := fmt.Sprintf("SELECT count(), sum(%s) FROM usage_logs FINAL WHERE created_at >= ? AND %s", clickHouseTokenExpression, where)
	var requests, tokens int64
	if err := r.store.db.QueryRowContext(ctx, query, queryArgs...).Scan(&requests, &tokens); err != nil {
		return 0, 0, err
	}
	return requests / 5, tokens / 5, nil
}
