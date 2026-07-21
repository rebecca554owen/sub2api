package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

const clickHouseAccountCostExpression = "ifNull(account_stats_cost, total_cost) * ifNull(account_rate_multiplier, 1)"
const clickHouseTokenExpression = "input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens"

func (r *clickHouseUsageLogRepository) aggregateUsageStats(ctx context.Context, where string, args ...any) (*usagestats.UsageStats, error) {
	if strings.TrimSpace(where) == "" {
		where = "1 = 1"
	}
	query := fmt.Sprintf(`SELECT
    count(), sum(input_tokens), sum(output_tokens),
    sum(cache_creation_tokens + cache_read_tokens),
    sum(cache_creation_tokens), sum(cache_read_tokens),
    sum(total_cost), sum(actual_cost), sum(%s),
    if(count() = 0, 0, avg(ifNull(duration_ms, 0)))
FROM usage_logs FINAL WHERE %s`, clickHouseAccountCostExpression, where)
	stats := &usagestats.UsageStats{}
	var accountCost float64
	if err := r.store.db.QueryRowContext(ctx, query, args...).Scan(
		&stats.TotalRequests, &stats.TotalInputTokens, &stats.TotalOutputTokens,
		&stats.TotalCacheTokens, &stats.TotalCacheCreationTokens, &stats.TotalCacheReadTokens,
		&stats.TotalCost, &stats.TotalActualCost, &accountCost, &stats.AverageDurationMs,
	); err != nil {
		return nil, err
	}
	stats.TotalTokens = stats.TotalInputTokens + stats.TotalOutputTokens + stats.TotalCacheTokens
	stats.TotalAccountCost = &accountCost
	return stats, nil
}

func (r *clickHouseUsageLogRepository) GetUserStatsAggregated(ctx context.Context, userID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error) {
	return r.aggregateUsageStats(ctx, "user_id = ? AND created_at >= ? AND created_at < ?", userID, startTime.UTC(), endTime.UTC())
}

func (r *clickHouseUsageLogRepository) GetAPIKeyStatsAggregated(ctx context.Context, apiKeyID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error) {
	return r.aggregateUsageStats(ctx, "api_key_id = ? AND created_at >= ? AND created_at < ?", apiKeyID, startTime.UTC(), endTime.UTC())
}

func (r *clickHouseUsageLogRepository) GetAccountStatsAggregated(ctx context.Context, accountID int64, startTime, endTime time.Time) (*usagestats.UsageStats, error) {
	return r.aggregateUsageStats(ctx, "account_id = ? AND created_at >= ? AND created_at < ?", accountID, startTime.UTC(), endTime.UTC())
}

func (r *clickHouseUsageLogRepository) GetModelStatsAggregated(ctx context.Context, modelName string, startTime, endTime time.Time) (*usagestats.UsageStats, error) {
	return r.aggregateUsageStats(ctx, "model = ? AND created_at >= ? AND created_at < ?", modelName, startTime.UTC(), endTime.UTC())
}

func (r *clickHouseUsageLogRepository) GetDailyStatsAggregated(ctx context.Context, userID int64, startTime, endTime time.Time) ([]map[string]any, error) {
	query := fmt.Sprintf(`SELECT
    formatDateTime(toTimeZone(created_at, ?), '%%F') AS date,
    count(), sum(input_tokens), sum(output_tokens),
    sum(cache_creation_tokens + cache_read_tokens), sum(total_cost), sum(actual_cost),
    if(count() = 0, 0, avg(ifNull(duration_ms, 0)))
FROM usage_logs FINAL
WHERE user_id = ? AND created_at >= ? AND created_at < ?
GROUP BY date ORDER BY date`)
	rows, err := r.store.db.QueryContext(ctx, query, resolveUsageStatsTimezone(), userID, startTime.UTC(), endTime.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]map[string]any, 0)
	for rows.Next() {
		var date string
		var requests, input, output, cache int64
		var cost, actualCost, average float64
		if err := rows.Scan(&date, &requests, &input, &output, &cache, &cost, &actualCost, &average); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"date": date, "total_requests": requests, "total_input_tokens": input,
			"total_output_tokens": output, "total_cache_tokens": cache,
			"total_tokens": input + output + cache, "total_cost": cost,
			"total_actual_cost": actualCost, "average_duration_ms": average,
		})
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetAccountTodayStats(ctx context.Context, accountID int64) (*usagestats.AccountStats, error) {
	return r.accountStats(ctx, "account_id = ? AND created_at >= ?", accountID, timezone.Today().UTC())
}

func (r *clickHouseUsageLogRepository) GetAccountWindowStats(ctx context.Context, accountID int64, startTime time.Time) (*usagestats.AccountStats, error) {
	return r.accountStats(ctx, "account_id = ? AND created_at >= ?", accountID, startTime.UTC())
}

func (r *clickHouseUsageLogRepository) accountStats(ctx context.Context, where string, args ...any) (*usagestats.AccountStats, error) {
	query := fmt.Sprintf(`SELECT count(), sum(%s), sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE %s`, clickHouseTokenExpression, clickHouseAccountCostExpression, where)
	stats := &usagestats.AccountStats{}
	if err := r.store.db.QueryRowContext(ctx, query, args...).Scan(
		&stats.Requests, &stats.Tokens, &stats.Cost, &stats.StandardCost, &stats.UserCost,
	); err != nil {
		return nil, err
	}
	return stats, nil
}

func (r *clickHouseUsageLogRepository) GetAccountWindowStatsBatch(ctx context.Context, accountIDs []int64, startTime time.Time) (map[int64]*usagestats.AccountStats, error) {
	result := make(map[int64]*usagestats.AccountStats, len(accountIDs))
	if len(accountIDs) == 0 {
		return result, nil
	}
	placeholders, args := clickHouseInArgs(accountIDs)
	args = append(args, startTime.UTC())
	query := fmt.Sprintf(`SELECT account_id, count(), sum(%s), sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE account_id IN (%s) AND created_at >= ? GROUP BY account_id`,
		clickHouseTokenExpression, clickHouseAccountCostExpression, placeholders)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var accountID int64
		stats := &usagestats.AccountStats{}
		if err := rows.Scan(&accountID, &stats.Requests, &stats.Tokens, &stats.Cost, &stats.StandardCost, &stats.UserCost); err != nil {
			return nil, err
		}
		result[accountID] = stats
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, accountID := range accountIDs {
		if _, exists := result[accountID]; !exists {
			result[accountID] = &usagestats.AccountStats{}
		}
	}
	return result, nil
}

func (r *clickHouseUsageLogRepository) GetGeminiUsageTotalsBatch(ctx context.Context, accountIDs []int64, startTime, endTime time.Time) (map[int64]service.GeminiUsageTotals, error) {
	result := make(map[int64]service.GeminiUsageTotals, len(accountIDs))
	if len(accountIDs) == 0 {
		return result, nil
	}
	placeholders, args := clickHouseInArgs(accountIDs)
	args = append(args, startTime.UTC(), endTime.UTC())
	flash := "positionCaseInsensitive(model, 'flash') > 0 OR positionCaseInsensitive(model, 'lite') > 0"
	query := fmt.Sprintf(`SELECT account_id,
    countIf(%s), countIf(NOT (%s)),
    sumIf(%s, %s), sumIf(%s, NOT (%s)),
    sumIf(actual_cost, %s), sumIf(actual_cost, NOT (%s))
FROM usage_logs FINAL WHERE account_id IN (%s) AND created_at >= ? AND created_at < ? GROUP BY account_id`,
		flash, flash, clickHouseTokenExpression, flash, clickHouseTokenExpression, flash, flash, flash, placeholders)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var accountID int64
		var totals service.GeminiUsageTotals
		if err := rows.Scan(&accountID, &totals.FlashRequests, &totals.ProRequests, &totals.FlashTokens, &totals.ProTokens, &totals.FlashCost, &totals.ProCost); err != nil {
			return nil, err
		}
		result[accountID] = totals
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, accountID := range accountIDs {
		if _, exists := result[accountID]; !exists {
			result[accountID] = service.GeminiUsageTotals{}
		}
	}
	return result, nil
}

func (r *clickHouseUsageLogRepository) GetGlobalStats(ctx context.Context, startTime, endTime time.Time) (*usagestats.UsageStats, error) {
	return r.aggregateUsageStats(ctx, "created_at >= ? AND created_at < ?", startTime.UTC(), endTime.UTC())
}

func (r *clickHouseUsageLogRepository) GetStatsWithFilters(ctx context.Context, filters usagestats.UsageLogFilters) (*usagestats.UsageStats, error) {
	where, args := clickHouseUsageFilters(filters)
	stats, err := r.aggregateUsageStats(ctx, where, args...)
	if err != nil {
		return nil, err
	}
	start := time.Unix(0, 0).UTC()
	if filters.StartTime != nil {
		start = filters.StartTime.UTC()
	}
	end := time.Now().UTC()
	if filters.EndTime != nil {
		end = filters.EndTime.UTC()
	}
	stats.Endpoints, _ = r.endpointStats(ctx, "inbound_endpoint", start, end, filters)
	stats.UpstreamEndpoints, _ = r.endpointStats(ctx, "upstream_endpoint", start, end, filters)
	stats.EndpointPaths, _ = r.endpointPathStats(ctx, start, end, filters)
	return stats, nil
}

func (r *clickHouseUsageLogRepository) GetEndpointStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.EndpointStat, error) {
	return r.endpointStats(ctx, "inbound_endpoint", startTime, endTime, usagestats.UsageLogFilters{UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID, Model: model, RequestType: requestType, Stream: stream, BillingType: billingType})
}

func (r *clickHouseUsageLogRepository) GetUpstreamEndpointStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.EndpointStat, error) {
	return r.endpointStats(ctx, "upstream_endpoint", startTime, endTime, usagestats.UsageLogFilters{UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID, Model: model, RequestType: requestType, Stream: stream, BillingType: billingType})
}

func (r *clickHouseUsageLogRepository) endpointStats(ctx context.Context, column string, startTime, endTime time.Time, filters usagestats.UsageLogFilters) ([]usagestats.EndpointStat, error) {
	if column != "inbound_endpoint" && column != "upstream_endpoint" {
		return nil, fmt.Errorf("unsupported endpoint column %q", column)
	}
	filters.StartTime = &startTime
	filters.EndTime = &endTime
	where, args := clickHouseUsageFilters(filters)
	where += " AND NOT isNull(" + column + ") AND NOT empty(" + column + ")"
	query := fmt.Sprintf(`SELECT %s, count(), sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE %s GROUP BY %s ORDER BY sum(actual_cost) DESC LIMIT 100`, column, clickHouseTokenExpression, where, column)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.EndpointStat, 0)
	for rows.Next() {
		var item usagestats.EndpointStat
		if err := rows.Scan(&item.Endpoint, &item.Requests, &item.TotalTokens, &item.Cost, &item.ActualCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) endpointPathStats(ctx context.Context, startTime, endTime time.Time, filters usagestats.UsageLogFilters) ([]usagestats.EndpointStat, error) {
	filters.StartTime = &startTime
	filters.EndTime = &endTime
	where, args := clickHouseUsageFilters(filters)
	pathExpr := "concat(ifNull(inbound_endpoint, ''), ' -> ', ifNull(upstream_endpoint, ''))"
	where += " AND (NOT isNull(inbound_endpoint) OR NOT isNull(upstream_endpoint))"
	query := fmt.Sprintf(`SELECT %s AS endpoint_path, count(), sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE %s GROUP BY endpoint_path ORDER BY sum(actual_cost) DESC LIMIT 100`, pathExpr, clickHouseTokenExpression, where)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.EndpointStat, 0)
	for rows.Next() {
		var item usagestats.EndpointStat
		if err := rows.Scan(&item.Endpoint, &item.Requests, &item.TotalTokens, &item.Cost, &item.ActualCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetBatchUserUsageStats(ctx context.Context, userIDs []int64, startTime, endTime time.Time) (map[int64]*usagestats.BatchUserUsageStats, error) {
	result := make(map[int64]*usagestats.BatchUserUsageStats, len(userIDs))
	if len(userIDs) == 0 {
		return result, nil
	}
	if startTime.IsZero() {
		startTime = time.Now().AddDate(0, 0, -30)
	}
	if endTime.IsZero() {
		endTime = time.Now()
	}
	today := timezone.Today().UTC()
	placeholders, ids := clickHouseInArgs(userIDs)
	query := fmt.Sprintf(`SELECT user_id, if(empty(group_platform), account_platform, group_platform) AS platform,
    sumIf(actual_cost, created_at >= ? AND created_at < ?), sumIf(actual_cost, created_at >= ?)
FROM usage_logs FINAL WHERE user_id IN (%s) AND created_at >= least(?, ?) AND actual_cost > 0
GROUP BY user_id, platform`, placeholders)
	args := append([]any{startTime.UTC(), endTime.UTC(), today}, ids...)
	args = append(args, startTime.UTC(), today)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var userID int64
		var platform string
		var totalCost, todayCost float64
		if err := rows.Scan(&userID, &platform, &totalCost, &todayCost); err != nil {
			return nil, err
		}
		item := result[userID]
		if item == nil {
			item = &usagestats.BatchUserUsageStats{UserID: userID}
			result[userID] = item
		}
		item.TodayActualCost += todayCost
		item.TotalActualCost += totalCost
		item.ByPlatform = append(item.ByPlatform, usagestats.PlatformUsage{Platform: platform, TodayActualCost: todayCost, TotalActualCost: totalCost})
	}
	for _, userID := range userIDs {
		if result[userID] == nil {
			result[userID] = &usagestats.BatchUserUsageStats{UserID: userID}
		}
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetBatchAPIKeyUsageStats(ctx context.Context, apiKeyIDs []int64, startTime, endTime time.Time) (map[int64]*usagestats.BatchAPIKeyUsageStats, error) {
	result := make(map[int64]*usagestats.BatchAPIKeyUsageStats, len(apiKeyIDs))
	if len(apiKeyIDs) == 0 {
		return result, nil
	}
	if startTime.IsZero() {
		startTime = time.Now().AddDate(0, 0, -30)
	}
	if endTime.IsZero() {
		endTime = time.Now()
	}
	today := timezone.Today().UTC()
	placeholders, ids := clickHouseInArgs(apiKeyIDs)
	args := append([]any{startTime.UTC(), endTime.UTC(), today}, ids...)
	args = append(args, startTime.UTC(), today)
	query := fmt.Sprintf(`SELECT api_key_id,
    sumIf(actual_cost, created_at >= ? AND created_at < ?), sumIf(actual_cost, created_at >= ?)
FROM usage_logs FINAL WHERE api_key_id IN (%s) AND created_at >= least(?, ?)
GROUP BY api_key_id`, placeholders)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		item := &usagestats.BatchAPIKeyUsageStats{}
		if err := rows.Scan(&item.APIKeyID, &item.TotalActualCost, &item.TodayActualCost); err != nil {
			return nil, err
		}
		result[item.APIKeyID] = item
	}
	for _, apiKeyID := range apiKeyIDs {
		if result[apiKeyID] == nil {
			result[apiKeyID] = &usagestats.BatchAPIKeyUsageStats{APIKeyID: apiKeyID}
		}
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetAccountUsageStats(ctx context.Context, accountID int64, startTime, endTime time.Time) (*usagestats.AccountUsageStatsResponse, error) {
	response := &usagestats.AccountUsageStatsResponse{}
	dateExpr := clickHouseGranularityExpression("day")
	query := fmt.Sprintf(`SELECT %s AS date, count(), sum(%s), sum(total_cost),
    sum(%s), sum(actual_cost), if(count() = 0, 0, avg(ifNull(duration_ms, 0)))
FROM usage_logs FINAL
WHERE account_id = ? AND created_at >= ? AND created_at < ?
GROUP BY date ORDER BY date`, dateExpr, clickHouseTokenExpression, clickHouseAccountCostExpression)
	rows, err := r.store.db.QueryContext(ctx, query, accountID, startTime.UTC(), endTime.UTC())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item usagestats.AccountUsageHistory
		if err := rows.Scan(&item.Date, &item.Requests, &item.Tokens, &item.Cost,
			&item.ActualCost, &item.UserCost, &response.Summary.AvgDurationMs); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if parsed, parseErr := time.Parse("2006-01-02", item.Date); parseErr == nil {
			item.Label = parsed.Format("01/02")
		} else {
			item.Label = item.Date
		}
		response.History = append(response.History, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	stats, err := r.aggregateUsageStats(ctx, "account_id = ? AND created_at >= ? AND created_at < ?", accountID, startTime.UTC(), endTime.UTC())
	if err != nil {
		return nil, err
	}
	response.Summary.Days = int(endTime.Sub(startTime).Hours()/24) + 1
	if response.Summary.Days < 1 {
		response.Summary.Days = 30
	}
	response.Summary.ActualDaysUsed = len(response.History)
	actualDays := response.Summary.ActualDaysUsed
	if actualDays == 0 {
		actualDays = 1
	}
	var highestCost, highestRequests *usagestats.AccountUsageHistory
	for i := range response.History {
		item := &response.History[i]
		response.Summary.TotalStandardCost += item.Cost
		response.Summary.TotalCost += item.ActualCost
		response.Summary.TotalUserCost += item.UserCost
		response.Summary.TotalRequests += item.Requests
		response.Summary.TotalTokens += item.Tokens
		if highestCost == nil || item.ActualCost > highestCost.ActualCost {
			highestCost = item
		}
		if highestRequests == nil || item.Requests > highestRequests.Requests {
			highestRequests = item
		}
		if item.Date == timezone.Now().Format("2006-01-02") {
			response.Summary.Today = &struct {
				Date     string  `json:"date"`
				Cost     float64 `json:"cost"`
				UserCost float64 `json:"user_cost"`
				Requests int64   `json:"requests"`
				Tokens   int64   `json:"tokens"`
			}{Date: item.Date, Cost: item.ActualCost, UserCost: item.UserCost, Requests: item.Requests, Tokens: item.Tokens}
		}
	}
	response.Summary.AvgDurationMs = stats.AverageDurationMs
	response.Summary.AvgDailyCost = response.Summary.TotalCost / float64(actualDays)
	response.Summary.AvgDailyUserCost = response.Summary.TotalUserCost / float64(actualDays)
	response.Summary.AvgDailyRequests = float64(response.Summary.TotalRequests) / float64(actualDays)
	response.Summary.AvgDailyTokens = float64(response.Summary.TotalTokens) / float64(actualDays)
	if highestCost != nil {
		response.Summary.HighestCostDay = &struct {
			Date     string  `json:"date"`
			Label    string  `json:"label"`
			Cost     float64 `json:"cost"`
			UserCost float64 `json:"user_cost"`
			Requests int64   `json:"requests"`
		}{Date: highestCost.Date, Label: highestCost.Label, Cost: highestCost.ActualCost, UserCost: highestCost.UserCost, Requests: highestCost.Requests}
	}
	if highestRequests != nil {
		response.Summary.HighestRequestDay = &struct {
			Date     string  `json:"date"`
			Label    string  `json:"label"`
			Requests int64   `json:"requests"`
			Cost     float64 `json:"cost"`
			UserCost float64 `json:"user_cost"`
		}{Date: highestRequests.Date, Label: highestRequests.Label, Requests: highestRequests.Requests, Cost: highestRequests.ActualCost, UserCost: highestRequests.UserCost}
	}
	response.Models, err = r.GetModelStatsWithFilters(ctx, startTime, endTime, 0, 0, accountID, 0, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	response.Endpoints, err = r.GetEndpointStatsWithFilters(ctx, startTime, endTime, 0, 0, accountID, 0, "", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	response.UpstreamEndpoints, err = r.GetUpstreamEndpointStatsWithFilters(ctx, startTime, endTime, 0, 0, accountID, 0, "", nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func clickHouseInArgs(ids []int64) (string, []any) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i := range ids {
		placeholders[i] = "?"
		args[i] = ids[i]
	}
	return strings.Join(placeholders, ","), args
}
