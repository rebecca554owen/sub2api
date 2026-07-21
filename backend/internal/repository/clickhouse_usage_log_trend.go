package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
)

func (r *clickHouseUsageLogRepository) GetUsageTrendWithFilters(ctx context.Context, startTime, endTime time.Time, granularity string, userID, apiKeyID, accountID, groupID int64, model string, requestType *int16, stream *bool, billingType *int8) ([]usagestats.TrendDataPoint, error) {
	return r.GetUsageTrendWithUsageFilters(ctx, startTime, endTime, granularity, usagestats.UsageLogFilters{
		UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID,
		Model: model, RequestType: requestType, Stream: stream, BillingType: billingType,
	})
}

func (r *clickHouseUsageLogRepository) GetUsageTrendWithUsageFilters(ctx context.Context, startTime, endTime time.Time, granularity string, filters usagestats.UsageLogFilters) ([]usagestats.TrendDataPoint, error) {
	filters.StartTime = &startTime
	filters.EndTime = &endTime
	where, args := clickHouseUsageFilters(filters)
	dateExpr := clickHouseGranularityExpression(granularity)
	query := fmt.Sprintf(`SELECT %s AS date, count(), sum(input_tokens), sum(output_tokens),
    sum(cache_creation_tokens), sum(cache_read_tokens), sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE %s GROUP BY date ORDER BY date`, dateExpr, clickHouseTokenExpression, where)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.TrendDataPoint, 0)
	for rows.Next() {
		var item usagestats.TrendDataPoint
		if err := rows.Scan(&item.Date, &item.Requests, &item.InputTokens, &item.OutputTokens,
			&item.CacheCreationTokens, &item.CacheReadTokens, &item.TotalTokens, &item.Cost, &item.ActualCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetModelStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, requestType *int16, stream *bool, billingType *int8) ([]usagestats.ModelStat, error) {
	return r.GetModelStatsWithUsageFiltersBySource(ctx, startTime, endTime, usagestats.UsageLogFilters{
		UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID,
		RequestType: requestType, Stream: stream, BillingType: billingType,
	}, usagestats.ModelSourceRequested)
}

func (r *clickHouseUsageLogRepository) GetModelStatsWithFiltersBySource(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, requestType *int16, stream *bool, billingType *int8, source string) ([]usagestats.ModelStat, error) {
	return r.GetModelStatsWithUsageFiltersBySource(ctx, startTime, endTime, usagestats.UsageLogFilters{
		UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID,
		RequestType: requestType, Stream: stream, BillingType: billingType,
	}, source)
}

func (r *clickHouseUsageLogRepository) GetModelStatsWithUsageFiltersBySource(ctx context.Context, startTime, endTime time.Time, filters usagestats.UsageLogFilters, source string) ([]usagestats.ModelStat, error) {
	filters.StartTime = &startTime
	filters.EndTime = &endTime
	where, args := clickHouseUsageFilters(filters)
	modelExpr := clickHouseModelExpression(source)
	query := fmt.Sprintf(`SELECT %s AS model_name, count(), sum(input_tokens), sum(output_tokens),
    sum(cache_creation_tokens), sum(cache_read_tokens), sum(%s), sum(total_cost), sum(actual_cost), sum(%s)
FROM usage_logs FINAL WHERE %s GROUP BY model_name ORDER BY sum(actual_cost) DESC`,
		modelExpr, clickHouseTokenExpression, clickHouseAccountCostExpression, where)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.ModelStat, 0)
	for rows.Next() {
		var item usagestats.ModelStat
		if err := rows.Scan(&item.Model, &item.Requests, &item.InputTokens, &item.OutputTokens,
			&item.CacheCreationTokens, &item.CacheReadTokens, &item.TotalTokens,
			&item.Cost, &item.ActualCost, &item.AccountCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetGroupStatsWithFilters(ctx context.Context, startTime, endTime time.Time, userID, apiKeyID, accountID, groupID int64, requestType *int16, stream *bool, billingType *int8) ([]usagestats.GroupStat, error) {
	return r.GetGroupStatsWithUsageFilters(ctx, startTime, endTime, usagestats.UsageLogFilters{
		UserID: userID, APIKeyID: apiKeyID, AccountID: accountID, GroupID: groupID,
		RequestType: requestType, Stream: stream, BillingType: billingType,
	})
}

func (r *clickHouseUsageLogRepository) GetGroupStatsWithUsageFilters(ctx context.Context, startTime, endTime time.Time, filters usagestats.UsageLogFilters) ([]usagestats.GroupStat, error) {
	filters.StartTime = &startTime
	filters.EndTime = &endTime
	where, args := clickHouseUsageFilters(filters)
	query := fmt.Sprintf(`SELECT ifNull(group_id, 0), any(group_name), count(), sum(%s),
    sum(total_cost), sum(actual_cost), sum(%s)
FROM usage_logs FINAL WHERE %s GROUP BY group_id ORDER BY sum(actual_cost) DESC`,
		clickHouseTokenExpression, clickHouseAccountCostExpression, where)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.GroupStat, 0)
	for rows.Next() {
		var item usagestats.GroupStat
		if err := rows.Scan(&item.GroupID, &item.GroupName, &item.Requests, &item.TotalTokens, &item.Cost, &item.ActualCost, &item.AccountCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetUserBreakdownStats(ctx context.Context, startTime, endTime time.Time, dim usagestats.UserBreakdownDimension, limit int) ([]usagestats.UserBreakdownItem, error) {
	filters := usagestats.UsageLogFilters{
		UserID: dim.UserID, APIKeyID: dim.APIKeyID, AccountID: dim.AccountID,
		RequestType: dim.RequestType, Stream: dim.Stream, BillingType: dim.BillingType,
		StartTime: &startTime, EndTime: &endTime,
	}
	if dim.GroupID > 0 {
		filters.GroupID = dim.GroupID
	}
	if strings.TrimSpace(dim.Model) != "" {
		filters.Model = dim.Model
		filters.ModelFilterSource = dim.ModelType
	}
	where, args := clickHouseUsageFilters(filters)
	if strings.TrimSpace(dim.Endpoint) != "" {
		column := "inbound_endpoint"
		if dim.EndpointType == "upstream" {
			column = "upstream_endpoint"
		} else if dim.EndpointType == "path" {
			column = "concat(ifNull(inbound_endpoint, ''), ' -> ', ifNull(upstream_endpoint, ''))"
		}
		where += " AND " + column + " = ?"
		args = append(args, dim.Endpoint)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	order := "actual_cost"
	switch dim.SortBy {
	case "requests", "total_tokens", "cost", "account_cost":
		order = dim.SortBy
	}
	query := fmt.Sprintf(`SELECT user_id, any(user_email), count() AS requests,
    sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,
    sum(cache_creation_tokens + cache_read_tokens) AS cache_tokens,
    sum(%s) AS total_tokens, sum(total_cost) AS cost, sum(actual_cost) AS actual_cost,
    sum(%s) AS account_cost
FROM usage_logs FINAL WHERE %s GROUP BY user_id ORDER BY %s DESC LIMIT ?`,
		clickHouseTokenExpression, clickHouseAccountCostExpression, where, order)
	args = append(args, limit)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.UserBreakdownItem, 0)
	for rows.Next() {
		var item usagestats.UserBreakdownItem
		if err := rows.Scan(&item.UserID, &item.Email, &item.Requests, &item.InputTokens,
			&item.OutputTokens, &item.CacheTokens, &item.TotalTokens, &item.Cost,
			&item.ActualCost, &item.AccountCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetAllGroupUsageSummary(ctx context.Context, todayStart time.Time) ([]usagestats.GroupUsageSummary, error) {
	query := `SELECT ifNull(group_id, 0), sumIf(actual_cost, created_at >= ?), sum(actual_cost)
FROM usage_logs FINAL GROUP BY group_id`
	rows, err := r.store.db.QueryContext(ctx, query, todayStart.UTC())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.GroupUsageSummary, 0)
	for rows.Next() {
		var item usagestats.GroupUsageSummary
		if err := rows.Scan(&item.GroupID, &item.TodayCost, &item.TotalCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetAPIKeyUsageTrend(ctx context.Context, startTime, endTime time.Time, granularity string, limit int) ([]usagestats.APIKeyUsageTrendPoint, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	dateExpr := clickHouseGranularityExpression(granularity)
	query := fmt.Sprintf(`SELECT %s AS date, api_key_id, any(api_key_name), count(), sum(%s)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?
GROUP BY date, api_key_id ORDER BY date, sum(%s) DESC LIMIT ?`, dateExpr, clickHouseTokenExpression, clickHouseTokenExpression)
	rows, err := r.store.db.QueryContext(ctx, query, startTime.UTC(), endTime.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.APIKeyUsageTrendPoint, 0)
	for rows.Next() {
		var item usagestats.APIKeyUsageTrendPoint
		if err := rows.Scan(&item.Date, &item.APIKeyID, &item.KeyName, &item.Requests, &item.Tokens); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetUserUsageTrend(ctx context.Context, startTime, endTime time.Time, granularity string, limit int) ([]usagestats.UserUsageTrendPoint, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	dateExpr := clickHouseGranularityExpression(granularity)
	query := fmt.Sprintf(`SELECT %s AS date, user_id, any(user_email), any(username), count(),
    sum(%s), sum(total_cost), sum(actual_cost)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?
GROUP BY date, user_id ORDER BY date, sum(actual_cost) DESC LIMIT ?`, dateExpr, clickHouseTokenExpression)
	rows, err := r.store.db.QueryContext(ctx, query, startTime.UTC(), endTime.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]usagestats.UserUsageTrendPoint, 0)
	for rows.Next() {
		var item usagestats.UserUsageTrendPoint
		if err := rows.Scan(&item.Date, &item.UserID, &item.Email, &item.Username,
			&item.Requests, &item.Tokens, &item.Cost, &item.ActualCost); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetUserSpendingRanking(ctx context.Context, startTime, endTime time.Time, limit int) (*usagestats.UserSpendingRankingResponse, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	response := &usagestats.UserSpendingRankingResponse{}
	if err := r.store.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT sum(actual_cost), count(), sum(%s)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?`, clickHouseTokenExpression), startTime.UTC(), endTime.UTC()).Scan(
		&response.TotalActualCost, &response.TotalRequests, &response.TotalTokens,
	); err != nil {
		return nil, err
	}
	rows, err := r.store.db.QueryContext(ctx, fmt.Sprintf(`SELECT user_id, any(user_email), sum(actual_cost), count(), sum(%s)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?
GROUP BY user_id ORDER BY sum(actual_cost) DESC LIMIT ?`, clickHouseTokenExpression), startTime.UTC(), endTime.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item usagestats.UserSpendingRankingItem
		if err := rows.Scan(&item.UserID, &item.Email, &item.ActualCost, &item.Requests, &item.Tokens); err != nil {
			return nil, err
		}
		response.Ranking = append(response.Ranking, item)
	}
	return response, rows.Err()
}

func (r *clickHouseUsageLogRepository) GetUserUsageTrendByUserID(ctx context.Context, userID int64, startTime, endTime time.Time, granularity string) ([]usagestats.TrendDataPoint, error) {
	return r.GetUsageTrendWithFilters(ctx, startTime, endTime, granularity, userID, 0, 0, 0, "", nil, nil, nil)
}

func (r *clickHouseUsageLogRepository) GetUserModelStats(ctx context.Context, userID int64, startTime, endTime time.Time) ([]usagestats.ModelStat, error) {
	return r.GetModelStatsWithFilters(ctx, startTime, endTime, userID, 0, 0, 0, nil, nil, nil)
}

func clickHouseGranularityExpression(granularity string) string {
	tz := strings.ReplaceAll(resolveUsageStatsTimezone(), "'", "")
	value := fmt.Sprintf("toTimeZone(created_at, '%s')", tz)
	switch strings.ToLower(strings.TrimSpace(granularity)) {
	case "hour":
		return fmt.Sprintf("formatDateTime(toStartOfHour(%s), '%%F %%H:00')", value)
	case "week":
		return fmt.Sprintf("formatDateTime(toStartOfWeek(%s), '%%G-%%V')", value)
	case "month":
		return fmt.Sprintf("formatDateTime(toStartOfMonth(%s), '%%Y-%%m')", value)
	default:
		return fmt.Sprintf("formatDateTime(toStartOfDay(%s), '%%F')", value)
	}
}
