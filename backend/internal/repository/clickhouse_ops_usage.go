package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func clickHouseOpsUsageWhere(filter *service.OpsDashboardFilter, start, end time.Time) (string, []any) {
	conditions := []string{"created_at >= ?", "created_at < ?"}
	args := []any{start.UTC(), end.UTC()}
	if filter != nil {
		if filter.GroupID != nil && *filter.GroupID > 0 {
			conditions = append(conditions, "group_id = ?")
			args = append(args, *filter.GroupID)
		}
		if platform := strings.TrimSpace(strings.ToLower(filter.Platform)); platform != "" {
			conditions = append(conditions, "if(empty(group_platform), account_platform, group_platform) = ?")
			args = append(args, platform)
		}
	}
	return strings.Join(conditions, " AND "), args
}

func (r *opsRepository) queryClickHouseUsageCounts(ctx context.Context, filter *service.OpsDashboardFilter, start, end time.Time) (int64, int64, error) {
	where, args := clickHouseOpsUsageWhere(filter, start, end)
	var requests, tokens int64
	query := fmt.Sprintf("SELECT count(), sum(%s) FROM usage_logs FINAL WHERE %s", clickHouseTokenExpression, where)
	if err := r.usageStore.db.QueryRowContext(ctx, query, args...).Scan(&requests, &tokens); err != nil {
		return 0, 0, err
	}
	return requests, tokens, nil
}

func (r *opsRepository) queryClickHouseUsageLatency(ctx context.Context, filter *service.OpsDashboardFilter, start, end time.Time) (service.OpsPercentiles, service.OpsPercentiles, int64, error) {
	where, args := clickHouseOpsUsageWhere(filter, start, end)
	query := fmt.Sprintf(`SELECT
    quantileExactIf(0.50)(assumeNotNull(duration_ms), NOT isNull(duration_ms)),
    quantileExactIf(0.90)(assumeNotNull(duration_ms), NOT isNull(duration_ms)),
    quantileExactIf(0.95)(assumeNotNull(duration_ms), NOT isNull(duration_ms)),
    quantileExactIf(0.99)(assumeNotNull(duration_ms), NOT isNull(duration_ms)),
    avg(duration_ms), max(duration_ms), count(duration_ms),
    quantileExactIf(0.50)(assumeNotNull(first_token_ms), NOT isNull(first_token_ms)),
    quantileExactIf(0.90)(assumeNotNull(first_token_ms), NOT isNull(first_token_ms)),
    quantileExactIf(0.95)(assumeNotNull(first_token_ms), NOT isNull(first_token_ms)),
    quantileExactIf(0.99)(assumeNotNull(first_token_ms), NOT isNull(first_token_ms)),
    avg(first_token_ms), max(first_token_ms), count(first_token_ms)
FROM usage_logs FINAL WHERE %s`, where)
	var dP50, dP90, dP95, dP99, dAvg sql.NullFloat64
	var dMax sql.NullInt64
	var durationSamples int64
	var tP50, tP90, tP95, tP99, tAvg sql.NullFloat64
	var tMax sql.NullInt64
	var samples int64
	if err := r.usageStore.db.QueryRowContext(ctx, query, args...).Scan(
		&dP50, &dP90, &dP95, &dP99, &dAvg, &dMax, &durationSamples,
		&tP50, &tP90, &tP95, &tP99, &tAvg, &tMax, &samples,
	); err != nil {
		return service.OpsPercentiles{}, service.OpsPercentiles{}, 0, err
	}
	duration := service.OpsPercentiles{}
	if durationSamples > 0 {
		duration.P50 = floatToIntPtr(dP50)
		duration.P90 = floatToIntPtr(dP90)
		duration.P95 = floatToIntPtr(dP95)
		duration.P99 = floatToIntPtr(dP99)
		duration.Avg = floatToIntPtr(dAvg)
		if dMax.Valid {
			value := int(dMax.Int64)
			duration.Max = &value
		}
	}
	ttft := service.OpsPercentiles{}
	if samples > 0 {
		ttft.P50 = floatToIntPtr(tP50)
		ttft.P90 = floatToIntPtr(tP90)
		ttft.P95 = floatToIntPtr(tP95)
		ttft.P99 = floatToIntPtr(tP99)
		ttft.Avg = floatToIntPtr(tAvg)
		if tMax.Valid {
			value := int(tMax.Int64)
			ttft.Max = &value
		}
	}
	return duration, ttft, samples, nil
}

type opsUsageBucket struct {
	requests int64
	tokens   int64
}

func (r *opsRepository) loadClickHouseOpsUsageBuckets(ctx context.Context, filter *service.OpsDashboardFilter, start, end time.Time, bucketSeconds int) (map[int64]opsUsageBucket, error) {
	where, args := clickHouseOpsUsageWhere(filter, start, end)
	bucketExpr := "toStartOfMinute(created_at)"
	switch bucketSeconds {
	case 300:
		bucketExpr = "toStartOfInterval(created_at, INTERVAL 5 MINUTE)"
	case 3600:
		bucketExpr = "toStartOfHour(created_at)"
	}
	query := fmt.Sprintf(`SELECT toUnixTimestamp(%s), count(), sum(%s)
FROM usage_logs FINAL WHERE %s GROUP BY 1 ORDER BY 1`, bucketExpr, clickHouseTokenExpression, where)
	rows, err := r.usageStore.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[int64]opsUsageBucket)
	for rows.Next() {
		var bucket int64
		var item opsUsageBucket
		if err := rows.Scan(&bucket, &item.requests, &item.tokens); err != nil {
			return nil, err
		}
		result[bucket] = item
	}
	return result, rows.Err()
}

func (r *opsRepository) queryClickHousePeakRates(ctx context.Context, filter *service.OpsDashboardFilter, start, end time.Time) (float64, float64, error) {
	usage, err := r.loadClickHouseOpsUsageBuckets(ctx, filter, start, end, 60)
	if err != nil {
		return 0, 0, err
	}
	errorWhere, errorArgs, _ := buildErrorWhere(filter, start, end, 1)
	rows, err := r.db.QueryContext(ctx, `SELECT EXTRACT(EPOCH FROM date_trunc('minute', created_at))::bigint, COUNT(*)
FROM ops_error_logs `+errorWhere+` AND COALESCE(status_code, 0) >= 400 GROUP BY 1`, errorArgs...)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = rows.Close() }()
	errorsByBucket := make(map[int64]int64)
	for rows.Next() {
		var bucket, count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			return 0, 0, err
		}
		errorsByBucket[bucket] = count
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	var maxRequests, maxTokens int64
	for bucket, item := range usage {
		maxRequests = max(maxRequests, item.requests+errorsByBucket[bucket])
		maxTokens = max(maxTokens, item.tokens)
		delete(errorsByBucket, bucket)
	}
	for _, count := range errorsByBucket {
		maxRequests = max(maxRequests, count)
	}
	return roundTo1DP(float64(maxRequests) / 60), roundTo1DP(float64(maxTokens) / 60), nil
}

func (r *opsRepository) clickHouseRawUsageExists(ctx context.Context, filter *service.OpsDashboardFilter, start, end time.Time) (bool, error) {
	where, args := clickHouseOpsUsageWhere(filter, start, end)
	var count uint8
	if err := r.usageStore.db.QueryRowContext(ctx, "SELECT toUInt8(count() > 0) FROM usage_logs FINAL WHERE "+where, args...).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func clickHouseLatencyHistogramLabel(duration int64) (string, int) {
	for index, bucket := range latencyHistogramBuckets {
		if bucket.upperMs <= 0 || duration < int64(bucket.upperMs) {
			return bucket.label, index + 1
		}
	}
	return latencyHistogramBuckets[len(latencyHistogramBuckets)-1].label, len(latencyHistogramBuckets)
}

func (r *opsRepository) getClickHouseLatencyHistogram(ctx context.Context, filter *service.OpsDashboardFilter) (*service.OpsLatencyHistogramResponse, error) {
	start := filter.StartTime.UTC()
	end := filter.EndTime.UTC()
	where, args := clickHouseOpsUsageWhere(filter, start, end)
	query := `SELECT duration_ms, count() FROM usage_logs FINAL WHERE ` + where + ` AND NOT isNull(duration_ms) GROUP BY duration_ms`
	rows, err := r.usageStore.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	counts := make(map[string]int64, len(latencyHistogramOrderedRanges))
	var total int64
	for rows.Next() {
		var duration, count int64
		if err := rows.Scan(&duration, &count); err != nil {
			return nil, err
		}
		label, _ := clickHouseLatencyHistogramLabel(duration)
		counts[label] += count
		total += count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	buckets := make([]*service.OpsLatencyHistogramBucket, 0, len(latencyHistogramOrderedRanges))
	for _, label := range latencyHistogramOrderedRanges {
		buckets = append(buckets, &service.OpsLatencyHistogramBucket{Range: label, Count: counts[label]})
	}
	return &service.OpsLatencyHistogramResponse{
		StartTime: start, EndTime: end, Platform: strings.TrimSpace(filter.Platform),
		GroupID: filter.GroupID, TotalRequests: total, Buckets: buckets,
	}, nil
}

func (r *opsRepository) getClickHouseRealtimeTrafficSummary(ctx context.Context, filter *service.OpsDashboardFilter) (*service.OpsRealtimeTrafficSummary, error) {
	start := filter.StartTime.UTC()
	end := filter.EndTime.UTC()
	window := end.Sub(start)
	usage, err := r.loadClickHouseOpsUsageBuckets(ctx, filter, start, end, 60)
	if err != nil {
		return nil, err
	}
	errorWhere, errorArgs, _ := buildErrorWhere(filter, start, end, 1)
	rows, err := r.db.QueryContext(ctx, `SELECT EXTRACT(EPOCH FROM date_trunc('minute', created_at))::bigint, COUNT(*)
FROM ops_error_logs `+errorWhere+` AND COALESCE(status_code, 0) >= 400 GROUP BY 1`, errorArgs...)
	if err != nil {
		return nil, err
	}
	errorsByBucket := make(map[int64]int64)
	for rows.Next() {
		var bucket, count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			_ = rows.Close()
			return nil, err
		}
		errorsByBucket[bucket] = count
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var successTotal, errorTotal, tokenTotal, peakRequests, peakTokens int64
	for bucket, item := range usage {
		successTotal += item.requests
		tokenTotal += item.tokens
		peakRequests = max(peakRequests, item.requests+errorsByBucket[bucket])
		peakTokens = max(peakTokens, item.tokens)
		delete(errorsByBucket, bucket)
	}
	for _, count := range errorsByBucket {
		errorTotal += count
		peakRequests = max(peakRequests, count)
	}
	// Counts for buckets that also had successful requests were consumed above.
	errorTotalAll, _, _, _, _, _, err := r.queryErrorCounts(ctx, filter, start, end)
	if err != nil {
		return nil, err
	}
	errorTotal = errorTotalAll
	seconds := window.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	qpsCurrent, tpsCurrent, err := r.queryCurrentRates(ctx, filter, end)
	if err != nil {
		return nil, err
	}
	return &service.OpsRealtimeTrafficSummary{
		StartTime: start, EndTime: end, Platform: strings.TrimSpace(filter.Platform), GroupID: filter.GroupID,
		QPS: service.OpsRateSummary{
			Current: qpsCurrent, Peak: roundTo1DP(float64(peakRequests) / 60),
			Avg: roundTo1DP(float64(successTotal+errorTotal) / seconds),
		},
		TPS: service.OpsRateSummary{
			Current: tpsCurrent, Peak: roundTo1DP(float64(peakTokens) / 60),
			Avg: roundTo1DP(float64(tokenTotal) / seconds),
		},
	}, nil
}

func (r *opsRepository) getClickHouseThroughputTrend(ctx context.Context, filter *service.OpsDashboardFilter, bucketSeconds int) (*service.OpsThroughputTrendResponse, error) {
	start := filter.StartTime.UTC()
	end := filter.EndTime.UTC()
	usage, err := r.loadClickHouseOpsUsageBuckets(ctx, filter, start, end, bucketSeconds)
	if err != nil {
		return nil, err
	}
	errorWhere, errorArgs, _ := buildErrorWhere(filter, start, end, 1)
	bucketExpr := opsBucketExprForError(bucketSeconds)
	errorsByBucket, err := loadPostgresOpsBucketCounts(ctx, r.db,
		"SELECT EXTRACT(EPOCH FROM "+bucketExpr+")::bigint, COUNT(*) FROM ops_error_logs "+errorWhere+" AND COALESCE(status_code, 0) >= 400 GROUP BY 1",
		errorArgs,
	)
	if err != nil {
		return nil, err
	}
	switchesByBucket, err := loadPostgresOpsBucketCounts(ctx, r.db,
		`SELECT EXTRACT(EPOCH FROM `+bucketExpr+`)::bigint,
COALESCE(SUM(CASE WHEN split_part(ev->>'kind', ':', 1) IN ('failover', 'retry_exhausted_failover', 'failover_on_400') THEN 1 ELSE 0 END), 0)
FROM ops_error_logs CROSS JOIN LATERAL jsonb_array_elements(COALESCE(NULLIF(upstream_errors, 'null'::jsonb), '[]'::jsonb)) AS ev `+
			errorWhere+` AND upstream_errors IS NOT NULL GROUP BY 1`, errorArgs)
	if err != nil {
		return nil, err
	}
	all := make(map[int64]struct{}, len(usage)+len(errorsByBucket)+len(switchesByBucket))
	for bucket := range usage {
		all[bucket] = struct{}{}
	}
	for bucket := range errorsByBucket {
		all[bucket] = struct{}{}
	}
	for bucket := range switchesByBucket {
		all[bucket] = struct{}{}
	}
	keys := make([]int64, 0, len(all))
	for bucket := range all {
		keys = append(keys, bucket)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	points := make([]*service.OpsThroughputTrendPoint, 0, len(keys))
	for _, bucket := range keys {
		item := usage[bucket]
		requests := item.requests + errorsByBucket[bucket]
		denom := float64(bucketSeconds)
		points = append(points, &service.OpsThroughputTrendPoint{
			BucketStart: time.Unix(bucket, 0).UTC(), RequestCount: requests,
			TokenConsumed: item.tokens, SwitchCount: switchesByBucket[bucket],
			QPS: roundTo1DP(float64(requests) / denom), TPS: roundTo1DP(float64(item.tokens) / denom),
		})
	}
	points = fillOpsThroughputBuckets(start, end, bucketSeconds, points)
	var byPlatform []*service.OpsThroughputPlatformBreakdownItem
	var topGroups []*service.OpsThroughputGroupBreakdownItem
	platform := strings.TrimSpace(strings.ToLower(filter.Platform))
	if platform == "" && (filter.GroupID == nil || *filter.GroupID <= 0) {
		byPlatform, err = r.getClickHouseThroughputBreakdownByPlatform(ctx, start, end)
	} else if platform != "" && (filter.GroupID == nil || *filter.GroupID <= 0) {
		topGroups, err = r.getClickHouseThroughputTopGroups(ctx, start, end, platform, 10)
	}
	if err != nil {
		return nil, err
	}
	return &service.OpsThroughputTrendResponse{
		Bucket: opsBucketLabel(bucketSeconds), Points: points, ByPlatform: byPlatform, TopGroups: topGroups,
	}, nil
}

func loadPostgresOpsBucketCounts(ctx context.Context, db *sql.DB, query string, args []any) (map[int64]int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[int64]int64)
	for rows.Next() {
		var bucket, count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			return nil, err
		}
		result[bucket] = count
	}
	return result, rows.Err()
}

func (r *opsRepository) getClickHouseThroughputBreakdownByPlatform(ctx context.Context, start, end time.Time) ([]*service.OpsThroughputPlatformBreakdownItem, error) {
	rows, err := r.usageStore.db.QueryContext(ctx, fmt.Sprintf(`SELECT
    if(empty(group_platform), account_platform, group_platform) AS platform,
    count(), sum(%s)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ? AND NOT empty(platform)
GROUP BY platform`, clickHouseTokenExpression), start, end)
	if err != nil {
		return nil, err
	}
	type totals struct{ requests, tokens int64 }
	combined := make(map[string]totals)
	for rows.Next() {
		var platform string
		var value totals
		if err := rows.Scan(&platform, &value.requests, &value.tokens); err != nil {
			_ = rows.Close()
			return nil, err
		}
		combined[platform] = value
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	errorRows, err := r.db.QueryContext(ctx, `SELECT platform, COUNT(*) FROM ops_error_logs
WHERE created_at >= $1 AND created_at < $2 AND COALESCE(status_code, 0) >= 400
AND is_count_tokens = FALSE AND platform IS NOT NULL AND platform <> '' GROUP BY platform`, start, end)
	if err != nil {
		return nil, err
	}
	for errorRows.Next() {
		var platform string
		var count int64
		if err := errorRows.Scan(&platform, &count); err != nil {
			_ = errorRows.Close()
			return nil, err
		}
		value := combined[platform]
		value.requests += count
		combined[platform] = value
	}
	if err := errorRows.Close(); err != nil {
		return nil, err
	}
	items := make([]*service.OpsThroughputPlatformBreakdownItem, 0, len(combined))
	for platform, value := range combined {
		items = append(items, &service.OpsThroughputPlatformBreakdownItem{Platform: platform, RequestCount: value.requests, TokenConsumed: value.tokens})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].RequestCount > items[j].RequestCount })
	return items, nil
}

func (r *opsRepository) getClickHouseThroughputTopGroups(ctx context.Context, start, end time.Time, platform string, limit int) ([]*service.OpsThroughputGroupBreakdownItem, error) {
	rows, err := r.usageStore.db.QueryContext(ctx, fmt.Sprintf(`SELECT group_id, any(group_name), count(), sum(%s)
FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ?
AND group_id IS NOT NULL AND if(empty(group_platform), account_platform, group_platform) = ?
GROUP BY group_id`, clickHouseTokenExpression), start, end, platform)
	if err != nil {
		return nil, err
	}
	type totals struct {
		name             string
		requests, tokens int64
	}
	combined := make(map[int64]totals)
	for rows.Next() {
		var groupID int64
		var value totals
		if err := rows.Scan(&groupID, &value.name, &value.requests, &value.tokens); err != nil {
			_ = rows.Close()
			return nil, err
		}
		combined[groupID] = value
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	errorRows, err := r.db.QueryContext(ctx, `SELECT group_id, COUNT(*) FROM ops_error_logs
WHERE created_at >= $1 AND created_at < $2 AND platform = $3 AND group_id IS NOT NULL
AND COALESCE(status_code, 0) >= 400 AND is_count_tokens = FALSE GROUP BY group_id`, start, end, platform)
	if err != nil {
		return nil, err
	}
	for errorRows.Next() {
		var groupID, count int64
		if err := errorRows.Scan(&groupID, &count); err != nil {
			_ = errorRows.Close()
			return nil, err
		}
		value := combined[groupID]
		value.requests += count
		combined[groupID] = value
	}
	if err := errorRows.Close(); err != nil {
		return nil, err
	}
	items := make([]*service.OpsThroughputGroupBreakdownItem, 0, len(combined))
	for groupID, value := range combined {
		items = append(items, &service.OpsThroughputGroupBreakdownItem{GroupID: groupID, GroupName: value.name, RequestCount: value.requests, TokenConsumed: value.tokens})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].RequestCount > items[j].RequestCount })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (r *opsRepository) getClickHouseOpenAITokenStats(ctx context.Context, filter *service.OpsOpenAITokenStatsFilter) (*service.OpsOpenAITokenStatsResponse, error) {
	dashboardFilter := &service.OpsDashboardFilter{
		StartTime: filter.StartTime.UTC(), EndTime: filter.EndTime.UTC(),
		Platform: strings.TrimSpace(strings.ToLower(filter.Platform)), GroupID: filter.GroupID,
	}
	where, args := clickHouseOpsUsageWhere(dashboardFilter, dashboardFilter.StartTime, dashboardFilter.EndTime)
	where += " AND model LIKE 'gpt%'"
	var total int64
	if err := r.usageStore.db.QueryRowContext(ctx,
		"SELECT count() FROM (SELECT model FROM usage_logs FINAL WHERE "+where+" GROUP BY model)", args...).Scan(&total); err != nil {
		return nil, err
	}
	query := `SELECT model, count(),
    round(avgOrNullIf(output_tokens * 1000.0 / duration_ms, duration_ms > 0 AND output_tokens > 0), 2),
    round(avg(first_token_ms), 2), sum(output_tokens),
    toInt64(ifNull(round(avg(duration_ms), 0), 0)), count(first_token_ms)
FROM usage_logs FINAL WHERE ` + where + ` GROUP BY model ORDER BY count() DESC, model ASC`
	queryArgs := append([]any{}, args...)
	if filter.IsTopNMode() {
		query += " LIMIT ?"
		queryArgs = append(queryArgs, filter.TopN)
	} else {
		query += " LIMIT ? OFFSET ?"
		queryArgs = append(queryArgs, filter.PageSize, (filter.Page-1)*filter.PageSize)
	}
	rows, err := r.usageStore.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]*service.OpsOpenAITokenStatsItem, 0)
	for rows.Next() {
		item := &service.OpsOpenAITokenStatsItem{}
		var avgTPS, avgFirstToken sql.NullFloat64
		if err := rows.Scan(&item.Model, &item.RequestCount, &avgTPS, &avgFirstToken,
			&item.TotalOutputTokens, &item.AvgDurationMs, &item.RequestsWithFirstToken); err != nil {
			return nil, err
		}
		if avgTPS.Valid && !math.IsNaN(avgTPS.Float64) {
			value := avgTPS.Float64
			item.AvgTokensPerSec = &value
		}
		if avgFirstToken.Valid && !math.IsNaN(avgFirstToken.Float64) {
			value := avgFirstToken.Float64
			item.AvgFirstTokenMs = &value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	response := &service.OpsOpenAITokenStatsResponse{
		TimeRange: strings.TrimSpace(filter.TimeRange), StartTime: dashboardFilter.StartTime,
		EndTime: dashboardFilter.EndTime, Platform: dashboardFilter.Platform,
		GroupID: dashboardFilter.GroupID, Items: items, Total: total,
	}
	if filter.IsTopNMode() {
		topN := filter.TopN
		response.TopN = &topN
	} else {
		response.Page = filter.Page
		response.PageSize = filter.PageSize
	}
	return response, nil
}
