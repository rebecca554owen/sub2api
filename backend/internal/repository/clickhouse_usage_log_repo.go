package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clickHouseUsageLogRepository struct {
	store     *clickHouseUsageLogStore
	queue     *usageLogDurableQueue
	consumer  *usageLogConsumer
	billingDB *sql.DB
}

var _ service.UsageLogRepository = (*clickHouseUsageLogRepository)(nil)

func (r *clickHouseUsageLogRepository) Create(ctx context.Context, log *service.UsageLog) (bool, error) {
	if log == nil {
		return false, nil
	}
	if r == nil || r.queue == nil {
		return false, errors.New("ClickHouse usage log queue is nil")
	}
	if err := r.queue.Enqueue(ctx, newUsageLogQueueEvent(log, false)); err != nil {
		return false, err
	}
	return true, nil
}

func (r *clickHouseUsageLogRepository) CreateBestEffort(ctx context.Context, log *service.UsageLog) error {
	_, err := r.Create(ctx, log)
	return err
}

func (r *clickHouseUsageLogRepository) PrepareBillingUsageLog(ctx context.Context, log *service.UsageLog, billingFingerprint string) error {
	if log == nil {
		return nil
	}
	if r == nil || r.queue == nil {
		return errors.New("ClickHouse usage log queue is nil")
	}
	if strings.TrimSpace(log.RequestID) == "" {
		return errors.New("billable ClickHouse usage log requires request_id")
	}
	if strings.TrimSpace(billingFingerprint) == "" {
		return errors.New("billable ClickHouse usage log requires billing fingerprint")
	}
	return r.queue.Enqueue(ctx, newUsageLogQueueEvent(log, true, billingFingerprint))
}

func (r *clickHouseUsageLogRepository) Start(ctx context.Context) error {
	if r == nil || r.consumer == nil {
		return nil
	}
	return r.consumer.Start(ctx)
}

func (r *clickHouseUsageLogRepository) Stop() {
	if r == nil {
		return
	}
	if r.consumer != nil {
		r.consumer.Stop()
	}
	if r.queue != nil && r.queue.wal != nil {
		_ = r.queue.wal.Close()
	}
	if r.store != nil {
		_ = r.store.Close()
	}
}

func (r *clickHouseUsageLogRepository) QueueStats() UsageLogQueueStats {
	if r == nil || r.queue == nil {
		return UsageLogQueueStats{}
	}
	return r.queue.Stats()
}

const clickHouseUsageLogSelectColumns = `
    id, toString(event_id), user_id, api_key_id, account_id, request_id,
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
    image_size_breakdown, media_type, video_resolution, created_at`

func (r *clickHouseUsageLogRepository) GetByID(ctx context.Context, id int64) (*service.UsageLog, error) {
	query := "SELECT " + clickHouseUsageLogSelectColumns + " FROM usage_logs FINAL WHERE id = ? ORDER BY ingested_at DESC LIMIT 1"
	rows, err := r.store.db.QueryContext(ctx, query, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrUsageLogNotFound
	}
	return scanClickHouseUsageLog(rows)
}

func (r *clickHouseUsageLogRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.store.db.ExecContext(ctx, "ALTER TABLE usage_logs DELETE WHERE id = ? SETTINGS mutations_sync = 1", id)
	return err
}

func (r *clickHouseUsageLogRepository) ListByUser(ctx context.Context, userID int64, params pagination.PaginationParams) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.list(ctx, "user_id = ?", []any{userID}, params, true)
}

func (r *clickHouseUsageLogRepository) ListByAPIKey(ctx context.Context, apiKeyID int64, params pagination.PaginationParams) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.list(ctx, "api_key_id = ?", []any{apiKeyID}, params, true)
}

func (r *clickHouseUsageLogRepository) ListByAccount(ctx context.Context, accountID int64, params pagination.PaginationParams) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.list(ctx, "account_id = ?", []any{accountID}, params, true)
}

func (r *clickHouseUsageLogRepository) ListByUserAndTimeRange(ctx context.Context, userID int64, startTime, endTime time.Time) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.listTimeRange(ctx, "user_id = ?", []any{userID}, startTime, endTime)
}

func (r *clickHouseUsageLogRepository) ListByAPIKeyAndTimeRange(ctx context.Context, apiKeyID int64, startTime, endTime time.Time) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.listTimeRange(ctx, "api_key_id = ?", []any{apiKeyID}, startTime, endTime)
}

func (r *clickHouseUsageLogRepository) ListByAccountAndTimeRange(ctx context.Context, accountID int64, startTime, endTime time.Time) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.listTimeRange(ctx, "account_id = ?", []any{accountID}, startTime, endTime)
}

func (r *clickHouseUsageLogRepository) ListByModelAndTimeRange(ctx context.Context, modelName string, startTime, endTime time.Time) ([]service.UsageLog, *pagination.PaginationResult, error) {
	return r.listTimeRange(ctx, "model = ?", []any{modelName}, startTime, endTime)
}

func (r *clickHouseUsageLogRepository) ListWithFilters(ctx context.Context, params pagination.PaginationParams, filters usagestats.UsageLogFilters) ([]service.UsageLog, *pagination.PaginationResult, error) {
	where, args := clickHouseUsageFilters(filters)
	return r.list(ctx, where, args, params, filters.ExactTotal)
}

func (r *clickHouseUsageLogRepository) listTimeRange(ctx context.Context, condition string, args []any, startTime, endTime time.Time) ([]service.UsageLog, *pagination.PaginationResult, error) {
	condition += " AND created_at >= ? AND created_at < ?"
	args = append(args, startTime.UTC(), endTime.UTC())
	query := "SELECT " + clickHouseUsageLogSelectColumns + " FROM usage_logs FINAL WHERE " + condition + " ORDER BY created_at DESC, id DESC LIMIT 10000"
	logs, err := r.queryLogs(ctx, query, args...)
	return logs, nil, err
}

func (r *clickHouseUsageLogRepository) list(ctx context.Context, where string, args []any, params pagination.PaginationParams, exactTotal bool) ([]service.UsageLog, *pagination.PaginationResult, error) {
	if strings.TrimSpace(where) == "" {
		where = "1 = 1"
	}
	countWhere := where
	countArgs := append([]any{}, args...)
	limit := params.Limit()
	offset := params.Offset()
	useCursor := strings.TrimSpace(params.Cursor) != "" && strings.ToLower(strings.TrimSpace(params.SortBy)) != "model"
	if useCursor {
		cursor, err := decodeUsageLogCursor(params.Cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid usage log cursor: %w", err)
		}
		operator := "<"
		if params.NormalizedSortOrder(pagination.SortOrderDesc) == pagination.SortOrderAsc {
			operator = ">"
		}
		where += fmt.Sprintf(" AND (created_at %s ? OR (created_at = ? AND id %s ?))", operator, operator)
		args = append(args, cursor.CreatedAt.UTC(), cursor.CreatedAt.UTC(), cursor.ID)
		offset = 0
	}
	queryArgs := append(append([]any{}, args...), limit+1)
	limitClause := "LIMIT ?"
	if !useCursor {
		queryArgs = append(queryArgs, offset)
		limitClause += " OFFSET ?"
	}
	query := fmt.Sprintf(
		"SELECT %s FROM usage_logs FINAL WHERE %s ORDER BY %s %s",
		clickHouseUsageLogSelectColumns, where, clickHouseUsageLogOrder(params), limitClause,
	)
	logs, err := r.queryLogs(ctx, query, queryArgs...)
	if err != nil {
		return nil, nil, err
	}
	hasMore := len(logs) > limit
	if hasMore {
		logs = logs[:limit]
	}
	var total int64
	if exactTotal {
		if err := r.store.db.QueryRowContext(ctx, "SELECT count() FROM usage_logs FINAL WHERE "+countWhere, countArgs...).Scan(&total); err != nil {
			return nil, nil, err
		}
	} else {
		total = int64(offset + len(logs))
		if hasMore {
			total++
		}
	}
	result := clickHousePaginationResult(total, params)
	if hasMore && len(logs) > 0 {
		result.NextCursor = encodeUsageLogCursor(usageLogCursor{CreatedAt: logs[len(logs)-1].CreatedAt, ID: logs[len(logs)-1].ID})
	}
	return logs, result, nil
}

type usageLogCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        int64     `json:"id"`
}

func encodeUsageLogCursor(cursor usageLogCursor) string {
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeUsageLogCursor(value string) (usageLogCursor, error) {
	var cursor usageLogCursor
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return cursor, err
	}
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return cursor, err
	}
	if cursor.CreatedAt.IsZero() || cursor.ID <= 0 {
		return cursor, errors.New("cursor is missing created_at or id")
	}
	return cursor, nil
}

func (r *clickHouseUsageLogRepository) queryLogs(ctx context.Context, query string, args ...any) ([]service.UsageLog, error) {
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	logs := make([]service.UsageLog, 0)
	for rows.Next() {
		log, err := scanClickHouseUsageLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, *log)
	}
	return logs, rows.Err()
}

func clickHouseUsageLogOrder(params pagination.PaginationParams) string {
	order := strings.ToUpper(params.NormalizedSortOrder(pagination.SortOrderDesc))
	switch strings.ToLower(strings.TrimSpace(params.SortBy)) {
	case "model":
		return fmt.Sprintf("if(empty(requested_model), model, requested_model) %s, created_at %s, id %s", order, order, order)
	case "created_at":
		return fmt.Sprintf("created_at %s, id %s", order, order)
	default:
		return fmt.Sprintf("created_at %s, id %s", order, order)
	}
}

func clickHouseUsageFilters(filters usagestats.UsageLogFilters) (string, []any) {
	conditions := make([]string, 0, 10)
	args := make([]any, 0, 10)
	add := func(condition string, value any) {
		conditions = append(conditions, condition)
		args = append(args, value)
	}
	if filters.UserID > 0 {
		add("user_id = ?", filters.UserID)
	}
	if filters.APIKeyID > 0 {
		add("api_key_id = ?", filters.APIKeyID)
	}
	if filters.AccountID > 0 {
		add("account_id = ?", filters.AccountID)
	}
	if filters.GroupID > 0 {
		add("group_id = ?", filters.GroupID)
	}
	if strings.TrimSpace(filters.Model) != "" {
		add(clickHouseModelExpression(filters.ModelFilterSource)+" = ?", strings.TrimSpace(filters.Model))
	}
	if filters.RequestType != nil {
		add("request_type = ?", *filters.RequestType)
	} else if filters.Stream != nil {
		add("stream = ?", *filters.Stream)
	}
	if filters.BillingType != nil {
		add("billing_type = ?", *filters.BillingType)
	}
	if strings.TrimSpace(filters.BillingMode) != "" {
		add("coalesce(billing_mode, if(image_count > 0, 'image', 'token')) = ?", strings.TrimSpace(filters.BillingMode))
	}
	if filters.StartTime != nil {
		add("created_at >= ?", filters.StartTime.UTC())
	}
	if filters.EndTime != nil {
		add("created_at < ?", filters.EndTime.UTC())
	}
	if len(conditions) == 0 {
		return "1 = 1", args
	}
	return strings.Join(conditions, " AND "), args
}

func clickHouseModelExpression(source string) string {
	requested := "if(empty(trim(requested_model)), model, requested_model)"
	switch usagestats.NormalizeModelSource(source) {
	case usagestats.ModelSourceUpstream:
		return "if(isNull(upstream_model) OR empty(trim(upstream_model)), " + requested + ", upstream_model)"
	case usagestats.ModelSourceMapping:
		return "concat(" + requested + ", ' -> ', if(isNull(upstream_model) OR empty(trim(upstream_model)), " + requested + ", upstream_model))"
	default:
		if strings.TrimSpace(source) == "" {
			return "model"
		}
		return requested
	}
}

func clickHousePaginationResult(total int64, params pagination.PaginationParams) *pagination.PaginationResult {
	page := params.Page
	if page < 1 {
		page = 1
	}
	pageSize := params.Limit()
	pages := 0
	if total > 0 {
		pages = int((total + int64(pageSize) - 1) / int64(pageSize))
	}
	return &pagination.PaginationResult{Total: total, Page: page, PageSize: pageSize, Pages: pages}
}

func scanClickHouseUsageLog(scanner interface{ Scan(...any) error }) (*service.UsageLog, error) {
	var event usageLogEvent
	var (
		groupID, subscriptionID, channelID                              sql.NullInt64
		upstreamModel, modelMappingChain, billingTier, billingMode      clickHouseNullString
		serviceTier, reasoningEffort, inboundEndpoint, upstreamEndpoint clickHouseNullString
		videoDuration, duration, firstToken                             sql.NullInt64
		accountRate, accountStats                                       sql.NullFloat64
		userAgent, ipAddress                                            clickHouseNullString
		imageSize, imageInputSize, imageOutputSize, imageSizeSource     clickHouseNullString
		mediaType, videoResolution                                      clickHouseNullString
		breakdown                                                       string
		requestType                                                     int16
	)
	err := scanner.Scan(
		&event.ID, &event.EventID, &event.UserID, &event.APIKeyID, &event.AccountID, &event.RequestID,
		&event.UserEmail, &event.Username, &event.APIKeyName, &event.AccountName, &event.AccountPlatform,
		&groupID, &event.GroupName, &event.GroupPlatform, &subscriptionID,
		&event.Model, &event.RequestedModel, &upstreamModel, &channelID, &modelMappingChain,
		&billingTier, &billingMode, &serviceTier, &reasoningEffort, &inboundEndpoint, &upstreamEndpoint,
		&event.InputTokens, &event.OutputTokens, &event.CacheCreationTokens, &event.CacheReadTokens,
		&event.CacheCreation5mTokens, &event.CacheCreation1hTokens,
		&event.ImageInputTokens, &event.ImageOutputTokens, &event.ImageCount, &event.VideoCount,
		&videoDuration, &duration, &firstToken,
		&event.ImageInputCost, &event.ImageOutputCost, &event.InputCost, &event.OutputCost,
		&event.CacheCreationCost, &event.CacheReadCost, &event.TotalCost, &event.ActualCost,
		&event.RateMultiplier, &accountRate, &accountStats,
		&event.LongContextBillingApplied, &event.BillingType, &requestType, &event.Stream,
		&event.OpenAIWSMode, &userAgent, &ipAddress, &event.CacheTTLOverridden,
		&imageSize, &imageInputSize, &imageOutputSize, &imageSizeSource,
		&breakdown, &mediaType, &videoResolution, &event.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	event.GroupID = scanNullInt64Ptr(groupID)
	event.SubscriptionID = scanNullInt64Ptr(subscriptionID)
	event.ChannelID = scanNullInt64Ptr(channelID)
	event.UpstreamModel = scanNullStringPtr(upstreamModel)
	event.ModelMappingChain = scanNullStringPtr(modelMappingChain)
	event.BillingTier = scanNullStringPtr(billingTier)
	event.BillingMode = scanNullStringPtr(billingMode)
	event.ServiceTier = scanNullStringPtr(serviceTier)
	event.ReasoningEffort = scanNullStringPtr(reasoningEffort)
	event.InboundEndpoint = scanNullStringPtr(inboundEndpoint)
	event.UpstreamEndpoint = scanNullStringPtr(upstreamEndpoint)
	event.VideoDurationSeconds = scanNullIntPtr(videoDuration)
	event.DurationMs = scanNullIntPtr(duration)
	event.FirstTokenMs = scanNullIntPtr(firstToken)
	event.AccountRateMultiplier = nullFloat64Ptr(accountRate)
	event.AccountStatsCost = nullFloat64Ptr(accountStats)
	event.UserAgent = scanNullStringPtr(userAgent)
	event.IPAddress = scanNullStringPtr(ipAddress)
	event.ImageSize = scanNullStringPtr(imageSize)
	event.ImageInputSize = scanNullStringPtr(imageInputSize)
	event.ImageOutputSize = scanNullStringPtr(imageOutputSize)
	event.ImageSizeSource = scanNullStringPtr(imageSizeSource)
	event.MediaType = scanNullStringPtr(mediaType)
	event.VideoResolution = scanNullStringPtr(videoResolution)
	event.RequestType = service.RequestTypeFromInt16(requestType)
	if strings.TrimSpace(breakdown) != "" && breakdown != "null" {
		_ = json.Unmarshal([]byte(breakdown), &event.ImageSizeBreakdown)
	}
	log := event.toService()
	hydrateUsageLogSnapshots(&log)
	return &log, nil
}

func hydrateUsageLogSnapshots(log *service.UsageLog) {
	if log == nil {
		return
	}
	log.User = &service.User{ID: log.UserID, Email: log.UserEmail, Username: log.Username}
	log.APIKey = &service.APIKey{ID: log.APIKeyID, UserID: log.UserID, Name: log.APIKeyName, GroupID: log.GroupID}
	log.Account = &service.Account{ID: log.AccountID, Name: log.AccountName, Platform: log.AccountPlatform}
	if log.GroupID != nil {
		log.Group = &service.Group{ID: *log.GroupID, Name: log.GroupName, Platform: log.GroupPlatform, Hydrated: true}
		log.APIKey.Group = log.Group
	}
	log.APIKey.User = log.User
}

func scanNullInt64Ptr(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func scanNullIntPtr(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	copy := int(value.Int64)
	return &copy
}

// clickHouseNullString accepts the *string values returned by clickhouse-go
// for LowCardinality(Nullable(String)) columns, in addition to normal SQL
// nullable string representations.
type clickHouseNullString struct {
	sql.NullString
}

func (value *clickHouseNullString) Scan(source any) error {
	if pointer, ok := source.(*string); ok {
		if pointer == nil {
			value.String = ""
			value.Valid = false
			return nil
		}
		value.String = *pointer
		value.Valid = true
		return nil
	}
	return value.NullString.Scan(source)
}

func scanNullStringPtr(value clickHouseNullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}
