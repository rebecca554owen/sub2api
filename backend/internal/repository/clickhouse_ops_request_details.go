package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *opsRepository) listClickHouseRequestDetails(ctx context.Context, filter *service.OpsRequestDetailFilter) ([]*service.OpsRequestDetail, int64, error) {
	page, pageSize, start, end := filter.Normalize()
	if filter == nil {
		filter = &service.OpsRequestDetailFilter{}
	}
	offset := (page - 1) * pageSize
	candidateLimit := offset + pageSize
	kind := strings.TrimSpace(strings.ToLower(filter.Kind))
	if kind != "" && kind != "all" && kind != string(service.OpsRequestKindSuccess) && kind != string(service.OpsRequestKindError) {
		return nil, 0, fmt.Errorf("invalid kind")
	}
	sortMode := strings.TrimSpace(strings.ToLower(filter.Sort))
	if sortMode != "" && sortMode != "created_at_desc" && sortMode != "duration_desc" {
		return nil, 0, fmt.Errorf("invalid sort")
	}

	items := make([]*service.OpsRequestDetail, 0, candidateLimit*2)
	var total int64
	if kind == "" || kind == "all" || kind == string(service.OpsRequestKindSuccess) {
		successes, count, err := r.listClickHouseSuccessfulRequestDetails(ctx, filter, start, end, candidateLimit, sortMode)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, successes...)
		total += count
	}
	if kind == "" || kind == "all" || kind == string(service.OpsRequestKindError) {
		errors, count, err := r.listPostgresErrorRequestDetails(ctx, filter, start, end, candidateLimit, sortMode)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, errors...)
		total += count
	}
	if sortMode == "duration_desc" {
		sort.SliceStable(items, func(i, j int) bool {
			left, right := items[i].DurationMs, items[j].DurationMs
			if left == nil || right == nil {
				if left == nil && right == nil {
					return items[i].CreatedAt.After(items[j].CreatedAt)
				}
				return left != nil
			}
			if *left == *right {
				return items[i].CreatedAt.After(items[j].CreatedAt)
			}
			return *left > *right
		})
	} else {
		sort.SliceStable(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	}
	if offset >= len(items) {
		return []*service.OpsRequestDetail{}, total, nil
	}
	items = items[offset:]
	if len(items) > pageSize {
		items = items[:pageSize]
	}
	return items, total, nil
}

func (r *opsRepository) listClickHouseSuccessfulRequestDetails(ctx context.Context, filter *service.OpsRequestDetailFilter, start, end time.Time, limit int, sortMode string) ([]*service.OpsRequestDetail, int64, error) {
	conditions := []string{"created_at >= ?", "created_at < ?"}
	args := []any{start.UTC(), end.UTC()}
	add := func(condition string, value any) {
		conditions = append(conditions, condition)
		args = append(args, value)
	}
	if platform := strings.TrimSpace(strings.ToLower(filter.Platform)); platform != "" {
		add("if(empty(group_platform), account_platform, group_platform) = ?", platform)
	}
	if filter.GroupID != nil && *filter.GroupID > 0 {
		add("group_id = ?", *filter.GroupID)
	}
	if filter.UserID != nil && *filter.UserID > 0 {
		add("user_id = ?", *filter.UserID)
	}
	if filter.APIKeyID != nil && *filter.APIKeyID > 0 {
		add("api_key_id = ?", *filter.APIKeyID)
	}
	if filter.AccountID != nil && *filter.AccountID > 0 {
		add("account_id = ?", *filter.AccountID)
	}
	if model := strings.TrimSpace(filter.Model); model != "" {
		add("model = ?", model)
	}
	if requestID := strings.TrimSpace(filter.RequestID); requestID != "" {
		add("request_id = ?", requestID)
	}
	if query := strings.TrimSpace(filter.Query); query != "" {
		conditions = append(conditions, "(positionCaseInsensitive(request_id, ?) > 0 OR positionCaseInsensitive(model, ?) > 0)")
		args = append(args, query, query)
	}
	if filter.MinDurationMs != nil {
		add("duration_ms >= ?", *filter.MinDurationMs)
	}
	if filter.MaxDurationMs != nil {
		add("duration_ms <= ?", *filter.MaxDurationMs)
	}
	where := strings.Join(conditions, " AND ")
	var total int64
	if err := r.usageStore.db.QueryRowContext(ctx, "SELECT count() FROM usage_logs FINAL WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	order := "created_at DESC"
	if sortMode == "duration_desc" {
		order = "isNull(duration_ms) ASC, duration_ms DESC, created_at DESC"
	}
	queryArgs := append(append([]any{}, args...), limit)
	rows, err := r.usageStore.db.QueryContext(ctx, `SELECT created_at, request_id,
    if(empty(group_platform), account_platform, group_platform), model, duration_ms,
    user_id, api_key_id, account_id, group_id, stream
FROM usage_logs FINAL WHERE `+where+` ORDER BY `+order+` LIMIT ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]*service.OpsRequestDetail, 0, min(limit, 1000))
	for rows.Next() {
		var item service.OpsRequestDetail
		var duration, groupID sql.NullInt64
		var userID, apiKeyID, accountID int64
		if err := rows.Scan(&item.CreatedAt, &item.RequestID, &item.Platform, &item.Model, &duration,
			&userID, &apiKeyID, &accountID, &groupID, &item.Stream); err != nil {
			return nil, 0, err
		}
		item.Kind = service.OpsRequestKindSuccess
		item.DurationMs = opsNullIntToIntPtr(duration)
		item.UserID = &userID
		item.APIKeyID = &apiKeyID
		item.AccountID = &accountID
		item.GroupID = opsNullIntToInt64Ptr(groupID)
		if strings.TrimSpace(item.Platform) == "" {
			item.Platform = "unknown"
		}
		items = append(items, &item)
	}
	return items, total, rows.Err()
}

func (r *opsRepository) listPostgresErrorRequestDetails(ctx context.Context, filter *service.OpsRequestDetailFilter, start, end time.Time, limit int, sortMode string) ([]*service.OpsRequestDetail, int64, error) {
	conditions := []string{"o.created_at >= $1", "o.created_at < $2", "COALESCE(o.status_code, 0) >= 400"}
	args := []any{start.UTC(), end.UTC()}
	add := func(format string, value any) {
		conditions = append(conditions, fmt.Sprintf(format, len(args)+1))
		args = append(args, value)
	}
	if platform := strings.TrimSpace(strings.ToLower(filter.Platform)); platform != "" {
		add("COALESCE(NULLIF(o.platform,''), NULLIF(g.platform,''), NULLIF(a.platform,''), '') = $%d", platform)
	}
	if filter.GroupID != nil && *filter.GroupID > 0 {
		add("o.group_id = $%d", *filter.GroupID)
	}
	if filter.UserID != nil && *filter.UserID > 0 {
		add("o.user_id = $%d", *filter.UserID)
	}
	if filter.APIKeyID != nil && *filter.APIKeyID > 0 {
		add("o.api_key_id = $%d", *filter.APIKeyID)
	}
	if filter.AccountID != nil && *filter.AccountID > 0 {
		add("o.account_id = $%d", *filter.AccountID)
	}
	if model := strings.TrimSpace(filter.Model); model != "" {
		add("o.model = $%d", model)
	}
	if requestID := strings.TrimSpace(filter.RequestID); requestID != "" {
		add("COALESCE(NULLIF(o.request_id,''), NULLIF(o.client_request_id,''), '') = $%d", requestID)
	}
	if query := strings.TrimSpace(filter.Query); query != "" {
		like := "%" + strings.ToLower(query) + "%"
		index := len(args) + 1
		conditions = append(conditions, fmt.Sprintf("(LOWER(COALESCE(o.request_id,'')) LIKE $%d OR LOWER(COALESCE(o.model,'')) LIKE $%d OR LOWER(COALESCE(o.error_message,'')) LIKE $%d)", index, index+1, index+2))
		args = append(args, like, like, like)
	}
	if filter.MinDurationMs != nil {
		add("o.duration_ms >= $%d", *filter.MinDurationMs)
	}
	if filter.MaxDurationMs != nil {
		add("o.duration_ms <= $%d", *filter.MaxDurationMs)
	}
	from := ` FROM ops_error_logs o LEFT JOIN groups g ON g.id = o.group_id LEFT JOIN accounts a ON a.id = o.account_id WHERE ` + strings.Join(conditions, " AND ")
	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*)"+from, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	order := "o.created_at DESC"
	if sortMode == "duration_desc" {
		order = "o.duration_ms DESC NULLS LAST, o.created_at DESC"
	}
	queryArgs := append(append([]any{}, args...), limit)
	rows, err := r.db.QueryContext(ctx, `SELECT o.created_at,
    COALESCE(NULLIF(o.request_id,''), NULLIF(o.client_request_id,''), ''),
    COALESCE(NULLIF(o.platform,''), NULLIF(g.platform,''), NULLIF(a.platform,''), ''),
    COALESCE(o.model,''), o.duration_ms, o.status_code, o.id,
    COALESCE(o.error_phase,''), COALESCE(o.severity,''), COALESCE(o.error_message,''),
    o.user_id, o.api_key_id, o.account_id, o.group_id, o.stream`+from+
		fmt.Sprintf(" ORDER BY %s LIMIT $%d", order, len(args)+1), queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]*service.OpsRequestDetail, 0, min(limit, 1000))
	for rows.Next() {
		item := &service.OpsRequestDetail{Kind: service.OpsRequestKindError}
		var duration, status, errorID, userID, apiKeyID, accountID, groupID sql.NullInt64
		if err := rows.Scan(&item.CreatedAt, &item.RequestID, &item.Platform, &item.Model,
			&duration, &status, &errorID, &item.Phase, &item.Severity, &item.Message,
			&userID, &apiKeyID, &accountID, &groupID, &item.Stream); err != nil {
			return nil, 0, err
		}
		item.DurationMs = opsNullIntToIntPtr(duration)
		item.StatusCode = opsNullIntToIntPtr(status)
		item.ErrorID = opsNullIntToInt64Ptr(errorID)
		item.UserID = opsNullIntToInt64Ptr(userID)
		item.APIKeyID = opsNullIntToInt64Ptr(apiKeyID)
		item.AccountID = opsNullIntToInt64Ptr(accountID)
		item.GroupID = opsNullIntToInt64Ptr(groupID)
		if strings.TrimSpace(item.Platform) == "" {
			item.Platform = "unknown"
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func opsNullIntToIntPtr(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}

func opsNullIntToInt64Ptr(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}
