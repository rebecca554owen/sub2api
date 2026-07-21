package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type clickHouseUsageCleanupRepository struct {
	service.UsageCleanupRepository
	store *clickHouseUsageLogStore
}

func ProvideUsageCleanupRepository(
	client *dbent.Client,
	sqlDB *sql.DB,
	bundle *UsageLogRepositoryBundle,
	cfg *config.Config,
) service.UsageCleanupRepository {
	postgres := NewUsageCleanupRepository(client, sqlDB)
	if cfg == nil || !cfg.UsageLogStorage.Enabled() || bundle == nil || bundle.Runtime == nil || bundle.Runtime.external == nil {
		return postgres
	}
	return &clickHouseUsageCleanupRepository{
		UsageCleanupRepository: postgres,
		store:                  bundle.Runtime.external.store,
	}
}

func (r *clickHouseUsageCleanupRepository) DeleteUsageLogsBatch(ctx context.Context, filters service.UsageCleanupFilters, limit int) (int64, error) {
	if filters.StartTime.IsZero() || filters.EndTime.IsZero() {
		return 0, fmt.Errorf("cleanup filters missing time range")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("cleanup batch limit must be positive")
	}
	conditions := []string{"created_at >= ?", "created_at <= ?"}
	args := []any{filters.StartTime.UTC(), filters.EndTime.UTC()}
	add := func(condition string, value any) {
		conditions = append(conditions, condition)
		args = append(args, value)
	}
	if filters.UserID != nil {
		add("user_id = ?", *filters.UserID)
	}
	if filters.APIKeyID != nil {
		add("api_key_id = ?", *filters.APIKeyID)
	}
	if filters.AccountID != nil {
		add("account_id = ?", *filters.AccountID)
	}
	if filters.GroupID != nil {
		add("group_id = ?", *filters.GroupID)
	}
	if filters.Model != nil {
		if model := strings.TrimSpace(*filters.Model); model != "" {
			add("model = ?", model)
		}
	}
	if filters.RequestType != nil {
		add("request_type = ?", *filters.RequestType)
	} else if filters.Stream != nil {
		add("stream = ?", *filters.Stream)
	}
	if filters.BillingType != nil {
		add("billing_type = ?", *filters.BillingType)
	}
	where := strings.Join(conditions, " AND ")
	selectArgs := append(append([]any{}, args...), limit)
	rows, err := r.store.db.QueryContext(ctx,
		"SELECT toString(event_id) FROM usage_logs FINAL WHERE "+where+" ORDER BY created_at, event_id LIMIT ?",
		selectArgs...,
	)
	if err != nil {
		return 0, err
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(ids))
	deleteArgs := make([]any, len(ids))
	for i := range ids {
		placeholders[i] = "toUUID(?)"
		deleteArgs[i] = ids[i]
	}
	query := "ALTER TABLE usage_logs DELETE WHERE event_id IN (" + strings.Join(placeholders, ",") + ") SETTINGS mutations_sync = 1"
	if _, err := r.store.db.ExecContext(ctx, query, deleteArgs...); err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}
