package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestClickHouseUsageLogSchemaUsesMonthlyReplacingTableAndTTL(t *testing.T) {
	t.Parallel()
	schema := clickHouseUsageLogSchema(90)
	require.Contains(t, schema, "ReplacingMergeTree(ingested_at)")
	require.Contains(t, schema, "PARTITION BY toYYYYMM(created_at)")
	require.Contains(t, schema, "event_id UUID")
	require.Contains(t, schema, "TTL created_at + INTERVAL 90 DAY DELETE")
	require.Contains(t, schema, "user_email String")
	require.Contains(t, schema, "group_platform LowCardinality(String)")
}

func TestUsageLogCursorRoundTrip(t *testing.T) {
	t.Parallel()
	want := usageLogCursor{CreatedAt: time.Date(2026, 7, 21, 12, 30, 0, 0, time.UTC), ID: 42}
	encoded := encodeUsageLogCursor(want)
	decoded, err := decodeUsageLogCursor(encoded)
	require.NoError(t, err)
	require.Equal(t, want, decoded)
	_, err = decodeUsageLogCursor("not-base64")
	require.Error(t, err)
}

func TestClickHouseCursorCountUsesOriginalFilter(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &clickHouseUsageLogRepository{store: &clickHouseUsageLogStore{db: db}}
	cursorTime := time.Date(2026, 7, 21, 12, 30, 0, 0, time.UTC)
	cursor := encodeUsageLogCursor(usageLogCursor{CreatedAt: cursorTime, ID: 42})

	mock.ExpectQuery(regexp.QuoteMeta("FROM usage_logs FINAL WHERE user_id = ? AND (created_at < ? OR (created_at = ? AND id < ?))")).
		WithArgs(int64(7), cursorTime, cursorTime, int64(42), 21).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT count() FROM usage_logs FINAL WHERE user_id = ?")).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(50))

	logs, result, err := repo.list(context.Background(), "user_id = ?", []any{int64(7)}, pagination.PaginationParams{
		Page: 1, PageSize: 20, SortBy: "created_at", SortOrder: "desc", Cursor: cursor,
	}, true)
	require.NoError(t, err)
	require.Empty(t, logs)
	require.Equal(t, int64(50), result.Total)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareBillingUsageLogRejectsUnverifiableEvent(t *testing.T) {
	t.Parallel()
	repo := &clickHouseUsageLogRepository{queue: &usageLogDurableQueue{}}
	err := repo.PrepareBillingUsageLog(context.Background(), &service.UsageLog{APIKeyID: 1}, "fingerprint")
	require.ErrorContains(t, err, "request_id")
	err = repo.PrepareBillingUsageLog(context.Background(), &service.UsageLog{RequestID: "request-1", APIKeyID: 1}, "")
	require.ErrorContains(t, err, "billing fingerprint")
}

func TestClickHouseLastWriteDelayHandlesEmptyTable(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(regexp.QuoteMeta("SELECT maxOrNull(created_at) FROM usage_logs FINAL")).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(nil))
	delay, err := (&clickHouseUsageLogStore{db: db}).LastWriteDelay(context.Background())
	require.NoError(t, err)
	require.Zero(t, delay)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMigrationFloatEqualUsesRelativeTolerance(t *testing.T) {
	t.Parallel()
	require.True(t, migrationFloatEqual(1_000_000, 1_000_000.0005))
	require.False(t, migrationFloatEqual(1_000_000, 1_000_000.01))
}
