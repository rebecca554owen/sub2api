package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpsUsageCountsUseClickHouseSnapshots(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	groupID := int64(7)
	start := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(), sum(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens) FROM usage_logs FINAL WHERE created_at >= ? AND created_at < ? AND group_id = ? AND if(empty(group_platform), account_platform, group_platform) = ?")).
		WithArgs(start, end, groupID, "openai").
		WillReturnRows(sqlmock.NewRows([]string{"requests", "tokens"}).AddRow(3, 120))
	repo := &opsRepository{usageStore: &clickHouseUsageLogStore{db: db}}
	requests, tokens, err := repo.queryUsageCounts(context.Background(), &service.OpsDashboardFilter{
		Platform: " OpenAI ", GroupID: &groupID,
	}, start, end)
	require.NoError(t, err)
	require.Equal(t, int64(3), requests)
	require.Equal(t, int64(120), tokens)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOpsUsageLatencyKeepsEmptyPercentilesNil(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	start := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	mock.ExpectQuery("quantileExactIf").WithArgs(start, end).WillReturnRows(sqlmock.NewRows([]string{
		"d50", "d90", "d95", "d99", "davg", "dmax", "dcount",
		"t50", "t90", "t95", "t99", "tavg", "tmax", "tcount",
	}).AddRow(0, 0, 0, 0, nil, nil, 0, 0, 0, 0, 0, nil, nil, 0))
	repo := &opsRepository{usageStore: &clickHouseUsageLogStore{db: db}}
	duration, ttft, samples, err := repo.queryUsageLatency(context.Background(), &service.OpsDashboardFilter{}, start, end)
	require.NoError(t, err)
	require.Equal(t, service.OpsPercentiles{}, duration)
	require.Equal(t, service.OpsPercentiles{}, ttft)
	require.Zero(t, samples)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAPIKeyLatestIPUsesClickHouse(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("argMax").WithArgs(int64(2), int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"api_key_id", "ip_address"}).AddRow(2, "203.0.113.2"))
	repo := &apiKeyRepository{sql: db, usageStore: &clickHouseUsageLogStore{db: db}}
	values, err := repo.latestUsageLogIPs(context.Background(), []int64{2, 3})
	require.NoError(t, err)
	require.Equal(t, map[int64]string{2: "203.0.113.2"}, values)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUserLatestUsageUsesClickHouse(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	want := time.Date(2026, 7, 21, 2, 3, 4, 0, time.UTC)
	mock.ExpectQuery("SELECT user_id, max\\(created_at\\)").WithArgs(int64(10), int64(11)).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "created_at"}).AddRow(10, want))
	repo := &userRepository{sql: db, usageStore: &clickHouseUsageLogStore{db: db}}
	values, err := repo.GetLatestUsedAtByUserIDs(context.Background(), []int64{10, 11})
	require.NoError(t, err)
	require.NotNil(t, values[10])
	require.Equal(t, want, *values[10])
	require.Nil(t, values[11])
	require.NoError(t, mock.ExpectationsWereMet())
}
