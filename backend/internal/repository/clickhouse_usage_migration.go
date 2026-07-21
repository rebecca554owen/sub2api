package repository

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type UsageLogMigrationOptions struct {
	ClickHouseDSN       string
	TTLDays             int
	BatchSize           int
	Since               time.Time
	AllowNonEmptyTarget bool
}

type UsageLogMigrationReport struct {
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	Since           time.Time `json:"since"`
	MaxSourceID     int64     `json:"max_source_id"`
	SourceRows      int64     `json:"source_rows"`
	ExpiredRows     int64     `json:"expired_rows"`
	DestinationRows int64     `json:"destination_rows"`
	MigratedBatches int64     `json:"migrated_batches"`
	MigratedRows    int64     `json:"migrated_rows"`
	ValidatedDays   int       `json:"validated_days"`
	ValidatedUsers  int       `json:"validated_users"`
	ValidatedModels int       `json:"validated_models"`
}

// MigrateAndClearUsageLogsToClickHouse copies the bounded PostgreSQL window,
// validates row and aggregate parity, then removes only rows covered by the
// captured high-water mark. Rows older than the ClickHouse retention window
// are counted as expired and cleared without copying.
func MigrateAndClearUsageLogsToClickHouse(ctx context.Context, postgres *sql.DB, opts UsageLogMigrationOptions) (*UsageLogMigrationReport, error) {
	report, err := MigrateUsageLogsToClickHouse(ctx, postgres, opts)
	if err != nil {
		return nil, err
	}
	if report.MaxSourceID == 0 {
		return report, nil
	}
	tx, err := postgres.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin PostgreSQL usage log cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "LOCK TABLE usage_logs IN ACCESS EXCLUSIVE MODE"); err != nil {
		return nil, fmt.Errorf("lock PostgreSQL usage logs for cutover: %w", err)
	}
	var currentMaxID int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM usage_logs").Scan(&currentMaxID); err != nil {
		return nil, fmt.Errorf("recheck PostgreSQL usage log high-water mark: %w", err)
	}
	if currentMaxID != report.MaxSourceID {
		return nil, fmt.Errorf("usage log source changed during migration: high-water mark moved from %d to %d", report.MaxSourceID, currentMaxID)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM usage_logs WHERE id <= $1", report.MaxSourceID); err != nil {
		return nil, fmt.Errorf("clear migrated PostgreSQL usage logs: %w", err)
	}
	var remaining int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_logs WHERE id <= $1", report.MaxSourceID).Scan(&remaining); err != nil {
		return nil, fmt.Errorf("verify cleared PostgreSQL usage logs: %w", err)
	}
	if remaining != 0 {
		return nil, fmt.Errorf("usage log cleanup incomplete: %d rows remain", remaining)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL usage log cleanup: %w", err)
	}
	return report, nil
}

type usageMigrationAggregate struct {
	Requests   int64
	Tokens     int64
	TotalCost  float64
	ActualCost float64
}

func MigrateUsageLogsToClickHouse(ctx context.Context, postgres *sql.DB, opts UsageLogMigrationOptions) (*UsageLogMigrationReport, error) {
	if postgres == nil {
		return nil, fmt.Errorf("PostgreSQL database is nil")
	}
	if opts.BatchSize <= 0 || opts.BatchSize > 10000 {
		opts.BatchSize = 1000
	}
	if opts.TTLDays <= 0 {
		opts.TTLDays = 90
	}
	if opts.Since.IsZero() {
		opts.Since = time.Now().UTC().AddDate(0, 0, -opts.TTLDays)
	} else {
		opts.Since = opts.Since.UTC()
	}
	report := &UsageLogMigrationReport{StartedAt: time.Now().UTC(), Since: opts.Since}
	store, err := openClickHouseUsageLogStore(ctx, opts.ClickHouseDSN, opts.TTLDays)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	if err := postgres.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM usage_logs").Scan(&report.MaxSourceID); err != nil {
		return nil, fmt.Errorf("find PostgreSQL usage log high-water mark: %w", err)
	}
	if err := postgres.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_logs WHERE created_at >= $1 AND id <= $2", opts.Since, report.MaxSourceID).Scan(&report.SourceRows); err != nil {
		return nil, fmt.Errorf("count PostgreSQL usage logs: %w", err)
	}
	if err := postgres.QueryRowContext(ctx, "SELECT COUNT(*) FROM usage_logs WHERE created_at < $1 AND id <= $2", opts.Since, report.MaxSourceID).Scan(&report.ExpiredRows); err != nil {
		return nil, fmt.Errorf("count expired PostgreSQL usage logs: %w", err)
	}
	if report.SourceRows == 0 {
		report.FinishedAt = time.Now().UTC()
		return report, nil
	}
	if !opts.AllowNonEmptyTarget {
		var targetRows int64
		if err := store.db.QueryRowContext(ctx, "SELECT count() FROM usage_logs").Scan(&targetRows); err != nil {
			return nil, fmt.Errorf("count existing ClickHouse usage logs: %w", err)
		}
		if targetRows != 0 {
			return nil, fmt.Errorf("usage log migration aborted: ClickHouse target is not empty (%d rows)", targetRows)
		}
	}
	var lastID int64
	for {
		logs, err := loadPostgresUsageLogMigrationBatch(ctx, postgres, opts.Since, lastID, report.MaxSourceID, opts.BatchSize)
		if err != nil {
			return nil, err
		}
		if len(logs) == 0 {
			break
		}
		if err := hydrateUsageLogMigrationSnapshots(ctx, postgres, logs); err != nil {
			return nil, err
		}
		events := make([]usageLogEvent, len(logs))
		for i := range logs {
			events[i] = usageLogEventFromService(&logs[i])
		}
		if err := store.InsertBatch(ctx, events); err != nil {
			return nil, err
		}
		report.MigratedBatches++
		report.MigratedRows += int64(len(logs))
		lastID = logs[len(logs)-1].ID
	}
	if err := store.db.QueryRowContext(ctx, "SELECT count() FROM usage_logs FINAL WHERE created_at >= ? AND id <= ?", opts.Since, report.MaxSourceID).Scan(&report.DestinationRows); err != nil {
		return nil, fmt.Errorf("count ClickHouse usage logs: %w", err)
	}
	if report.SourceRows != report.DestinationRows {
		return nil, fmt.Errorf("usage log row count mismatch: PostgreSQL=%d ClickHouse=%d", report.SourceRows, report.DestinationRows)
	}
	if err := validateUsageLogMigration(ctx, postgres, store.db, opts.Since, report.MaxSourceID, report); err != nil {
		return nil, err
	}
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func loadPostgresUsageLogMigrationBatch(ctx context.Context, db *sql.DB, since time.Time, lastID, maxID int64, limit int) ([]service.UsageLog, error) {
	query := "SELECT " + usageLogSelectColumns + " FROM usage_logs WHERE created_at >= $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4"
	rows, err := db.QueryContext(ctx, query, since, lastID, maxID, limit)
	if err != nil {
		return nil, fmt.Errorf("query PostgreSQL usage log migration batch: %w", err)
	}
	defer func() { _ = rows.Close() }()
	logs := make([]service.UsageLog, 0, limit)
	for rows.Next() {
		log, err := scanUsageLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, *log)
	}
	return logs, rows.Err()
}

func hydrateUsageLogMigrationSnapshots(ctx context.Context, db *sql.DB, logs []service.UsageLog) error {
	ids := collectUsageLogIDs(logs)
	type userSnapshot struct{ email, username string }
	users := make(map[int64]userSnapshot)
	if len(ids.userIDs) > 0 {
		rows, err := db.QueryContext(ctx, "SELECT id, email, username FROM users WHERE id = ANY($1)", pq.Array(ids.userIDs))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var value userSnapshot
			if err := rows.Scan(&id, &value.email, &value.username); err != nil {
				_ = rows.Close()
				return err
			}
			users[id] = value
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	apiKeys, err := loadStringSnapshots(ctx, db, "api_keys", "name", ids.apiKeyIDs)
	if err != nil {
		return err
	}
	accounts, err := loadNamedPlatformSnapshots(ctx, db, "accounts", ids.accountIDs)
	if err != nil {
		return err
	}
	groups, err := loadNamedPlatformSnapshots(ctx, db, "groups", ids.groupIDs)
	if err != nil {
		return err
	}
	for i := range logs {
		logs[i].UserEmail = users[logs[i].UserID].email
		logs[i].Username = users[logs[i].UserID].username
		logs[i].APIKeyName = apiKeys[logs[i].APIKeyID]
		logs[i].AccountName = accounts[logs[i].AccountID].name
		logs[i].AccountPlatform = accounts[logs[i].AccountID].platform
		if logs[i].GroupID != nil {
			logs[i].GroupName = groups[*logs[i].GroupID].name
			logs[i].GroupPlatform = groups[*logs[i].GroupID].platform
		}
	}
	return nil
}

func loadStringSnapshots(ctx context.Context, db *sql.DB, table, column string, ids []int64) (map[int64]string, error) {
	result := make(map[int64]string, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	query := fmt.Sprintf("SELECT id, %s FROM %s WHERE id = ANY($1)", column, table)
	rows, err := db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			return nil, err
		}
		result[id] = value
	}
	return result, rows.Err()
}

func loadNamedPlatformSnapshots(ctx context.Context, db *sql.DB, table string, ids []int64) (map[int64]struct{ name, platform string }, error) {
	result := make(map[int64]struct{ name, platform string }, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	query := fmt.Sprintf("SELECT id, name, platform FROM %s WHERE id = ANY($1)", table)
	rows, err := db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		var name, platform string
		if err := rows.Scan(&id, &name, &platform); err != nil {
			return nil, err
		}
		result[id] = struct{ name, platform string }{name: name, platform: platform}
	}
	return result, rows.Err()
}

func validateUsageLogMigration(ctx context.Context, postgres, clickhouseDB *sql.DB, since time.Time, maxID int64, report *UsageLogMigrationReport) error {
	checks := []struct {
		name       string
		pgQuery    string
		chQuery    string
		reportSize *int
	}{
		{"day",
			"SELECT TO_CHAR(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'), COUNT(*), COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens),0), COALESCE(SUM(total_cost),0), COALESCE(SUM(actual_cost),0) FROM usage_logs WHERE created_at >= $1 AND id <= $2 GROUP BY 1",
			"SELECT formatDateTime(created_at, '%F'), count(), sum(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), sum(total_cost), sum(actual_cost) FROM usage_logs FINAL WHERE created_at >= ? AND id <= ? GROUP BY 1",
			&report.ValidatedDays},
		{"user",
			"SELECT user_id::text, COUNT(*), COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens),0), COALESCE(SUM(total_cost),0), COALESCE(SUM(actual_cost),0) FROM usage_logs WHERE created_at >= $1 AND id <= $2 GROUP BY user_id",
			"SELECT toString(user_id), count(), sum(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), sum(total_cost), sum(actual_cost) FROM usage_logs FINAL WHERE created_at >= ? AND id <= ? GROUP BY user_id",
			&report.ValidatedUsers},
		{"model",
			"SELECT model, COUNT(*), COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens),0), COALESCE(SUM(total_cost),0), COALESCE(SUM(actual_cost),0) FROM usage_logs WHERE created_at >= $1 AND id <= $2 GROUP BY model",
			"SELECT model, count(), sum(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens), sum(total_cost), sum(actual_cost) FROM usage_logs FINAL WHERE created_at >= ? AND id <= ? GROUP BY model",
			&report.ValidatedModels},
	}
	for _, check := range checks {
		pgValues, err := loadMigrationAggregates(ctx, postgres, check.pgQuery, since, maxID)
		if err != nil {
			return fmt.Errorf("load PostgreSQL %s aggregates: %w", check.name, err)
		}
		chValues, err := loadMigrationAggregates(ctx, clickhouseDB, check.chQuery, since, maxID)
		if err != nil {
			return fmt.Errorf("load ClickHouse %s aggregates: %w", check.name, err)
		}
		if err := compareMigrationAggregates(check.name, pgValues, chValues); err != nil {
			return err
		}
		*check.reportSize = len(pgValues)
	}
	return nil
}

func loadMigrationAggregates(ctx context.Context, db *sql.DB, query string, since time.Time, maxID int64) (map[string]usageMigrationAggregate, error) {
	rows, err := db.QueryContext(ctx, query, since, maxID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string]usageMigrationAggregate)
	for rows.Next() {
		var key string
		var value usageMigrationAggregate
		if err := rows.Scan(&key, &value.Requests, &value.Tokens, &value.TotalCost, &value.ActualCost); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, rows.Err()
}

func compareMigrationAggregates(name string, source, destination map[string]usageMigrationAggregate) error {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(source) != len(destination) {
		return fmt.Errorf("%s aggregate cardinality mismatch: PostgreSQL=%d ClickHouse=%d", name, len(source), len(destination))
	}
	for _, key := range keys {
		left := source[key]
		right, exists := destination[key]
		if !exists || left.Requests != right.Requests || left.Tokens != right.Tokens ||
			!migrationFloatEqual(left.TotalCost, right.TotalCost) || !migrationFloatEqual(left.ActualCost, right.ActualCost) {
			return fmt.Errorf("%s aggregate mismatch for %q: PostgreSQL=%+v ClickHouse=%+v", name, key, left, right)
		}
	}
	return nil
}

func migrationFloatEqual(left, right float64) bool {
	delta := math.Abs(left - right)
	scale := math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
	return delta <= math.Max(1e-8, scale*1e-9)
}
