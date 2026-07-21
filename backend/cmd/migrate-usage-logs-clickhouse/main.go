package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	_ "github.com/lib/pq"
)

func main() {
	postgresDSN := flag.String("postgres-dsn", strings.TrimSpace(os.Getenv("DATABASE_URL")), "PostgreSQL DSN (or DATABASE_URL)")
	clickHouseDSN := flag.String("clickhouse-dsn", strings.TrimSpace(os.Getenv("LOG_SQL_DSN")), "ClickHouse DSN (or LOG_SQL_DSN)")
	ttlDays := flag.Int("ttl-days", envInt("LOG_SQL_CLICKHOUSE_TTL_DAYS", 90), "ClickHouse retention and default migration window in days")
	batchSize := flag.Int("batch-size", 1000, "rows per insert batch")
	since := flag.String("since", "", "optional RFC3339 lower bound; defaults to now minus ttl-days")
	flag.Parse()

	if strings.TrimSpace(*postgresDSN) == "" || strings.TrimSpace(*clickHouseDSN) == "" {
		log.Fatal("postgres-dsn/DATABASE_URL and clickhouse-dsn/LOG_SQL_DSN are required")
	}
	var sinceTime time.Time
	var err error
	if strings.TrimSpace(*since) != "" {
		sinceTime, err = time.Parse(time.RFC3339, strings.TrimSpace(*since))
		if err != nil {
			log.Fatalf("invalid --since: %v", err)
		}
	}
	postgres, err := sql.Open("postgres", *postgresDSN)
	if err != nil {
		log.Fatalf("open PostgreSQL: %v", err)
	}
	defer func() { _ = postgres.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	report, err := repository.MigrateUsageLogsToClickHouse(ctx, postgres, repository.UsageLogMigrationOptions{
		ClickHouseDSN: *clickHouseDSN, TTLDays: *ttlDays, BatchSize: *batchSize, Since: sinceTime,
	})
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("encode migration report: %v", err)
	}
	fmt.Println(string(output))
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
