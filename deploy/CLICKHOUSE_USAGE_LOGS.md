# ClickHouse usage logs

Sub2API keeps PostgreSQL as the system of record for users, balances,
subscriptions, billing and `usage_billing_dedup`. Setting `LOG_SQL_DSN` moves
the complete `usage_logs` detail rows to a dedicated ClickHouse database.
There is no long-term PostgreSQL/ClickHouse dual-write.

## Data path

1. A stable usage event is written to Redis Streams before billing.
2. If Redis is unavailable, the event is synchronously appended and `fsync`ed
   to `LOG_WAL_DIR`.
3. Billing commits atomically in PostgreSQL together with
   `usage_billing_dedup`.
4. The consumer verifies the request ID, API key and billing fingerprint, then
   writes a batch to ClickHouse and acknowledges the Redis entry.
5. If neither Redis nor the WAL can persist the event, billing fails closed.

The ClickHouse table uses monthly partitions, stable event IDs,
`ReplacingMergeTree`, and a configurable TTL (90 days by default).

Redis is required as the normal multi-instance queue. The WAL is not a second
database: it is an `fsync`ed local append-only emergency buffer used only when
Redis cannot accept an event. The WAL directory must be on persistent storage
and must not be shared by multiple application containers.

## Automatic startup migration

For the normal cutover, configure the target and enable startup migration in
the same deployment:

```bash
LOG_SQL_DSN=clickhouse://sub2api_logger:password@clickhouse:9000/sub2api_logs?compress=lz4
AUTO_MIGRATE_OLD_LOGS_TO_LOG_DB=true
LOG_MIGRATION_BATCH_SIZE=10000
ALLOW_LOG_MIGRATION_TO_NON_EMPTY_TARGET=false
```

When `OLD_LOG_SQL_DSN` is empty, the current main PostgreSQL database is used
as the legacy `usage_logs` source. Set `OLD_LOG_SQL_DSN` only when the old log
table is in another PostgreSQL database.

The application creates/updates the ClickHouse schema before opening the HTTP
listener. It then captures a PostgreSQL ID high-water mark, copies the current
retention window, validates row counts plus daily, user and model aggregates,
and clears the captured source rows. Rows older than the configured TTL are
counted as expired and cleared without copying. The final cleanup takes an
exclusive table lock and aborts if the high-water mark changed, so a concurrent
legacy writer cannot be silently skipped.

The ClickHouse target must be empty by default. Set
`ALLOW_LOG_MIGRATION_TO_NON_EMPTY_TARGET=true` only after reviewing a partial
migration; stable event IDs and `ReplacingMergeTree` make replay idempotent.

After a successful cutover, set `AUTO_MIGRATE_OLD_LOGS_TO_LOG_DB=false`. New
usage details continue to use Redis Streams/WAL and ClickHouse, while billing
and `usage_billing_dedup` remain in PostgreSQL.

## Manual preflight migration

The standalone command remains available for a non-destructive preflight. It
copies and validates but does not clear PostgreSQL:

```bash
cd backend
go run ./cmd/migrate-usage-logs-clickhouse \
  --postgres-dsn "$DATABASE_URL" \
  --clickhouse-dsn "$LOG_SQL_DSN" \
  --ttl-days 90
```

The ClickHouse database, user and credentials must be created separately. Give
the user permission to create/alter the dedicated `usage_logs` table and insert,
select and delete rows in that database.
