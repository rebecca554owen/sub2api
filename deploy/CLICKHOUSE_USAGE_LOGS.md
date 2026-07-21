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

## Migration before enabling the DSN

Run the migration while Sub2API is still using PostgreSQL usage logs:

```bash
cd backend
go run ./cmd/migrate-usage-logs-clickhouse \
  --postgres-dsn "$DATABASE_URL" \
  --clickhouse-dsn "$LOG_SQL_DSN" \
  --ttl-days 90
```

The command is restartable and validates row counts plus daily, user and model
aggregates for requests, tokens and costs. Each run captures a PostgreSQL ID
high-water mark, so traffic created during a long migration does not invalidate
that run's checks. By default it migrates the current TTL window. Run it once
more immediately before cutover to catch the tail, then enable `LOG_SQL_DSN`
only after validation succeeds.

The ClickHouse database, user and credentials must be created separately. Give
the user permission to create/alter the dedicated `usage_logs` table and insert,
select and delete rows in that database.
