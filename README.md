# Postgres vs SurrealDB benchmark (Rust)

Rust benchmark suite comparing Postgres and SurrealDB under similar durable settings.

## Stack

- Postgres client SDK: `tokio-postgres`
- SurrealDB client SDK: official `surrealdb` crate
- Runtime: `tokio`

## Project structure

- `src/main.rs` - orchestration and report output
- `src/cli.rs` - CLI options
- `src/seed.rs` - deterministic test data generation
- `src/postgres_bench.rs` - Postgres schema + workloads
- `src/surreal_bench.rs` - SurrealDB schema + workloads
- `src/report.rs` - report schema + metric conversion
- `src/util.rs` - benchmark timing helper

## Start databases

```bash
docker compose up -d
```

Images are configured as:
- `postgres:latest`
- `surrealdb/surrealdb:latest`

## Run benchmark

```bash
cargo run --release -- \
  --rows 5000 \
  --event-rows 10000 \
  --complex-reads 1000 \
  --timeseries-reads 1000 \
  --heavy-workers 32 \
  --heavy-read-ops-per-worker 500 \
  --heavy-write-ops-per-worker 500 \
  --benchmark-trials 5
```

## Workloads covered

### Basic operations
- inserts for all entity tables
- point reads by unique key
- updates by key

### Relationship-heavy operations
- one-to-one: `users` -> `user_profiles`
- one-to-many: `users` -> `orders`
- multi-table query over `users`, `user_profiles`, `orders`, `order_items`, `payments`
- includes left-join behavior (optional payment rows)

### Timeseries operations
- append-style inserts into `events`
- windowed aggregation queries by metric

### Heavy-load concurrency operations
- high-concurrency read test (many workers doing indexed user lookups)
- high-concurrency write test (many workers appending events)
- both tests run for Postgres and SurrealDB with the same worker/operation parameters

## Report metrics

Each operation now includes:
- `total_ms`, `avg_ms`, `ops_per_sec`
- `p50_ms`, `p95_ms`, `stddev_ms`

Percentile and standard deviation metrics are computed from per-trial per-operation latency (`--benchmark-trials`).

## Durability / fairness configuration

### Postgres
Compose command explicitly sets:
- `fsync=on`
- `synchronous_commit=on`
- `full_page_writes=on`
- `wal_level=replica`

At runtime, benchmark reads these from `pg_settings` and includes them in output.

### SurrealDB
Compose starts SurrealDB with:
- RocksDB storage (`rocksdb:/data/benchmark.db`)
- `SURREAL_SYNC_DATA=true`
- `--strict`

Benchmark captures `INFO FOR ROOT` and includes durability notes in output.
