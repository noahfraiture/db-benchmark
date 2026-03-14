use crate::cli::Cli;
use crate::report::{to_result, OperationResult};
use crate::seed::Seeds;
use crate::util::time_many;
use anyhow::{Context, Result};
use tokio_postgres::NoTls;

pub async fn run(cli: &Cli, seeds: &Seeds) -> Result<(serde_json::Value, Vec<OperationResult>)> {
    let (client, conn) = tokio_postgres::connect(&cli.postgres_dsn, NoTls)
        .await
        .context("failed to connect to postgres")?;
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("postgres connection task error: {err}");
        }
    });

    prepare_schema(&client).await?;
    let durability = verify_durability(&client).await?;
    let results = run_benchmarks(
        &client,
        seeds,
        cli.complex_reads,
        cli.timeseries_reads,
        cli.heavy_workers,
        cli.heavy_read_ops_per_worker,
        cli.heavy_write_ops_per_worker,
        cli.benchmark_trials,
    )
    .await?;
    Ok((durability, results))
}

async fn verify_durability(client: &tokio_postgres::Client) -> Result<serde_json::Value> {
    let rows = client
        .query(
            "SELECT name, setting FROM pg_settings WHERE name IN ('fsync','synchronous_commit','full_page_writes','wal_level') ORDER BY name",
            &[],
        )
        .await
        .context("failed to query postgres durability settings")?;

    let mut map = serde_json::Map::new();
    for r in rows {
        let name: String = r.get(0);
        let value: String = r.get(1);
        map.insert(name, serde_json::Value::String(value));
    }

    Ok(serde_json::Value::Object(map))
}

async fn prepare_schema(client: &tokio_postgres::Client) -> Result<()> {
    client
        .batch_execute(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                country TEXT NOT NULL,
                segment TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id BIGINT PRIMARY KEY REFERENCES users(id),
                bio TEXT NOT NULL,
                timezone TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id),
                total_cents BIGINT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS order_items (
                id BIGSERIAL PRIMARY KEY,
                order_id BIGINT NOT NULL REFERENCES orders(id),
                sku TEXT NOT NULL,
                quantity INT NOT NULL,
                unit_price_cents BIGINT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,
                order_id BIGINT NOT NULL REFERENCES orders(id),
                amount_cents BIGINT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id),
                ts TIMESTAMPTZ NOT NULL,
                metric_name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL
            );

            TRUNCATE TABLE events, payments, order_items, orders, user_profiles, users RESTART IDENTITY;

            CREATE INDEX IF NOT EXISTS idx_users_country ON users(country);
            CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
            CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
            CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
            CREATE INDEX IF NOT EXISTS idx_events_metric_ts ON events(metric_name, ts);
            "#,
        )
        .await
        .context("failed to prepare postgres schema")?;
    Ok(())
}

async fn run_benchmarks(
    client: &tokio_postgres::Client,
    seeds: &Seeds,
    complex_reads: usize,
    timeseries_reads: usize,
    heavy_workers: usize,
    heavy_read_ops_per_worker: usize,
    heavy_write_ops_per_worker: usize,
    benchmark_trials: usize,
) -> Result<Vec<OperationResult>> {
    let mut results = Vec::new();

    let insert_users = time_many(seeds.users.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("INSERT INTO users (email, country, segment) VALUES ($1, $2, $3)")
            .await?;
        for user in &seeds.users {
            client
                .execute(&stmt, &[&user.email, &user.country, &user.segment])
                .await?;
        }
        Ok(())
    })
    .await?;

    let user_ids = client
        .query("SELECT id FROM users ORDER BY id", &[])
        .await?
        .into_iter()
        .map(|r| r.get::<_, i64>(0))
        .collect::<Vec<_>>();

    let insert_profiles = time_many(seeds.profiles.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("INSERT INTO user_profiles (user_id, bio, timezone) VALUES ($1, $2, $3)")
            .await?;
        for profile in &seeds.profiles {
            let uid = user_ids[profile.user_idx];
            client
                .execute(&stmt, &[&uid, &profile.bio, &profile.timezone])
                .await?;
        }
        Ok(())
    })
    .await?;

    let insert_orders = time_many(seeds.orders.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("INSERT INTO orders (user_id, total_cents, status) VALUES ($1, $2, $3)")
            .await?;
        for order in &seeds.orders {
            let uid = user_ids[order.user_idx];
            client
                .execute(&stmt, &[&uid, &order.total_cents, &order.status])
                .await?;
        }
        Ok(())
    })
    .await?;

    let order_ids = client
        .query("SELECT id FROM orders ORDER BY id", &[])
        .await?
        .into_iter()
        .map(|r| r.get::<_, i64>(0))
        .collect::<Vec<_>>();

    let insert_order_items = time_many(seeds.order_items.len(), benchmark_trials, || async {
        let stmt = client
            .prepare(
                "INSERT INTO order_items (order_id, sku, quantity, unit_price_cents) VALUES ($1, $2, $3, $4)",
            )
            .await?;
        for item in &seeds.order_items {
            let oid = order_ids[item.order_idx];
            client
                .execute(
                    &stmt,
                    &[&oid, &item.sku, &item.quantity, &item.unit_price_cents],
                )
                .await?;
        }
        Ok(())
    })
    .await?;

    let insert_payments = time_many(seeds.payments.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, $3)")
            .await?;
        for p in &seeds.payments {
            let oid = order_ids[p.order_idx];
            client
                .execute(&stmt, &[&oid, &p.amount_cents, &p.status])
                .await?;
        }
        Ok(())
    })
    .await?;

    let insert_events = time_many(seeds.events.len(), benchmark_trials, || async {
        let stmt = client
            .prepare(
                "INSERT INTO events (user_id, ts, metric_name, value) VALUES ($1, to_timestamp($2), $3, $4)",
            )
            .await?;
        for e in &seeds.events {
            let uid = user_ids[e.user_idx];
            client
                .execute(&stmt, &[&uid, &e.ts_unix_secs, &e.metric_name, &e.value])
                .await?;
        }
        Ok(())
    })
    .await?;

    let point_reads = time_many(seeds.users.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("SELECT id, email, country, segment FROM users WHERE email = $1")
            .await?;
        for user in &seeds.users {
            let _ = client.query_one(&stmt, &[&user.email]).await?;
        }
        Ok(())
    })
    .await?;

    let updates = time_many(seeds.users.len(), benchmark_trials, || async {
        let stmt = client
            .prepare("UPDATE users SET segment = $1 WHERE email = $2")
            .await?;
        for user in &seeds.users {
            client.execute(&stmt, &[&"pro", &user.email]).await?;
        }
        Ok(())
    })
    .await?;

    let relationship_complex = time_many(complex_reads, benchmark_trials, || async {
        let stmt = client
            .prepare(
                r#"
                SELECT
                    u.id,
                    u.email,
                    up.timezone,
                    COUNT(DISTINCT o.id) AS order_count,
                    COALESCE(SUM(oi.quantity * oi.unit_price_cents), 0) AS item_total,
                    COALESCE(SUM(p.amount_cents), 0) AS paid_total
                FROM users u
                INNER JOIN user_profiles up ON up.user_id = u.id
                LEFT JOIN orders o ON o.user_id = u.id
                LEFT JOIN order_items oi ON oi.order_id = o.id
                LEFT JOIN payments p ON p.order_id = o.id AND p.status = 'captured'
                WHERE u.country = $1
                GROUP BY u.id, u.email, up.timezone
                ORDER BY paid_total DESC
                LIMIT 50
                "#,
            )
            .await?;

        for _ in 0..complex_reads {
            let _ = client.query(&stmt, &[&"US"]).await?;
        }
        Ok(())
    })
    .await?;

    let timeseries_window = time_many(timeseries_reads, benchmark_trials, || async {
        let stmt = client
            .prepare(
                r#"
                SELECT
                    date_trunc('hour', ts) AS bucket,
                    metric_name,
                    COUNT(*) AS points,
                    AVG(value) AS avg_value
                FROM events
                WHERE ts >= to_timestamp($1) AND ts < to_timestamp($2)
                GROUP BY bucket, metric_name
                ORDER BY bucket DESC, metric_name
                LIMIT 100
                "#,
            )
            .await?;

        let from = seeds.events.first().map(|e| e.ts_unix_secs).unwrap_or(0);
        let to = seeds.events.last().map(|e| e.ts_unix_secs + 1).unwrap_or(1);

        for _ in 0..timeseries_reads {
            let _ = client.query(&stmt, &[&from, &to]).await?;
        }
        Ok(())
    })
    .await?;

    let heavy_concurrent_reads = time_many(
        heavy_workers * heavy_read_ops_per_worker,
        benchmark_trials,
        || async {
            let mut join_set = tokio::task::JoinSet::new();
            for worker_id in 0..heavy_workers {
                let client = client.clone();
                let rows = seeds.users.len().max(1);
                join_set.spawn(async move {
                    let stmt = client
                        .prepare("SELECT id, email, country, segment FROM users WHERE email = $1")
                        .await?;
                    for i in 0..heavy_read_ops_per_worker {
                        let idx = (worker_id * heavy_read_ops_per_worker + i) % rows;
                        let email = format!("user{idx}@example.com");
                        let _ = client.query_one(&stmt, &[&email]).await?;
                    }
                    Ok::<(), tokio_postgres::Error>(())
                });
            }

            while let Some(joined) = join_set.join_next().await {
                joined.context("postgres heavy read worker join failure")??;
            }

            Ok(())
        },
    )
    .await?;

    let heavy_concurrent_writes = time_many(heavy_workers * heavy_write_ops_per_worker, benchmark_trials, || async {
        let mut join_set = tokio::task::JoinSet::new();
        for worker_id in 0..heavy_workers {
            let client = client.clone();
            let rows = seeds.users.len().max(1);
            let base_ts = 1_800_000_000_i64 + (worker_id as i64 * 10_000);
            join_set.spawn(async move {
                let stmt = client
                    .prepare(
                        "INSERT INTO events (user_id, ts, metric_name, value) VALUES ($1, to_timestamp($2), $3, $4)",
                    )
                    .await?;

                for i in 0..heavy_write_ops_per_worker {
                    let idx = (worker_id * heavy_write_ops_per_worker + i) % rows;
                    let uid = (idx + 1) as i64;
                    let ts = base_ts + i as i64;
                    let metric = "heavy_load";
                    let value = ((worker_id + i) % 100) as f64;
                    client.execute(&stmt, &[&uid, &ts, &metric, &value]).await?;
                }
                Ok::<(), tokio_postgres::Error>(())
            });
        }

        while let Some(joined) = join_set.join_next().await {
            joined.context("postgres heavy write worker join failure")??;
        }

        Ok(())
    })
    .await?;

    results.push(to_result("postgres", "insert_users", insert_users));
    results.push(to_result(
        "postgres",
        "insert_profiles_one_to_one",
        insert_profiles,
    ));
    results.push(to_result(
        "postgres",
        "insert_orders_one_to_many",
        insert_orders,
    ));
    results.push(to_result(
        "postgres",
        "insert_order_items",
        insert_order_items,
    ));
    results.push(to_result(
        "postgres",
        "insert_payments_optional",
        insert_payments,
    ));
    results.push(to_result(
        "postgres",
        "insert_events_timeseries",
        insert_events,
    ));
    results.push(to_result("postgres", "point_reads", point_reads));
    results.push(to_result("postgres", "updates", updates));
    results.push(to_result(
        "postgres",
        "complex_relational_join_inner_left",
        relationship_complex,
    ));
    results.push(to_result(
        "postgres",
        "timeseries_aggregate_window",
        timeseries_window,
    ));
    results.push(to_result(
        "postgres",
        "heavy_concurrent_reads",
        heavy_concurrent_reads,
    ));
    results.push(to_result(
        "postgres",
        "heavy_concurrent_writes",
        heavy_concurrent_writes,
    ));

    Ok(results)
}
