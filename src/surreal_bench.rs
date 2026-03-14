use crate::cli::Cli;
use crate::report::{to_result, OperationResult};
use crate::seed::Seeds;
use crate::util::time_many;
use anyhow::{anyhow, Context, Result};
use serde_json::json;
use surrealdb::engine::remote::ws::{Client as SurrealClient, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::sql::Thing;
use surrealdb::Surreal;

pub async fn run(cli: &Cli, seeds: &Seeds) -> Result<(serde_json::Value, Vec<OperationResult>)> {
    let surreal = Surreal::new::<Ws>(&cli.surreal_addr)
        .await
        .with_context(|| format!("failed to connect to surrealdb at {}", cli.surreal_addr))?;

    surreal
        .signin(Root {
            username: &cli.surreal_user,
            password: &cli.surreal_pass,
        })
        .await
        .context("failed to authenticate to surrealdb")?;

    surreal
        .use_ns(&cli.surreal_ns)
        .use_db(&cli.surreal_db)
        .await
        .context("failed to select namespace/database in surrealdb")?;

    prepare_schema(&surreal).await?;
    let durability = verify_durability(&surreal).await?;
    let results = run_benchmarks(
        &surreal,
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

async fn verify_durability(surreal: &Surreal<SurrealClient>) -> Result<serde_json::Value> {
    let mut out = serde_json::Map::new();
    let info = surreal
        .query("INFO FOR ROOT")
        .await
        .context("failed to query surrealdb root info")?;

    out.insert("info_for_root".to_string(), serde_json::to_value(info)?);
    out.insert(
        "durability_note".to_string(),
        serde_json::Value::String(
            "Durability is controlled at startup via RocksDB storage + SURREAL_SYNC_DATA=true in docker-compose.".to_string(),
        ),
    );
    Ok(serde_json::Value::Object(out))
}

async fn prepare_schema(surreal: &Surreal<SurrealClient>) -> Result<()> {
    surreal
        .query(
            r#"
            REMOVE TABLE events;
            REMOVE TABLE payments;
            REMOVE TABLE order_items;
            REMOVE TABLE orders;
            REMOVE TABLE user_profiles;
            REMOVE TABLE users;

            DEFINE TABLE users SCHEMAFULL;
            DEFINE FIELD email ON TABLE users TYPE string;
            DEFINE FIELD country ON TABLE users TYPE string;
            DEFINE FIELD segment ON TABLE users TYPE string;
            DEFINE INDEX users_email_idx ON TABLE users COLUMNS email UNIQUE;
            DEFINE INDEX users_country_idx ON TABLE users COLUMNS country;

            DEFINE TABLE user_profiles SCHEMAFULL;
            DEFINE FIELD user ON TABLE user_profiles TYPE record<users>;
            DEFINE FIELD bio ON TABLE user_profiles TYPE string;
            DEFINE FIELD timezone ON TABLE user_profiles TYPE string;
            DEFINE INDEX profile_user_unique ON TABLE user_profiles COLUMNS user UNIQUE;

            DEFINE TABLE orders SCHEMAFULL;
            DEFINE FIELD user ON TABLE orders TYPE record<users>;
            DEFINE FIELD total_cents ON TABLE orders TYPE int;
            DEFINE FIELD status ON TABLE orders TYPE string;
            DEFINE INDEX orders_user_idx ON TABLE orders COLUMNS user;

            DEFINE TABLE order_items SCHEMAFULL;
            DEFINE FIELD order ON TABLE order_items TYPE record<orders>;
            DEFINE FIELD sku ON TABLE order_items TYPE string;
            DEFINE FIELD quantity ON TABLE order_items TYPE int;
            DEFINE FIELD unit_price_cents ON TABLE order_items TYPE int;
            DEFINE INDEX order_items_order_idx ON TABLE order_items COLUMNS order;

            DEFINE TABLE payments SCHEMAFULL;
            DEFINE FIELD order ON TABLE payments TYPE record<orders>;
            DEFINE FIELD amount_cents ON TABLE payments TYPE int;
            DEFINE FIELD status ON TABLE payments TYPE string;
            DEFINE INDEX payments_order_idx ON TABLE payments COLUMNS order;

            DEFINE TABLE events SCHEMAFULL;
            DEFINE FIELD user ON TABLE events TYPE record<users>;
            DEFINE FIELD ts_unix_secs ON TABLE events TYPE int;
            DEFINE FIELD metric_name ON TABLE events TYPE string;
            DEFINE FIELD value ON TABLE events TYPE number;
            DEFINE INDEX events_ts_idx ON TABLE events COLUMNS ts_unix_secs;
            "#,
        )
        .await
        .context("failed to prepare surrealdb schema")?;
    Ok(())
}

async fn run_benchmarks(
    surreal: &Surreal<SurrealClient>,
    seeds: &Seeds,
    complex_reads: usize,
    timeseries_reads: usize,
    heavy_workers: usize,
    heavy_read_ops_per_worker: usize,
    heavy_write_ops_per_worker: usize,
    benchmark_trials: usize,
) -> Result<Vec<OperationResult>> {
    let mut results = Vec::new();

    let mut user_ids: Vec<Thing> = Vec::with_capacity(seeds.users.len());
    let insert_users = time_many(seeds.users.len(), benchmark_trials, || async {
        for user in &seeds.users {
            let created: Option<Thing> = surreal
                .create("users")
                .content(json!({
                    "email": user.email,
                    "country": user.country,
                    "segment": user.segment
                }))
                .await?;
            let id = created.ok_or_else(|| anyhow!("surreal user insert returned empty"))?;
            user_ids.push(id);
        }
        Ok(())
    })
    .await?;

    let insert_profiles = time_many(seeds.profiles.len(), benchmark_trials, || async {
        for profile in &seeds.profiles {
            let _created: Option<Thing> = surreal
                .create("user_profiles")
                .content(json!({
                    "user": user_ids[profile.user_idx].clone(),
                    "bio": profile.bio,
                    "timezone": profile.timezone
                }))
                .await?;
        }
        Ok(())
    })
    .await?;

    let mut order_ids: Vec<Thing> = Vec::with_capacity(seeds.orders.len());
    let insert_orders = time_many(seeds.orders.len(), benchmark_trials, || async {
        for order in &seeds.orders {
            let created: Option<Thing> = surreal
                .create("orders")
                .content(json!({
                    "user": user_ids[order.user_idx].clone(),
                    "total_cents": order.total_cents,
                    "status": order.status
                }))
                .await?;
            let id = created.ok_or_else(|| anyhow!("surreal order insert returned empty"))?;
            order_ids.push(id);
        }
        Ok(())
    })
    .await?;

    let insert_order_items = time_many(seeds.order_items.len(), benchmark_trials, || async {
        for item in &seeds.order_items {
            let _created: Option<Thing> = surreal
                .create("order_items")
                .content(json!({
                    "order": order_ids[item.order_idx].clone(),
                    "sku": item.sku,
                    "quantity": item.quantity,
                    "unit_price_cents": item.unit_price_cents
                }))
                .await?;
        }
        Ok(())
    })
    .await?;

    let insert_payments = time_many(seeds.payments.len(), benchmark_trials, || async {
        for p in &seeds.payments {
            let _created: Option<Thing> = surreal
                .create("payments")
                .content(json!({
                    "order": order_ids[p.order_idx].clone(),
                    "amount_cents": p.amount_cents,
                    "status": p.status
                }))
                .await?;
        }
        Ok(())
    })
    .await?;

    let insert_events = time_many(seeds.events.len(), benchmark_trials, || async {
        for e in &seeds.events {
            let _created: Option<Thing> = surreal
                .create("events")
                .content(json!({
                    "user": user_ids[e.user_idx].clone(),
                    "ts_unix_secs": e.ts_unix_secs,
                    "metric_name": e.metric_name,
                    "value": e.value
                }))
                .await?;
        }
        Ok(())
    })
    .await?;

    let point_reads = time_many(seeds.users.len(), benchmark_trials, || async {
        for user in &seeds.users {
            let mut response = surreal
                .query("SELECT * FROM users WHERE email = $email LIMIT 1")
                .bind(("email", user.email.clone()))
                .await?;
            let rows: Vec<serde_json::Value> = response.take(0)?;
            if rows.is_empty() {
                return Err(anyhow!("missing surreal user"));
            }
        }
        Ok(())
    })
    .await?;

    let updates = time_many(seeds.users.len(), benchmark_trials, || async {
        for user in &seeds.users {
            let _ = surreal
                .query("UPDATE users SET segment = 'pro' WHERE email = $email")
                .bind(("email", user.email.clone()))
                .await?;
        }
        Ok(())
    })
    .await?;

    let complex_relation_query = time_many(complex_reads, benchmark_trials, || async {
        for _ in 0..complex_reads {
            let mut response = surreal
                .query(
                    r#"
                    SELECT
                        id,
                        email,
                        (SELECT VALUE timezone FROM user_profiles WHERE user = users.id LIMIT 1)[0] AS timezone,
                        (SELECT count() FROM orders WHERE user = users.id GROUP ALL)[0].count AS order_count,
                        (SELECT math::sum(amount_cents) FROM payments WHERE order.user = users.id AND status = 'captured' GROUP ALL)[0].sum AS paid_total,
                        (SELECT math::sum(quantity * unit_price_cents) FROM order_items WHERE order.user = users.id GROUP ALL)[0].sum AS item_total
                    FROM users
                    WHERE country = $country
                    ORDER BY paid_total DESC
                    LIMIT 50
                    "#,
                )
                .bind(("country", "US".to_string()))
                .await?;
            let _rows: Vec<serde_json::Value> = response.take(0)?;
        }
        Ok(())
    })
    .await?;

    let timeseries_window = time_many(timeseries_reads, benchmark_trials, || async {
        let from = seeds.events.first().map(|e| e.ts_unix_secs).unwrap_or(0);
        let to = seeds.events.last().map(|e| e.ts_unix_secs + 1).unwrap_or(1);

        for _ in 0..timeseries_reads {
            let mut response = surreal
                .query(
                    r#"
                    SELECT
                        metric_name,
                        count() AS points,
                        math::mean(value) AS avg_value,
                        math::min(value) AS min_value,
                        math::max(value) AS max_value
                    FROM events
                    WHERE ts_unix_secs >= $from AND ts_unix_secs < $to
                    GROUP BY metric_name
                    ORDER BY metric_name
                    "#,
                )
                .bind(("from", from))
                .bind(("to", to))
                .await?;
            let _rows: Vec<serde_json::Value> = response.take(0)?;
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
                let surreal = surreal.clone();
                let rows = seeds.users.len().max(1);
                join_set.spawn(async move {
                    for i in 0..heavy_read_ops_per_worker {
                        let idx = (worker_id * heavy_read_ops_per_worker + i) % rows;
                        let email = format!("user{idx}@example.com");
                        let mut response = surreal
                            .query("SELECT * FROM users WHERE email = $email LIMIT 1")
                            .bind(("email", email))
                            .await?;
                        let rows: Vec<serde_json::Value> = response.take(0)?;
                        if rows.is_empty() {
                            return Err(anyhow!("missing surreal user during heavy reads"));
                        }
                    }
                    Ok(()) as Result<()>
                });
            }

            while let Some(joined) = join_set.join_next().await {
                joined.context("surreal heavy read worker join failure")??;
            }

            Ok(())
        },
    )
    .await?;

    let heavy_concurrent_writes = time_many(
        heavy_workers * heavy_write_ops_per_worker,
        benchmark_trials,
        || async {
            let mut join_set = tokio::task::JoinSet::new();
            for worker_id in 0..heavy_workers {
                let surreal = surreal.clone();
                let rows = seeds.users.len().max(1);
                let users = user_ids.clone();
                let base_ts = 1_800_000_000_i64 + (worker_id as i64 * 10_000);
                join_set.spawn(async move {
                    for i in 0..heavy_write_ops_per_worker {
                        let idx = (worker_id * heavy_write_ops_per_worker + i) % rows;
                        let ts = base_ts + i as i64;
                        let value = ((worker_id + i) % 100) as f64;
                        let _created: Option<Thing> = surreal
                            .create("events")
                            .content(json!({
                                "user": users[idx].clone(),
                                "ts_unix_secs": ts,
                                "metric_name": "heavy_load",
                                "value": value
                            }))
                            .await?;
                    }
                    Ok(()) as Result<()>
                });
            }

            while let Some(joined) = join_set.join_next().await {
                joined.context("surreal heavy write worker join failure")??;
            }

            Ok(())
        },
    )
    .await?;

    results.push(to_result("surrealdb", "insert_users", insert_users));
    results.push(to_result(
        "surrealdb",
        "insert_profiles_one_to_one",
        insert_profiles,
    ));
    results.push(to_result(
        "surrealdb",
        "insert_orders_one_to_many",
        insert_orders,
    ));
    results.push(to_result(
        "surrealdb",
        "insert_order_items",
        insert_order_items,
    ));
    results.push(to_result(
        "surrealdb",
        "insert_payments_optional",
        insert_payments,
    ));
    results.push(to_result(
        "surrealdb",
        "insert_events_timeseries",
        insert_events,
    ));
    results.push(to_result("surrealdb", "point_reads", point_reads));
    results.push(to_result("surrealdb", "updates", updates));
    results.push(to_result(
        "surrealdb",
        "complex_relational_join_like",
        complex_relation_query,
    ));
    results.push(to_result(
        "surrealdb",
        "timeseries_aggregate_window",
        timeseries_window,
    ));
    results.push(to_result(
        "surrealdb",
        "heavy_concurrent_reads",
        heavy_concurrent_reads,
    ));
    results.push(to_result(
        "surrealdb",
        "heavy_concurrent_writes",
        heavy_concurrent_writes,
    ));

    Ok(results)
}
