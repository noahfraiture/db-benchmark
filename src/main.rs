mod cli;
mod postgres_bench;
mod report;
mod seed;
mod surreal_bench;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use report::Report;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let seeds = seed::generate_seeds(cli.rows, cli.event_rows);

    let mut report = Report::new(
        cli.rows,
        cli.event_rows,
        cli.complex_reads,
        cli.timeseries_reads,
        cli.heavy_workers,
        cli.heavy_read_ops_per_worker,
        cli.heavy_write_ops_per_worker,
        cli.benchmark_trials,
    );

    let (pg_durability, pg_results) = postgres_bench::run(&cli, &seeds).await?;
    report.durability_checks.insert("postgres", pg_durability);
    report.results.extend(pg_results);

    let (surreal_durability, surreal_results) = surreal_bench::run(&cli, &seeds).await?;
    report
        .durability_checks
        .insert("surrealdb", surreal_durability);
    report.results.extend(surreal_results);

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
