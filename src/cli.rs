use clap::Parser;

#[derive(Debug, Parser, Clone)]
#[command(author, version, about = "Benchmark Postgres vs SurrealDB")]
pub struct Cli {
    #[arg(
        long,
        default_value = "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=bench"
    )]
    pub postgres_dsn: String,

    #[arg(long, default_value = "127.0.0.1:8000")]
    pub surreal_addr: String,

    #[arg(long, default_value = "root")]
    pub surreal_user: String,

    #[arg(long, default_value = "root")]
    pub surreal_pass: String,

    #[arg(long, default_value = "bench")]
    pub surreal_ns: String,

    #[arg(long, default_value = "bench")]
    pub surreal_db: String,

    #[arg(long, default_value_t = 5_000)]
    pub rows: usize,

    #[arg(long, default_value_t = 10_000)]
    pub event_rows: usize,

    #[arg(long, default_value_t = 1_000)]
    pub complex_reads: usize,

    #[arg(long, default_value_t = 1_000)]
    pub timeseries_reads: usize,

    #[arg(long, default_value_t = 32)]
    pub heavy_workers: usize,

    #[arg(long, default_value_t = 500)]
    pub heavy_read_ops_per_worker: usize,

    #[arg(long, default_value_t = 500)]
    pub heavy_write_ops_per_worker: usize,

    #[arg(long, default_value_t = 5)]
    pub benchmark_trials: usize,
}
