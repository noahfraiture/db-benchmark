use crate::util::BenchmarkSummary;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct OperationResult {
    pub db: &'static str,
    pub op: &'static str,
    pub runs: usize,
    pub trials: usize,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub ops_per_sec: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub stddev_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub rows: usize,
    pub event_rows: usize,
    pub complex_reads: usize,
    pub timeseries_reads: usize,
    pub heavy_workers: usize,
    pub heavy_read_ops_per_worker: usize,
    pub heavy_write_ops_per_worker: usize,
    pub benchmark_trials: usize,
    pub durability_checks: HashMap<&'static str, serde_json::Value>,
    pub results: Vec<OperationResult>,
}

impl Report {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rows: usize,
        event_rows: usize,
        complex_reads: usize,
        timeseries_reads: usize,
        heavy_workers: usize,
        heavy_read_ops_per_worker: usize,
        heavy_write_ops_per_worker: usize,
        benchmark_trials: usize,
    ) -> Self {
        Self {
            rows,
            event_rows,
            complex_reads,
            timeseries_reads,
            heavy_workers,
            heavy_read_ops_per_worker,
            heavy_write_ops_per_worker,
            benchmark_trials,
            durability_checks: HashMap::new(),
            results: vec![],
        }
    }
}

pub fn to_result(db: &'static str, op: &'static str, stats: BenchmarkSummary) -> OperationResult {
    OperationResult {
        db,
        op,
        runs: stats.runs,
        trials: stats.trials,
        total_ms: stats.total_ms,
        avg_ms: stats.avg_ms,
        ops_per_sec: stats.ops_per_sec,
        p50_ms: stats.p50_ms,
        p95_ms: stats.p95_ms,
        stddev_ms: stats.stddev_ms,
    }
}
