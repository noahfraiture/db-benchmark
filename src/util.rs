use anyhow::{anyhow, Result};
use std::future::Future;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub runs: usize,
    pub trials: usize,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub ops_per_sec: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub stddev_ms: f64,
}

pub async fn time_many<F, Fut>(runs: usize, trials: usize, mut f: F) -> Result<BenchmarkSummary>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if runs == 0 {
        return Err(anyhow!("runs must be greater than 0"));
    }
    if trials == 0 {
        return Err(anyhow!("trials must be greater than 0"));
    }

    let mut per_op_ms_trials = Vec::with_capacity(trials);
    let mut total_ms = 0.0;

    for _ in 0..trials {
        let start = Instant::now();
        f().await?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        total_ms += elapsed_ms;
        per_op_ms_trials.push(elapsed_ms / runs as f64);
    }

    per_op_ms_trials.sort_by(f64::total_cmp);

    let avg_ms = total_ms / (runs * trials) as f64;
    let ops_per_sec = (runs * trials) as f64 / (total_ms / 1000.0);
    let p50_ms = percentile(&per_op_ms_trials, 0.50);
    let p95_ms = percentile(&per_op_ms_trials, 0.95);
    let stddev_ms = stddev(&per_op_ms_trials);

    Ok(BenchmarkSummary {
        runs,
        trials,
        total_ms,
        avg_ms,
        ops_per_sec,
        p50_ms,
        p95_ms,
        stddev_ms,
    })
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

fn stddev(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}
