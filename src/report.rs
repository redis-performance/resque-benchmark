use crate::metrics::{IdlePollResult, LatencyStats, TrialResult};
use anyhow::Result;
use chrono::{DateTime, Utc};
use comfy_table::{Cell, Table};
use serde::Serialize;
use std::collections::HashMap;

/// Format a microsecond duration as a human-readable string.
fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{us} µs")
    } else if us < 1_000_000 {
        format!("{:.1} ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2} s", us as f64 / 1_000_000.0)
    }
}

/// Format a large integer with comma thousands separators.
pub fn format_n(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn format_jobs_per_sec(jps: f64) -> String {
    if !jps.is_finite() || jps < 0.0 {
        return "n/a".to_string();
    }
    format_n(jps as u64)
}

fn format_qps(q: f64) -> String {
    if !q.is_finite() || q < 0.0 {
        return "n/a".to_string();
    }
    format!("{q:.1}")
}

/// Print a single drain-trial line as it completes.
pub fn print_trial_line(r: &TrialResult) {
    let marker = if r.timed_out { " [TIMEOUT]" } else { "" };
    println!(
        "  [{:>4} workers]  {:>10} jobs/s  p50={:<8} p99={:<8} p99.9={:<8} max={}{}",
        r.workers,
        format_jobs_per_sec(r.jobs_per_sec),
        fmt_us(r.latency.p50),
        fmt_us(r.latency.p99),
        fmt_us(r.latency.p99_9),
        fmt_us(r.latency.max),
        marker,
    );
}

/// Print a single idle-poll-phase line as it completes. This is the tool's
/// headline new metric — Resque has no blocking dequeue, so idle workers
/// generate a steady LPOP poll-storm rather than zero traffic.
pub fn print_idle_poll_line(r: &IdlePollResult) {
    if let Some(reason) = &r.skipped_reason {
        println!(
            "  [{:>4} workers]  idle-poll: SKIPPED — {reason}",
            r.workers
        );
        return;
    }
    println!(
        "  [{:>4} workers]  idle-poll: {:>10} LPOP/s  ({:.1} LPOP/s/worker over {:.1}s, {} calls)",
        r.workers,
        format_qps(r.idle_poll_qps),
        r.per_worker_qps,
        r.duration_s,
        format_n(r.total_lpop_calls),
    );
}

/// Print the drain-phase summary table after all trials.
pub fn print_summary(results: &[TrialResult]) {
    println!();
    println!("--- Drain summary (backlog throughput + latency) ---");
    let mut table = Table::new();
    table.set_header(vec![
        "Workers", "jobs/s", "p50", "p99", "p99.9", "max", "errors",
    ]);
    for r in results {
        let workers_label = if r.timed_out {
            format!("{} [timeout]", r.workers)
        } else {
            r.workers.to_string()
        };
        table.add_row(vec![
            Cell::new(workers_label),
            Cell::new(format_jobs_per_sec(r.jobs_per_sec)),
            Cell::new(fmt_us(r.latency.p50)),
            Cell::new(fmt_us(r.latency.p99)),
            Cell::new(fmt_us(r.latency.p99_9)),
            Cell::new(fmt_us(r.latency.max)),
            Cell::new(r.errors),
        ]);
    }
    println!("{table}");
}

/// Print the idle-poll summary table after all trials.
pub fn print_idle_poll_summary(results: &[IdlePollResult]) {
    println!();
    println!("--- Idle-poll summary (steady-state polling load, empty queue) ---");
    let mut table = Table::new();
    table.set_header(vec![
        "Workers",
        "duration_s",
        "LPOP calls",
        "LPOP/s (fleet)",
        "LPOP/s/worker",
    ]);
    for r in results {
        if r.skipped_reason.is_some() {
            table.add_row(vec![
                Cell::new(r.workers),
                Cell::new("SKIPPED"),
                Cell::new("-"),
                Cell::new("-"),
                Cell::new("-"),
            ]);
            continue;
        }
        table.add_row(vec![
            Cell::new(r.workers),
            Cell::new(format!("{:.1}", r.duration_s)),
            Cell::new(format_n(r.total_lpop_calls)),
            Cell::new(format_qps(r.idle_poll_qps)),
            Cell::new(format!("{:.2}", r.per_worker_qps)),
        ]);
    }
    println!("{table}");
}

// ── JSON serialization ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct JsonOutput<'a> {
    tag: &'a str,
    timestamp: String,
    config: JsonConfig<'a>,
    results: Vec<JsonResult>,
}

#[derive(Serialize)]
struct JsonConfig<'a> {
    url: &'a str, // redacted — password replaced with ****
    workers: &'a [usize],
    jobs_per_trial: u64,
    queues: &'a [String],
    warmup_jobs: u64,
    poll_interval_ms: u64,
    idle_poll_duration_s: u64,
}

#[derive(Serialize)]
struct JsonResult {
    workers: usize,
    total_jobs: u64,
    duration_s: f64,
    jobs_per_sec: f64,
    timed_out: bool,
    throughput_per_sec: Vec<u64>,
    errors_per_sec: Vec<u64>,
    latency_per_sec_us: HashMap<String, Vec<u64>>,
    latency_us: LatencyStats,
    errors: u64,
    /// Clearly-separate section for the new metric this tool exists to
    /// surface — steady-state LPOP polling load against an empty queue.
    /// Sidekiq-benchmark has no equivalent field (BRPOP blocks — there is no
    /// polling load to measure).
    idle_poll: JsonIdlePoll,
}

#[derive(Serialize)]
struct JsonIdlePoll {
    duration_s: f64,
    total_lpop_calls: u64,
    idle_poll_qps: f64,
    per_worker_qps: f64,
    /// Set when the idle-poll phase was NOT run because the queue could not
    /// be verified empty after the drain phase (e.g. it hit --timeout with
    /// backlog remaining). All other fields in this object are 0 when set —
    /// consumers must check this before trusting idle_poll_qps.
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped_reason: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn write_json(
    results: &[TrialResult],
    idle_results: &[IdlePollResult],
    tag: &str,
    display_url: &str, // redacted form — never contains a real password
    workers: &[usize],
    jobs_per_trial: u64,
    queues: &[String],
    warmup_jobs: u64,
    poll_interval_ms: u64,
    idle_poll_duration_s: u64,
    output: &str,
) -> Result<()> {
    let timestamp: DateTime<Utc> = Utc::now();
    let out = JsonOutput {
        tag,
        timestamp: timestamp.to_rfc3339(),
        config: JsonConfig {
            url: display_url,
            workers,
            jobs_per_trial,
            queues,
            warmup_jobs,
            poll_interval_ms,
            idle_poll_duration_s,
        },
        results: results
            .iter()
            .zip(idle_results.iter())
            .map(|(r, idle)| JsonResult {
                workers: r.workers,
                total_jobs: r.total_jobs,
                duration_s: (r.duration_s * 1000.0).round() / 1000.0,
                jobs_per_sec: if r.jobs_per_sec.is_finite() {
                    (r.jobs_per_sec * 10.0).round() / 10.0
                } else {
                    0.0
                },
                timed_out: r.timed_out,
                throughput_per_sec: r.throughput_per_sec.clone(),
                errors_per_sec: r.errors_per_sec.clone(),
                latency_per_sec_us: r.latency_per_sec.clone(),
                latency_us: r.latency.clone(),
                errors: r.errors,
                idle_poll: JsonIdlePoll {
                    duration_s: (idle.duration_s * 1000.0).round() / 1000.0,
                    total_lpop_calls: idle.total_lpop_calls,
                    idle_poll_qps: if idle.idle_poll_qps.is_finite() {
                        (idle.idle_poll_qps * 10.0).round() / 10.0
                    } else {
                        0.0
                    },
                    per_worker_qps: if idle.per_worker_qps.is_finite() {
                        (idle.per_worker_qps * 100.0).round() / 100.0
                    } else {
                        0.0
                    },
                    skipped_reason: idle.skipped_reason.clone(),
                },
            })
            .collect(),
    };

    let json = serde_json::to_string_pretty(&out)?;
    if output == "-" {
        println!("{json}");
    } else {
        if let Some(parent) = std::path::Path::new(output).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(output, &json)?;
        println!("Results saved → {output}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_n_groups_thousands() {
        assert_eq!(format_n(0), "0");
        assert_eq!(format_n(999), "999");
        assert_eq!(format_n(1_000), "1,000");
        assert_eq!(format_n(1_000_000), "1,000,000");
        assert_eq!(format_n(12_345_678), "12,345,678");
    }

    #[test]
    fn format_jobs_per_sec_handles_non_finite() {
        assert_eq!(format_jobs_per_sec(f64::NAN), "n/a");
        assert_eq!(format_jobs_per_sec(f64::INFINITY), "n/a");
        assert_eq!(format_jobs_per_sec(-1.0), "n/a");
        assert_eq!(format_jobs_per_sec(11_062.3), "11,062");
    }

    #[test]
    fn format_qps_handles_non_finite() {
        assert_eq!(format_qps(f64::NAN), "n/a");
        assert_eq!(format_qps(-1.0), "n/a");
        assert_eq!(format_qps(123.456), "123.5");
    }
}
