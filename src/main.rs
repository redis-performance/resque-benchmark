use anyhow::{Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use metrics::{IdlePollResult, LatencyStats, Metrics, TrialResult};
use resque_bench::{metrics, producer, report, worker};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use worker::PollWorker;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "resque-bench",
    version,
    about = "Resque protocol load benchmark — measures job throughput/latency AND steady-state idle polling load against any Redis endpoint"
)]
struct Cli {
    /// Redis URL (takes precedence over --host/--port).
    /// Defaults to db 13 for parity with the sister sidekiq-benchmark tool's
    /// safety default — Resque itself has no equivalent convention, since
    /// its worker db is whatever REDIS_URL/config points it at.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379/13")]
    url: String,

    /// Override host in the Redis URL
    #[arg(long)]
    host: Option<String>,

    /// Override port in the Redis URL
    #[arg(long)]
    port: Option<u16>,

    /// Redis password — prefer REDIS_PASSWORD env var; passing on CLI exposes it in process list
    #[arg(long, env = "REDIS_PASSWORD")]
    password: Option<String>,

    /// Enable TLS (upgrades scheme to rediss://)
    #[arg(long, env = "REDIS_TLS")]
    tls: bool,

    /// Redis database number (default 13 — see --url doc)
    #[arg(long, default_value = "13")]
    db: u8,

    /// Comma-separated concurrency levels — each becomes a separate trial
    #[arg(long, default_value = "10,50,100,200", value_delimiter = ',')]
    workers: Vec<usize>,

    /// Total jobs per drain trial
    #[arg(long, default_value = "500000")]
    jobs: u64,

    /// Jobs for warmup run before each trial (0 = skip)
    #[arg(long, default_value = "0")]
    warmup_jobs: u64,

    /// Base Resque queue name
    #[arg(long, default_value = "default")]
    queue: String,

    /// Number of queues to distribute jobs across (1 = single queue). Resque
    /// workers scan configured queues in priority order every poll
    /// (resque/lib/resque/worker.rb:359-370, `queues.each`); with N > 1 each
    /// idle poll cycle costs up to N LPOP calls per worker. Queue names are
    /// generated as <queue>_0, <queue>_1, … when > 1.
    #[arg(long, default_value = "1")]
    num_queues: usize,

    /// Per-second latency percentiles to record (comma-separated).
    /// Supported values: p50, p75, p90, p95, p99, p999, p9999, max, mean.
    #[arg(long, default_value = "p50,p90,p99,p999,max", value_delimiter = ',')]
    latency_percentiles: Vec<String>,

    /// Label for output (defaults to redis_version from INFO)
    #[arg(long)]
    tag: Option<String>,

    /// Output file path, or '-' for stdout
    #[arg(long)]
    output: Option<String>,

    /// Per-trial timeout in seconds (applies to the drain phase; the idle-poll
    /// phase is bounded by --idle-poll-duration-s instead)
    #[arg(long, default_value = "300")]
    timeout: u64,

    /// Suppress per-second progress output
    #[arg(long)]
    quiet: bool,

    /// Allow FLUSHDB before each trial (clears the entire database).
    /// Default: only deletes the specific queue key, which is safe on shared Redis.
    #[arg(long, env = "RESQUE_BENCH_ALLOW_FLUSHDB")]
    allow_flushdb: bool,

    /// Fixed poll interval between empty-queue LPOP retries, in milliseconds.
    /// Models Resque's default `work(interval)` steady-state behavior: with
    /// MIN_INTERVAL/MAX_INTERVAL unset (the common case — see
    /// resque/lib/resque/tasks.rb:20-24), `work`'s additive backoff
    /// (worker.rb:273-278) collapses to this fixed value on every empty poll.
    /// Resque's own default is 5000 (worker.rb:252, `interval = 5.0`).
    #[arg(long, default_value = "5000")]
    poll_interval_ms: u64,

    /// Duration in seconds to run the idle-poll measurement phase after the
    /// queue has fully drained. Workers keep polling an intentionally-empty
    /// queue for this long; the LPOP calls/sec issued during this window is
    /// the tool's headline new metric (see README).
    #[arg(long, default_value = "30")]
    idle_poll_duration_s: u64,
}

// ── Redis URL helpers ─────────────────────────────────────────────────────────

fn build_redis_url(cli: &Cli) -> Result<String> {
    let mut u = url::Url::parse(&cli.url)
        .with_context(|| format!("invalid Redis URL: {}", redact_url(&cli.url)))?;

    if let Some(host) = &cli.host {
        u.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("invalid --host: {host}"))?;
    }
    if let Some(port) = cli.port {
        u.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set port on URL: {}", redact_url(&cli.url)))?;
    }
    if cli.tls && u.scheme() == "redis" {
        u.set_scheme("rediss")
            .map_err(|_| anyhow::anyhow!("cannot upgrade scheme to rediss"))?;
    }
    if let Some(password) = &cli.password {
        // url::Url::set_password percent-encodes special characters (e.g. '@', '/', ':').
        // NOTE: the error path below must never echo `password` — only the (already
        // redacted) base URL — since printing the raw value would leak the secret.
        u.set_password(Some(password))
            .map_err(|_| anyhow::anyhow!("cannot set password on URL: {}", redact_url(&cli.url)))?;
    }
    // Ensure db path is present
    if u.path().trim_matches('/').is_empty() {
        u.set_path(&format!("/{}", cli.db));
    }

    Ok(u.to_string())
}

/// Return the URL with the password replaced by **** for logging and JSON output.
fn redact_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("****"));
            }
            u.to_string()
        }
        Err(_) => raw.to_string(),
    }
}

/// Sanitize a tag string to characters safe for use in filenames.
fn sanitize_tag(tag: &str) -> String {
    let s: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "unknown".to_string()
    } else {
        s
    }
}

/// Reject output paths containing '..' to prevent path traversal.
fn validate_output_path(path: &str) -> Result<()> {
    if path == "-" {
        return Ok(());
    }
    for component in std::path::Path::new(path).components() {
        if component == std::path::Component::ParentDir {
            anyhow::bail!("--output must not contain '..' segments: {path}");
        }
    }
    Ok(())
}

// ── Per-second latency percentile specs ──────────────────────────────────────

#[derive(Clone)]
enum PercentileSpec {
    Quantile { name: String, q: f64 },
    Max,
    Mean,
}

impl PercentileSpec {
    fn name(&self) -> &str {
        match self {
            Self::Quantile { name, .. } => name,
            Self::Max => "max",
            Self::Mean => "mean",
        }
    }

    fn value(&self, hist: &Histogram<u64>) -> u64 {
        if hist.is_empty() {
            return 0;
        }
        match self {
            Self::Quantile { q, .. } => hist.value_at_quantile(*q),
            Self::Max => hist.max(),
            Self::Mean => hist.mean() as u64,
        }
    }
}

/// Parse a percentile spec string: "p50" → 0.50, "p999" → 0.999, "max", "mean".
fn parse_percentile_spec(s: &str) -> Result<PercentileSpec> {
    match s {
        "max" => Ok(PercentileSpec::Max),
        "mean" => Ok(PercentileSpec::Mean),
        s if s.starts_with('p') => {
            let digits = &s[1..];
            anyhow::ensure!(!digits.is_empty(), "invalid percentile spec: '{s}'");
            // A crafted --latency-percentiles value with ~20 digits (e.g.
            // "p10000000000000000000", which still parses fine as a u64 —
            // u64::MAX itself has 20 digits) makes `10u64.pow(digits.len())`
            // overflow: 10^20 > u64::MAX. `pow` panics on overflow in debug
            // builds and silently wraps in release, either way turning a
            // malformed CLI flag into a crash or a bogus quantile instead of
            // a clean error. Cap the digit count well under that threshold
            // (no real percentile spec needs more than a handful of nines)
            // and use checked_pow as defense in depth.
            anyhow::ensure!(
                digits.len() <= 15,
                "percentile spec '{s}' has too many digits (max 15) — use e.g. p99, p999, p99999"
            );
            let n: u64 = digits
                .parse()
                .with_context(|| format!("invalid percentile spec: '{s}'"))?;
            let divisor = 10u64
                .checked_pow(digits.len() as u32)
                .ok_or_else(|| anyhow::anyhow!("percentile spec '{s}' out of range"))?;
            let q = n as f64 / divisor as f64;
            anyhow::ensure!(q > 0.0 && q <= 1.0, "percentile out of range (0, 1]: '{s}'");
            Ok(PercentileSpec::Quantile {
                name: s.to_string(),
                q,
            })
        }
        _ => anyhow::bail!("unknown percentile spec '{s}' — use p50, p99, p999, max, mean"),
    }
}

/// Generate queue names from a base name and count.
/// With n=1 returns `["default"]`; with n=4 returns `["default_0".."default_3"]`.
fn make_queue_names(base: &str, n: usize) -> Vec<String> {
    if n <= 1 {
        vec![base.to_string()]
    } else {
        (0..n).map(|i| format!("{base}_{i}")).collect()
    }
}

async fn fetch_tag(url: &str) -> String {
    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("warning: could not build Redis client for tag lookup: {e}");
            return "unknown".to_string();
        }
    };
    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            match redis::cmd("INFO")
                .arg("server")
                .query_async::<String>(&mut conn)
                .await
            {
                Ok(info) => {
                    for line in info.lines() {
                        if let Some(v) = line.strip_prefix("redis_version:") {
                            return format!("redis-{}", v.trim());
                        }
                    }
                    "unknown".to_string()
                }
                Err(e) => {
                    eprintln!("warning: could not fetch Redis INFO for tag: {e}");
                    "unknown".to_string()
                }
            }
        }
        Err(e) => {
            eprintln!("warning: could not connect to Redis for tag lookup: {e}");
            "unknown".to_string()
        }
    }
}

// ── Drain trial (backlog throughput + latency) ────────────────────────────────

struct TrialConfig<'a> {
    url: &'a str,
    queue_keys: &'a [String],
    jobs: u64,
    timeout_secs: u64,
    quiet: bool,
    percentile_specs: &'a [PercentileSpec],
    poll_interval: Duration,
}

fn empty_histogram() -> Histogram<u64> {
    // HDRHistogram requires low >= 1; values are clamped to .max(1) before recording
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

/// Open `n` dedicated multiplexed connections — one per worker, mirroring N
/// independent Resque worker processes each holding their own Redis connection.
async fn open_worker_connections(
    client: &redis::Client,
    n: usize,
) -> Result<Vec<redis::aio::MultiplexedConnection>> {
    let mut conns = Vec::with_capacity(n);
    for _ in 0..n {
        conns.push(
            client
                .get_multiplexed_async_connection()
                .await
                .context("failed to open worker Redis connection")?,
        );
    }
    Ok(conns)
}

/// Await a batch of worker JoinHandles, giving each up to `grace` to notice a
/// shutdown signal and return; anything still running past that is aborted.
///
/// Each handle is watched CONCURRENTLY (one tiny supervisor task per worker),
/// not sequentially. A worker only fails to notice the shutdown watch signal
/// promptly if it's blocked mid-flight inside a `query_async` call that never
/// returns (e.g. Redis wedged / network partition) — the shutdown select! in
/// `PollWorker::run` otherwise reacts near-instantly regardless of
/// `--poll-interval-ms`. If that happens to ALL n workers at once (Redis
/// disappearing mid-benchmark is exactly the scenario where it would), a
/// naive sequential await would take up to `n_workers * grace` to finish —
/// e.g. 200 workers * 5s = ~17 minutes — before the program could proceed to
/// the next trial or exit. Concurrent awaiting bounds total wall time at
/// `grace` regardless of how many workers are stuck.
async fn join_workers(handles: Vec<tokio::task::JoinHandle<()>>, grace: Duration) {
    let waiters: Vec<_> = handles
        .into_iter()
        .map(|h| {
            tokio::spawn(async move {
                let abort = h.abort_handle();
                if tokio::time::timeout(grace, h).await.is_err() {
                    abort.abort();
                }
            })
        })
        .collect();
    for w in waiters {
        let _ = w.await;
    }
}

async fn run_drain_trial(cfg: &TrialConfig<'_>, n_workers: usize) -> Result<TrialResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (latency_tx, latency_rx) = mpsc::unbounded_channel::<u64>();

    // Per-second latency windows are pulled by the monitor, not pushed on a
    // separate timer: each tick the monitor sends a oneshot responder over
    // `snapshot_tx`, and the collector replies with the current window
    // histogram and resets it. This keeps latency_per_sec[i] aligned with the
    // throughput/error deltas measured on the same tick. See sidekiq-benchmark's
    // main.rs for the fuller rationale (identical design, ported verbatim).
    let (snapshot_tx, snapshot_rx) = mpsc::unbounded_channel::<oneshot::Sender<Histogram<u64>>>();

    let collector = tokio::spawn(async move {
        let mut hist = empty_histogram();
        let mut per_sec_hist = empty_histogram();
        let mut rx = latency_rx;
        let mut snapshot_rx = snapshot_rx;
        loop {
            tokio::select! {
                maybe_us = rx.recv() => {
                    match maybe_us {
                        Some(us) => {
                            let v = us.max(1);
                            let _ = hist.record(v);
                            let _ = per_sec_hist.record(v);
                        }
                        None => break,
                    }
                }
                Some(resp) = snapshot_rx.recv() => {
                    let _ = resp.send(per_sec_hist.clone());
                    per_sec_hist.reset();
                }
            }
        }
        hist
    });

    let client = redis::Client::open(cfg.url).context("invalid Redis URL for worker pool")?;
    let conns = open_worker_connections(&client, n_workers).await?;

    let mut handles = Vec::with_capacity(n_workers);
    for conn in conns {
        let w = PollWorker {
            metrics: metrics.clone(),
            latency_tx: latency_tx.clone(),
            done_tx: done_tx.clone(),
            target_jobs: Some(cfg.jobs),
            poll_interval: cfg.poll_interval,
            queue_keys: cfg.queue_keys.to_vec(),
        };
        let rx = shutdown_rx.clone();
        handles.push(tokio::spawn(w.run(conn, rx)));
    }
    drop(latency_tx); // drop sentinel; channel closes once every worker clone is dropped too

    // Per-second samples collected by the monitor task
    let throughput_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let latency_sec_samples: Arc<Mutex<HashMap<String, Vec<u64>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let tput_for_monitor = throughput_samples.clone();
    let err_for_monitor = errors_samples.clone();
    let lat_for_monitor = latency_sec_samples.clone();
    let metrics_mon = metrics.clone();
    let specs_for_monitor = cfg.percentile_specs.to_vec();
    let quiet = cfg.quiet;

    let monitor = tokio::spawn(async move {
        let mut prev_completed = 0u64;
        let mut prev_errors = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let cur = metrics_mon.get_completed();
            let tput_delta = cur - prev_completed;
            prev_completed = cur;
            if let Ok(mut v) = tput_for_monitor.lock() {
                v.push(tput_delta);
            }

            let cur_err = metrics_mon.get_errors();
            let err_delta = cur_err - prev_errors;
            prev_errors = cur_err;
            if let Ok(mut v) = err_for_monitor.lock() {
                v.push(err_delta);
            }

            let (resp_tx, resp_rx) = oneshot::channel();
            if snapshot_tx.send(resp_tx).is_ok() {
                if let Ok(snap) = resp_rx.await {
                    if let Ok(mut map) = lat_for_monitor.lock() {
                        for spec in &specs_for_monitor {
                            map.entry(spec.name().to_string())
                                .or_default()
                                .push(spec.value(&snap));
                        }
                    }
                }
            }

            if !quiet {
                if err_delta > 0 {
                    print!("[e:{err_delta}]");
                } else {
                    print!(".");
                }
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    });

    let start = Instant::now();
    let mut timed_out = false;

    if cfg.jobs == 0 {
        // Nothing to drain — skip straight to completion (used by warmup=0 guards).
    } else {
        tokio::select! {
            _ = done_rx.wait_for(|v| *v) => {},
            _ = tokio::time::sleep(Duration::from_secs(cfg.timeout_secs)) => {
                if !cfg.quiet { eprintln!(); }
                eprintln!("  [timeout after {}s]", cfg.timeout_secs);
                timed_out = true;
            }
        }
    }

    let duration = start.elapsed();
    if !cfg.quiet && !timed_out {
        println!();
    }

    monitor.abort();

    // Signal workers to stop; watch::Receiver::changed() wakes them immediately
    // even mid-sleep, so this is near-instant regardless of --poll-interval-ms.
    let _ = shutdown_tx.send(true);
    join_workers(handles, Duration::from_secs(5)).await;

    // All worker latency_tx clones are now dropped — collector drains and returns.
    let hist = collector.await.unwrap_or_else(|_| empty_histogram());

    let total_jobs = metrics.get_completed();
    let errors = metrics.get_errors();
    let throughput_per_sec = throughput_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();
    let errors_per_sec = errors_samples.lock().map(|v| v.clone()).unwrap_or_default();
    let latency_per_sec = latency_sec_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();

    let jobs_per_sec = if duration.as_secs_f64() > 0.0 {
        total_jobs as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(TrialResult {
        workers: n_workers,
        total_jobs,
        duration_s: duration.as_secs_f64(),
        jobs_per_sec,
        throughput_per_sec,
        errors_per_sec,
        latency_per_sec,
        latency: LatencyStats::from_histogram(&hist),
        errors,
        timed_out,
    })
}

// ── Idle-poll trial (steady-state polling load against an empty queue) ───────

/// Derive fleet-wide and per-worker idle-poll QPS from the raw counters.
/// Pulled out of `run_idle_poll_trial` as a pure function so the boundary
/// cases (zero duration, zero workers, huge counters) are unit-testable
/// without spinning up real workers/Redis:
/// - `duration_s <= 0.0`: guards not just literal 0.0 but also NaN/negative,
///   returning (0.0, 0.0) instead of a division that could produce `inf`/NaN
///   masquerading as a real throughput figure downstream (JSON, table
///   printing). `--idle-poll-duration-s` is separately validated to be > 0
///   in `validate_cli`, but this function stays defensive on its own since
///   it's also reachable with an arbitrary caller-supplied `duration_s`.
/// - `total_calls` is a u64 counter incremented once per LPOP issued; even
///   at the maximum realistic sustained rate (millions/sec) for the maximum
///   realistic trial length, it stays far below 2^53 (the point where an
///   f64 can no longer represent every integer exactly), so the
///   `as f64` conversion here never silently loses precision in practice.
/// - `workers == 0` guards the per-worker division; `validate_cli` also
///   rejects a 0 in `--workers` up front, so this is defense in depth.
fn compute_idle_poll_qps(total_calls: u64, duration_s: f64, workers: usize) -> (f64, f64) {
    let idle_poll_qps = if duration_s.is_finite() && duration_s > 0.0 {
        total_calls as f64 / duration_s
    } else {
        0.0
    };
    let per_worker_qps = if workers > 0 {
        idle_poll_qps / workers as f64
    } else {
        0.0
    };
    (idle_poll_qps, per_worker_qps)
}

async fn run_idle_poll_trial(
    url: &str,
    queue_keys: &[String],
    n_workers: usize,
    poll_interval: Duration,
    idle_poll_duration_s: u64,
) -> Result<IdlePollResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, _done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();

    // No collector needed — latency is out of scope for this phase (the queue
    // is deliberately empty, so hits should be ~0). Drain-and-discard so the
    // channel never blocks a worker in the rare case of a stray hit.
    let drain_task = tokio::spawn(async move { while latency_rx.recv().await.is_some() {} });

    let client = redis::Client::open(url).context("invalid Redis URL for worker pool")?;
    let conns = open_worker_connections(&client, n_workers).await?;

    let mut handles = Vec::with_capacity(n_workers);
    for conn in conns {
        let w = PollWorker {
            metrics: metrics.clone(),
            latency_tx: latency_tx.clone(),
            done_tx: done_tx.clone(),
            target_jobs: None, // run until externally shut down
            poll_interval,
            queue_keys: queue_keys.to_vec(),
        };
        let rx = shutdown_rx.clone();
        handles.push(tokio::spawn(w.run(conn, rx)));
    }
    drop(latency_tx);

    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(idle_poll_duration_s)).await;
    let elapsed = start.elapsed();

    let _ = shutdown_tx.send(true);
    join_workers(handles, Duration::from_secs(5)).await;
    drain_task.abort();

    let total_lpop_calls = metrics.get_polls();
    let duration_s = elapsed.as_secs_f64();
    let (idle_poll_qps, per_worker_qps) =
        compute_idle_poll_qps(total_lpop_calls, duration_s, n_workers);

    Ok(IdlePollResult {
        workers: n_workers,
        duration_s,
        total_lpop_calls,
        idle_poll_qps,
        per_worker_qps,
        skipped_reason: None,
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Validate CLI arguments that clap's type system can't express (all-zero
/// checks, cross-field sanity) and print any non-fatal advisory warnings.
/// Split out from `main` so it's independently unit-testable.
fn validate_cli(cli: &Cli) -> Result<()> {
    anyhow::ensure!(cli.jobs > 0, "--jobs must be > 0");
    anyhow::ensure!(cli.num_queues > 0, "--num-queues must be > 0");
    anyhow::ensure!(cli.timeout > 0, "--timeout must be > 0");
    // idle_poll_qps = total_lpop_calls / duration_s uses the REAL elapsed
    // wall time, not the requested duration — with --idle-poll-duration-s 0
    // that elapsed time is a near-zero (but not exactly zero) scheduling
    // epsilon, so the `duration_s > 0.0` guard in run_idle_poll_trial would
    // NOT catch it: a handful of polls divided by a sub-millisecond window
    // extrapolates to a wildly inflated, meaningless QPS figure that still
    // looks like a real result. Reject the degenerate input instead.
    anyhow::ensure!(
        cli.idle_poll_duration_s > 0,
        "--idle-poll-duration-s must be > 0"
    );
    // A literal 0 here would make every empty-queue poll cycle sleep for
    // Duration::ZERO — tokio::time::sleep(ZERO) yields once and returns
    // immediately, so this would busy-loop LPOP calls against Redis as fast
    // as the network round-trip allows (a self-inflicted DoS on both this
    // process and the target Redis) rather than measuring any real steady
    // -state polling rate. Reject it outright instead of silently producing
    // a number that looks like a benchmark result.
    anyhow::ensure!(cli.poll_interval_ms > 0, "--poll-interval-ms must be > 0");
    anyhow::ensure!(
        !cli.workers.is_empty(),
        "--workers must list at least one concurrency level"
    );
    anyhow::ensure!(
        cli.workers.iter().all(|&w| w > 0),
        "--workers values must all be > 0 — a 0-worker trial can never reach \
         its target completion count, so it silently hangs for the full \
         --timeout instead of failing fast"
    );
    if cli.poll_interval_ms < 10 {
        eprintln!(
            "warning: --poll-interval-ms {} is very aggressive — each idle worker will issue \
             up to ~{} LPOP/s against Redis (Resque's own out-of-the-box default is 5000ms). \
             This is a legitimate stress-test mode, but make sure it's intentional before \
             pointing it at shared infrastructure.",
            cli.poll_interval_ms,
            1000 / cli.poll_interval_ms.max(1),
        );
    }
    if let Some(&max_workers) = cli.workers.iter().max() {
        if max_workers > 5000 {
            eprintln!(
                "warning: --workers {max_workers} opens one dedicated Redis connection PER \
                 worker — this may exhaust file descriptors / ephemeral ports on this machine, \
                 or `maxclients` on the Redis server. Check `ulimit -n` before running."
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let url = build_redis_url(&cli)?;
    let display_url = redact_url(&url);

    // Warn loudly if FLUSHDB is enabled on db 0 — application data lives there by default.
    if cli.allow_flushdb {
        let db_in_url = url::Url::parse(&url)
            .ok()
            .and_then(|u| u.path().trim_matches('/').parse::<u8>().ok())
            .unwrap_or(0);
        if db_in_url == 0 {
            eprintln!(
                "warning: --allow-flushdb is set on db 0 — this will destroy ALL keys in the \
                 database. Use --db 13 (or any non-zero db) to isolate benchmark data."
            );
        }
    }

    if let Some(out) = &cli.output {
        validate_output_path(out)?;
    }

    let tag = match &cli.tag {
        Some(t) => sanitize_tag(t),
        None => sanitize_tag(&fetch_tag(&url).await),
    };

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| format!("resque_bench_{tag}.json"));

    let queue_names = make_queue_names(&cli.queue, cli.num_queues);
    let queue_keys: Vec<String> = queue_names.iter().map(|q| format!("queue:{q}")).collect();
    let queues_label = if queue_names.len() == 1 {
        queue_names[0].clone()
    } else {
        format!(
            "{} queues ({}…{})",
            queue_names.len(),
            queue_names[0],
            queue_names[queue_names.len() - 1]
        )
    };

    println!("\n=== resque-bench — {tag} ===");
    println!(
        "    {}  jobs={}  queues={}  poll-interval={}ms",
        display_url,
        report::format_n(cli.jobs),
        queues_label,
        cli.poll_interval_ms,
    );
    println!();

    let client = redis::Client::open(url.as_str()).context("invalid Redis URL")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let percentile_specs: Vec<PercentileSpec> = cli
        .latency_percentiles
        .iter()
        .map(|s| parse_percentile_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let poll_interval = Duration::from_millis(cli.poll_interval_ms);

    let cfg = TrialConfig {
        url: &url,
        queue_keys: &queue_keys,
        jobs: cli.jobs,
        timeout_secs: cli.timeout,
        quiet: cli.quiet,
        percentile_specs: &percentile_specs,
        poll_interval,
    };

    let workers_list = cli.workers.clone();
    let mut results: Vec<TrialResult> = Vec::new();
    let mut idle_results: Vec<IdlePollResult> = Vec::new();
    let mut any_timeout = false;

    // Warn if the queue fill will likely use significant Redis memory.
    // ~250 B per job (class, args array with idx/hash/timestamp).
    let estimated_mb = cli.jobs as f64 * 250.0 / (1024.0 * 1024.0);
    if estimated_mb > 100.0 {
        eprintln!(
            "warning: estimated peak Redis memory ~{:.0} MB ({} jobs × ~250 B/job)",
            estimated_mb,
            report::format_n(cli.jobs)
        );
    }

    for &n_workers in &workers_list {
        if cli.warmup_jobs > 0 {
            pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
            producer::bulk_enqueue(&mut conn, &queue_names, cli.warmup_jobs).await?;
            if !cli.quiet {
                print!("  [{n_workers:>4} workers] warmup … ");
            }
            let warmup_cfg = TrialConfig {
                jobs: cli.warmup_jobs,
                ..cfg
            };
            run_drain_trial(&warmup_cfg, n_workers).await?;
        }

        // Drain phase — measure backlog throughput + latency.
        pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
        producer::bulk_enqueue(&mut conn, &queue_names, cli.jobs).await?;

        if !cli.quiet {
            print!("  [{n_workers:>4} workers] ");
        }

        let result = run_drain_trial(&cfg, n_workers).await?;
        if result.timed_out {
            any_timeout = true;
        }
        report::print_trial_line(&result);

        // Idle-poll phase — only trustworthy once the queue is verified
        // empty. A drain trial that hit --timeout (result.timed_out) may
        // still have real backlog sitting in the queue; starting the
        // idle-poll phase against that backlog would let its workers race
        // through genuine hits (no sleep on a hit — see worker.rs) and
        // silently contaminate idle_poll_qps with leftover drain-phase
        // throughput instead of measuring true empty-queue polling load. So
        // this is a real LLEN check, not an assumption from `timed_out`:
        // reaching the target completion count during the drain phase is
        // itself a race-free "queue is empty" signal (every completion is a
        // job actually removed, and target == the exact count pushed), but
        // a timeout provides no such guarantee.
        let backlog = producer::total_queue_len(&mut conn, &queue_names).await?;
        let idle_result = if backlog > 0 {
            let reason = format!(
                "drain trial left {} job(s) still queued (--timeout hit with backlog \
                 remaining) — skipping idle-poll measurement to avoid contaminating \
                 idle_poll_qps with real job hits",
                report::format_n(backlog)
            );
            if !cli.quiet {
                println!("  [{n_workers:>4} workers] idle-poll SKIPPED: {reason}");
            }
            IdlePollResult::skipped(n_workers, reason)
        } else {
            if !cli.quiet {
                print!(
                    "  [{n_workers:>4} workers] idle-poll running ({}s)… ",
                    cli.idle_poll_duration_s
                );
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
            run_idle_poll_trial(
                &url,
                &queue_keys,
                n_workers,
                poll_interval,
                cli.idle_poll_duration_s,
            )
            .await?
        };
        report::print_idle_poll_line(&idle_result);

        results.push(result);
        idle_results.push(idle_result);
    }

    report::print_summary(&results);
    report::print_idle_poll_summary(&idle_results);

    report::write_json(
        &results,
        &idle_results,
        &tag,
        &display_url,
        &workers_list,
        cli.jobs,
        &queue_names,
        cli.warmup_jobs,
        cli.poll_interval_ms,
        cli.idle_poll_duration_s,
        &output,
    )?;

    if any_timeout {
        eprintln!("warning: one or more trials timed out — results are incomplete");
        std::process::exit(1);
    }

    Ok(())
}

/// Clear queues before a trial. Uses DEL by default; FLUSHDB only when explicitly allowed.
async fn pre_trial_clear(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    allow_flushdb: bool,
) -> Result<()> {
    if allow_flushdb {
        producer::flushdb(conn).await
    } else {
        producer::clear_queue(conn, queues).await
    }
}

// ── TrialConfig Copy impl ─────────────────────────────────────────────────────

impl<'a> Copy for TrialConfig<'a> {}
impl<'a> Clone for TrialConfig<'a> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cli() -> Cli {
        Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: None,
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            poll_interval_ms: 5000,
            idle_poll_duration_s: 30,
        }
    }

    #[test]
    fn sanitize_tag_strips_unsafe_chars() {
        assert_eq!(sanitize_tag("redis-8.0"), "redis-8.0"); // dots and dashes kept
        assert_eq!(sanitize_tag("redis/8.0"), "redis-8.0"); // slash → dash
        assert_eq!(sanitize_tag("../evil"), "..-evil");
        assert_eq!(sanitize_tag("foo bar"), "foo-bar"); // space → dash
        assert_eq!(sanitize_tag(""), "unknown");
    }

    #[test]
    fn validate_output_path_rejects_traversal() {
        assert!(validate_output_path("../evil.json").is_err());
        assert!(validate_output_path("foo/../bar.json").is_err());
        assert!(validate_output_path("results/out.json").is_ok());
        assert!(validate_output_path("-").is_ok());
        assert!(validate_output_path("out.json").is_ok());
    }

    #[test]
    fn redact_url_hides_password() {
        let raw = "redis://:hunter2@127.0.0.1:6379/0";
        let redacted = redact_url(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password still visible: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_leaves_no_password_url_unchanged() {
        let raw = "redis://127.0.0.1:6379/0";
        assert_eq!(redact_url(raw), raw);
    }

    #[test]
    fn build_redis_url_encodes_special_chars_in_password() {
        let mut cli = base_cli();
        cli.password = Some("p@ss/word".into());
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "127.0.0.1");
        let raw_pw = parsed.password().unwrap();
        assert!(raw_pw.contains("%40"), "@ not percent-encoded: {raw_pw}");
        assert!(!url.contains(":p@ss"), "raw '@' leaked into URL: {url}");
    }

    #[test]
    fn build_redis_url_upgrades_scheme_with_tls() {
        let mut cli = base_cli();
        cli.tls = true;
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("rediss://"), "expected rediss:// got {url}");
    }

    #[test]
    fn build_redis_url_host_port_override() {
        let mut cli = base_cli();
        cli.host = Some("10.0.0.1".into());
        cli.port = Some(6380);
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "10.0.0.1");
        assert_eq!(parsed.port().unwrap(), 6380);
    }

    #[test]
    fn parse_percentile_spec_valid() {
        let cases: &[(&str, f64)] = &[
            ("p50", 0.50),
            ("p90", 0.90),
            ("p99", 0.99),
            ("p999", 0.999),
            ("p9999", 0.9999),
            ("p75", 0.75),
        ];
        for &(s, expected_q) in cases {
            match parse_percentile_spec(s).unwrap() {
                PercentileSpec::Quantile { q, name } => {
                    assert!((q - expected_q).abs() < 1e-9, "{s}: got {q}");
                    assert_eq!(name, s);
                }
                other => panic!("{s} parsed as non-quantile: {}", other.name()),
            }
        }
        assert!(matches!(
            parse_percentile_spec("max").unwrap(),
            PercentileSpec::Max
        ));
        assert!(matches!(
            parse_percentile_spec("mean").unwrap(),
            PercentileSpec::Mean
        ));
    }

    #[test]
    fn parse_percentile_spec_invalid() {
        assert!(parse_percentile_spec("p0").is_err()); // 0/10 = 0.0 out of range
        assert!(parse_percentile_spec("p").is_err());
        assert!(parse_percentile_spec("pxyz").is_err());
        assert!(parse_percentile_spec("99").is_err());
        assert!(parse_percentile_spec("").is_err());
    }

    #[test]
    fn make_queue_names_single_and_multi() {
        assert_eq!(make_queue_names("default", 1), vec!["default"]);
        assert_eq!(make_queue_names("q", 3), vec!["q_0", "q_1", "q_2"]);
    }

    #[test]
    fn queue_keys_carry_queue_prefix() {
        let names = make_queue_names("default", 1);
        let keys: Vec<String> = names.iter().map(|q| format!("queue:{q}")).collect();
        assert_eq!(keys, vec!["queue:default"]);
    }

    // ── Adversarial edge cases ──────────────────────────────────────────

    #[test]
    fn parse_percentile_spec_rejects_digit_overflow() {
        // Regression test: "p10000000000000000000" (20 digits) parses fine
        // as a u64 (10^19 <= u64::MAX), but 10u64.pow(20) overflows u64 —
        // panics in debug builds, silently wraps in release. Must be a clean
        // error either way, not a crash or a bogus quantile.
        assert!(parse_percentile_spec("p10000000000000000000").is_err());
        // A value that overflows u64 parsing itself must also error cleanly
        // (belt-and-braces — this path was already safe via digits.parse()).
        assert!(parse_percentile_spec("p999999999999999999999999999999").is_err());
        // Still-reasonable long specs must keep working.
        assert!(parse_percentile_spec("p999999999999").is_ok()); // 12 nines
    }

    #[test]
    fn validate_cli_rejects_zero_jobs() {
        let mut cli = base_cli();
        cli.jobs = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_num_queues() {
        let mut cli = base_cli();
        cli.num_queues = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_poll_interval() {
        // The specific case this round of hardening centers on: a literal
        // --poll-interval-ms 0 would make every empty-queue retry sleep for
        // Duration::ZERO, i.e. spin as fast as the network allows — a
        // self-inflicted DoS that could be mistaken for a real result rather
        // than the busy-loop it actually is. Must be rejected outright.
        let mut cli = base_cli();
        cli.poll_interval_ms = 0;
        let err = validate_cli(&cli).unwrap_err();
        assert!(err.to_string().contains("poll-interval-ms"));
    }

    #[test]
    fn validate_cli_rejects_zero_idle_poll_duration() {
        let mut cli = base_cli();
        cli.idle_poll_duration_s = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_timeout() {
        let mut cli = base_cli();
        cli.timeout = 0;
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_empty_workers_list() {
        let mut cli = base_cli();
        cli.workers = vec![];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_rejects_zero_in_workers_list() {
        // A 0 anywhere in --workers (e.g. "--workers 0,10") would spin up a
        // trial with no workers at all: it can never reach its target
        // completion count, so it silently burns the full --timeout instead
        // of failing fast with a clear error.
        let mut cli = base_cli();
        cli.workers = vec![10, 0, 50];
        assert!(validate_cli(&cli).is_err());
    }

    #[test]
    fn validate_cli_accepts_sane_defaults() {
        assert!(validate_cli(&base_cli()).is_ok());
    }

    #[test]
    fn compute_idle_poll_qps_normal_case() {
        // 5 workers, 5000ms interval, one poll each per interval → 1 call/s
        // fleet-wide over a 30s window = 30 calls total.
        let (qps, per_worker) = compute_idle_poll_qps(30, 30.0, 5);
        assert!((qps - 1.0).abs() < 1e-9, "got {qps}");
        assert!((per_worker - 0.2).abs() < 1e-9, "got {per_worker}");
    }

    #[test]
    fn compute_idle_poll_qps_zero_duration_is_safe() {
        // Must not divide by zero / produce inf — the exact boundary
        // validate_cli's --idle-poll-duration-s > 0 check exists to prevent
        // upstream, but this pure function stays defensive independently.
        let (qps, per_worker) = compute_idle_poll_qps(1000, 0.0, 10);
        assert_eq!(qps, 0.0);
        assert_eq!(per_worker, 0.0);
    }

    #[test]
    fn compute_idle_poll_qps_non_finite_duration_is_safe() {
        let (qps_nan, _) = compute_idle_poll_qps(100, f64::NAN, 10);
        let (qps_neg, _) = compute_idle_poll_qps(100, -5.0, 10);
        let (qps_inf, _) = compute_idle_poll_qps(100, f64::INFINITY, 10);
        assert_eq!(qps_nan, 0.0);
        assert_eq!(qps_neg, 0.0);
        // +inf duration is technically "finite() == false" so it also falls
        // back to the safe 0.0 branch rather than computing 100/inf == 0.0
        // legitimately — either way the result must not be NaN/inf itself.
        assert!(qps_inf.is_finite());
    }

    #[test]
    fn compute_idle_poll_qps_zero_workers_is_safe() {
        let (qps, per_worker) = compute_idle_poll_qps(500, 10.0, 0);
        assert!((qps - 50.0).abs() < 1e-9);
        assert_eq!(per_worker, 0.0);
    }

    #[test]
    fn compute_idle_poll_qps_handles_very_small_interval_large_fleet() {
        // Very aggressive config: 1ms poll interval, 5000 workers. Over a
        // 1s window that's up to 5,000,000 calls — well within u64/f64
        // exact-integer range, no overflow, and the fleet-wide number
        // divided back out by worker count should round-trip sanely.
        let (qps, per_worker) = compute_idle_poll_qps(5_000_000, 1.0, 5000);
        assert!((qps - 5_000_000.0).abs() < 1e-6);
        assert!((per_worker - 1000.0).abs() < 1e-6);
    }

    #[test]
    fn compute_idle_poll_qps_huge_counter_stays_finite() {
        // Sanity bound on "could this overflow / produce a misleading
        // number": even at u64::MAX / 2 total calls (nowhere near a real
        // run, but a defensive upper bound) over a normal-length window,
        // the f64 conversion must stay finite and non-negative, not wrap or
        // silently corrupt into a nonsense/negative figure.
        let (qps, per_worker) = compute_idle_poll_qps(u64::MAX / 2, 30.0, 200);
        assert!(qps.is_finite() && qps > 0.0);
        assert!(per_worker.is_finite() && per_worker > 0.0);
    }
}
