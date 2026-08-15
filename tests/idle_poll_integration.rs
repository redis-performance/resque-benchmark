//! Real-Redis integration tests.
//!
//! These drive actual `resque_bench::worker::PollWorker` tasks and
//! `resque_bench::producer` calls against a **live** Redis instance — real
//! RPUSH/LPOP traffic over the wire, not a mock — and assert on the
//! resulting numbers as an automated regression check, rather than
//! something that was only ever eyeballed once during development.
//!
//! CI always has Redis available (the `services:` block in `ci.yml` runs
//! `redis:8` on `127.0.0.1:6379`). Locally, if nothing is reachable at
//! `REDIS_URL` (default `redis://127.0.0.1:6379/15` — db 15, kept distinct
//! from the db-0 smoke test in `ci.yml`/`CONTRIBUTING.md` so the two never
//! collide when run against the same instance), each test prints a `SKIP:`
//! notice and returns early instead of failing, so `cargo test` still works
//! for contributors who haven't started a local Redis.

use futures_util::StreamExt;
use redis::AsyncCommands;
use resque_bench::{job::ResqueJob, metrics::Metrics, producer, worker::PollWorker};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

fn test_redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".to_string())
}

/// Build a client and prove it's reachable, or print a SKIP notice and
/// return None. Every test starts by calling this and returning early on
/// None — see module doc for why.
async fn connect_or_skip() -> Option<redis::Client> {
    let url = test_redis_url();
    let client = match redis::Client::open(url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP: invalid REDIS_URL '{url}': {e}");
            return None;
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(_)) => Some(client),
        _ => {
            eprintln!(
                "SKIP: no Redis reachable at {url} — start one locally to run these \
                 integration tests (CI always has one via ci.yml's `services:` block)"
            );
            None
        }
    }
}

async fn open_conns(client: &redis::Client, n: usize) -> Vec<redis::aio::MultiplexedConnection> {
    let mut conns = Vec::with_capacity(n);
    for _ in 0..n {
        conns.push(client.get_multiplexed_async_connection().await.unwrap());
    }
    conns
}

/// Spawn `n` PollWorkers with `target_jobs: None` (idle-poll mode), let them
/// run for exactly `run_for`, then shut them down and return the raw poll
/// count plus the actual wall-clock elapsed time.
async fn run_idle_workers(
    client: &redis::Client,
    queue_keys: Vec<String>,
    n_workers: usize,
    poll_interval: Duration,
    run_for: Duration,
) -> (u64, Duration) {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, _done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();
    let drain = tokio::spawn(async move { while latency_rx.recv().await.is_some() {} });

    let conns = open_conns(client, n_workers).await;
    let mut handles = Vec::with_capacity(n_workers);
    for conn in conns {
        let w = PollWorker {
            metrics: metrics.clone(),
            latency_tx: latency_tx.clone(),
            done_tx: done_tx.clone(),
            target_jobs: None,
            poll_interval,
            queue_keys: queue_keys.clone(),
        };
        handles.push(tokio::spawn(w.run(conn, shutdown_rx.clone())));
    }
    drop(latency_tx);

    let start = Instant::now();
    tokio::time::sleep(run_for).await;
    let elapsed = start.elapsed();

    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
    drain.abort();

    (metrics.get_polls(), elapsed)
}

/// Spawn `n` PollWorkers with `target_jobs: Some(n_jobs)` (drain mode), wait
/// for them to reach that many completions (or time out), shut them down,
/// and return (completed, errors, whether it finished before timing out).
async fn run_drain_workers(
    client: &redis::Client,
    queue_keys: Vec<String>,
    n_workers: usize,
    poll_interval: Duration,
    n_jobs: u64,
    timeout: Duration,
) -> (u64, u64, bool) {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();
    let drain = tokio::spawn(async move { while latency_rx.recv().await.is_some() {} });

    let conns = open_conns(client, n_workers).await;
    let mut handles = Vec::with_capacity(n_workers);
    for conn in conns {
        let w = PollWorker {
            metrics: metrics.clone(),
            latency_tx: latency_tx.clone(),
            done_tx: done_tx.clone(),
            target_jobs: Some(n_jobs),
            poll_interval,
            queue_keys: queue_keys.clone(),
        };
        handles.push(tokio::spawn(w.run(conn, shutdown_rx.clone())));
    }
    drop(latency_tx);

    let finished = tokio::time::timeout(timeout, done_rx.wait_for(|v| *v))
        .await
        .is_ok();

    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
    drain.abort();

    (metrics.get_completed(), metrics.get_errors(), finished)
}

/// Regression check for the tool's headline metric: with N workers each
/// polling an empty queue on a fixed interval, the fleet-wide LPOP/s should
/// land close to `workers * 1000 / poll_interval_ms` — the same sanity
/// check documented in the README. This asserts it automatically instead of
/// relying on someone eyeballing a console run.
#[tokio::test]
async fn idle_poll_qps_matches_theoretical_rate_within_tolerance() {
    let Some(client) = connect_or_skip().await else {
        return;
    };
    let queue = "resque_bench_inttest_idle_qps".to_string();
    let queue_key = format!("queue:{queue}");
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    producer::clear_queue(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();

    let n_workers = 5usize;
    let poll_interval = Duration::from_millis(100);
    let run_for = Duration::from_secs(5);

    let (total_polls, elapsed) = run_idle_workers(
        &client,
        vec![queue_key.clone()],
        n_workers,
        poll_interval,
        run_for,
    )
    .await;

    let expected_qps = n_workers as f64 * 1000.0 / poll_interval.as_millis() as f64;
    let actual_qps = total_polls as f64 / elapsed.as_secs_f64();

    // Empty queue the whole time — no real hits should have occurred.
    let remaining = producer::total_queue_len(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();
    assert_eq!(remaining, 0, "queue was never populated, should stay empty");

    let tolerance = 0.30; // scheduler jitter over a short window; generous on purpose
    let lower = expected_qps * (1.0 - tolerance);
    let upper = expected_qps * (1.0 + tolerance);
    assert!(
        actual_qps >= lower && actual_qps <= upper,
        "idle_poll_qps {actual_qps:.1} not within {tolerance:.0}% of theoretical \
         {expected_qps:.1} (workers={n_workers} * 1000 / poll_interval_ms={}) — \
         total_polls={total_polls}, elapsed={:.2}s",
        poll_interval.as_millis(),
        elapsed.as_secs_f64(),
    );

    // The shutdown signal must be race-free regardless of poll_interval —
    // wall-clock elapsed should track the requested duration closely, not
    // overrun waiting on a blocking call or a slow shutdown path.
    assert!(
        (elapsed.as_secs_f64() - run_for.as_secs_f64()).abs() < 1.0,
        "idle-poll phase took {:.2}s, requested {:.2}s — shutdown may not be race-free",
        elapsed.as_secs_f64(),
        run_for.as_secs_f64(),
    );

    producer::clear_queue(&mut conn, &[queue]).await.unwrap();
}

/// Regression check for the drain→idle-poll transition race: if the drain
/// phase's queue-empty signal were trusted blindly (or based on a timeout
/// rather than a real LLEN check), leftover backlog could leak into the
/// idle-poll measurement and inflate idle_poll_qps with real job hits
/// instead of true empty-queue polling load. This enqueues real jobs,
/// drains them for real, verifies the queue is actually empty via
/// `producer::total_queue_len` (the same check `main.rs` now performs before
/// starting the idle-poll phase), and only then measures idle QPS — proving
/// the two phases don't contaminate each other.
#[tokio::test]
async fn drain_then_idle_poll_has_no_backlog_contamination() {
    let Some(client) = connect_or_skip().await else {
        return;
    };
    let queue = "resque_bench_inttest_drain_contam".to_string();
    let queue_key = format!("queue:{queue}");
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    producer::clear_queue(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();

    let n_jobs = 300u64;
    producer::bulk_enqueue(&mut conn, std::slice::from_ref(&queue), n_jobs)
        .await
        .unwrap();

    let n_workers = 4usize;
    let poll_interval = Duration::from_millis(50);
    let (completed, errors, finished) = run_drain_workers(
        &client,
        vec![queue_key.clone()],
        n_workers,
        poll_interval,
        n_jobs,
        Duration::from_secs(30),
    )
    .await;

    assert!(finished, "drain did not reach target within 30s timeout");
    assert_eq!(completed, n_jobs, "should have dequeued exactly n_jobs");
    assert_eq!(
        errors, 0,
        "no malformed payloads expected — we wrote them ourselves"
    );

    // The race-free signal: total_queue_len must be exactly 0 now that
    // `completed == n_jobs` (every completion is a job actually popped, and
    // exactly n_jobs were pushed) — this is the same check main.rs performs
    // before trusting the idle-poll phase.
    let remaining = producer::total_queue_len(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();
    assert_eq!(
        remaining, 0,
        "drain completed to target but queue is not empty"
    );

    // Now measure idle QPS on the verified-empty queue — this must reflect
    // true idle polling (small, interval-governed), not leftover drain
    // throughput (which would look orders of magnitude higher, since a hit
    // loops with no sleep — see worker.rs).
    let run_for = Duration::from_secs(3);
    let (total_polls, elapsed) =
        run_idle_workers(&client, vec![queue_key], n_workers, poll_interval, run_for).await;

    let expected_qps = n_workers as f64 * 1000.0 / poll_interval.as_millis() as f64;
    let actual_qps = total_polls as f64 / elapsed.as_secs_f64();
    let tolerance = 0.35;
    assert!(
        actual_qps <= expected_qps * (1.0 + tolerance),
        "idle_poll_qps {actual_qps:.1} looks contaminated by drain-phase activity — \
         expected close to {expected_qps:.1} (workers * 1000 / poll_interval_ms)"
    );

    producer::clear_queue(&mut conn, &[queue]).await.unwrap();
}

/// Behavioral proof that only LPOP (dequeue) and RPUSH (enqueue) ever hit
/// the wire — never a blocking variant (BLPOP/BRPOP/BLMPOP/BLMOVE/
/// BRPOPLPUSH). Uses a real `MONITOR` connection rather than a timing
/// heuristic: it's the direct, unambiguous way to see every command byte
/// actually sent, and Resque's real protocol (see worker.rs / README
/// "Protocol compatibility") is single-key, non-blocking, no COUNT
/// argument — this is exactly the claim worth pinning down precisely.
#[tokio::test]
async fn only_lpop_and_rpush_observed_on_wire() {
    let Some(client) = connect_or_skip().await else {
        return;
    };
    let queue = "resque_bench_inttest_monitor".to_string();
    let queue_key = format!("queue:{queue}");
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    producer::clear_queue(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();

    let mut monitor = client.get_async_monitor().await.unwrap();
    let mut messages = monitor.on_message::<String>();

    // Drive real enqueue + drain + a bit of idle polling while MONITOR is
    // attached.
    producer::bulk_enqueue(&mut conn, std::slice::from_ref(&queue), 20)
        .await
        .unwrap();
    let (completed, _errors, finished) = run_drain_workers(
        &client,
        vec![queue_key.clone()],
        3,
        Duration::from_millis(50),
        20,
        Duration::from_secs(10),
    )
    .await;
    assert!(finished && completed == 20);
    // A short idle-poll burst so LPOP misses are captured too.
    let _ = run_idle_workers(
        &client,
        vec![queue_key],
        3,
        Duration::from_millis(50),
        Duration::from_millis(500),
    )
    .await;

    // Collect whatever MONITOR captured in a bounded window, then stop.
    let mut lines = Vec::new();
    let collect_deadline = Duration::from_secs(2);
    let collected = tokio::time::timeout(collect_deadline, async {
        while let Some(line) = messages.next().await {
            lines.push(line);
            if lines.len() >= 200 {
                break;
            }
        }
    })
    .await;
    let _ = collected; // timeout is expected — MONITOR never ends on its own

    assert!(
        !lines.is_empty(),
        "MONITOR captured no traffic at all — test is not exercising anything"
    );

    let blocking = ["BLPOP", "BRPOP", "BLMPOP", "BLMOVE", "BRPOPLPUSH"];
    let mut saw_lpop = false;
    let mut saw_rpush = false;
    for line in &lines {
        let upper = line.to_uppercase();
        for b in blocking {
            assert!(
                !upper.contains(b),
                "observed a blocking command on the wire: {line}"
            );
        }
        if upper.contains("\"LPOP\"") {
            saw_lpop = true;
        }
        if upper.contains("\"RPUSH\"") {
            saw_rpush = true;
        }
    }
    assert!(
        saw_lpop,
        "expected at least one LPOP in captured MONITOR traffic: {lines:?}"
    );
    assert!(
        saw_rpush,
        "expected at least one RPUSH in captured MONITOR traffic: {lines:?}"
    );

    producer::clear_queue(&mut conn, &[queue]).await.unwrap();
}

/// Sanity check that the benchmark-only `enqueued_at_ns` convention
/// round-trips through a real RPUSH/LPOP cycle (not just the in-memory
/// serde round-trip already covered by job::tests) — i.e. latency
/// measurement works against a real Redis, not just against a String.
#[tokio::test]
async fn real_dequeue_recovers_job_index_and_timestamp() {
    let Some(client) = connect_or_skip().await else {
        return;
    };
    let queue = "resque_bench_inttest_job_shape".to_string();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    producer::clear_queue(&mut conn, std::slice::from_ref(&queue))
        .await
        .unwrap();
    producer::bulk_enqueue(&mut conn, std::slice::from_ref(&queue), 1)
        .await
        .unwrap();

    let raw: String = conn.lpop(format!("queue:{queue}"), None).await.unwrap();
    let job: ResqueJob = serde_json::from_str(&raw).unwrap();
    assert_eq!(job.class, "LoadWorker");
    assert_eq!(job.args[1].as_u64(), Some(0));
    assert!(ResqueJob::enqueued_at_ns(&job.args).is_some());

    producer::clear_queue(&mut conn, &[queue]).await.unwrap();
}
