use crate::job::ResqueJob;
use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// A Resque-protocol poll worker.
///
/// Resque workers are **not** blocking-command users. `Worker#reserve`
/// (resque/lib/resque/worker.rb:359-370) scans each configured queue in
/// priority order calling `Resque.reserve(queue)` (resque.rb:529), which
/// calls `Job.reserve(queue)` (job.rb:142-145) → `Resque.pop(queue)`
/// (resque.rb ~369-372, `decode(data_store.pop_from_queue(queue))`) →
/// `data_store.pop_from_queue` (data_store.rb:112-113):
///   `@redis.lpop(redis_key_for_queue(queue))`
/// — a single plain **LPOP**, no COUNT argument, no blocking variant anywhere
/// in the call chain.
///
/// The `work(interval, ...)` loop (worker.rb:252-290) branches on whether
/// `work_one_job` found something:
///   - hit: `interval = min_interval` (worker.rb:267-268), loop continues
///     immediately with no sleep.
///   - miss: additive backoff, `interval = (interval + backoff_interval)
///     .clamp(nil, max_interval)` (worker.rb:273-274), then
///     `sleep interval` (worker.rb:278).
///
/// Critically, the *default* production config (`rake resque:work`, see
/// resque/lib/resque/tasks.rb:20-24, which forwards `ENV['MIN_INTERVAL']`,
/// `ENV['MAX_INTERVAL']`, `ENV['BACKOFF_INTERVAL']` — all unset unless the
/// operator opts in) collapses this to a **fixed** interval: with
/// `min_interval == max_interval == interval` (worker.rb:257-261,
/// `max_interval` and `min_interval` both default to `interval` when their
/// env vars are absent), the backoff computation is clamped straight back to
/// the same value every miss. So out-of-the-box Resque polls at a constant
/// rate (default 5.0s, worker.rb:252 `interval = 5.0`) — additive backoff is
/// an opt-in feature, not the default steady-state behavior. This worker
/// models the default (and overwhelmingly common) fixed-interval mode via
/// `--poll-interval-ms`; see README for the (documented, not yet
/// implemented) min/max/backoff extension.
pub struct PollWorker {
    pub metrics: Arc<Metrics>,
    /// Sends latency_us values to the histogram collector task (drain phase only).
    pub latency_tx: mpsc::UnboundedSender<u64>,
    /// Signals the trial orchestrator when target_jobs completions is reached.
    pub done_tx: Arc<watch::Sender<bool>>,
    /// None during the idle-poll phase (run until externally shut down).
    pub target_jobs: Option<u64>,
    pub poll_interval: Duration,
    /// Redis queue keys ("queue:<name>") in the priority order Resque checks
    /// them (worker.rb:359-370, `queues.each`).
    pub queue_keys: Vec<String>,
}

impl PollWorker {
    /// Runs until `shutdown_rx` observes `true`. Each cycle: try LPOP on every
    /// configured queue in order; on a hit, record latency and loop again
    /// immediately (no sleep — matches worker.rb:267-268); on a full pass with
    /// no hits, sleep `poll_interval` then retry (matches worker.rb:270-278).
    pub async fn run(
        self,
        mut conn: redis::aio::MultiplexedConnection,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown_rx.borrow() {
                return;
            }

            let mut hit: Option<String> = None;
            for key in &self.queue_keys {
                self.metrics.inc_poll();
                match redis::cmd("LPOP")
                    .arg(key)
                    .query_async::<Option<String>>(&mut conn)
                    .await
                {
                    Ok(Some(payload)) => {
                        hit = Some(payload);
                        break;
                    }
                    Ok(None) => continue,
                    Err(_) => {
                        self.metrics.inc_error();
                        continue;
                    }
                }
            }

            match hit {
                Some(payload) => {
                    self.record_job(&payload);
                    let done = self.metrics.inc_completed();
                    if let Some(target) = self.target_jobs {
                        if done >= target {
                            let _ = self.done_tx.send(true);
                        }
                    }
                    // No sleep on a hit — worker.rb:267-268 resets interval to
                    // min_interval and loops straight back into work_one_job.
                }
                None => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.poll_interval) => {}
                        _ = shutdown_rx.changed() => {
                            return;
                        }
                    }
                }
            }
        }
    }

    fn record_job(&self, payload: &str) {
        match serde_json::from_str::<ResqueJob>(payload) {
            Ok(job) => match ResqueJob::enqueued_at_ns(&job.args) {
                Some(enqueued_at_ns) => {
                    // Same rationale as job.rs::ResqueJob::new: a clock reading
                    // before the epoch is a host misconfiguration, not something
                    // a malformed queue payload can trigger — but a poll worker
                    // panicking mid-run would silently drop out of the fleet
                    // without ever reporting an error, so fail soft instead.
                    let now_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    let latency_us = if now_ns >= enqueued_at_ns {
                        (now_ns - enqueued_at_ns) / 1_000
                    } else {
                        // Clock skew: avoid a 0 that HDR's lower bound would discard.
                        1
                    };
                    let _ = self.latency_tx.send(latency_us.max(1));
                }
                None => self.metrics.inc_error(),
            },
            Err(_) => self.metrics.inc_error(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::ResqueJob;

    fn test_worker() -> (PollWorker, mpsc::UnboundedReceiver<u64>) {
        let (latency_tx, latency_rx) = mpsc::unbounded_channel();
        let (done_tx, _done_rx) = watch::channel(false);
        let worker = PollWorker {
            metrics: Arc::new(Metrics::new()),
            latency_tx,
            done_tx: Arc::new(done_tx),
            target_jobs: Some(1),
            poll_interval: Duration::from_millis(1),
            queue_keys: vec!["queue:default".to_string()],
        };
        (worker, latency_rx)
    }

    #[test]
    fn record_job_valid_payload_sends_latency() {
        let (worker, mut latency_rx) = test_worker();
        let job = ResqueJob::new(7);
        let payload = serde_json::to_string(&job).unwrap();

        worker.record_job(&payload);

        let latency = latency_rx.try_recv().expect("latency should be sent");
        assert!(latency >= 1);
        assert_eq!(worker.metrics.get_errors(), 0);
    }

    #[test]
    fn record_job_malformed_json_counts_error() {
        let (worker, mut latency_rx) = test_worker();
        worker.record_job("not json");
        assert_eq!(worker.metrics.get_errors(), 1);
        assert!(latency_rx.try_recv().is_err());
    }

    #[test]
    fn record_job_missing_timestamp_arg_counts_error() {
        let (worker, mut latency_rx) = test_worker();
        let payload = serde_json::json!({"class": "LoadWorker", "args": ["only one"]}).to_string();
        worker.record_job(&payload);
        assert_eq!(worker.metrics.get_errors(), 1);
        assert!(latency_rx.try_recv().is_err());
    }

    #[test]
    fn shutdown_flag_is_observable_before_any_poll() {
        let (_tx, rx) = watch::channel(true);
        assert!(*rx.borrow());
    }
}
