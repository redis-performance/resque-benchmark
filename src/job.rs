use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Resque-wire-compatible job payload.
///
/// Resque's envelope is exactly `{"class": "...", "args": [...]}` — see
/// `Resque::Job.create` (resque/lib/resque/job.rb:87-96), which for the
/// non-inline path calls `Resque.push(queue, :class => klass.to_s, :args => args)`
/// (job.rb:95). `Resque#push` (resque/lib/resque.rb:363-365) does
/// `data_store.push_to_queue(queue, encode(item))` where `encode` is
/// `MultiJson.dump` (resque.rb:34-39) over the `{:class, :args}` hash — symbol
/// keys serialize to the JSON string keys "class"/"args" used below.
/// A worked example from the source comments (job.rb:109-110):
///   `{ 'class' => 'UpdateGraph', 'args' => ['defunkt'] }`
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResqueJob {
    pub class: String,
    /// args[3] carries enqueued_at_ns (u64 nanoseconds since epoch) for latency
    /// measurement — a benchmark-only convention (not part of the Resque wire
    /// format itself, which allows arbitrary args). Full layout:
    /// ["string", idx, {"mike":"bob"}, enqueued_at_ns]
    pub args: Vec<serde_json::Value>,
}

impl ResqueJob {
    pub fn new(idx: u64) -> Self {
        // duration_since only errs if the system clock reads before the Unix
        // epoch (a misconfigured host clock, not attacker input) — fall back
        // to 0 rather than panic and take down the whole producer loop.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let enqueued_at_ns = now.as_nanos() as u64;

        ResqueJob {
            class: "LoadWorker".to_string(),
            args: vec![
                serde_json::Value::String("string".to_string()),
                serde_json::Value::Number(idx.into()),
                serde_json::json!({"mike": "bob"}),
                serde_json::Value::Number(enqueued_at_ns.into()),
            ],
        }
    }

    /// Extract the enqueue timestamp embedded in args[3] (nanoseconds since epoch).
    #[allow(dead_code)]
    pub fn enqueued_at_ns(args: &[serde_json::Value]) -> Option<u64> {
        args.get(3)?.as_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_envelope_matches_resque_shape() {
        let job = ResqueJob::new(42);
        let json = serde_json::to_value(&job).unwrap();
        let obj = json.as_object().unwrap();
        // Exactly two top-level fields: class, args — matching job.rb:95
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("class"));
        assert!(obj.contains_key("args"));
    }

    #[test]
    fn job_args_roundtrip() {
        let job = ResqueJob::new(42);
        let json = serde_json::to_string(&job).unwrap();
        let back: ResqueJob = serde_json::from_str(&json).unwrap();
        assert_eq!(back.args[1].as_u64().unwrap(), 42);
        assert!(ResqueJob::enqueued_at_ns(&back.args).is_some());
    }

    #[test]
    fn enqueued_at_ns_missing_when_args_short() {
        assert_eq!(ResqueJob::enqueued_at_ns(&[]), None);
        assert_eq!(
            ResqueJob::enqueued_at_ns(&[serde_json::Value::String("x".into())]),
            None
        );
    }
}
