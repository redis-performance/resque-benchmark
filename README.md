# resque-benchmark

[![CI](https://github.com/redis-performance/resque-benchmark/actions/workflows/ci.yml/badge.svg)](https://github.com/redis-performance/resque-benchmark/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/redis/resque-benchmark)](https://hub.docker.com/r/redis/resque-benchmark)
[![Docker Image Size](https://img.shields.io/docker/image-size/redis/resque-benchmark/latest)](https://hub.docker.com/r/redis/resque-benchmark)
[![Docker Platforms](https://img.shields.io/badge/platform-linux%2Famd64%20%7C%20linux%2Farm64-blue)](https://hub.docker.com/r/redis/resque-benchmark)

A [Resque](https://github.com/resque/resque) protocol load benchmark written
in Rust. Measures job throughput and full latency spectrum (p50→p99.99)
against any Redis endpoint — **and**, because Resque has no blocking dequeue
command, measures the steady-state polling load an idle worker fleet puts on
Redis, as a first-class metric rather than an afterthought.

## Why this exists

Resque is architecturally different from Sidekiq/BullMQ-style queues: it does
not block waiting for work. A Resque worker polls with a plain `LPOP` and, if
the queue is empty, sleeps before trying again (see
[Protocol compatibility](#protocol-compatibility) below). A fleet of N idle
Resque workers is therefore not "zero cost" the way a fleet of N idle
`BRPOP`-blocked clients is — it's a steady drumbeat of `LPOP` commands hitting
Redis, whether or not there's anything to dequeue. That poll-storm load is the
main reason to benchmark Resque's *actual* wire behavior instead of modeling
it as "yet another blocking-queue client."

## Why Rust?

| | Ruby `rake resque:work` | This tool |
|---|---|---|
| Per-job overhead | Forks a child process per job by default (`fork_per_job?`, worker.rb:821-823 — `ENV["FORK_PER_JOB"] != 'false'`) | No forking — tokio async tasks |
| Concurrency model | One OS process per worker; `COUNT=N rake resque:workers` spawns N separate Ruby processes (tasks.rb `resque:workers`) | Scales to 200+ concurrent workers as async tasks in one process |
| Idle-poll load visibility | None — polling is opaque, buried in worker.rb's `sleep` loop | First-class metric: LPOP calls/sec, fleet-wide and per-worker |
| Latency recording | None | HDRHistogram per job (p50→p99.99) |
| Per-second time series | None | throughput + latency percentiles + errors, per second |
| Multi-queue | Priority-ordered `QUEUE=high,low` string, no built-in load distribution | `--num-queues N` (round-robin producer distribution; workers scan in priority order like real Resque) |
| Dependency | Resque gem + Ruby + Redis | Single static binary |

## Protocol compatibility

This tool implements the real Resque wire protocol from source — not an
approximation. Every claim below is cited to file + line in
[`resque/resque`](https://github.com/resque/resque) (`master` branch, checked
2026-08-13). If you're modifying this tool, re-verify against current
upstream source rather than trusting this table — Resque's source can move.

### Queue key naming

Queue keys are `queue:<name>` — `redis_key_for_queue`
(`lib/resque/data_store.rb:165-167`):
```ruby
def redis_key_for_queue(queue)
  "queue:#{queue}"
end
```
There is no additional `resque:` namespace prefix at this layer.

### Enqueue: RPUSH

`QueueAccess#push_to_queue` (`lib/resque/data_store.rb:104-109`):
```ruby
def push_to_queue(queue,encoded_item)
  @redis.pipelined do |piped|
    watch_queue(queue, redis: piped)
    piped.rpush redis_key_for_queue(queue), encoded_item
  end
end
```
`watch_queue` (`data_store.rb:150-152`) does `redis.sadd(:queues,
[queue.to_s])` — every push registers the queue in the `queues` set for
resque-web visibility. `producer.rs::bulk_enqueue` does both: `RPUSH
queue:<name>` per job, `SADD queues <name>` once per queue.

### Dequeue: plain LPOP — no blocking command anywhere in the call chain

`QueueAccess#pop_from_queue` (`data_store.rb:112-113`):
```ruby
def pop_from_queue(queue)
  @redis.lpop(redis_key_for_queue(queue))
end
```
Reached via `Worker#reserve` (`lib/resque/worker.rb:359-370`, iterates
configured queues in priority order) → `Resque.reserve(queue)`
(`lib/resque.rb:529`, `Job.reserve(queue)`) → `Job.reserve`
(`lib/resque/job.rb:142-145`, `Resque.pop(queue)`) → `Resque#pop`
(`lib/resque.rb`, `decode(data_store.pop_from_queue(queue))`) →
`pop_from_queue` above. A single-key, single-value `LPOP` — no `COUNT`
argument, no `BLPOP`/`BRPOP`/`BLMPOP` anywhere in this chain.
`worker.rs::PollWorker::run` issues exactly this: `LPOP queue:<name>`, tried
against each configured queue key in order per poll cycle.

### Job envelope

`Job.create` (`lib/resque/job.rb:87-96`):
```ruby
def self.create(queue, klass, *args)
  Resque.validate(klass, queue)
  if Resque.inline?
    new(:inline, {'class' => klass, 'args' => decode(encode(args))}).perform
  else
    Resque.push(queue, :class => klass.to_s, :args => args)
  end
end
```
`Resque#push` (`lib/resque.rb:363-365`) does
`data_store.push_to_queue(queue, encode(item))`, where `encode`
(`resque.rb:34-39`) is `MultiJson.dump` — symbol keys `:class`/`:args`
serialize to JSON string keys `"class"`/`"args"`. The source's own worked
example (`job.rb:109-110`):
```ruby
{ 'class' => 'UpdateGraph', 'args' => ['defunkt'] }
```
`job.rs::ResqueJob` mirrors this exactly — two top-level fields, `class` and
`args`. `args[3]` additionally carries a nanosecond `enqueued_at` timestamp
(a benchmark-only convention, since Resque's `args` accepts arbitrary
arguments) so dequeue latency is measurable, the same technique
`sidekiq-benchmark` uses.

### Poll interval: fixed by default, *optionally* additive backoff

`Worker#work` (`worker.rb:252-290`):
```ruby
def work(interval = 5.0,
         min_interval: nil,     # defaults to interval
         max_interval: nil,     # defaults to interval
         backoff_interval: nil, # defaults to 0.1
         &block)
  interval = Float(interval || 5.0)
  max_interval = Float(max_interval || interval)
  min_interval = Float(min_interval || interval).clamp(nil, max_interval)
  backoff_interval = Float(backoff_interval || 0.1).clamp(nil, max_interval)
  interval = interval.clamp(min_interval, max_interval)

  loop do
    break if shutdown?
    if work_one_job(&block)
      interval = min_interval
    else
      break if interval.zero?
      interval = (interval + backoff_interval).clamp(nil, max_interval)
      sleep interval
    end
  end
  ...
```
On a **hit**, `interval` resets to `min_interval` and the loop continues
immediately with no sleep (`worker.rb:267-268`). On a **miss**, `interval`
grows *additively* (not exponentially/doubling) by `backoff_interval` each
cycle, clamped to `max_interval`, then sleeps that long (`worker.rb:270-278`).

**The default production configuration collapses this to a fixed interval.**
`rake resque:work` (`lib/resque/tasks.rb:20-24`) calls:
```ruby
worker.work(ENV["INTERVAL"],
            max_interval:     ENV['MAX_INTERVAL'],
            min_interval:     ENV['MIN_INTERVAL'],
            backoff_interval: ENV['BACKOFF_INTERVAL'])
```
When `MIN_INTERVAL`/`MAX_INTERVAL` are unset (the overwhelmingly common
case), `min_interval == max_interval == interval` (both default to
`interval` per `worker.rb:258-259`), so the additive-backoff arithmetic on
every miss is clamped straight back to the same value it started at —
observably a **fixed poll interval**, default 5000 ms (`worker.rb:252,
interval = 5.0`). Additive backoff is an *opt-in* feature (setting
`MIN_INTERVAL`/`MAX_INTERVAL` to different values), not Resque's steady-state
default behavior.

`worker.rs::PollWorker` models exactly the default (fixed-interval) mode via
`--poll-interval-ms` (default `5000`, matching Resque's own default): sleep
`poll_interval` after a miss, no sleep after a hit. It does **not** currently
implement the opt-in additive-backoff extension — a benchmark run always uses
a single fixed interval per trial. If you need to model an operator who sets
`MIN_INTERVAL`/`MAX_INTERVAL`/`BACKOFF_INTERVAL` explicitly, that would be a
natural follow-up (`--poll-min-interval-ms`/`--poll-max-interval-ms`/
`--poll-backoff-ms`), but it is a deliberately deferred non-default case, not
a hidden approximation of the default one.

## Quick start

### Docker Hub

Multi-platform image (`linux/amd64`, `linux/arm64`) published to
[`redis/resque-benchmark`](https://hub.docker.com/r/redis/resque-benchmark):

```bash
docker pull redis/resque-benchmark:latest
```

> **Memory:** the default run pre-fills 500,000 jobs (~250 B each) → **~120
> MB** peak Redis memory before workers drain the queue. Use `--jobs 50000`
> (~12 MB) for a quick local smoke test.

```bash
# Run against local Redis (default: db 13, 500k jobs, workers 10/50/100/200)
docker run --rm --network host redis/resque-benchmark

# Lighter local run
docker run --rm --network host redis/resque-benchmark \
  --workers 10,50 --jobs 50000

# Custom settings
docker run --rm --network host redis/resque-benchmark \
  --url redis://127.0.0.1:6379/0 \
  --workers 10,50,100 \
  --jobs 100000 \
  --num-queues 4

# Point at a remote Redis
docker run --rm redis/resque-benchmark \
  --url redis://myhost:6379/0 \
  --workers 50,100,200 \
  --jobs 500000 \
  --output -
```

### docker compose (Redis included)

```bash
# Start Redis + run benchmark
docker compose run --rm bench

# Use a different Redis image
REDIS_IMAGE=redis:7.4 docker compose run --rm bench

# Point at an external Redis
REDIS_URL=redis://myhost:6379/0 docker compose run --rm bench
```

### Install from GitHub Release

Pre-built static binaries for `x86_64` and `aarch64` Linux are attached to every
[release](https://github.com/redis-performance/resque-benchmark/releases), each with
a `.sha256` checksum alongside it:

```bash
# Pick one target: linux-x86_64-gnu or linux-aarch64-gnu
TARGET=linux-x86_64-gnu
VERSION=v0.1.0

curl -sLO https://github.com/redis-performance/resque-benchmark/releases/download/$VERSION/resque-bench-$VERSION-$TARGET.tar.gz
curl -sLO https://github.com/redis-performance/resque-benchmark/releases/download/$VERSION/resque-bench-$VERSION-$TARGET.tar.gz.sha256
sha256sum -c resque-bench-$VERSION-$TARGET.tar.gz.sha256

tar xzf resque-bench-$VERSION-$TARGET.tar.gz
./resque-bench-$VERSION-$TARGET/resque-bench --help
```

### From source

```bash
cargo build --release
./target/release/resque-bench --workers 5 --jobs 10000
```

## CLI flags

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--url` | `REDIS_URL` | `redis://127.0.0.1:6379/13` | Full Redis URL |
| `--host` | — | — | Override host component of URL |
| `--port` | — | — | Override port component of URL |
| `--password` | `REDIS_PASSWORD` | — | Auth (prefer env var — CLI exposes it in `ps`) |
| `--tls` | `REDIS_TLS` | false | Enable TLS (`rediss://`) |
| `--db` | — | `13` | Database number (not a Resque convention — chosen for parity with sidekiq-benchmark's safety default) |
| `--workers` | — | `10,50,100,200` | Comma-separated concurrency levels — one trial each |
| `--jobs` | — | `500000` | Total jobs per drain trial |
| `--warmup-jobs` | — | `0` | Warmup pass before each trial (0 = skip) |
| `--queue` | — | `default` | Base Resque queue name |
| `--num-queues` | — | `1` | Number of queues (jobs distributed round-robin); names are `<queue>_0…<queue>_{N-1}` when N > 1. Workers scan queues in priority order every poll, matching `Worker#reserve` |
| `--latency-percentiles` | — | `p50,p90,p99,p999,max` | Per-second latency series to record; supports `p50`, `p75`, `p90`, `p95`, `p99`, `p999`, `p9999`, `max`, `mean` |
| `--tag` | — | from Redis `INFO` | Label for output filename and JSON |
| `--output` | — | `resque_bench_<tag>.json` | JSON output path; `-` for stdout |
| `--timeout` | — | `300` | Drain-phase per-trial timeout in seconds |
| `--quiet` | — | false | Suppress per-second progress dots |
| `--allow-flushdb` | `RESQUE_BENCH_ALLOW_FLUSHDB` | false | FLUSHDB before each trial (default: DEL only the queue keys — safe on shared Redis) |
| `--poll-interval-ms` | — | `5000` | Fixed poll interval between empty-queue LPOP retries. Matches Resque's own default (`worker.rb:252`, `interval = 5.0`) — see [Protocol compatibility](#protocol-compatibility) |
| `--idle-poll-duration-s` | — | `30` | How long to run the idle-poll measurement phase after each trial's queue has drained |

### Multi-queue mode

Resque workers check configured queues in a fixed priority order every poll
cycle (`worker.rb:359-370`). With `--num-queues N > 1`, an idle poll costs up
to N `LPOP` calls per worker per cycle — the idle-poll-QPS number scales with
`num_queues`, not just `workers`. Keep `--num-queues 1` (the default) if you
want the simple `workers × (1000 / poll-interval-ms)` sanity check to hold
exactly.

## Output

**Console:**
```
=== resque-bench — redis-8.6 ===
    redis://127.0.0.1:6379/13  jobs=500,000  queues=default  poll-interval=5000ms

  [  10 workers] ........  9,842 jobs/s  p50=620 µs  p99=3.1 ms  p99.9=6.9 ms  max=52 ms
  [  10 workers] idle-poll running (30s)…   [  10 workers]  idle-poll:        2.0 LPOP/s  (0.2 LPOP/s/worker over 30.0s, 60 calls)

--- Drain summary (backlog throughput + latency) ---
+---------+--------+--------+--------+---------+---------+--------+
| Workers | jobs/s | p50    | p99    | p99.9   | max     | errors |
+=========+========+========+========+=========+=========+========+
|      10 |  9,842 | 620 µs | 3.1 ms | 6.9 ms  | 52 ms   | 0      |
+---------+--------+--------+--------+---------+---------+--------+

--- Idle-poll summary (steady-state polling load, empty queue) ---
+---------+------------+------------+----------------+---------------+
| Workers | duration_s | LPOP calls | LPOP/s (fleet) | LPOP/s/worker |
+=========+============+============+================+===============+
|      10 | 30.0       | 60         | 2.0            | 0.20          |
+---------+------------+------------+----------------+---------------+
Results saved → resque_bench_redis-8.6.json
```

At the default `--poll-interval-ms 5000`, idle-poll QPS is intentionally
small (0.2/worker = 1000ms / 5000ms) — that's the real, unglamorous cost of
Resque's default poll rate. Lower `--poll-interval-ms` to stress-test more
aggressive (non-default) worker configurations.

**JSON** (`resque_bench_<tag>.json`):

```json
{
  "tag": "redis-8.6",
  "timestamp": "2026-08-13T01:30:00Z",
  "config": {
    "url": "redis://127.0.0.1:6379/13",
    "workers": [10, 50, 100, 200],
    "jobs_per_trial": 500000,
    "queues": ["default"],
    "warmup_jobs": 0,
    "poll_interval_ms": 5000,
    "idle_poll_duration_s": 30
  },
  "results": [{
    "workers": 10,
    "total_jobs": 500000,
    "duration_s": 50.8,
    "jobs_per_sec": 9842.5,
    "timed_out": false,
    "throughput_per_sec": [9900, 9820, 9805],
    "errors_per_sec":     [0, 0, 0],
    "latency_per_sec_us": {
      "p50":  [610, 625, 615],
      "p90":  [1800, 1850, 1790],
      "p99":  [3050, 3120, 3080],
      "p999": [6800, 6950, 6900],
      "max":  [51000, 52000, 49000]
    },
    "latency_us": {
      "p50": 620, "p75": 900, "p90": 1820,
      "p95": 2100, "p99": 3100, "p99_9": 6900,
      "p99_99": 15000, "max": 52000,
      "mean": 780.4, "total_count": 500000
    },
    "errors": 0,
    "idle_poll": {
      "duration_s": 30.02,
      "total_lpop_calls": 60,
      "idle_poll_qps": 2.0,
      "per_worker_qps": 0.2
    }
  }]
}
```

All latency values are in **microseconds**. `latency_per_sec_us` contains one
value per elapsed second of the trial. The `idle_poll` object is a clearly
separate section — Sidekiq-benchmark has no equivalent field, since `BRPOP`
blocks and there is no polling load to measure.

> **Note on latency:** the benchmark pre-fills the queue then starts workers.
> Latency = time a job spends in the queue until dequeued (wall-clock, same
> host as producer). Workers dequeue via plain **LPOP** (real Resque
> protocol, not an approximation — see Protocol compatibility).

> **Password safety:** passwords passed via `--password` are visible in `ps aux`.
> Prefer the `REDIS_PASSWORD` environment variable. Passwords are redacted
> (`****`) in all output and JSON.

## Safety notes

### Default database: 13

The default Redis database is **13**. This is not a Resque convention (Resque
has none) — it's chosen purely for parity with the sister `sidekiq-benchmark`
tool's safety default, to avoid colliding with application data (typically db
0) and to make `--allow-flushdb` safe by default. Always confirm the target
db before running against a shared Redis.

### Shared / production Redis

Do **not** run this benchmark against a production Redis instance. The
benchmark pre-fills the queue with hundreds of thousands of jobs and
(optionally) flushes the entire database. Use a dedicated benchmark instance
or an isolated database number.

### Intentionally omitted Resque housekeeping keys

Production Resque writes additional bookkeeping (`stat:processed`,
`stat:failed`, `worker:*` heartbeat hashes, the `workers` set) as a
side-effect of normal worker operation. This benchmark measures **queue
mechanics in isolation** — enqueue throughput, LPOP dequeue latency, and idle
poll load — so those keys are intentionally omitted.

## Building

Requires Rust stable (1.75+).

```bash
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
```

`cargo test` includes real-Redis integration tests
(`tests/idle_poll_integration.rs`) that enqueue real jobs, drain them with
real workers, and assert `idle_poll_qps` lands within tolerance of
`workers * 1000 / poll_interval_ms` — the same sanity check documented
above, checked automatically rather than eyeballed. They target
`REDIS_URL` (default `redis://127.0.0.1:6379/15`) and skip gracefully
with a `SKIP:` notice if nothing is reachable there, so plain `cargo test`
still works without a local Redis running.

## Docker image

Multi-platform image (`linux/amd64`, `linux/arm64`) published to
[`redis/resque-benchmark`](https://hub.docker.com/r/redis/resque-benchmark)
on every push to `main`. Tagged `latest` on main; semver tags (`1.0.0`,
`1.0`) on `v*` git tags.

```bash
# Pull and run
docker pull redis/resque-benchmark
docker run --rm --network host redis/resque-benchmark --workers 10 --jobs 50000

# Build locally
docker build -t resque-bench .
docker run --rm resque-bench --url redis://host:6379/0 --workers 10 --jobs 50000
```

## License

Apache-2.0
