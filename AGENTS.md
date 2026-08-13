# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`resque-benchmark` is a Resque protocol load benchmark written in Rust. Unlike
Sidekiq (BRPOP), Resque workers dequeue via plain, non-blocking **LPOP** and
sleep a configurable interval when the queue is empty (see README "Protocol
compatibility" for exact source citations). This tool measures two distinct
things: (1) drain-phase throughput and dequeue latency when the queue has a
backlog, and (2) steady-state **idle-poll QPS** — the LPOP calls/sec an idle
worker fleet issues against Redis while the queue is empty. The second number
is the whole reason this tool exists: a synthetic benchmark that only measures
drain throughput would miss the poll-storm cost that is Resque's actual
distinguishing characteristic versus a blocking-command client.

The job envelope, queue key naming, and push/pop commands are implemented to
match verified Resque source (`resque/resque` on GitHub), not assumed by
analogy with Sidekiq. If you touch `producer.rs`, `worker.rs`, or `job.rs`,
re-verify the relevant behavior against current upstream source before
changing it — do not trust prior citations blindly, Resque's source can move.

## Local setup

Requires Rust stable (1.75+). No submodules — the protocol layer talks to
Redis directly via the `redis` crate; there is no vendored Ruby-compatible
worker crate (unlike `sidekiq-benchmark`'s `sidekiq-rs`).

```bash
git clone git@github.com:redis-performance/resque-benchmark.git
cd resque-benchmark
cargo build --release
```

Verify the build:

```bash
# Requires a running Redis on 127.0.0.1:6379
./target/release/resque-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --poll-interval-ms 100 \
  --idle-poll-duration-s 3 \
  --output -
```

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Protocol-behavior comments (queue keys, RPUSH/LPOP, envelope shape, poll
  timing) are the exception: they carry file+line citations into Resque
  source on purpose. Keep citations exact; update them if the behavior
  changes.
- Do not introduce new dependencies without checking with the maintainer.

## Running tests

Run the full suite before declaring a task complete:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

For a full end-to-end smoke test (requires Redis on `127.0.0.1:6379`):

```bash
cargo run --release -- \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --poll-interval-ms 100 \
  --idle-poll-duration-s 3 \
  --timeout 60 \
  --output /tmp/smoke.json \
  --quiet \
  --tag smoke
```

To confirm the wire protocol is what it claims to be, run a small job against
Redis while tailing `redis-cli MONITOR` and confirm only `RPUSH` (enqueue) and
plain `LPOP` (dequeue) appear — never `BLPOP`/`BRPOP`/any blocking variant.

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
- Do not run the benchmark against a production Redis instance — it pre-fills
  hundreds of thousands of jobs and can optionally flush the entire database.
- Do not silently change the default `--poll-interval-ms` or the idle-poll
  phase design without re-reading the "Protocol compatibility" section —
  misrepresenting Resque's poll/backoff behavior defeats the tool's purpose.
