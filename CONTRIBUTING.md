# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

Requires Rust stable (1.75+) and a running Redis instance (any version 6+).

```bash
git clone git@github.com:redis-performance/resque-benchmark.git
cd resque-benchmark
cargo build --release
```

To verify the build works end-to-end, spin up Redis and run a quick smoke test:

```bash
# Start Redis (or point REDIS_URL at an existing instance)
docker run --rm -d -p 6379:6379 redis:8

# Quick smoke test — 500 jobs, 2 workers, db 0, short idle-poll window
./target/release/resque-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --poll-interval-ms 100 \
  --idle-poll-duration-s 3 \
  --output -
```

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- Any change to protocol behavior (queue key naming, push/pop commands, job
  envelope shape, poll/backoff timing) must cite the exact Resque source
  file + line it was verified against — see README's "Protocol compatibility"
  section for the standard this repo holds itself to.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

All new behaviour must be covered by tests. Existing tests must pass before opening a PR. Run the full suite locally:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` includes real-Redis integration tests under `tests/`
(`REDIS_URL`, default `redis://127.0.0.1:6379/15`) — they enqueue real
jobs, drive real workers, and assert `idle_poll_qps` against the
`workers * 1000 / poll_interval_ms` sanity check as an automated
regression, plus a MONITOR-based check that only LPOP/RPUSH ever hit the
wire. They skip with a `SKIP:` notice (not a failure) if nothing is
reachable at `REDIS_URL`, so `cargo test` still works without Redis
running locally — but CI always has one, so these run for real there.

For a full end-to-end smoke test (requires a running Redis on `127.0.0.1:6379`):

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

Coverage should not decrease.

## Review process

- At least one maintainer approval is required before merge.
- CI must be green (format check, clippy, unit tests, smoke test all pass).
- Maintainers may request changes or close PRs that don't meet the bar — this is normal and not personal.
