# Cross-cutting nitpick taxonomy — resque-benchmark, real precedent only

Grounded in this repo's actual GitHub history as of 2026-08-26: 2 merged PRs, 3 issues, 0 open
PRs, and 0 review comments/formal reviews/issue comments of any kind. Every "real precedent"
category below is evidenced either by one of the two merged PRs' own self-written description, a
real merged bugfix diff, or `AGENTS.md`/`CONTRIBUTING.md`'s written text — never by a reviewer
comment, because none exist in this repo yet. Where a category has exactly one piece of evidence,
that is stated honestly, not inflated into "this project's reviewers always check this."

1. **A CLI flag can silently no-op instead of erroring — check every flag actually reaches the
   behavior it claims to control.** PR#4 (merged, self-authored, fixing issue #3): `--db` was
   documented and accepted, but `build_redis_url` only applied it when the `--url` flag's own path
   was empty — and the default `--url` (`redis://127.0.0.1:6379/13`) always has a path, so the
   `--db` branch could never fire, for *any* value including the documented `--db 0` escape hatch.
   No error, no warning; `redis-cli MONITOR` was required to even see the wrong `SELECT` being
   issued. This is real, on-point precedent for tracing any new or changed CLI flag all the way to
   the wire command it's supposed to affect, especially where two flags (`--url`'s embedded db vs.
   a separate `--db`) can both claim to set the same underlying value — check which one actually
   wins, in code, not by reading the flag's help text.

2. **A silent failure can read as a harness/network fault instead of a real bug, and "CI green"
   doesn't mean "it ran."** Both real merged PRs describe this exact shape of failure: PR#4's bug
   made runs "die within ~5 seconds... having written no output file... exits early enough that it
   reads as a harness or network fault rather than a rejected SELECT"; PR#2's bug made the release
   binary fail to even start on non-build-host distros, but only at the *loader* level, so a
   verification step that ran the binary only on the build host stayed green while the binary was
   unusable everywhere else. This is real, repeated precedent (2 for 2 in this repo's whole
   bugfix history) that a change touching startup/connection/verification logic should be checked
   for what happens when it fails, not just when it succeeds — does a failure produce a clear
   error, or does it look like "nothing happened" / "skipped" / "green"?

3. **Protocol-behavior claims must be wire-verified, not just asserted.** `AGENTS.md` and
   `CONTRIBUTING.md` both hold this repo to citing exact Resque source (file+line) for queue key
   naming, push/pop commands, job envelope shape, and poll/backoff timing, and both real merged
   PRs actually did this (PR#4 verified the fix with `MONITOR` and a table of `SELECT` commands
   issued per invocation; PR#2 verified the built binary's glibc symbol versions directly). Any PR
   touching `producer.rs`, `worker.rs`, or `job.rs` should show the same kind of wire-level
   evidence (a `MONITOR` transcript, an equivalent trace) that only `RPUSH`/plain `LPOP` are used —
   never `BLPOP`/`BRPOP` — rather than asserting protocol correctness from the diff alone.
   `CONTRIBUTING.md`'s own testing section documents a real, existing automated version of this
   check (a MONITOR-based integration test asserting only LPOP/RPUSH hit the wire) — if a PR
   changes wire behavior without touching that test, ask why.

4. **Cross-target build portability is a real, already-shipped failure mode for the release
   pipeline.** PR#2 (merged, self-authored, fixing issue #1): the published release binaries were
   built as `*-gnu` on `ubuntu-24.04`, requiring `GLIBC_2.39`, and could not even start on Ubuntu
   22.04, Debian 12, or RHEL 9 — real distros this tool exists to be run against. The fix moved to
   static `*-unknown-linux-musl` targets and added a build-time assertion that the artifact carries
   no `GLIBC_*` symbol versions. Any PR touching `.github/workflows/release.yml`, target triples,
   or the TLS/crypto dependency stack (this repo deliberately uses `rustls` + `webpki-roots`
   specifically to avoid an OpenSSL/glibc dependency, per PR#2) should be checked against this real
   precedent: does the change reintroduce a host-glibc or host-OpenSSL dependency the static-musl
   design was chosen to eliminate?

5. **New dependencies require maintainer sign-off, per written doctrine — this hasn't yet been
   tested against a real PR.** `AGENTS.md`, verbatim: "Do not introduce new dependencies without
   checking with the maintainer." This is real, checked-in text, safe to cite directly — but no PR
   in this repo's short history has actually added a new crate dependency, so there's no real
   precedent for how strictly it's enforced or what "checking with the maintainer" looks like in
   practice. If a PR adds a new `Cargo.toml` dependency, name the rule and ask whether it was
   discussed, without claiming a track record of enforcement that doesn't exist yet.

6. **Idle-poll QPS is this tool's whole differentiating purpose — a change that touches polling
   behavior needs to be checked against it specifically.** Both `README.md` and `AGENTS.md`
   describe idle-poll QPS (the LPOP rate an idle worker fleet issues while queues are empty) as
   "the whole reason this tool exists," and `CONTRIBUTING.md` documents a real, existing automated
   regression check (`idle_poll_qps` against a `workers * 1000 / poll_interval_ms` sanity bound).
   No merged PR has touched this path yet, so there's no real bugfix precedent here (unlike items
   1–4) — but given how explicitly the written docs flag it as load-bearing, any PR touching
   `--poll-interval-ms`, the idle-poll phase, or worker sleep/backoff timing deserves the same
   "does the number this produces still make sense" scrutiny the existing regression test applies,
   even though no one has yet had to catch a real bug here.

7. **A known, open, unfixed gap exists: cluster-mode is not supported.** Issue #5 (open, filed by
   the maintainer): a plain `redis::Client` does not follow `MOVED` redirects, so this tool cannot
   run against Redis Cluster today. This is not a merged fix or a reviewer catch — it's a real,
   named, currently-unaddressed limitation. If a PR claims to add or improve cluster support, check
   it against the actual issue text (`gh issue view 5 --repo redis-performance/resque-benchmark`)
   rather than assuming what "cluster support" should mean; if a PR touches connection setup for an
   unrelated reason, it's fair to ask whether it's aware of this open gap without demanding it be
   fixed as part of an unrelated change.

## What this taxonomy is honestly thin or silent on

Categories a project with a longer review history might have real reviewer precedent for, where
this repo's history gives **none** — say so plainly rather than inventing one:

- **Any human-authored review comment, on anything.** Zero exist across both merged PRs and all
  three issues. There is no "how a real reviewer here phrases a concern" to imitate — see SKILL.md.
- **Style/formatting nits.** `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
  are enforced in CI per `CONTRIBUTING.md`/`AGENTS.md`; no real PR or comment shows a human flagging
  a style issue tooling didn't already catch. Don't manufacture style nitpicks on top of what CI
  already gates.
- **Test-coverage enforcement in practice.** `CONTRIBUTING.md` states "Coverage should not
  decrease" and "all new behaviour must be covered by tests," but with only 2 merged PRs and no
  Codecov-style automated percentage visible in this survey, there's no real data on how strictly
  this is enforced in practice (unlike redisbench-admin, where real merged PRs with known low
  patch-coverage numbers exist). Cite the written rule; don't claim a specific enforcement track
  record.
- **Multi-round, back-and-forth review.** Never observed — both real PRs were merged within
  minutes of opening by their own author.
