---
name: resque-benchmark-maintainer-review
description: Review a redis-performance/resque-benchmark pull request, branch, or diff using this project's own written standards (AGENTS.md, CONTRIBUTING.md) and the two real, self-documented bugfixes in its actual history — not generic Rust code-review advice, and not an invented maintainer personality. Use this whenever the user asks to review a resque-benchmark PR, asks whether a resque-benchmark PR would pass real review or get merged, wants a resque-benchmark-specific pre-merge check, or is deciding accept/reject on a redis-performance/resque-benchmark PR. Prefer this over a generic code-review skill for anything touching redis-performance/resque-benchmark — the generic skill doesn't know this project's protocol-fidelity requirements or its (honestly very short) real history.
---

# resque-benchmark maintainer-style review

## Read this first: what this repo's history actually is

`resque-benchmark` was created 2026-08-13 — under two weeks old at the time this skill was
written. Its entire GitHub history was mined directly (`gh pr list --state all`, `gh api
.../pulls/<n>/reviews`, `.../pulls/<n>/comments`, `.../issues/<n>/comments`; the exact commands and
raw results are in `references/repo-history.md`) and it is genuinely tiny:

- **Two merged PRs total** (#2, #4), **both authored by, and self-merged by, the same person**
  (`fcostaoliveira`, the sole maintainer) — merged within minutes of opening.
- **Zero review comments, zero formal reviews, and zero issue comments exist anywhere in this
  repo's history** — not thin, *literally none*, on any of the four PRs/issues surveyed. There is
  no reviewer voice to mine, because no one has ever reviewed a PR here in writing.
- **Three issues total**: #1 and #3 each describe a bug that PR #2 and PR #4 (respectively) fixed;
  #5 is open (cluster-mode support is a known, unfixed gap — see below).
- `CONTRIBUTING.md` states "At least one maintainer approval is required before merge," but the
  real, observed practice for both merged PRs was same-author self-merge with no separate
  approval recorded. Treat the written rule as real (repeat it when relevant, e.g. for a PR that
  isn't self-evidently the sole maintainer's own work) but do not claim the self-merge pattern
  itself is a violation — with one maintainer and a brand-new repo, it may simply be how solo
  bootstrapping works here so far, and the record doesn't show anyone being asked to change that.

**Do not invent a maintainer personality, a house style of review comments, or a "this project's
reviewers always flag X" claim.** There is no such record. Where the reference files below cite a
real pattern, it is drawn from the *PR authors' own descriptions* (which are unusually detailed
self-write-ups of root cause, blast radius, and verification) or from `AGENTS.md`/`CONTRIBUTING.md`
written text — never from a review comment, because none exist. Say so plainly in the review
itself if you'd normally cite reviewer precedent and can't: "this repo's history doesn't have a
reviewer precedent for X; reasoning on this from the code and written docs instead."

## Process

1. **Get the material.** `gh pr view <n> --repo redis-performance/resque-benchmark
   --json body,commits,files,author` and `gh pr diff <n> --repo redis-performance/resque-benchmark`.
   Read the full PR description first — both real PRs here (#2, #4) are unusually thorough
   self-write-ups (root cause, a before/after comparison table, exact `MONITOR` wire verification,
   what was tested) that already do real work; if the author already addressed something in the
   description, acknowledge that instead of re-discovering it.

2. **Check author trust and diff risk**, honestly: with a single-maintainer repo this young,
   `gh pr list --author <login> --state merged --repo redis-performance/resque-benchmark` may
   return the maintainer's own prior PRs and nothing else. That's fine — use diff size and blast
   radius (does it touch the CLI flag surface, the wire protocol, or the release/build pipeline?)
   to set scrutiny more than author history, since author history here is too sparse to be
   meaningful signal either way.

3. **Work the checklist** in `references/nitpick-taxonomy.md` — grounded in this repo's two real,
   self-documented bugs (a CLI-flag no-op that silently selected the wrong Redis database, and a
   glibc-linked release binary that silently failed to run on most target distros) plus the
   written protocol-fidelity and dependency-approval rules in `AGENTS.md`/`CONTRIBUTING.md`.

4. **Write the review.** Since there is no mined reviewer voice to imitate, write as a careful,
   plain technical reviewer — not as an impersonation of `fcostaoliveira` or anyone else. Keep it
   short and concrete:
   - Lead with the most concrete, highest-risk finding, if any — same as both real PRs' own
     descriptions do (root cause first, then blast radius, then what was verified).
   - Cite `AGENTS.md`/`CONTRIBUTING.md` by name when a written rule applies (protocol-behavior
     citations, new-dependency approval, test coverage) — these are real, checked-in text, safe to
     cite directly.
   - Don't claim "reviewers on this project usually..." about anything — there's no reviewer
     history to say that about. If a concern has no precedent either way, say plainly that this
     repo's short history doesn't speak to it and reason from the code and docs.
   - Hedge like a human who isn't fully certain, when genuinely uncertain: "I think", "worth
     checking", "not blocking, but...".
   - Never literally `@`-mention any GitHub username, even the sole maintainer's — say "the
     maintainer" or "whoever owns the wire-protocol code" in prose instead.

5. **Land on a verdict.** With no real "approved with a comment" vs. "requested changes" precedent
   to draw on, keep it simple and honest: either the PR looks solid and worth a short note (or
   nothing, if it's routine — e.g. a version bump, a docs fix, a CI-only change), or there's a
   concrete, named concern worth raising before merge. Never write the literal word "Verdict",
   never a bolded/labeled summary line, never a trailing "TL;DR" section — end in plain prose, the
   way both real PR descriptions here do (they close with a plain paragraph, not a formatted
   block).

## Scope gate

If the PR's content falls entirely outside anything the taxonomy below covers — no Rust source
under `src/`, no `tests/`, nothing resembling CLI flags, the wire protocol, the release/build
pipeline, or CI — say so in one sentence and treat it as out of scope rather than force-fitting
the checklist onto, say, a pure license-file or README typo change.

## What NOT to do

- Don't invent a "this project's maintainers typically say..." voice. There is no reviewer-comment
  history in this repo at all — say that plainly rather than manufacturing one.
- Don't cite the two real PRs' self-descriptions as if they were independent maintainer review
  findings. They're the *author's own* write-up of their *own* bug — real and worth using as
  precedent for what a good PR description looks like here, but not evidence anyone reviewed it.
- Don't apply generic Python/JS code-review categories (this is a Rust, Tokio-async codebase) or
  memtier_benchmark's/redisbench-admin's categories wholesale — check `nitpick-taxonomy.md` for
  what's actually evidenced here first.
- Don't claim CONTRIBUTING.md's "one maintainer approval required" rule is being violated by the
  self-merge pattern seen so far — just note the rule exists, apply it going forward, and don't
  editorialize about a two-week-old, one-maintainer repo's bootstrapping.
- Don't close with a labeled, bolded verdict block — end in plain prose.
- Don't literally `@`-mention any GitHub username, ever.
