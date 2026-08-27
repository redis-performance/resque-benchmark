# Mined repo history — resque-benchmark, as of 2026-08-26

This file is the raw record the rest of this skill is grounded in. There is no "voice profiles"
file the way a longer-lived project's review skill might have one, because there is no reviewer
voice in this repo's history to profile — see below. Re-run the commands below yourself if this
skill is used much later than 2026-08-26; the record will have grown.

## What was queried

```
gh repo view redis-performance/resque-benchmark --json defaultBranchRef,description,createdAt,pushedAt
gh pr list --repo redis-performance/resque-benchmark --state all --limit 300 --json number,title,author,state,createdAt,mergedAt,body
gh issue list --repo redis-performance/resque-benchmark --state all --limit 300 --json number,title,author,state,createdAt
gh api repos/redis-performance/resque-benchmark/pulls/<n>/reviews     # for n = 2, 4
gh api repos/redis-performance/resque-benchmark/pulls/<n>/comments    # for n = 2, 4
gh api repos/redis-performance/resque-benchmark/issues/<n>/comments   # for n = 1, 2, 3, 4, 5
```

## Full result

- Repo created: 2026-08-13. Sole contributor and maintainer in all records found: **fcostaoliveira**.
- **PR #2** — "build: ship static musl release binaries instead of glibc-linked ones." Opened and
  merged 2026-08-17, ~5 minutes apart. Fixes issue #1. Author: fcostaoliveira.
- **PR #4** — "fix: --db was ignored, so every connection selected db 13." Opened and merged
  2026-08-18, ~14 minutes apart. Fixes issue #3. Author: fcostaoliveira.
- **Issue #1** (closed by PR #2), **Issue #3** (closed by PR #4), **Issue #5** (open, cluster-mode
  `MOVED`-following gap) — all three filed by fcostaoliveira.
- `gh api .../pulls/2/reviews`, `.../pulls/2/comments`, `.../pulls/4/reviews`, `.../pulls/4/comments`
  all returned `[]`.
- `gh api .../issues/1/comments`, `.../issues/2/comments` (PR #2's issue-thread comments),
  `.../issues/3/comments`, `.../issues/4/comments` (PR #4's issue-thread comments),
  `.../issues/5/comments` all returned `[]`.
- **Total review comments + formal reviews + issue comments found across the entire repo: zero.**

## What this means for how this skill should be used

- There is no maintainer review "voice" to imitate, because no review has ever been written down
  in this repo. Do not construct one — see SKILL.md's repeated instruction on this.
- The two real PR descriptions are, however, genuinely detailed and worth treating as this repo's
  real bar for a *good PR description*: each names the exact root cause, a before/after comparison
  table, what a wire-level (`MONITOR`) or binary-level (glibc symbol) check confirmed, and what
  tests were added. `nitpick-taxonomy.md` draws its technical categories from these two
  descriptions plus `AGENTS.md`/`CONTRIBUTING.md`, not from any review comment.
- If this skill is invoked again well after 2026-08-26 and a new PR has real review comments on
  it by then, prefer that fresh, real data over this file's "zero comments" finding — re-run the
  queries above rather than assuming the repo is still this quiet. Update this file if so.
