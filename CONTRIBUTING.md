# Contributing to Apache Iggy

## Issue First

Every new PR that introduces new functionality must link to an approved issue.
PRs without one may be closed at maintainer's discretion.

1. Create an issue or comment under existing
2. Wait for maintainer approval (`good-first-issue` label or comment)
    - Maintainer may request for more details or a different approach
3. Then code

## Size Limits

For new contributors we require to keep PRs under 500 lines of code, unless explicitly approved by a maintainer under linked issue.

## High-Risk Areas

These require design discussion in the issue before coding:

- Persistence (segments, indexes, state, crash recovery)
- Protocol (binary format, wire encoding)
- Concurrency (shards, inter-shard)
- Public API (HTTP, SDKs, CLI)
- Connectors

## PR Requirements

### Run It Locally

**If you can't run it, you can't submit it.**

Authors of PRs must run the code locally. "Relying on CI" is not acceptable.

### Single Purpose

One PR = one thing. Bug fix, refactor, feature - separate PRs. Mixed PRs will be closed.

### Quality Checks

For Rust code:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build
cargo test
cargo machete
cargo sort --workspace
```

For other languages, check the README in `foreign/{language}/` (e.g., `foreign/go/`, `foreign/java/`).

### Typos Checks

We use [typos](https://github.com/crate-ci/typos):

```bash
cargo install typos-cli --locked
typos
typos --write-changes
```

If it's indeed not a typo, you can set an exception in `.typos.toml`.

### Pre-commit Hooks

We use [prek](https://github.com/j178/prek):

```bash
cargo install prek
prek install
```

## Code Style

### Comments: WHY, Not WHAT

```rust
// Bad: Increment counter
counter += 1;

// Good: Offset by 1 because segment IDs are 1-indexed in the wire protocol
counter += 1;
```

Don't comment obvious code. Do explain non-obvious decisions, invariants, and constraints.

### Commit Messages

Format: `type(scope): subject`

**Good examples from this repo:**

```none
fix(server): prevent panic when segment rotates during async persistence
fix(server): chunk vectored writes to avoid exceeding IOV_MAX limit
feat(server): add SegmentedSlab collection
refactor(server): consolidate permissions into metadata crate
chore(integration): remove streaming tests superseded by API-level coverage
```

Keep subject under 72 chars. Use body for details if needed.

## PR Triage Commands

You can move a PR around the review queue by posting a slash command in the
PR conversation. The pattern is similar to `rust-lang/triagebot`. The
machinery lives in [`./.github/workflows/pr-triage.yml`](./.github/workflows/pr-triage.yml)
if you want to peek.

### Commands

| Command | Who can use it | What it does |
| --- | --- | --- |
| `/request-review @user-or-team ...` | the PR author or a maintainer | Requests review from one or more `@user` or `@org/team` handles |
| `/ready` | the PR author or a maintainer | Marks the PR `S-waiting-on-review` |
| `/author` | a maintainer | Marks the PR `S-waiting-on-author` |

A "maintainer" here means someone with write access to apache/iggy
(in practice, the `@apache/iggy-committers` team). Automated comments from
bots like Dependabot do not run commands.

Each command has to start its own line. Leading whitespace is fine, but
prose like `please /ready` will not match. You can put more than one command
in a single comment: `/request-review` plus `/ready`, or `/request-review`
plus `/author`. `/request-review` may carry several handles on one line and
may also repeat across lines; all of them are collected and requested
together. For `/ready` and `/author`, the last one wins, so `/ready` then
`/author` in the same comment ends up as `S-waiting-on-author`.

### Typical flow

1. You open a PR. CODEOWNERS pings `@apache/iggy-committers` automatically.
2. A maintainer reviews. If they want changes, they comment `/author` and
   the PR moves to your queue.
3. You push fixes, then comment `/ready`. The PR moves back to the review
   queue.
4. Either side can comment `/request-review @somebody` to pull in a
   specific person or team.

To find PRs waiting on review, filter with
`is:open is:pr label:S-waiting-on-review` on the Pulls tab.

### Lifecycle automation

Some labels are managed for you based on what happens to the PR:

| When | What happens |
| --- | --- |
| You open a PR (not a draft) | Gets `S-waiting-on-review`, unless an `S-*` label is already set |
| You mark a draft "Ready for review" | Gets `S-waiting-on-review`, unless an `S-*` label is already set |
| You convert the PR back to a draft | Both `S-*` labels are removed |
| The PR is closed or merged | Both `S-*` labels are removed |

Reopening a PR does not re-label it. Drop a `/ready` or `/author` comment
to put it back in a queue.

Drafts are skipped by the automatic labelling, but `/ready` and `/author`
still work on drafts if you want to signal intent before clicking "Ready
for review".

### Tips

- **One comment per command burst.** To request several reviewers at once,
  list them on a single `/request-review` line (or several lines) in one
  comment. Posting separate comments back-to-back can lose the middle one
  due to how CI runs are scheduled.
- **Edits don't re-trigger.** If you typo a command and edit the comment, it
  will not run. Post a new comment instead.
- **Use the main conversation, not inline review replies.** Commands posted
  as replies on a specific line of the diff are ignored. Post them in the
  PR's main comment thread.
- **Suffixes don't match.** `/ready-to-merge` or `/readyish` will not flip
  state - only `/ready` followed by a space or end-of-line counts.

### When something goes wrong

The workflow never comments back. If your command didn't seem to do
anything, open the PR's "Checks" or "Actions" tab and look at the
`PR Triage` run for the comment you posted. The run log says exactly what
it saw and why (no permission, unknown user, etc.).

### Examples

Mark your PR ready after addressing feedback:

```text
/ready
```

Ask the author to take another look:

```text
/author
```

Request a specific reviewer and mark ready in one comment:

```text
/request-review @somebody
/ready
```

Request several reviewers in one go (do this instead of posting separate
comments). They can share one line, span multiple lines, or both:

```text
/request-review @alice @bob @apache/iggy-committers
```

## Close Policy

PRs may be closed if:

- Maintainer feels like proxy between maintainer and LLM
- No approved issue or no approval from a maintainer
- Code not ran and tested locally
- Mixed purposes or purposes not clear
- Can't answer questions about the change
- Inactivity for longer than 7 days

## Questions?

[Discussions](https://github.com/apache/iggy/discussions) or [Discord](https://discord.gg/apache-iggy)
