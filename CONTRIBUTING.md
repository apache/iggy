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

### Green CI

Maintainers will not start reviewing a PR while its CI is failing. Get the
pipeline green first - a red build, lint, or test means the PR is not ready
for review.

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

Move a PR around the review queue by posting a slash command on its own
line in a regular PR comment (not an inline review reply):

| Command | Who | Effect |
| --- | --- | --- |
| `/ready` | author or maintainer | mark `S-waiting-on-review` |
| `/author` | maintainer or returning contributor | mark `S-waiting-on-author` |
| `/request-review @user-or-team ...` | author or maintainer | request review from the listed `@user` / `@org/team` handles |

Some labels move on their own: opening or marking a non-draft PR ready sets
`S-waiting-on-review`; a "Request changes" review sets `S-waiting-on-author`;
closing or converting to draft clears both.

Commands take up to ~90s. A 👍 reaction means applied, 😕 means you lacked
permission; if neither shows up, check the `PR Triage Apply` run in the
Actions tab.

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
