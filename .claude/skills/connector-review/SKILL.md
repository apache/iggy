---
name: connector-review
description: Adversarial 4-expert review of a connectors PR, branch, or ref range (sinks, sources, runtime, SDK, transforms) with clean-room validation of every finding. Experts work alone, no peer debate. Expensive, one run spawns ~10 subagents. Load connectors-overview first.
argument-hint: "[PR number | branch | ref range]"
disable-model-invocation: true
---

# Apache Iggy Connector Review

`<TARGET>` = `$ARGUMENTS`: a PR number, a branch, or a ref range. Empty means `origin/master..HEAD`. Scope: anything under `core/connectors/` plus connector integration tests under `core/integration/tests/connectors/`.

Prereq: load `connectors-overview` first as router (sibling skill in this directory; repo-wide rules in `<repo>/AGENTS.md`). This file owns **review mechanics only**.

You = **moderator**. You never open the diff or a source file: you route paths, merge claims, synthesize. Every token you load rides along every later turn. Reviewers and validators are one-shot agents that deliver by writing a file; nobody chats.

## Charter (paste VERBATIM into every expert, validator, and tiebreak prompt)

> You think big brain. You speak caveman. Separate things.
>
> **Thinking, unchanged.** Read the diff, then every changed file in full from the local checkout, then whatever call sites you need. Trace call chains. Verify invariants. Prove findings, don't guess. Cite `path::Symbol`, plus `file:line` as secondary anchor. Running tests or builds needs a stated justification: reading and tracing settles most claims, and parallel cargo runs block on one target-dir lock.
>
> **Output style.** Drop articles, filler, pleasantries, hedging. Fragments OK. Keep EXACT: symbol, `file:line`, error quotes, code, technical terms, severity and confidence labels.
>
> - Finding, one line each: `[sev] path::Symbol (file:line) - problem. Fix: action. (origin, conf:H|M|L)`
> - `sev`: `critical` = correctness/safety/data-loss/security, blocks merge; `warning` = real defect, perf hit, API issue; `nit` = style/naming; `simplify` = complexity/dead-code reduction, format `[simplify] path::Symbol (file:line) - what's complex. Simpler: alternative. Saves: ~N lines / removes indirection. (origin, conf)`.
> - `origin`: `intro` (PR introduced), `pre-surfaced` (existed, exposed by PR), `pre-untouched` (existed, not touched).
> - Never flag em dashes or other punctuation style as a finding.
> - Simplification mandate: less code > more code. Per changed file ask whether ~30% smaller keeps correctness: dead fields/params/branches/imports, duplication of an existing helper (cite it), single-impl traits, premature generics, checks for impossible states. Do not propose simplifications that change semantics or break public API. If nothing qualifies, write `Simplifications: none`.
>
> **Connector mandatory checks (every expert, where in scope).** Verify, don't assume:
> `SecretString` on credential fields, no `Serialize` on plugin config (or redacted serializer with justification); FFI return codes honored (`0` ok, nonzero fail; duplicate-ID guard intact); sink redelivery claim vs `AutoCommit::When(PollingMessages)` + FFI swallow path; idempotency key on retry (message `id` dedup, `ON CONFLICT`, composite `_id`, deterministic run id + 409 handling).
> `Permanent*` vs transient classification vs retry strategy agreement; drop accounting (`errors` vs `messages_filtered`, flushed once per batch via pre-built labels).
> Config forward-compat (`Option`/`#[serde(default)]`); `consume`/`poll` take `&self`; no `tokio::spawn`/`block_on` in plugins; no lock held across `.await`; `BTreeMap` headers; `try_to_bytes(&self)` no-clone for JSON; `Vec::with_capacity`; `[lib] crate-type = ["cdylib", "lib"]`; benchmark/verbose flags wired; closest exemplar named.
>
> Caveman = output compression, not analysis compression. Dig deep. Write short.

## Step 1: Identify the target (no reading)

- Classify `<TARGET>`: matches `^#?(pr)?[0-9]+$` case-insensitively -> PR, the digits are `<PR>`. Anything else -> ref range or bare branch. Empty -> review `origin/master..HEAD`.
- `<TOPIC>`: `<TARGET>` lowercased, chars outside `[a-z0-9-]` replaced by `-`, repeats collapsed, trimmed, max 40 chars (`PR3123` -> `pr3123`, `origin/master..HEAD` -> `origin-master-head`). Empty -> `date +%s`.
- `<DIR>` = `<session scratchpad dir from your system prompt>/review-<TOPIC>`. `mkdir -p` it.
- PR: `gh pr view <PR> --json title,body,headRefOid > <DIR>/pr.json`, `gh pr diff <PR> > <DIR>/diff.patch`, `gh pr diff <PR> --name-only > <DIR>/files.txt`. `<SHORTCOMMIT>` = first 8 of `headRefOid`.
- Ref range or bare branch: `git diff $(git merge-base origin/master HEAD)..HEAD > <DIR>/diff.patch`, same with `--name-only`, `<SHORTCOMMIT>` = `git rev-parse --short=8 HEAD`. No `pr.json` on this path.
- Guard: `git rev-parse HEAD` must equal the reviewed head. Experts read the local checkout; if it differs, stop and ask the user to check out the reviewed head.
- `<DESCR>`: 1-3 word `snake_case` summary, `[a-z0-9_]`, <= 24 chars. From the PR title; no PR -> from `git log -1 --format=%s`.
- Report path: `<DIR>/report.md`.

Do not `cat` any of the files you just wrote. `wc -l <DIR>/diff.patch` is the only look you take.

## Step 2: Round 1, four one-shot experts (one message, parallel)

Spawn 4 `Agent` calls in a single message: `subagent_type: general-purpose`, `name: <role>-<TOPIC>` (bare role names collide with concurrent sessions: one shared agent namespace), no `model` (inherits). Prompt = role block + Charter + this brief, with `<DIR>`, `<TARGET>`, `<SHORTCOMMIT>` filled in:

> Target: `<TARGET>` at `<SHORTCOMMIT>`. Diff: `<DIR>/diff.patch`. Changed files: `<DIR>/files.txt`. PR title and body: `<DIR>/pr.json` (drop this sentence when there is no PR). Classify each finding's origin; check existing codebase conventions before calling a deviation `intro`. Name the closest exemplar plugin compared (`stdout_sink`, `postgres_sink`, `http_sink`, `random_source`, ...).
> Deliverable = the file `<DIR>/<role>.md`, written with the Write tool BEFORE you end your turn: findings in Charter format, then `Simplifications: ...`, then `Verdict: APPROVE | REQUEST CHANGES - reason`. A previous worker finished reading and then idled without delivering; the Write call IS the delivery, your final message is just the path. Budget 3/4 reading, 1/4 writing; partial beats unshipped.
> You work alone: no teammates, no SendMessage, no questions back.

Role blocks:

- **plugin**: Senior connector-plugin engineer. Owns sink/source impls.
  Focus: lifecycle (`&self` consume/poll, `open` connectivity fail-fast, `close` flush/take), config `Option` defaults + `new` vs `open` vs `consume` validation order, payload dispatch + `try_to_bytes` + `mem::replace` + `with_capacity`, header `BTreeMap` handling, error-variant mapping (transient vs `Permanent*`), retry strategy agreement, idempotency on redelivery, batching (`chunks`, no cross-`consume` buffering), logging redaction + `verbose_logging`.
  Simplify: dead payload arms, skip branches that never fire, single-variant enums, near-duplicate path/config helpers.
- **runtime**: Connectors runtime + FFI host engineer. Owns `runtime/src/`.
  Focus: FFI pointer lifetimes, `plugin_id` identity, container `Arc` ownership (no unload mid-call), sink consume loop (autocommit timing, decode/transform drop-and-continue, postcard encode, nonzero-return handling), source forwarding loop (flume handoff, `cleanup_sender` order, state save after Iggy send), state atomic-rename protocol, `restart_guard`, metrics/drop-accounting wired sites, benchmark event + histogram labels, config provider + `ConfigEnv` addressing.
  Simplify: duplicated consume/forward paths, label lookups per message, re-expanded log-layer matches.
- **sdk**: SDK contract guardian. Owns `sdk/src/`.
  Focus: `#[repr(C)]` layouts, `Schema`/`Payload`/`Error` variant changes, decoder/encoder round-trips, `sink_connector!`/`source_connector!` return codes + duplicate-ID guard, `ConnectorState` msgpack format, `retry.rs` (breaker, middleware, `max_retries` total-vs-extra semantics), `convert.rs` bridges, transform trait (`&self`, sync, `Ok(None)` filter vs `Err`), ser/de split (postcard FFI, JSON config, msgpack state).
  Simplify: single-impl traits, premature generics, predicates enforced twice.
- **testing**: Connector test + docs lead.
  Focus: BDD unit naming consistency per file, `test_config()` helper, four canonical source state tests, sink pure-logic coverage (defaults, payload variants, header encoding, query building, transient/permanent classification), transform four branches, integration layout (`#[iggy_harness]` + `testcontainers-modules` + `iggy-test-` prefix + polling not sleeping).
  Also: README + `example_config` TOML sync (all fields incl. `max_connections`, retry knobs, benchmark/verbose), error-message clarity, breaking-change notes.
  Simplify: mocked-backend tests that should be real-infra, sleeps that should poll, `Display`-string assertions.

Collect: wait for the completion notifications, then `ls <DIR>/*.md`.
A role with no file gets one `SendMessage` nudge to `<role>-<TOPIC>` ("Write `<DIR>/<role>.md` now, then stop."); still missing after that, respawn the role once with the same prompt. Never open a subagent transcript via `TaskOutput` (it is the whole JSONL).

## Step 3: Merge into neutral claims (moderator)

Read the 4 role files. Write `<DIR>/claims.md`, one line per claim: `C<N> [sev] path::Symbol (file:line) - claim. Fix: action. (origin)`. Strip role names, confidence, and argument. Same symbol + same defect from several roles = one claim at the highest severity; keep a private raised-by map for the report. Simplify items are claims too.

No claims at all: skip Steps 4 and 5, go to Step 6 with empty sections and `Verdict: APPROVE`. The report file still gets written.

## Step 4: Clean-room validation (one message, parallel)

Shard claims ~5 per validator. Spawn one `Agent` per shard plus one sweep validator, all in one message: `subagent_type: general-purpose`, `model: opus`, `name: validator-<k>-<TOPIC>` / `sweep-<TOPIC>`. Each gets ONLY: its claims verbatim, `<DIR>/files.txt`, `<DIR>/diff.patch`, the target identity, the Charter. Not the role files, not raised-by, not your reasoning; the missing context is what removes the anchoring bias.

Validator mandate (adversarial): for each claim open the cited symbol, trace call sites, then rate `C<N>: PASS | FIX: <correction, correct symbol/line, correct severity> | REMOVE: <why false or unverifiable>`; judge whether the severity is calibrated; re-check the anchor. Deliverable `<DIR>/validate-<k>.md` via Write, same idle rule as Step 2.

Sweep mandate: all claims + the diff. Two questions only: which real defects in the diff are missing from the list, and which listed items wrongly clear a bug. Deliverable `<DIR>/sweep.md`, additions in Charter format tagged `(sweep)`.

Apply: drop REMOVE, apply FIX (wording, symbol, line, severity), fold sweep additions in as `(sweep, unvalidated)`. A `critical` sweep addition gets one extra validator before it may block the verdict.

## Step 5: Contested items (only when triggered)

Contested = a validator REMOVEs or downgrades a `critical` or `warning`, or a sweep addition contradicts a PASS. Per item spawn one `Agent` (`model: opus`) with the claim, the validator's verdict text, the expert's original line, and the paths; it writes `UPHELD | OVERTURNED - reason (cite symbol)` to `<DIR>/contested-<N>.md`. Cap 5 per run; past the cap you adjudicate and mark `(moderator call)`.

## Step 6: Synthesize, write, done

Output in caveman style:

```text
## Review: [change desc]

### Confirmed (expert + clean-room validator)
- [sev] path::Symbol (file:line) - problem. Fix: action. (raised: role[, role]; validated: PASS|FIX)

### Contested
- path::Symbol (file:line) - problem.
  Expert: position. Validator: counter. **Tiebreak**: UPHELD|OVERTURNED - why.

### Retracted (validator REMOVE)
- finding - why.

### Pre-existing (origin pre-*, not blocking)
- path::Symbol (file:line) - follows pattern in [ref].

### Simplification opportunities (non-blocking)
- path::Symbol (file:line) - current shape. Simpler: alternative. Saves: ~N lines / removes indirection.

### Verdict: APPROVE | REQUEST CHANGES
Confirmed critical + warning only. Simplifications informational. Reason: one line.

Counts: critical N, warning N, nit N, simplify N (Confirmed + Simplification sections)
```

Then write `<DIR>/report.md` with:

1. H1 `# Connector Review - <change desc> (<SHORTCOMMIT>)`.
2. Metadata, one line each: target `<TARGET>`, reviewed commit, ISO timestamp, roles, validator count, contested count.
3. The report above, verbatim.
4. Appendix `## Raw findings per expert`: each role file verbatim in a fenced block.
5. `## Validation record`: counts of PASS / FIX / REMOVE, sweep additions, contested outcomes.

Last user-facing line: `Findings written: <DIR>/report.md`. No cleanup: one-shot agents end themselves, `<DIR>` stays in the scratchpad.
