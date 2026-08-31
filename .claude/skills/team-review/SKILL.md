---
name: team-review
description: Adversarial 4-expert review (storage, perf, distsys, ecosystem) of a PR, branch, or ref range, with clean-room validation of every finding. Experts work alone, no peer debate. Invoke only when the user explicitly asks for a team review: it spawns ~10 subagents and is expensive.
---

# Apache Iggy Team Review

`<TARGET>` = the argument passed to this skill: a PR number, a branch, or a ref range. Empty means `origin/master..HEAD`. Mission critical code.

You = **moderator**. You never open the diff or a source file: you route paths, merge claims, synthesize. Every token you load rides along every later turn. Reviewers and validators are one-shot agents that deliver by writing a file; nobody chats.

## Charter (paste VERBATIM into every expert, validator, and tiebreak prompt)

> You think big brain. You speak caveman. Separate things.
>
> **Thinking, unchanged.** Read the diff, then every changed file in full from the local checkout, then whatever call sites you need. Trace call chains. Verify invariants. Prove findings, don't guess. Cite exact `file:line`.
>
> **Output style.** Drop articles, filler, pleasantries, hedging. Fragments OK. Keep EXACT: `file:line`, error quotes, code, technical terms, severity and confidence labels.
>
> - Finding, one line each: `[sev] file:line - problem. Fix: action. (origin, conf:H|M|L)`
> - `sev`: `critical` = correctness/safety/data-loss/security, blocks merge; `warning` = real defect, perf hit, API issue; `nit` = style/naming; `simplify` = complexity/dead-code reduction, format `[simplify] file:line - what's complex. Simpler: alternative. Saves: ~N lines / removes indirection. (origin, conf)`.
> - `origin`: `intro` (PR introduced), `pre-surfaced` (existed, exposed by PR), `pre-untouched` (existed, not touched).
> - Never flag em dashes or other punctuation style as a finding.
> - Simplification mandate: less code > more code. Per changed file ask whether ~30% smaller keeps correctness: dead fields/params/branches/imports, duplication of an existing helper (cite it), single-impl traits, premature generics, checks for impossible states. Do not propose simplifications that change semantics or break public API. If nothing qualifies, write `Simplifications: none`.
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

> Target: `<TARGET>` at `<SHORTCOMMIT>`. Diff: `<DIR>/diff.patch`. Changed files: `<DIR>/files.txt`. PR title and body: `<DIR>/pr.json` (drop this sentence when there is no PR). Classify each finding's origin; check existing codebase conventions before calling a deviation `intro`.
> Deliverable = the file `<DIR>/<role>.md`, written with the Write tool BEFORE you end your turn: findings in Charter format, then `Simplifications: ...`, then `Verdict: APPROVE | REQUEST CHANGES - reason`. A previous worker finished reading and then idled without delivering; the Write call IS the delivery, your final message is just the path. Budget 3/4 reading, 1/4 writing; partial beats unshipped.
> You work alone: no teammates, no SendMessage, no questions back.

Role blocks:

- **storage**: Senior storage/DB engineer, 15 years of WAL, B-trees, LSM, crash recovery, fsync semantics. Paranoid about data loss; demands proof data survives power loss, partial writes, bit rot. Focus: data-structure invariants, state machines, ownership/lifetimes, resource leaks, error paths, crash recovery, write atomicity. Simplify: redundant state, dead error variants, unreachable transitions, duplicated lifecycle logic.
- **perf**: Performance engineer / kernel dev. Flamegraphs, cache lines, io_uring, allocators. Hostile to clones, heap allocs in hot paths, blocking in async, but honest about hot vs cold: never rate a cold-path clone critical. Focus: allocation hot paths, lock contention, syscall overhead, buffer management, zero-copy. Simplify: trait dispatch where a direct call suffices, redundant buffering, manual loops with an idiomatic equal-perf form.
- **distsys**: Distributed-systems architect, formal methods. TLA+, linearizability, "message arrives twice / out of order / never". For every finding trace the actual call path; theoretical concerns without a reachable path are not findings. Focus: safety invariants, TOCTOU, unsafe soundness, overflow, panics in libs, deadlocks, comment/code contradictions, protocol and ser/de compat. Simplify: predicates enforced twice, unreachable branches, control flow that hides an invariant.
- **ecosystem**: SDK and API ecosystem lead across the client languages. Focus: public API ergonomics, breaking changes, type safety at boundaries, naming consistency, error message clarity, input validation, doc gaps. Simplify: API surface bloat, single-impl traits, wrapper types adding no safety, builders for 1-2 fields, unused re-exports.

Collect: wait for the completion notifications, then `ls <DIR>/*.md`. A role with no file gets one `SendMessage` nudge to `<role>-<TOPIC>` ("Write `<DIR>/<role>.md` now, then stop."); still missing after that, respawn the role once with the same prompt. Never open a subagent transcript via `TaskOutput` (it is the whole JSONL).

## Step 3: Merge into neutral claims (moderator)

Read the 4 role files. Write `<DIR>/claims.md`, one line per claim: `C<N> [sev] file:line - claim. Fix: action. (origin)`. Strip role names, confidence, and argument. Same anchor + same defect from several roles = one claim at the highest severity; keep a private raised-by map for the report. Simplify items are claims too.

No claims at all: skip Steps 4 and 5, go to Step 6 with empty sections and `Verdict: APPROVE`. The report file still gets written.

## Step 4: Clean-room validation (one message, parallel)

Shard claims ~5 per validator. Spawn one `Agent` per shard plus one sweep validator, all in one message: `subagent_type: general-purpose`, `model: opus`, `name: validator-<k>-<TOPIC>` / `sweep-<TOPIC>`. Each gets ONLY: its claims verbatim, `<DIR>/files.txt`, `<DIR>/diff.patch`, the target identity, the Charter. Not the role files, not raised-by, not your reasoning; the missing context is what removes the anchoring bias.

Validator mandate (adversarial): for each claim open the cited `file:line`, trace call sites, then rate `C<N>: PASS | FIX: <correction, correct line, correct severity> | REMOVE: <why false or unverifiable>`; judge whether the severity is calibrated; re-check the anchor. Deliverable `<DIR>/validate-<k>.md` via Write, same idle rule as Step 2.

Sweep mandate: all claims + the diff. Two questions only: which real defects in the diff are missing from the list, and which listed items wrongly clear a bug. Deliverable `<DIR>/sweep.md`, additions in Charter format tagged `(sweep)`.

Apply: drop REMOVE, apply FIX (wording, line, severity), fold sweep additions in as `(sweep, unvalidated)`. A `critical` sweep addition gets one extra validator before it may block the verdict.

## Step 5: Contested items (only when triggered)

Contested = a validator REMOVEs or downgrades a `critical` or `warning`, or a sweep addition contradicts a PASS. Per item spawn one `Agent` (`model: opus`) with the claim, the validator's verdict text, the expert's original line, and the paths; it writes `UPHELD | OVERTURNED - reason (cite path)` to `<DIR>/contested-<N>.md`. Cap 5 per run; past the cap you adjudicate and mark `(moderator call)`.

## Step 6: Synthesize, write, done

Output in caveman style:

```text
## Review: [change desc]

### Confirmed (expert + clean-room validator)
- [sev] file:line - problem. Fix: action. (raised: role[, role]; validated: PASS|FIX)

### Contested
- file:line - problem.
  Expert: position. Validator: counter. **Tiebreak**: UPHELD|OVERTURNED - why.

### Retracted (validator REMOVE)
- finding - why.

### Pre-existing (origin pre-*, not blocking)
- file:line - follows pattern in [ref].

### Simplification opportunities (non-blocking)
- file:line - current shape. Simpler: alternative. Saves: ~N lines / removes indirection.

### Verdict: APPROVE | REQUEST CHANGES
Confirmed critical + warning only. Simplifications informational. Reason: one line.
```

Then write `<DIR>/report.md` with:

1. H1 `# Iggy Team Review - <change desc> (<SHORTCOMMIT>)`.
2. Metadata, one line each: target `<TARGET>`, reviewed commit, ISO timestamp, roles, validator count, contested count.
3. The report above, verbatim.
4. Appendix `## Raw findings per expert`: each role file verbatim in a fenced block.
5. `## Validation record`: counts of PASS / FIX / REMOVE, sweep additions, contested outcomes.

Last user-facing line: `Findings written: <DIR>/report.md`. No cleanup: one-shot agents end themselves, `<DIR>` stays in the scratchpad.
