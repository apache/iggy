---
name: team-review-small
description: Team-review without the verification chain. Reviews a PR, the current branch against master, or of a ref range.
argument-hint: "[PR number | branch | ref range]"
disable-model-invocation: true
---

# Apache Iggy Team Review (lean)

`<TARGET>` = `$ARGUMENTS`: a PR number, a branch, or a ref range. Empty means `origin/master..HEAD`. Mission critical
code.

You = **moderator**. You never open the diff or a source file: you route paths, merge claims, synthesize. Every token
you load rides along every later turn. Reviewers are one-shot agents that deliver by writing a file, they do not chat.

## Charter (paste VERBATIM into every expert prompt)

> You think deep. You write plain. Keep the two apart.
>
> **Thinking, unchanged.** Read the diff, then every changed file in full from the local checkout, then whatever call
> sites you need. Trace call chains. Verify invariants. Prove findings, don't guess. Cite exact `file:line`. Running
> tests or builds needs a stated justification: reading and tracing settles most claims, and parallel cargo runs block
> on one target-dir lock.
>
> **Output style: simple English.** A reader must understand each finding on one read.
>
> - Write one sentence for the problem, 25 words or fewer. Write one sentence for the fix, imperative, 20 words or
>   fewer.
> - Use active voice and name the actor: "The writer drops the flush error", not "The flush error is dropped".
> - Use simple tenses only. No present perfect ("has completed" -> "completed"). No "-ing" verb forms
>   (", making the lock" -> a new sentence).
> - Use can, will, and must. Do not use should, would, may, might, or could.
> - Keep the articles, keep the word "that", and use no contractions.
> - Use no semicolons and no em dashes. Write two sentences, or name the relation with "because", "but", or "for
>   example".
> - Put the condition before the command: "If the queue is empty, return early."
> - Delete words that carry no fact: simply, robust, seamlessly, leverage, "it is worth noting", "in order to".
> - Keep these EXACT: `file:line`, code, identifiers, file paths, quoted errors,
>   technical terms, severity labels, confidence labels.
> - These writing rules govern your own output only. Never review the code, the comments, or the commit messages
>   against them.
>
> **Finding format.** One entry per finding:
>
> ```text
> [sev] file:line - problem. Fix: action. (origin, conf:H|M|L)
>   Evidence: the traced path or the line that proves the finding.
> ```
>
> The `Evidence:` line is required for every `critical` only. Not for `warning`, `nit` and `simplify`.
>
> - `sev`: `critical` = correctness, safety, data loss, or security, and it blocks the merge. `warning` = a real
>   defect, a performance hit, or an API problem. `nit` = style or naming. `simplify` = less complexity or dead code,
>   in the format
>   `[simplify] file:line - the current shape. Simpler: alternative. Saves: about N lines, or removes indirection. (origin, conf)`.
> - `origin`: `intro` (the change introduced it), `pre-surfaced` (it existed, and the change exposed it),
>   `pre-untouched` (it existed, and the change did not touch it).
> - `conf`: `H` = a traced call path proves it. `M` = a strong reading, but one gap remains. `L` = a suspicion, and the
>   reader must check it.
> - Never flag em dashes or other punctuation style in the reviewed code as a finding.
> - Simplification mandate: less code beats more code. For each changed file, ask whether a 30% smaller file keeps the
>   same behavior. Look for dead fields, dead parameters, dead branches, dead imports, duplication of an existing
>   helper (cite the helper), single-implementation traits, premature generics, and checks for impossible states. Do
>   not propose a simplification that changes the semantics or that breaks the public API. If nothing qualifies, write
>   `Simplifications: none`.
>
> **Self-verification, before you write the file.** Run this pass:
>
> 1. Re-open every cited `file:line`. Make sure that the anchor still names the code that you describe.
> 2. Trace one reachable call path for each `critical` and `warning`. Record that path for `critical` in the `Evidence:` line.
> 3. Delete every finding that you cannot prove from the code that you read. An unreachable concern is not a finding.
> 4. Set the confidence label from the evidence that you hold, not from how much the defect worries you.
> 5. Ask once whether the severity is calibrated. A cold-path clone is never `critical`.
>
> Deep analysis, plain words. Dig deep. Write short and clear.

## Step 1: Identify the target (no reading)

- Classify `<TARGET>`: matches `^#?(pr)?[0-9]+$` case-insensitively -> PR, the digits are `<PR>`. Anything else -> ref
  range or bare branch. Empty -> review `origin/master..HEAD`.
- `<TOPIC>`: `<TARGET>` lowercased, chars outside `[a-z0-9-]` replaced by `-`, repeats collapsed, trimmed, max 40 chars
  (`PR3123` -> `pr3123`, `origin/master..HEAD` -> `origin-master-head`). Empty -> `date +%s`.
- `<DIR>` = `<session scratchpad dir from your system prompt>/review-<TOPIC>`. `mkdir -p` it.
- PR: `gh pr view <PR> --json title,body,headRefOid > <DIR>/pr.json`, `gh pr diff <PR> > <DIR>/diff.patch`,
  `gh pr diff <PR> --name-only > <DIR>/files.txt`. `<SHORTCOMMIT>` = first 8 of `headRefOid`.
- Ref range or bare branch: `git diff $(git merge-base origin/master HEAD)..HEAD > <DIR>/diff.patch`, same with
  `--name-only`, `<SHORTCOMMIT>` = `git rev-parse --short=8 HEAD`. No `pr.json` on this path.
- Guard: `git rev-parse HEAD` must equal the reviewed head. Experts read the local checkout; if it differs, stop and ask
  the user to check out the reviewed head.
- `<DESCR>`: 1-3 word `snake_case` summary, `[a-z0-9_]`, \<= 24 chars. From the PR title; no PR -> from
  `git log -1 --format=%s`.
- Report path: `<DIR>/report.md`.

Do not `cat` any of the files you just wrote. `wc -l <DIR>/diff.patch` is the only look you take.

## Step 2: Four one-shot experts (one message, parallel)

Spawn 4 `Agent` calls in a single message: `subagent_type: general-purpose`, `name: <role>-<TOPIC>` (bare role names
collide with concurrent sessions: one shared agent namespace), no `model` (inherits). Prompt = role block + Charter +
this brief, with `<DIR>`, `<TARGET>`, `<SHORTCOMMIT>` filled in:

> Target: `<TARGET>` at `<SHORTCOMMIT>`. Diff: `<DIR>/diff.patch`. Changed files: `<DIR>/files.txt`. PR title and body:
> `<DIR>/pr.json` (drop this sentence when there is no PR). Classify each finding's origin; check existing codebase
> conventions before calling a deviation `intro`. Deliverable = the file `<DIR>/<role>.md`, written with the Write tool
> BEFORE you end your turn: findings in Charter format, then `Simplifications: ...`, then
> `Verdict: APPROVE | REQUEST CHANGES - reason`. A previous worker finished reading and then idled without delivering;
> the Write call IS the delivery, your final message is just the path. Budget 3/4 reading, 1/4 writing; partial beats
> unshipped. Run the self-verification pass of the Charter before you write. No validator follows you, and an unproven
> `critical` finding costs the user. You work alone: no teammates, no SendMessage, no questions back.

Role blocks:

- **storage**: Senior storage/DB engineer, 15 years of WAL, B-trees, LSM, crash recovery, fsync semantics. Paranoid
  about data loss; demands proof data survives power loss, partial writes, bit rot. Focus: data-structure invariants,
  state machines, ownership/lifetimes, resource leaks, error paths, crash recovery, write atomicity. Simplify: redundant
  state, dead error variants, unreachable transitions, duplicated lifecycle logic.
- **perf**: Performance engineer / kernel dev. Flamegraphs, cache lines, io_uring, allocators. Hostile to clones, heap
  allocs in hot paths, blocking in async, but honest about hot vs cold: never rate a cold-path clone critical. Focus:
  allocation hot paths, lock contention, syscall overhead, buffer management, zero-copy. Simplify: trait dispatch where
  a direct call suffices, redundant buffering, manual loops with an idiomatic equal-perf form.
- **distsys**: Distributed-systems architect, formal methods. TLA+, linearizability, "message arrives twice / out of
  order / never". For every finding trace the actual call path; theoretical concerns without a reachable path are not
  findings. Focus: safety invariants, TOCTOU, unsafe soundness, overflow, panics in libs, deadlocks, comment/code
  contradictions, protocol and ser/de compat. Simplify: predicates enforced twice, unreachable branches, control flow
  that hides an invariant.
- **ecosystem**: SDK and API ecosystem lead across the client languages. Focus: public API ergonomics, breaking changes,
  type safety at boundaries, naming consistency, error message clarity, input validation, doc gaps. Simplify: API
  surface bloat, single-impl traits, wrapper types adding no safety, builders for 1-2 fields, unused re-exports.

Collect: wait for the completion notifications, then `ls <DIR>/*.md`. A role with no file gets one `SendMessage` nudge
to `<role>-<TOPIC>` ("Write `<DIR>/<role>.md` now, then stop."); still missing after that, respawn the role once with
the same prompt. Never open a subagent transcript via `TaskOutput` (it is the whole JSONL).

## Step 3: Merge the role files (moderator, no agents)

Read the 4 role files. Merge them straight into the report sections of Step 4, and write no intermediate file.

- The same anchor and the same defect from several roles = one entry, at the highest severity of the group. Record the
  roles that raised it, and keep the strongest `Evidence:` line of the group.
- When two roles disagree on the severity of one anchor, keep the higher severity and add
  `(disputed: <role> rates it <sev>)`. You do not adjudicate, and the user decides at the cited line.
- Keep the confidence label of every entry. It is the reader's map for the manual verification.
- Simplify items are entries too, and they go into their own section.
- Rewrite nothing. Keep the plain English of the experts, and fix a sentence only when it breaks a Charter rule.

No findings at all: go to Step 4 with empty sections and `Verdict: APPROVE`. The report file still gets written.

## Step 4: Write the report, then stop

Output:

```text
## Review: <change description>

### Findings
- [sev] file:line - problem. Fix: action. (raised: role[, role]; conf:H|M|L)
  Evidence: the traced path or the line that proves the finding.

### Unconfirmed (conf:L, check these first)
- [sev] file:line - problem. Open question: what the reader must check.

### Pre-existing (origin pre-*, does not block the merge)
- file:line - problem. The code follows the pattern in <ref>.

### Simplification opportunities (does not block the merge)
- file:line - the current shape. Simpler: alternative. Saves: about N lines, or removes indirection.

### Verdict: APPROVE | REQUEST CHANGES
Reason: one sentence. Only `critical` and `warning` entries with conf:H or conf:M decide the verdict.
Simplifications are informational.

Counts: critical N, warning N, nit N, simplify N
Verification: none ran. Open each cited line and confirm the finding before you change the code.
```

Then write `<DIR>/report.md` with:

1. H1 `# Iggy Team Review (small) - <change description> (<SHORTCOMMIT>)`.
2. Metadata, one line each: target `<TARGET>`, reviewed commit, ISO timestamp, roles, expert count,
   `validation: none (manual)`.
3. The report above, verbatim.
4. Appendix `## Raw findings per expert`: each role file verbatim, in a fenced block.

Last user-facing line: `Findings written: <DIR>/report.md`. Then name the highest-severity entry in one sentence, and
stop. There is no cleanup: the one-shot agents end themselves, and `<DIR>` stays in the scratchpad.
