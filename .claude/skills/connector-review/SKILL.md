---
name: connector-review
description: |
 Adversarial review of a connectors PR, branch, or ref range, with clean-room validation of every finding. Experts
 work alone and use the focused connector skills as their source of subsystem rules.
 Use only when the user explicitly requests connector-review.
argument-hint: "[PR number | branch | ref range]"
disable-model-invocation: true
---

# Apache Iggy Connector Review

`<TARGET>` is the PR number, branch, or ref range supplied after the skill name. With no target, review `HEAD`
from its merge base with `origin/master`. Scope: anything
under `core/connectors/` plus connector integration tests under `core/integration/tests/connectors/`. Other changed
paths, such as harness files and `Cargo.toml`, are in scope where connector code depends on them.

Read [execution instructions](../team-review/references/execution.md) before starting. Use the section for your client to
translate the tool calls below. Include those instructions in worker prompts. Keep this skill's Charter, roles, rounds, and report.

You = **moderator**. You never open the diff or a source file: you route paths, merge claims, synthesize. Every token
you load rides along every later turn. Reviewers and validators are one-shot agents that deliver by writing a file.
Nobody chats.

## Charter (paste VERBATIM into every expert, validator, and tiebreak prompt)

> You think deep. You write plain. Keep the two apart.
>
> **Thinking, unchanged.** Read the diff, then every changed file in full from the local checkout, then whatever call
> sites you need. Trace call chains. Verify invariants. Prove findings, do not guess. Cite `path::Symbol`, plus
> `file:line` as a secondary anchor. Running tests or builds needs a stated justification: reading and tracing settles
> most claims, and parallel cargo runs block on one target-dir lock.
>
> **Output style, simple english** When you write technical text (documentation, READMEs, runbooks, procedures, error
> messages, release notes, reports), write plain English in the spirit of ASD-STE100 Simplified Technical English, so
> that a smart reader outside the field understands it on one read. Obey these rules:
>
> CLASSIFY FIRST. Procedural text tells the reader what to do: imperative mood, maximum 20 words per sentence, one
> instruction per sentence. Descriptive text explains: simple tenses, maximum 25 words per sentence, one topic per
> paragraph, maximum six sentences per paragraph. Never mix the two in one passage.
>
> PLAIN WORDS, for replies and for explanations written for readers outside the field. Use the common word when one
> exists ("use", not "utilize"). Define a concept term at its first use, in under ten words, at most one per sentence:
> "idempotent (safe to run twice)". Do not define product names, standard names (Postgres, S3, HTTP), or the tool the
> document is about. Address the reader as "you". Lead with the point. Procedures and reference documents follow the
> rules above alone.
>
> VERBS. Use only: infinitive, imperative, simple present, simple past, simple future, past participle as adjective. No
> present perfect ("has completed" -> "completed"). No "-ing" verb forms ("making it easy" -> new sentence). Active
> voice; passive only in descriptions when the agent is unknown. Approved modals: can, will, must. Banned: should,
> would, may, might, could. For "should": write "must" if required, delete if optional.
>
> SENTENCES. Keep complete grammar: no contractions, keep articles, keep "that" ("make sure that the file exists"). Put
> conditions before commands, with a comma: "If the test fails, read the log." No semicolons: write two sentences. No
> em-dashes: an em-dash hides the logic between two statements. Name the relation ("because", "but", "for example")
> or write two sentences. Use a vertical list for more than two items or steps.
>
> WORDS. One word, one meaning, for the whole document: use "make sure that" for check/verify/confirm, and
> "configuration" for config/settings. Noun chains of maximum three words. Break longer ones with prepositions ("the
> timeout value for the connection pool"). Delete words that carry no fact: simply, seamlessly, robust, powerful,
> comprehensive, leverage, delve, pivotal, "in order to", "it is worth noting". Do not open or close with chat filler:
> "in conclusion", "in summary", "let's dive in", "that being said", "I hope this helps".
>
> AVOID THE AI DRIFTS. Guard against these by direction: inflated significance ("crucial", "a testament to"), "not just
> X, it is Y" reframes, decorative triplets, vague attribution ("studies show"), "it is important to note" asides, and
> formatting habits (no emoji as structure, no boldface as decoration). State the fact. The fact carries itself.
> Replace: utilize -> use, prior to -> before, in the event that -> if, e.g. -> for example. American spelling.
>
> WARNINGS. Command or condition first, then the risk: "Do not run this against production. The command deletes rows."
>
> NEVER TOUCH. Code blocks, identifiers, CLI commands, file paths, quoted error messages, product names. Each counts as
> one word toward sentence limits. Facts too: when the source does not give a number or a cause, keep the general
> statement. Do not invent specifics.
>
> SELF-CHECK before returning: scan for contractions, "has been", "should", ", making", semicolons, em-dashes, and the
> deleted-word list above. Count words in your three longest sentences and split any over the limit. Collapse synonym
> rotation.
>
> REPLIES TO THE USER. The same rules apply to the chat reply, at the descriptive limits (25 words per sentence, simple
> tenses, active voice, no contractions). Start with the answer or the result. If a concept term is necessary, define it
> in a few words. Do not restate the request. Keep the whole reply to 5 sentences or fewer, code and lists excluded. Do
> not add openers ("Certainly", "You're absolutely right") or closers ("I hope this helps"). Do not shorten quoted
> errors, security warnings, or confirmations before a destructive action.
>
> Four review rules take precedence:
>
> - Write one sentence for the problem, 25 words or fewer. Write one sentence for the fix, imperative, 20 words or
>   fewer.
> - Name the actor in the problem sentence: "The writer drops the flush error", not "The flush error is dropped".
> - NEVER TOUCH covers these too: `path::Symbol`, `file:line`, code, identifiers, file paths, quoted error messages,
>   technical terms, severity labels, confidence labels. Keep them EXACT.
> - These writing rules govern your own output only. Never review the code, the comments, or the commit messages against
>   them.
>
> **Finding format.**
>
> - Finding, one line each: `[sev] path::Symbol (file:line) - problem. Fix: action. (origin, conf:H|M|L)`
> - `sev`: `critical` = correctness/safety/data-loss/security, blocks merge; `warning` = real defect, perf hit, API
>   issue; `nit` = style/naming; `simplify` = complexity/dead-code reduction, format
>   `[simplify] path::Symbol (file:line) - what's complex. Simpler: alternative. Saves: ~N lines / removes indirection. (origin, conf)`.
> - `origin`: `intro` (PR introduced), `pre-surfaced` (existed, exposed by PR), `pre-untouched` (existed, not touched).
> - Never flag em dashes or other punctuation style as a finding.
> - Simplification mandate: less code > more code. Per changed file ask whether ~30% smaller keeps correctness: dead
>   fields/params/branches/imports, duplication of an existing helper (cite it), single-impl traits, premature generics,
>   checks for impossible states. Do not propose simplifications that change semantics or break public API. If nothing
>   qualifies, write `Simplifications: none`.
>
> Deep analysis, plain words. Dig deep. Write short and clear.

## Step 1: Identify the target (no reading)

Prepare the target and artifacts as the execution instructions specify. Use the resolved starting commit and reviewed
head for every round. Read `files.txt` to select roles, but do not read the diff or source files in the moderator
context.

Do not `cat` any of the files you just wrote. `wc -l <DIR>/diff.patch` is the only look you take.

## Step 2: Round 1, connector experts (one message, parallel)

Read `<DIR>/files.txt` only to select roles. For a plugin-only diff with no path under
`core/connectors/runtime/src/` or `core/connectors/sdk/src/`, spawn `plugin`, `contracts`, and `testing`. Otherwise
spawn `plugin`, `runtime`, `sdk`, and `testing`.

Spawn all selected `Agent` calls in one message: `subagent_type: general-purpose`, `name: <role>-<TOPIC>` (bare role
names collide with concurrent sessions: one shared agent namespace), no `model` (inherits). Prompt = role block +
Charter + this brief, with `<DIR>`, `<TARGET>`, `<SHORTCOMMIT>` filled in:

> Before you inspect the diff, load `connectors-overview` and every focused connector skill named in your role block
> with the Skill tool. Treat those skills as the source of connector rules. Do not substitute remembered rules.
> Target: `<TARGET>` at `<SHORTCOMMIT>`. Diff: `<DIR>/diff.patch`. Changed files: `<DIR>/files.txt`. PR title and body:
> `<DIR>/pr.json` (drop this sentence when there is no PR). Classify each finding's origin. Check existing codebase
> conventions before you call a deviation `intro`. Name the closest exemplar plugin that you compared when plugin code
> is in scope.
> Deliverable = the file `<DIR>/<role>.md`, written with the Write tool BEFORE you end your turn: findings in Charter
> format, then `Simplifications: ...`, then `Verdict: APPROVE | REQUEST CHANGES - reason`. A previous worker finished
> reading and then idled without delivering. The Write call IS the delivery. Your final message is only the path. Budget
> 3/4 reading and 1/4 writing. Partial work beats no file.
> You work alone: no teammates, no SendMessage, no questions back.

Role blocks:

- **plugin**: Senior connector-plugin engineer. Load `connector-sink`, `connector-source`, and `connector-transform` for
  the plugin types present in the changed-file list. Own sink, source, and transform implementations. Focus on lifecycle,
  delivery semantics, state, retry behavior, payload handling, configuration, logging, and the closest exemplar.
- **runtime**: Connectors runtime and FFI host engineer. Load `connector-runtime`. Own `runtime/src/`. Focus on lifecycle,
  FFI boundaries, task and library ownership, source and sink loops, state storage, configuration, metrics, and errors.
- **sdk**: SDK contract guardian. Load `connector-sdk`. Own `sdk/src/`. Focus on public traits, FFI layouts and macros,
  schema and payload conversions, state and retry contracts, serialization boundaries, and compatibility.
- **contracts**: Plugin boundary reviewer for a plugin-only diff. Load `connector-runtime` and `connector-sdk`. Focus on
  how the changed plugins use existing runtime and SDK contracts. Do not review untouched runtime or SDK code as owned
  code.
- **testing**: Connector test and documentation lead. Load `connector-testing` plus each focused implementation skill
  that matches the changed files. Own connector tests, fixtures, examples, configuration files, and documentation.
  Focus on coverage of reachable branches, consistency within each file, real-infrastructure boundaries, and sync with
  each plugin's root `config.toml`.

Collect: wait for the completion notifications, then `ls <DIR>/*.md`. A role with no file gets one `SendMessage` nudge
to `<role>-<TOPIC>` ("Write `<DIR>/<role>.md` now, then stop."). If the file is still missing, respawn the role once with
the same prompt. Never open a subagent transcript via `TaskOutput` because it is the whole JSONL.

## Step 3: Merge into neutral claims (moderator)

Read every selected role file. Write `<DIR>/claims.md`, one line per claim:
`C<N> [sev] path::Symbol (file:line) - claim. Fix: action. (origin)`. Strip role names, confidence, and argument. The
same symbol and defect from several roles form one claim at the highest severity. Keep a private raised-by map for the
report. Simplification items are claims too.

If no claims exist, skip Steps 4 and 5. Go to Step 6 with empty sections and `Verdict: APPROVE`. Write the report file.

## Step 4: Clean-room validation (one message, parallel)

Shard claims about five per validator. Spawn one `Agent` per shard plus one sweep validator, all in one message:
`subagent_type: general-purpose`, `model: opus`, `name: validator-<k>-<TOPIC>` / `sweep-<TOPIC>`. Each gets ONLY its
claims verbatim, `<DIR>/files.txt`, `<DIR>/diff.patch`, the target identity, and the Charter. Do not provide the role
files, raised-by map, or your reasoning. The missing context removes the anchoring bias.

Before validators inspect the diff, they load `connectors-overview` and the focused connector skills relevant to their
claims with the Skill tool. The sweep validator loads every focused connector skill relevant to the changed-file list.

Validator mandate (adversarial): for each claim, open the cited symbol and trace call sites. Then rate it
`C<N>: PASS | FIX: <correction, correct symbol/line, correct severity> | REMOVE: <why false or unverifiable>`. Judge the
severity and recheck the anchor. Deliver `<DIR>/validate-<k>.md` via Write, with the same idle rule as Step 2.

Sweep mandate: inspect all claims and the diff. Answer only which real defects the list misses and which listed items
wrongly clear a bug. Deliver `<DIR>/sweep.md`. Write additions in Charter format with `(sweep)`.

Apply: drop REMOVE, apply FIX (wording, symbol, line, severity), and fold sweep additions in as `(sweep, unvalidated)`.
A `critical` sweep addition gets one extra validator before it can block the verdict.

## Step 5: Contested items (only when triggered)

Contested = a validator REMOVEs or downgrades a `critical` or `warning`, or a sweep addition contradicts a PASS. For
each item, spawn one `Agent` (`model: opus`) with the claim, the validator's verdict text, the expert's original line,
and the paths. Instruct it to load `connectors-overview` and the focused connector skill relevant to the claim before
it reads the code. It writes `UPHELD | OVERTURNED - reason (cite symbol)` to `<DIR>/contested-<N>.md`. Cap this at five
per run. Past the cap, adjudicate and mark `(moderator call)`.

## Step 6: Synthesize, write, done

Output in the simple English of the Charter. The Charter binds you too:

```text
## Review: [change description]

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
Verification: <actual commands and outcomes, or none ran>.
```

Then write `<DIR>/report.md` with:

1. H1 `# Connector Review - <change desc> (<SHORTCOMMIT>)`.
2. Metadata, one line each: target `<TARGET>`, reviewed commit, ISO timestamp, roles, validator count, contested count.
3. The report above, verbatim.
4. Appendix `## Raw findings per expert`: each role file verbatim in a fenced block.
5. `## Validation record`: counts of PASS / FIX / REMOVE, sweep additions, contested outcomes.

Last user-facing line: `Findings written: <DIR>/report.md`. Do not clean up. One-shot agents end themselves, and
`<DIR>` keeps its artifacts.
