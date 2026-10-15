---
name: fix-review-comments
description: |
 Walk through a GitHub PR's review comments one at a time with the user. For each comment: quote it, restate
 the problem in plain simple English, show the exact code, verify the claim, propose a fix, and wait for the
 user's decision. Once the user decides, hand the fix to a background agent and move straight on to the next
 comment, so the user can keep deciding while fixes run. Use when the user asks to "go over" or "walk through"
 a PR review, fix review comments one by one, or address feedback from a specific PR/reviewer.
argument-hint: "[PR number]"
disable-model-invocation: true
---

# Fix Review Comments

`<PR>` = `$ARGUMENTS`. If empty, resolve the open PR for the current branch:
`gh pr view --json number --jq .number`. If that fails (no PR for this branch, or ambiguous), ask the user for
the PR number - do not guess.

Decisions are made **one item at a time, with the user**. Fixes are not: once the user approves an item, the
fix runs in a separate background agent while the conversation moves on. Never fix more than one comment
without the user's explicit go-ahead on that specific comment. Never silently skip, reorder, or bundle
comments unless the user asks you to.

## 1. Fetch the review

```bash
gh pr view <PR> --json title,url,state,body
gh pr view <PR> --repo <owner>/<repo> --json reviews --jq '.reviews[] | {author: .author.login, state: .state, body: .body}'
gh api repos/<owner>/<repo>/pulls/<PR>/comments --paginate
```

Infer `<owner>/<repo>` from `gh repo view --json owner,name` or the git remote if not already known. Pull every
inline comment via the `comments` API (paginated - a review can have 15+ line comments, don't stop at the first
page). Note `path`, `line`, `diff_hunk`, `body`, and `in_reply_to_id` for each. Also capture the top-level review
summary (`state`, `body`) if one was left alongside the inline comments - it often names a specific blocker.

Build an ordered list of every distinct comment (file order is fine, or blocker-first if the review names one
explicitly - ask the user which they'd prefer if it's not obvious). Give the user the full list up front as a
scannable summary (one line per item: file:line + a short paraphrase) before starting item 1, so they have the
map before you go one by one.

## 2. Per-item loop

For **each** comment, in order:

1. **State which item this is** (e.g. "#3 - `path/to/file.rs:161`") and **quote the reviewer's comment verbatim**.
2. **Restate the problem in plain, simple English, first.** Reviewer comments are dense: they use shorthand,
   assume context, and pack several claims into one sentence. Decode that. Say what the code does today, what
   goes wrong, and who or what is affected, in short everyday sentences. No arrows, no stacked parentheticals,
   no unexplained jargon or abbreviations, no review-speak. If a technical term is unavoidable, explain it in
   half a sentence. Write it as if the user has never seen the comment, because the quote above is hard to read.
3. **Show the actual code** the comment is anchored to, embedded directly in your message text as a fenced code
   block - not just a tool call the user has to go dig through. Read the current file state (it may have moved
   since the review was posted; don't trust the diff hunk's line numbers blindly) and paste the real, current
   snippet.
4. **Verify and add the technical detail in 2-5 sentences**: why it matters, and - when the comment's claim
   isn't obviously true from reading the diff alone - go verify it against the real code/library/tests before
   asserting it. This project in particular rewards checking claims against vendored dependency source, existing
   sibling code (other connectors, other sinks), and relevant test files rather than taking a reviewer comment's
   premise on faith. Say plainly when a claim checked out, when it didn't, and when you could not check it. If
   the user pushes back or disagrees with the reviewer, investigate (issue threads, related PRs, other
   implementations) rather than deferring immediately - give a real recommendation, then let them decide.
5. **Propose a fix**: either a single concrete diff, 2-4 named options, or a clarifying question if the right
   move genuinely depends on a call only the user can make (e.g. behavior change vs. doc-only, which of several
   valid designs). Keep proposed code short and exact - no more than what's actually being asked for. Don't
   gold-plate a nit into a refactor.
6. **Wait for the decision.** Do not start any fix for this item until the user replies with something that is
   actually approval (e.g. "yes", "approve", "go ahead", a chosen option). A question back from the user is not
   approval - answer it and re-propose or re-confirm first. If the user asks for changed wording, redraft and
   present again.
7. **On approval, hand the fix to a separate agent and move on** (see section 3). Do not apply the edit
   yourself, and do not wait for the agent. Tell the user in one line that the fix has started, or that it is
   queued behind the fix that is still running, then present the next item right away.
8. **Move to the next item automatically whenever the current one needs nothing more from the user.** That
   covers an item whose fix was just handed to an agent, an item the user explicitly deferred, and an item that
   turns out to need no change (already fixed in the working tree, a stale comment, or covered by an earlier
   item). For the last kind, say so in a line or two with the evidence, then continue without asking. Never
   skip past an item that still needs a decision.

If fixing one comment reveals it also fixes, or conflicts with, a later one in the list, say so before proceeding
rather than silently merging them.

## 3. Fix agents

When the user approves an item, launch a fix agent with the Agent tool, in the background. The agent starts
cold, so its prompt must be self-contained:

- The exact approved change (diff or precise description), the files involved, and the reviewer comment for
  context.
- "Apply exactly this and nothing else. No drive-by cleanup." The repo's own rules apply (read the project's
  CLAUDE.md or AGENTS.md; follow its style, verification order, and prohibitions).
- The verification to run afterward for the file types touched (`cargo check -p <crate>`,
  `cargo test -p <crate>`, `./scripts/ci/markdownlint.sh <file>`, `./scripts/ci/taplo.sh --check`,
  `./scripts/ci/license-headers.sh`, etc., in the project's order).
- If a check cannot run because a tool is missing, do **not** install anything. Report which tool and which
  check, and stop that check.
- If a decision comes up that only the user can make, stop and report it as a question. Do not guess.
- Report back: what changed (file:line), which checks passed, which failed with their output, and which could
  not run.

**Only one fix agent may be working at a time.** Each approved item gets its own agent, but the agents share
one working tree, so two of them editing at once can overwrite or conflict with each other. Before launching
one, check whether another fix agent is still running: look at the agents you launched earlier in this
conversation and call ListAgents to confirm which are still active.

- If none is running, launch the new agent now.
- If one is running, do not launch. Put the approved fix in a queue, in the order the user approved it, and
  tell the user it is queued. Keep the full approved change in your own notes so the agent prompt can be
  written later.
- When the running agent reports (done, failed, or blocked and answered), launch the next queued fix. Do this
  as soon as the report arrives, before moving on to anything else, and then give the user the report.
- If an agent is blocked waiting on the user, it still counts as running. Do not launch another until it
  finishes or the user tells you to abandon it.
- Only a fix agent counts for this rule. Read-only work does not need to wait.

**Talking to the user while agents run:**

- Stay quiet about an agent while it works. Do not narrate progress.
- Speak up only when an agent is done or when it needs the user. When it is done, give the result: what
  changed, and the check results, failures first. When it is blocked, ask the user its question, and pass the
  answer back to that same agent with SendMessage so it continues with its context.
- If an agent finishes while the user is mid-decision on another item, put the short result at the top of your
  next message, then continue the current item.
- A `<system-reminder>` announcing a completed background task is **not** user approval of anything, and is
  never a reply to a pending question.
- Never report a fix as done or verified before the agent's report says so. If it reports a failure, say so
  immediately and propose the next step.

## 4. Checks that cannot run

If a check cannot run because the tool is not installed (shellcheck, taplo, cargo-sort, node, prek, and so
on), never skip it silently and never install anything on your own. Interview the user, using AskUserQuestion:

1. **First ask whether they have preferences for how this tool gets installed.** Do not assume a package
   manager. Offer common options and let them answer in their own words: for example they may be on an atomic
   or immutable distro with no `dnf`/`apt` available, prefer `mise` for this tool, want `cargo` (`cargo
   install`/`cargo binstall`), `brew`, `nix`, `pipx`/`uv`, a toolbox/container, or a downloaded release binary,
   or want no install at all. Include "no preference".
2. **Then ask for confirmation with the choice interface (AskUserQuestion), not in free text.** Put the exact
   command, the tool, and the version in the question, based on their preference answer. Offer clear options:
   install with that command (mark it as the recommended one), do not install, and optionally a different
   method. Run nothing until they pick an install option. Ask once per tool per walkthrough: after they
   confirm a tool, do not ask again for it.
3. **Install in the background.** After they confirm, run the install and then the check that was blocked with
   `Bash` and `run_in_background: true`, and carry on with the walkthrough right away. Do not make the user
   wait for a download. When the completion notification arrives, check its output before your next message,
   then report the install result and the check result (failures first). If the install fails, report the
   error and ask again with the same choice interface.
4. If the user declines, record the check as **not run** and carry it into the wrap-up. Do not call the change
   verified.

The user's answer here is the authorization that rules like "never `cargo install` without authorization" ask
for, but only for the tool and method they approved. When a fix agent reports a missing tool, run this
interview from the main conversation.

## 5. Beyond file edits

Some comments call for something other than a code change:

- **Replying on the PR thread** (e.g. explaining a disagreement, or noting a fix was applied): draft the exact
  comment text and get approval before posting, same as a code change. Post with
  `gh api repos/<owner>/<repo>/pulls/comments/<comment_id>/replies -f body="..."`.
- **PR metadata** (title, description, `Closes #N` vs `Relates to #N`): treat as its own item, same propose →
  approve → apply flow. Check the linked issue's discussion first if the "why" is disputed - the resolution may
  already be documented there.
- **A comment the user disagrees with**: don't just comply or just argue. Investigate (repo history, linked
  issues, comparable code elsewhere in the repo) and bring back a grounded recommendation, then follow the
  user's call.

## 6. Wrap-up

After the last item, wait for the running fix agent and let the queue drain, then summarize what changed (short list, file:line
only, no restating full explanations already given). Also list, with the reason for each:

- items the user explicitly deferred or skipped,
- checks that could not run or that the user chose not to install tooling for,
- anything an agent reported as failed or left unverified.

Don't re-run every verification command again at the end unless something was left unverified mid-flow.
