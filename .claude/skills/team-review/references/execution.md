# Review execution in Claude Code and Codex

These instructions supply target preparation and client-specific tools for `team-review`, `team-review-slim`, and
`connector-review`. They replace the tool examples in those skills, without changing their roles, Charters, or rounds.
An explicit skill invocation authorizes its reviewers and validators. Workers do not delegate further.

## Prepare the target

Resolve the repository root with `git rev-parse --show-toplevel`. Run commands there without `git -C`.
`<TARGET>` is the invocation argument. Claude Code supplies `$ARGUMENTS`; Codex supplies the trailing user text.
Resolve all refs to full commit IDs before writing artifacts:

- PR number, `#number`, or `prnumber`, case-insensitive: query `gh pr view` for `title`, `body`, `baseRefName`,
  `baseRefOid`, and `headRefOid`. The reviewed head is `headRefOid`; the start is its merge base with `baseRefOid`.
- No target: the reviewed head is `HEAD`; the start is its merge base with `origin/master`.
- Bare branch: the reviewed head is that branch; the start is its merge base with `origin/master`.
- `A..B`: the start is `A`; the reviewed head is `B`.
- `A...B`: the reviewed head is `B`; the start is its merge base with `A`.

Fetch missing refs if needed. If resolution fails, report the unresolved target. Never substitute `HEAD` for a named
branch or use the local `master` tip as the PR base.

Require that the checkout's `HEAD` equals the reviewed head and that tracked files are clean. If either check fails,
ask the user to prepare the reviewed checkout. Do not reset or stash their changes. Recheck both conditions before
writing the final report. If the recheck fails, report an incomplete review and give no verdict. Workers read deleted
files from the resolved starting commit and other files from the checkout.

Derive `<TOPIC>` from the supplied target, or `head-<SHORTCOMMIT>` when absent: lowercase, replace characters outside
`[a-z0-9-]` with `-`, collapse repeated hyphens, trim them, and keep at most 40 characters. If empty, use `date +%s`.
Create `<DIR>` with `mktemp -d /tmp/iggy-review-<TOPIC>.XXXXXX`. Retain its artifacts.

- For PRs, capture the complete metadata as `<DIR>/pr.json`. Omit it for other targets.
- Capture `diff.patch` and `files.txt` from the same resolved start and head. Use `git diff --no-ext-diff --no-color`
  and its `--name-only` variant. Preserve exact output, including complete hunks and paths.
- For `connector-review`, if `files.txt` lists no path under `core/connectors/` or `core/integration/tests/connectors/`,
  report that the target contains no connector changes and stop without a review verdict.
- `<SHORTCOMMIT>` is the first eight characters of the reviewed head. The report path is `<DIR>/report.md`.

Follow local RTK instructions for ordinary commands. If RTK is in use, use `rtk proxy git diff` and
`rtk proxy gh pr view` for artifact capture because filtered patches and JSON are unsuitable for workers.
Give every worker the resolved start and head, repository path, artifact paths, applicable repository instructions,
its role and Charter, and the execution instructions for its client.

## Claude Code tools

Use the tool calls in each skill as written.

## Codex tools

- Use the available subagent tools and their actual schemas. With `collaboration`, use `spawn_agent`,
  `wait_agent`, `send_message`, and `followup_task`. Route messages using returned agent IDs.
- Start independent workers with fresh context, using `fork_turns: "none"` when supported.
- Inherit the session model. Omit Claude-only fields such as `subagent_type`, `name`, and `model: opus`.
- Use shell tools for reads and commands, and `apply_patch` for authored files. Read connector guidance from each
  skill's `SKILL.md`; a Claude `Skill` tool is not required. Resolve relative links against the skill directory.
- Wait with the available completion tools. Nudge a running worker with `send_message`; resume a completed worker
  with `followup_task` if its deliverable is missing. Do not request full worker transcripts.

## Clients without a shell

A client without a shell, such as the `/skill` CI runner, cannot run the commands or the checkout checks in this file.
Skip them. Use the diff and the file list from the client prompt as `diff.patch` and `files.txt`, and create `<DIR>`
where that prompt puts scratch files.

## Worker lifecycle and reports

Respect the client's concurrency limit, including the moderator's slot. Run roles and validation shards in batches
when necessary. Run every required role and validation round independently, even when they cannot run together.
Finish each round before the next consumes its output. If delegation is unavailable, report the limitation.
Do not present a solo review as the completed team workflow.

Each worker writes its required artifact before returning. Check every expected file after completion. Give a missing
deliverable one follow-up and, if still absent, one replacement worker. If it remains missing, report the missing role
or validation shard and an incomplete review. Do not issue a completed verdict.

Workers list each test or build command that they need, with its reason, under `Checks to run:` at the end of their
deliverable. The moderator runs those checks serially after the workers finish. Record actual commands and outcomes
in the report's `Verification:` line. For `team-review-slim`, `validation: none (manual)` means no independent
validation rounds; it does not mean that no tests ran.
Keep reports local unless the user also requests publication.
