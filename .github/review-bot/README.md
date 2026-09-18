# Skill review bot

A repository committer comments `/skill <name>` on a pull request. A headless agent runs that skill against the pull request head, and the findings arrive as one pull request review with inline comments.

## Commands

- `/skill <name>` runs the skill on the default branch at `.claude/skills/<name>/SKILL.md`, or `.agents/skills/<name>/SKILL.md`.
- `/skill` answers with the list of available skill names.

Any committer can write a skill and use it this way. The gate is a repository write permission, so the PR author alone cannot start a run.

The run reads the skill file, so a skill marked `disable-model-invocation: true` still works here. That flag keeps a skill out of an agent's own reach, which is the point of a human typing the command.

## The two stages

1. `pr-skill-review-run.yml` runs on `issue_comment`. First it parses the command and gates the author. It then makes sure that the skill exists on the default branch. After that it installs the Claude Code CLI, checks out the pull request head, and runs the agent. The agent writes `findings.json`.
2. `pr-skill-review-post.yml` runs on `workflow_run`, where the token can write. It reads the trigger comment back from the API, so the run artifact cannot aim the review elsewhere. Then it anchors each finding on a changed line and posts one review.

The review body opens with the verdict, the reason, and a count per severity. A finding with no anchor goes into the body as text. When the head moved during the run, the whole set goes to the body. The poster answers every conclusion, a cancelled run included. It also answers a pull request that closed while the run worked, and a post that the API refused.

## What the agent can do

The agent reads. It cannot run a command, build the workspace or start a test. The run installs no Rust toolchain, so no build script, proc macro, cargo configuration or rustc wrapper from the pull request can execute.

- Tools: Read, Grep, Glob, Write and Agent. The CLI runs with `--restricted`, which removes every command tool and confines the file tools to two directories: the checkout and the agent directory under the runner temp. Subagents get the same set.
- Writes: one allow rule, the agent directory. `--permission-prompts none` denies every other write, a write into the checkout included.
- Inputs: the diff, its stat and file list, the CI status of the commit, and `comment-style.md`, all in the agent directory.
- Model: the Claude Code CLI at `CLAUDE_CODE_VERSION`, pointed at the DeepSeek Anthropic-compatible endpoint with the model in `DEEPSEEK_MODEL`. Both pins live in one job-level `env` block, and the posted review closes with a footer that names them.

A skill written for a terminal session still works. The prompt tells the agent which file replaces which command: the diff files replace `git diff` and `gh pr diff`, and the CI status file replaces a build or a test run. A finding that only a test can settle is dropped.

## Watching a run

The agent step streams each tool call into the job log, one line per call, and each denied call as a `deny:` line. An in-progress run is therefore readable in the Actions UI. The raw event stream and the CLI error output sit in the `review-out` artifact next to `findings.json`.

The job log closes with the cost of the run. The line prices the token counts at DeepSeek rates and at two Claude tiers, next to the estimate the CLI itself reports.

Nothing appears on the pull request while a run works. An `issue_comment` run of a fork receives a read-only token, so the first workflow cannot post. The answer arrives with the review at the end.

## Files

- `prompt.md` - the task and the output contract for the agent.
- `comment-style.md` - how each posted comment must read, wording rules included.

Both files come from the default branch, never from the pull request under review.

## Secret

`DEEPSEEK_AUTH_TOKEN` holds the DeepSeek key. Add it as a repository secret on GitHub, or with `gh secret set DEEPSEEK_AUTH_TOKEN`.

## Cost and duration

A run installs one npm package and reads code. Expect a few minutes of wall clock for a small skill, and up to 45 minutes for one that spawns subagents, on one hosted runner. The agent tokens come from the DeepSeek key.

## Security notes

- No pull request code runs. The agent has no shell, and nothing in the run builds, tests or installs from the checkout.
- The DeepSeek key sits in the environment of the agent step. Without a shell the agent cannot read that environment, and `/proc` is outside its working directories. Keep the key scoped to this bot and renewable all the same.
- No GitHub token reaches the agent step. The checkout sets `persist-credentials: false` and the step receives no token, so the agent has no way to post or to push.
- The control file that names the pull request to answer sits outside the agent directory, so the agent cannot rewrite it. The poster reads the trigger comment, its author and its pull request from the API regardless.
- `.claude` and `.agents` are deleted from the pull request checkout, and the default branch copy is restored. A pull request therefore cannot redefine the skill that reviews it. The same holds for `AGENTS.md` and `CLAUDE.md` at any depth, which the CLI loads as its own orders.
- Project configuration files, hooks and MCP servers never load. `--restricted` ignores the project configuration files, and `--strict-mcp-config` with no MCP configuration loads no server.
- The pull request text is data. The prompt says so, and a comment in the diff that addresses the reviewer can become a finding, never a command. That is a rule for the model, not a fence. A model that follows such a comment can still only read the tree and write its own directory.
- No cache is read or written. The run has no build, so it has no cache to poison.
