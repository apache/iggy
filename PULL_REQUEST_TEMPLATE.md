## Which issue does this PR address?

<!--
We generally require a GitHub issue for all bug fixes and enhancements. Link it with GitHub syntax, keep the line that applies and delete the other:
- `Closes #123` auto-closes the issue when this PR merges (full fix).
- `Relates to #123` links without closing (partial or related work).
-->

Closes #
Relates to #

## Rationale

<!--
Why is this change needed? If the issue explains it well, a one-liner is fine.
-->

## What changed?

<!--
2-4 sentences. Problem first (before), then solution (after).

GOOD:

"Messages were unavailable when background message_saver committed the
journal and started async disk I/O before completion. Polling during
this window found neither journal nor disk data.

The fix freezes journal batches in the in-flight buffer before async persist."

GOOD:

"When many small messages accumulate in the journal, the flush passes
thousands of IO vectors to writev(), exceeding IOV_MAX (1024 on Linux)."

BAD:
- Walls of text
- "This PR adds..." (we can see the diff)
-->

## Local Execution

- Passed / not passed
- Pre-commit hooks ran / not ran

<!--
You must run your code locally before submitting.
"Relying on CI" is not acceptable - PRs from authors who haven't run the code will be closed.

Did you have `prek` installed? It runs automatically on commit and covers all project languages. See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md).
-->

## AI Usage

<!--
If AI tools were used, please answer:
1. Which tools? (e.g., GitHub Copilot, Claude, ChatGPT)
2. Scope of usage? (e.g., autocomplete, generated functions, entire implementation)
3. How did you verify the generated code works correctly?
4. Can you explain every line of the code if asked?

If no AI tools were used, write "None" or delete this section.
-->
