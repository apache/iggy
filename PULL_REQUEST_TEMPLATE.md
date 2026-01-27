## Which issue does this PR close?

<!--
We generally require a GitHub issue to be filed for all bug fixes and enhancements. You can link an issue to this PR using the GitHub syntax. For example `Closes #123` indicates that this PR will close issue #123.
-->

Closes #

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

Not passed / passed.

<!--
You must run your code locally before submitting.
"Relying on CI" is not acceptable - PRs from authors who haven't run the code will be closed.
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
