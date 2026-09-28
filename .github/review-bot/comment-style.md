# Comment style

The author reads each comment once, with the code open. Every rule below serves that reading.

## Budget

Two sentences and 40 words is the target. Three sentences and 60 words is the ceiling, and reaching the ceiling needs a reason you can say out loud. One sentence is often the whole comment. A finding that does not fit is two findings, or it is an explanation where a pointer was needed.

## Shape

Severity, then what is wrong, then the fix. Nothing else. The publisher adds the severity prefix, so the comment text carries none.

## Rules

- Write plain English with short common words. One sentence carries one idea, in 25 words or fewer.
- Use active voice and name the actor: "`consume()` drops the flush error", not "the flush error is dropped".
- No hedging opener such as "I think" or "consider". State the fact, not its importance.
- Use sentence case and American spelling. Backticked code keeps its original casing: `Vec<ConsumedMessage>`, `MAX_RETRIES`, `send_with_retry()`.
- Use `-` instead of an em dash. Never `--`.
- No bold title. No heading. No list.
- One finding per comment. A comment that says "also" twice is three comments.
- State the conclusion, not the derivation. Do not restate the diff, and do not explain the author's own code back to them.
- The fix is part of the comment. A comment that reports a problem alone is incomplete.
- Never cite a file that the reader cannot open: no `CLAUDE.md` quote, no review note, no private tracker id.
- Never mention the review machinery: no skill name, no agent, no sweep, no model, no validator. Each comment is a direct observation about the code.
- A repeated identical issue can end with one line: `Also at lines 727, 799.`
- Never post a performance number that you did not measure in this run.

## Example

A good comment, with the prefix the publisher adds:

```text
warning: `consume()` takes `messages` by value, and every send method iterates `&messages` while cloning each payload, so each message costs a heap copy. Iterate by value instead, because `payload_to_json()` already takes `Payload`.
```

The same finding, over budget and not to be posted:

```text
This disjunct can never fire. `ReplyHeader` is 256 bytes, and `try_into_typed` rejects any backing shorter than the header, so the byte budget already trips at 33 replies, which makes the capacity check at 64 unreachable and the flood that its comment describes impossible. Drop the disjunct, then delete the constant or define it from the retention budget, and rename the test at line 3215 either way.
```

That version carries four findings and the proof of each one. The author needs the claim, not the derivation. Split it, or send the first sentence alone.
