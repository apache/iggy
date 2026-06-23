# To Be Decided

## `PollingKind::First` TCP bug (open since 2026-06-22)

### Observed behavior

`PollingStrategy::first()` over the TCP binary protocol returns `InvalidOffset(0)` even
when the topic has messages and offset 0 is valid. The same poll over HTTP works fine.

### Investigation findings (2026-06-22, session 14)

**Both TCP and HTTP converge on the same server code path.** After topic/partition
resolution both routes arrive at `IggyShard::poll_messages()` →
`poll_messages_from_local_partition()` → `ops.rs::poll_messages()`. There is no
TCP-specific validation. The TCP handler (`binary/handlers/messages/poll_messages_handler.rs`)
does not call `validate_partition_offset`.

**`PollingKind::First` resolution in `ops.rs:85-89`:**
```rust
PollingKind::First => partition.log.segments().first()
    .map(|segment| segment.start_offset)
    .unwrap_or(0),
```
Returns the first segment's start offset -- correct even after retention removes old segments.

**All `InvalidOffset` sources in the server:**

| File | Line | When |
|------|------|------|
| `server/src/shard/system/utils.rs` | 152 | `store_consumer_offset` only -- not reachable from poll |
| `server_common/src/messages_batch_mut.rs` | 455 | `validate_checksums_and_offsets` during disk load -- reachable from poll |
| `server_common/src/messages_batch_mut.rs` | 682 | send-path `validate()` -- not reachable from poll |

**TCP response deserialization loses the inner value:**
`TcpClient::handle_response` calls `IggyError::from_code(4100)` → `IggyError::from_repr(4100)`.
`FromRepr` on a `#[repr(u32)]` enum with a tuple variant initializes the inner `u64` to zero.
Any `InvalidOffset(N)` from the server arrives as `InvalidOffset(0)` at the SDK.

**Most likely server-side trigger:** `validate_checksums_and_offsets(start_offset)` in
`ops.rs:553` (called from `load_segment_messages` during disk poll). If a segment's on-disk
message headers have offsets that do not match the expected sequential order starting at
`start_offset`, this returns `InvalidOffset(actual_bad_offset)` -- which the TCP client
reconstructs as `InvalidOffset(0)`. HTTP could survive this differently depending on error
mapping, or the HTTP test hit a different partition/segment.

### Current workaround

PR #3525 uses `PollingStrategy::last()` (not `first()`) for the `InvalidOffset` recovery
fallback, because `last()` always succeeds (most recent message is always in an active
segment). The recovery poll commits the latest offset so subsequent polls continue normally.
Pre-arming: when a consumer group is newly created, `fallback_to_last` is set so the first
poll skips the guaranteed `InvalidOffset` that would occur if retention has run.

### Remaining work

- Reproduce deterministically (e.g. create topic, send 3 messages, delete first segment
  manually, poll with `PollingStrategy::first()` over TCP vs HTTP). Compare.
- Check if `validate_checksums_and_offsets` is called when disk is healthy -- if yes, the
  mismatch would mean a bug in how segment start offsets are written vs. the message header
  offsets on disk.
- If root cause is confirmed as the `from_repr` u64 loss, add the actual offset to the TCP
  error payload so diagnostics are not blind.
- Once fixed, PR #3525 can use `first()` instead of `last()` in the recovery path to avoid
  skipping history.
