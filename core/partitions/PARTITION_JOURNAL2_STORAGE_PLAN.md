# PartitionJournal2 Storage Proposal

## Objective
Use `journal::Storage` as the actual backing store for serialized prepare entries (`Bytes`) and decode to `Message<PrepareHeader>` on read.

## Current Problem
- `PartitionJournal2Impl` currently stores entries directly in:
  - `UnsafeCell<Vec<Message<PrepareHeader>>>`
- `Noop` storage is unused for real data.
- The old `Buffer = Entry` idea is too rigid for this path.
- `Storage::read(&self, offset, buffer)` still requires a fallback buffer argument.

## Design Direction

### 1. Use serialized buffer (`Bytes`) in storage
For `PartitionJournal2`, enforce:

```rust
S: Storage<Buffer = bytes::Bytes>
```

Journal entry remains:

```rust
type Entry = Message<PrepareHeader>;
```

Conversion boundary:
- write path: `Message<PrepareHeader> -> Bytes` (serialize/store)
- read path: `Bytes -> Message<PrepareHeader>` via `Message::from_bytes`

### 2. Replace `Noop` with in-memory prepare-message storage
Introduce a dedicated storage:

```rust
pub struct InMemoryPrepareStorage {
    entries: UnsafeCell<Vec<bytes::Bytes>>,
}
```

Behavior:
- `write(bytes)` appends serialized message bytes to `entries`.
- `read(offset, ...)` treats `offset` as `op_number` and returns that op entry.

This keeps storage raw and simple, while typed decoding happens at journal boundary.

### 2.1 `offset` semantics for this journal: `offset == op_number`
For `PartitionJournal2`, define:
- `Storage::read(offset, ...)` where `offset` is VSR `PrepareHeader.op` (op number), not byte offset.
- Journal append path stores entries in op order, so op lookup is O(1)-ish via index map (or direct vec index if contiguous).

Implementation detail:
- Maintain `op_to_index: HashMap<u64, usize>` (or rely on contiguous `op` if guaranteed).
- On `append(entry)`, cache `op_to_index.insert(entry.header().op, vec_index)`.
- On `entry(header)`, call storage read using `header.op as usize` or map-resolved index.

### 3. Make `PartitionJournal2Impl` storage-backed
Refactor `PartitionJournal2Impl` to own a storage instance:

```rust
pub struct PartitionJournal2Impl<S: Storage<Buffer = bytes::Bytes>> {
    storage: S,
    // metadata/indexes only
    headers: UnsafeCell<Vec<PrepareHeader>>,
    message_offset_to_op: UnsafeCell<HashMap<u64, usize>>,
    timestamp_to_op: UnsafeCell<HashMap<u64, usize>>,
}
```

Responsibilities split:
- `storage`: source of truth for serialized entry bytes
- `headers`: lightweight header cache to satisfy `header()` / `previous_header()` reference semantics
- maps: query acceleration for `get`

### 4. Journal method behavior with storage
- `append(entry)`:
  - decode send-messages info for maps
  - serialize: `let bytes = entry.into_inner()` (or equivalent)
  - `storage.write(bytes)`
  - push `*entry.header()` into `headers`
  - cache `op_number -> storage position`
- `entry(header)`:
  - resolve by `op_number` (from `header.op`)
  - fetch bytes from storage via read
  - decode immediately: `Message::<PrepareHeader>::from_bytes(bytes)`
- `header(idx)` and `previous_header(header)`:
  - use `headers` vector (no full entry decode needed)

### 5. `get` path stays batch-conversion based
`get` still performs:
- collect candidate bytes from storage
- decode into `Message<PrepareHeader>`
- convert `Vec<Message<PrepareHeader>> -> IggyMessagesBatchSet`
- apply `MessageLookup` filter (`get_by_offset` / `get_by_timestamp`)

The existing conversion helpers remain valid.

## Storage Trait Consideration

Current trait:

```rust
fn read(&self, offset: usize, buffer: Self::Buffer) -> impl Future<Output = Self::Buffer>;
```

For typed object lookups this is awkward. Recommended adjustment:

```rust
fn read(&self, offset: usize) -> impl Future<Output = Option<Self::Buffer>>;
```

Why:
- avoids fake fallback buffer construction
- maps naturally to indexed storage
- still works for byte storage by returning `Option<Bytes>`

If trait-wide change is too large right now, keep current signature temporarily and ignore/use the incoming `buffer` only as a fallback for out-of-range reads.

For this phase, no trait change is required: just interpret the existing `offset` argument as `op_number` in `PartitionJournal2` storage.

## Proposed File-Level Changes
- `core/partitions/src/journal.rs`
  - add `InMemoryPrepareStorage`
  - make `PartitionJournal2Impl` generic over storage (or concrete to `InMemoryPrepareStorage`)
  - remove direct `inner.set: Vec<Message<PrepareHeader>>` as primary store
  - keep lightweight header metadata cache
  - keep/adjust current lookup maps
- `core/partitions/src/iggy_partition.rs` (only if/when wiring `PartitionJournal2` into partition log)
  - replace `Noop` for this path with `InMemoryPrepareStorage`

## Migration Sequence (Low Risk)
1. Add `InMemoryPrepareStorage` without removing existing fields.
2. Mirror writes to both old `inner.set` and storage.
3. Switch reads (`entry`, `get`) to storage-backed path.
4. Remove old `inner.set` once parity is confirmed.
5. Optionally evolve `Storage::read` signature in a separate PR.

## Expected Outcome
- `Storage` is no longer a no-op for `PartitionJournal2`.
- Storage buffer is raw serialized data (`Bytes`), and decoding to `Message<PrepareHeader>` happens at read boundary.
- The in-memory backend stays simple (`UnsafeCell<Vec<Bytes>>`) and aligned with your proposed flow.
