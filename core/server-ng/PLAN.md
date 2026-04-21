# Server-NG Bootstrap Plan

## Goal

Implement startup for the new `server-ng` binary using:

- `partitions`
- `consensus`
- `message_bus`
- `metadata`
- `journal`

and support:

1. new `config.toml` handling in `server-ng`
2. config loading/parsing
3. metadata journal + snapshot loading
4. `IggyMetadata` restoration
5. partition + log loading (`IGGY-131`)
6. single-shard bootstrap only, without transport/runtime infra for now

## Plan

### 1. Config surface

- Reuse the existing server config surface instead of introducing a separate top-level `server-ng` config module.
- Add an extra config section or extension on top of the current config for `server-ng`-specific additions.
- The main new config requirement is broker capacity limits for:
  - maximum streams
  - maximum topics
  - maximum partitions
- Add validation that these three limits can be packed into a single `u64` namespace layout.
- The validation must be tied directly to `IggyNamespace`, since it packs `stream_id/topic_id/partition_id` into `u64`.
- Keep config loading through the existing config infrastructure, with the extra section layered on top.

### 2. Server-NG config file

- Create a dedicated `core/server-ng/config.toml`.
- It should be exactly the same as the existing server config file, plus the extra `server-ng` additions.
- Do not create a reduced or bootstrap-only config file.
- Keep it structurally aligned with the current server config so existing parsing behavior can be reused as much as possible.

### 3. Bootstrap entrypoint

- Replace the placeholder `server-ng` main startup with a bootstrap flow.
- Keep startup split into clear phases:
  - initialize tracing/logging
  - load config
  - prepare filesystem paths
  - restore metadata
  - restore partitions
  - build shard
- Leave listeners/timers out for now.
- Add an explicit `TODO` at the bootstrap boundary where transport/runtime services will be started once infrastructure exists.

### 4. Metadata recovery

- Reuse the existing recovery code from `core/metadata`.
- Load:
  - snapshot
  - metadata WAL / journal
- Restore the metadata state machine from snapshot.
- Replay journal entries after the snapshot point.
- Construct `IggyMetadata` from the recovered pieces.

### 5. Consensus state restoration

- Initialize it with recovered progress so sequence/commit state matches restored metadata.
- Keep this scoped to single-replica / single-shard bootstrap first.

### 6. Partition discovery

- Read restored metadata to determine which partitions exist.
- For each partition, resolve:
  - stream id
  - topic id
  - partition id
  - consensus group / namespace info
  - persisted storage paths

### 7. Partition + log loading

- Reuse the existing partition and log loading code from the current server binary because the on-disk format and directory layout have not changed.
- Use the current `server` binary startup path as the reference implementation.
- In particular, trace the existing logic from the current server `main`/bootstrap flow and adapt that code path into `server-ng`.
- Avoid inventing a parallel restore path unless extraction is required for reuse.
- Scope this part of the work to wiring existing logic into the new bootstrap, not redesigning the storage format.

### 8. Shard construction

- Build one shard using:
  - restored `IggyMetadata`
  - restored `IggyPartitions`
  - `message_bus`
  - `consensus`
- Keep the initial version fixed to one shard.
- Ensure routing tables and namespace ownership are initialized consistently for that single shard.

### 9. Runtime services

- Skip runtime services for now.
- Do not plan TCP listeners, timers, HTTP, QUIC, or related infra in this phase.
- Leave a clear `TODO` in the bootstrap where these services will be attached later.

### 10. Verification

- Verify config parsing against `core/server-ng/config.toml`.
- Run `cargo check -p server-ng`.
- Smoke-test startup with:
  - empty data directory
  - existing metadata snapshot/journal
  - existing partition logs

## Suggested implementation order

1. `core/server-ng/config.toml` aligned with existing server config plus extras
2. metadata restore path
3. partition restore path by reusing current server bootstrap logic
4. shard construction
5. compile + smoke verification

## Notes from Feedback

- Do not introduce a brand new top-level `server-ng` config model if the current config can be reused.
- The only clearly identified new config need right now is namespace-capacity validation for packed `u64` `IggyNamespace`.
- Do not scope this task around listeners or timer startup yet.
- Prefer extracting or reusing existing server bootstrap code over reimplementing storage restore logic from scratch.
