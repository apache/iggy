// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use consensus::VsrStateError;
use metadata::impls::recovery::RecoveryError;
use server_common::log::LogError;
use shard::ShardCtorError;
use shard_allocator::ShardingError;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServerError {
    #[error(transparent)]
    Iggy(Box<iggy_common::IggyError>),
    #[error("failed to load server config")]
    Config(#[source] configs::ConfigurationError),
    #[error("failed to allocate shards from sharding.cpu_allocation")]
    ShardAllocator(#[source] ShardingError),
    #[error("failed to bind shard {shard_id} to its CPU set")]
    CpuAffinityFailed {
        shard_id: u16,
        #[source]
        source: ShardingError,
    },
    #[error("failed to bind shard {shard_id} memory to its NUMA node")]
    MemoryAffinityFailed {
        shard_id: u16,
        #[source]
        source: ShardingError,
    },
    #[error("failed to spawn OS thread for shard {shard_id}")]
    ShardSpawnFailed {
        shard_id: u16,
        #[source]
        source: std::io::Error,
    },
    // `{source}` is deliberately part of the Display text: the shard-join
    // failure report and `%error` log fields print Display only, and the
    // source carries the io_uring remediation folded in by
    // `server_common::diagnostics::enrich_runtime_create_error`.
    #[error("failed to create io_uring runtime for shard {shard_id}: {source}")]
    ShardRuntimeCreateFailed {
        shard_id: u16,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "shard allocator produced zero shards; server must run at least one \
         shard (check [system.sharding] cpu_allocation)"
    )]
    ShardsCountZero,
    #[error(
        "computed shards_count = {count} exceeds the maximum of {} shards per \
         server; shard ids must fit in u16 and stay below the OWNER_NONE \
         sentinel",
        message_bus::OWNER_NONE - 1
    )]
    ShardsCountOverflow { count: usize },
    #[error(
        "shard {shard_id} message pump died instead of draining ({reason}); \
         committed journal tail may not have flushed"
    )]
    ShardPumpDied { shard_id: u16, reason: String },
    #[error(
        "shard {shard_id} message pump did not drain within {timeout:?}. \
         Committed journal tail may not have flushed"
    )]
    ShardPumpDrainTimedOut {
        shard_id: u16,
        timeout: std::time::Duration,
    },
    #[error("system.sharding.inbox_capacity must be in 1..={max}; got {value}")]
    InvalidInboxCapacity { value: usize, max: usize },
    #[error("system.sharding.reply_inbox_capacity must be in 1..={max}; got {value}")]
    InvalidReplyInboxCapacity { value: usize, max: usize },
    #[error("system.sharding.shutdown_drain_timeout must be in (0, {max:?}]; got {value:?}")]
    InvalidShutdownDrainTimeout {
        value: std::time::Duration,
        max: std::time::Duration,
    },
    #[error("system.sharding.shutdown_poll_interval must be in (0, {max:?}]; got {value:?}")]
    InvalidShutdownPollInterval {
        value: std::time::Duration,
        max: std::time::Duration,
    },
    #[error(
        "system.sharding.shutdown_poll_interval ({poll:?}) must be <= \
         shutdown_drain_timeout ({drain:?})"
    )]
    ShutdownPollExceedsDrain {
        poll: std::time::Duration,
        drain: std::time::Duration,
    },
    #[error("failed to serialize current server config")]
    CurrentConfigSerialize(#[source] toml::ser::Error),
    #[error("failed to write current server config at {path}")]
    CurrentConfigWrite {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize server logging")]
    Logging(#[source] LogError),
    #[error("failed to recover metadata snapshot and journal")]
    MetadataRecovery(#[source] RecoveryError),
    #[error("failed to open partition superblock at {dir}")]
    PartitionSuperblockIo {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    // Quarantines the one partition rather than treating the group as fresh or
    // reading through to a superseded view: mirrors the metadata plane's
    // `RecoveryError::SuperblockUnreadable` policy, minus the boot refusal,
    // because one unreadable partition directory must not strand every healthy
    // group on the shard.
    #[error(
        "partition superblock at {dir} is present but its format version \
         {version} is unrecognized by this build (a downgrade, or a corrupt \
         version field)"
    )]
    PartitionSuperblockVersionUnknown { dir: PathBuf, version: u16 },
    #[error(
        "partition superblock at {dir} is present but a copy holds bytes that \
         do not verify (bit-rot or a checksum failure), so its latest \
         generation cannot be established"
    )]
    PartitionSuperblockUnverifiable { dir: PathBuf },
    #[error(
        "partition superblock at {dir} was checksum-clean but did not decode; \
         tombstoning this partition rather than inferring a stale view"
    )]
    PartitionSuperblockUndecodable {
        dir: PathBuf,
        #[source]
        source: VsrStateError,
    },
    #[error(
        "partition superblock at {dir} belongs to a different {field}: expected \
         {expected}, found {found}; a copied or misplaced data directory, or the \
         cluster was resized without reconfiguration"
    )]
    PartitionSuperblockIdentityMismatch {
        dir: PathBuf,
        field: metadata::IdentityField,
        expected: u128,
        found: u128,
    },
    // Per-partition, not fatal: the boot path fences this one group instead of
    // taking the node down for one damaged local chain. Only STRUCTURAL
    // refusals route here -- shapes where the local files contradict
    // themselves, so a retried boot cannot help. Transient recovery I/O
    // failures (stat, open, read, truncate, fsync) stay node-fatal on purpose:
    // a retried boot can still serve that partition, while fencing it would
    // quarantine healthy data.
    //
    // The Display text deliberately claims nothing about what happens to the
    // refused files: disposition (quarantine into `.fenced.N` vs tombstone
    // with files left in place) is decided by the `bootstrap.rs` arms that
    // catch this error, and only they log it -- a claim here would render
    // beside theirs and contradict one branch or the other.
    #[error(
        "partition {stream_id}/{topic_id}/{partition_id} at {dir} refused segment \
         recovery: {reason}"
    )]
    PartitionRecoveryRefused {
        dir: PathBuf,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        reason: PartitionRecoveryRefusal,
    },
    #[error(
        "shard {shard_id} aborted while waiting for shard-0 to broadcast the metadata \
         factory bundle; shard 0 dropped its sender (most likely it failed to recover)"
    )]
    MetadataHandoffAborted { shard_id: u16 },
    #[error(
        "shard 0 aborted before binding listeners with {remaining} peer shard(s) still loading \
         their on-disk partitions; a peer most likely failed during bootstrap (shutdown flag set)"
    )]
    ShardBootstrapBarrierAborted { remaining: usize },
    #[error("failed to parse {context} socket address '{address}'")]
    SocketAddressParse {
        context: &'static str,
        address: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("cluster enabled but no node is configured for replica {replica_id}")]
    ClusterNodeNotFound { replica_id: u8 },
    #[error("cluster node count {count} exceeds supported u8 replica count")]
    ClusterReplicaCountTooLarge { count: usize },
    #[error("cluster mode requires --replica-id to identify the current node")]
    MissingReplicaId,
    #[error(
        "--replica-id {supplied} was passed with cluster.enabled=false; the WAL would commit \
         under replica {default} which permanently fixes this node's identity. Either set \
         cluster.enabled=true with a matching nodes[] entry, or drop --replica-id"
    )]
    ReplicaIdRequiresCluster { supplied: u8, default: u8 },
    #[error(
        "cluster node for replica {replica_id} is missing ports.{transport}; cluster mode \
         requires an explicit roster port for every enabled transport"
    )]
    ClusterPortMissing {
        transport: &'static str,
        replica_id: u8,
    },
    #[error(
        "cluster bootstrap with empty metadata requires both {username_env} and {password_env} to be set before server can create the root user deterministically"
    )]
    ClusterRootCredentialsRequired {
        username_env: &'static str,
        password_env: &'static str,
    },
    #[error(
        "{provided_env} is set but {missing_env} is not; the root user credentials must be \
         provided as a pair"
    )]
    RootCredentialsIncomplete {
        provided_env: &'static str,
        missing_env: &'static str,
    },
    #[error("{env_name} must be {min}..={max} characters long; got {length}")]
    RootCredentialLength {
        env_name: &'static str,
        length: usize,
        min: usize,
        max: usize,
    },
    #[error("--fresh could not remove the system path at {path}")]
    FreshWipeFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "failed to load persisted {consumer_kind} offsets for stream {stream_id}, topic {topic_id}, partition {partition_id} from {path}"
    )]
    ConsumerOffsetsLoad {
        consumer_kind: &'static str,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        path: String,
        #[source]
        source: Box<iggy_common::IggyError>,
    },
    #[error("failed to load {transport} listener credentials")]
    ListenerCredentials {
        transport: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build the HTTP forward client: {reason}")]
    HttpForwardClient { reason: String },
    #[error("failed to construct IggyShard from bootstrap inputs")]
    ShardConstruction(#[source] ShardCtorError),
    #[error("{} shard thread(s) failed: {}", failures.len(), format_shard_failures(failures))]
    ShardJoinFailures { failures: Vec<ShardJoinFailure> },
}

/// Why a partition's recovered segments cannot be served.
///
/// Every shape here is structural -- the local files contradict themselves or
/// each other -- but they are distinguished because they point at different
/// causes, and not all of them are at-rest corruption: an empty non-tail
/// segment is a failed rebuild's orphan pairing, a hole is a stray or
/// half-unlinked file, interior damage is bit rot (or a resurrected tail
/// appended over), a divergent index is a mis-strided or foreign write, and
/// offsets that do not continue the chain can be minted into byte-clean files
/// by an upstream crash window as well as by damage.
#[derive(Debug)]
pub enum PartitionRecoveryRefusal {
    /// `recoverable_bytes` on the two chain-shape refusals is the sum of
    /// walked, decodable bytes across the whole planned chain: the evidence
    /// the single-replica boot arm needs to decide whether fencing and
    /// rebuilding empty loses anything (0 means the chain provably held
    /// nothing servable; anything else is data a rebuild would hide).
    EmptyNonTailSegment {
        empty_start: u64,
        next_start: u64,
        recoverable_bytes: u64,
    },
    Hole {
        previous_start: u64,
        previous_end: u64,
        next_start: u64,
        recoverable_bytes: u64,
    },
    /// The index holds entries but no whole batch decodes AND verifies where
    /// its last entry points, so index and log describe different files. The
    /// damage probe ran first: anything verifying past the anchor's damage
    /// refuses as [`Self::InteriorDamage`] instead.
    IndexLogDivergence {
        start_offset: u64,
        end_offset: u64,
        messages_size_bytes: u64,
        indexed_size_bytes: u64,
    },
    /// A complete, checksum-verifying batch survives PAST bytes that do not
    /// decode. A torn tail has nothing after it, so this is interior damage,
    /// and truncating at it would silently discard the surviving batches.
    InteriorDamage {
        start_offset: u64,
        damage_position: u64,
        survivor_position: u64,
    },
    /// Bytes past the walked prefix that the damage probe could not
    /// classify: it ran out of a work budget before proving or disproving a
    /// survivor. The candidate budget is sized so a front-to-back scan of
    /// every residue in the load always fits (its exhaustion means offsets
    /// were re-examined -- a probe defect); the verification budget bounds
    /// the bytes handed to checksum verifies, whose claimed slices overlap,
    /// so residue packed with plausible headers can exhaust it from an
    /// on-disk shape. Truncation is only ever sound for a proven torn tail,
    /// so giving up keeps the bytes. The residue width is diagnostic only;
    /// it is not a gate.
    UnverifiedResidue {
        start_offset: u64,
        damage_position: u64,
        residue_bytes: u64,
        candidates_examined: u64,
        budget_units: u64,
        verified_bytes: u64,
        verify_budget_bytes: u64,
    },
    /// A batch whose checksum verifies does not continue the offset chain,
    /// so offsets are not contiguous inside one segment file. The verify is
    /// what earns the refusal: an UNVERIFIED mismatch is damage and goes to
    /// the probe (a torn tail truncates). The cause is not necessarily
    /// at-rest damage: a crash window that leaves the durable offset
    /// frontier past the recovered end offset stamps the same shape into
    /// byte-clean files.
    OffsetDiscontinuity {
        start_offset: u64,
        expected_offset: u64,
        found_offset: u64,
        position: u64,
    },
    /// A batch whose checksum verifies carries another partition's own
    /// `partition_id` stamp: a real record that landed in the wrong file (a
    /// misdirected write, a recycled block, an operator copy), not damage.
    /// Adopting it would seed this partition's offset space from foreign
    /// data; truncating it would destroy the only evidence of the misdirect.
    ForeignBatch {
        start_offset: u64,
        batch_partition_id: u64,
        position: u64,
    },
    /// Index entries must ascend in offset and position (they are appended,
    /// one per flushed chunk, over a growing log); a regression means the
    /// file was written mis-strided or over foreign bytes.
    IndexEntriesNotMonotone { start_offset: u64, entry_index: u64 },
    IndexEntryBeforeSegmentStart {
        start_offset: u64,
        first_entry_offset: u64,
    },
    /// A writer reopening over recovered bounds found the on-disk length
    /// diverging from the size recovery just validated and truncated to.
    StorageSizeMismatch {
        start_offset: u64,
        on_disk_bytes: u64,
        expected_bytes: u64,
    },
}

impl std::fmt::Display for PartitionRecoveryRefusal {
    // One arm per refusal shape; length tracks the enum, not complexity.
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyNonTailSegment {
                empty_start,
                next_start,
                recoverable_bytes,
            } => write!(
                f,
                "segment {empty_start} is empty yet {next_start} follows it, so the \
                 chain ({recoverable_bytes} recoverable bytes) cannot be served \
                 past it"
            ),
            Self::Hole {
                previous_start,
                previous_end,
                next_start,
                recoverable_bytes,
            } => write!(
                f,
                "segment {previous_start} ends at offset {previous_end} but the next \
                 starts at {next_start}, leaving a hole in a chain holding \
                 {recoverable_bytes} recoverable bytes"
            ),
            Self::IndexLogDivergence {
                start_offset,
                end_offset,
                messages_size_bytes,
                indexed_size_bytes,
            } => write!(
                f,
                "segment {start_offset} has message/index divergence: the index ends \
                 at offset {end_offset}, byte {indexed_size_bytes}, where the \
                 {messages_size_bytes}-byte log holds no batch that decodes and \
                 verifies"
            ),
            Self::InteriorDamage {
                start_offset,
                damage_position,
                survivor_position,
            } => write!(
                f,
                "segment {start_offset} holds undecodable bytes at {damage_position} \
                 with a complete verifying batch after them at {survivor_position}; \
                 not a torn tail, and truncating would discard durable batches"
            ),
            Self::UnverifiedResidue {
                start_offset,
                damage_position,
                residue_bytes,
                candidates_examined,
                budget_units,
                verified_bytes,
                verify_budget_bytes,
            } => write!(
                f,
                "segment {start_offset} holds {residue_bytes} bytes past the walked \
                 prefix at {damage_position} that the damage probe could not \
                 classify before exhausting its work budgets ({candidates_examined} \
                 candidate offsets examined of {budget_units} allowed; \
                 {verified_bytes} bytes handed to verification of \
                 {verify_budget_bytes} allowed); truncating unproven bytes could \
                 destroy durable batches"
            ),
            Self::OffsetDiscontinuity {
                start_offset,
                expected_offset,
                found_offset,
                position,
            } => write!(
                f,
                "segment {start_offset} holds a verified batch at byte {position} \
                 whose base offset {found_offset} does not continue the chain at \
                 {expected_offset}"
            ),
            Self::ForeignBatch {
                start_offset,
                batch_partition_id,
                position,
            } => write!(
                f,
                "segment {start_offset} holds a verified batch at byte {position} \
                 stamped for partition {batch_partition_id}; a foreign record in \
                 this log is preserved as evidence, not truncated"
            ),
            Self::IndexEntriesNotMonotone {
                start_offset,
                entry_index,
            } => write!(
                f,
                "segment {start_offset} index entry {entry_index} regresses in \
                 offset or position; the index was not appended over this log"
            ),
            Self::IndexEntryBeforeSegmentStart {
                start_offset,
                first_entry_offset,
            } => write!(
                f,
                "segment {start_offset} index claims offset {first_entry_offset}, \
                 below the segment's own start"
            ),
            Self::StorageSizeMismatch {
                start_offset,
                on_disk_bytes,
                expected_bytes,
            } => write!(
                f,
                "segment {start_offset} file length {on_disk_bytes} diverged from \
                 its recovered size {expected_bytes} at writer open"
            ),
        }
    }
}

/// Per-shard outcome captured by [`crate::bootstrap::ShardHandles::join_all`]
/// when a shard either returned `Err` or panicked.
///
/// Bundled into [`ServerError::ShardJoinFailures`] so the operator sees
/// every failing shard rather than only the first one, which previously
/// lived in the trace log alone.
#[derive(Debug)]
pub struct ShardJoinFailure {
    pub shard_id: u16,
    pub kind: ShardJoinFailureKind,
}

#[derive(Debug)]
pub enum ShardJoinFailureKind {
    Error(Box<ServerError>),
    Panic {
        message: String,
    },
    /// The shard thread never finished inside `shutdown_join_timeout`
    /// and was abandoned so process exit is not blocked forever.
    Wedged {
        waited: std::time::Duration,
    },
}

fn format_shard_failures(failures: &[ShardJoinFailure]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for (idx, failure) in failures.iter().enumerate() {
        if idx > 0 {
            out.push_str("; ");
        }
        match &failure.kind {
            ShardJoinFailureKind::Error(err) => {
                let _ = write!(out, "shard {} -> {err}", failure.shard_id);
            }
            ShardJoinFailureKind::Panic { message } => {
                let _ = write!(out, "shard {} panicked: {message}", failure.shard_id);
            }
            ShardJoinFailureKind::Wedged { waited } => {
                let _ = write!(
                    out,
                    "shard {} wedged: thread still running after {waited:?}, abandoned",
                    failure.shard_id
                );
            }
        }
    }
    out
}

impl From<iggy_common::IggyError> for ServerError {
    fn from(source: iggy_common::IggyError) -> Self {
        Self::Iggy(Box::new(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_join_failures_display_aggregates_all_entries() {
        let failures = vec![
            ShardJoinFailure {
                shard_id: 0,
                kind: ShardJoinFailureKind::Error(Box::new(ServerError::MissingReplicaId)),
            },
            ShardJoinFailure {
                shard_id: 2,
                kind: ShardJoinFailureKind::Panic {
                    message: "boom".to_string(),
                },
            },
        ];
        let rendered = ServerError::ShardJoinFailures { failures }.to_string();
        assert!(
            rendered.starts_with("2 shard thread(s) failed:"),
            "expected count prefix, got {rendered}"
        );
        assert!(
            rendered.contains("shard 0 ->"),
            "shard 0 entry missing: {rendered}"
        );
        assert!(
            rendered.contains("shard 2 panicked: boom"),
            "shard 2 panic entry missing: {rendered}"
        );
    }
}
