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

use crate::shard_allocator::ShardingError;
use consensus::VsrStateError;
use metadata::impls::recovery::RecoveryError;
use server_common::log::LogError;
use shard::ShardCtorError;
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
    /// A shard's message pump stopped because a partition could not commit
    /// an op the cluster had already committed. The partition is fenced and
    /// the server is shutting down; the exit is non-zero so an orchestrator
    /// does not read a durability fault as a clean stop.
    #[error(
        "shard {shard_id} stopped: partition {namespace_raw} could not commit op {op}, \
         which the cluster had already committed. The replica is divergent and was \
         fenced; the server shut down so it cannot serve a prefix the cluster has \
         moved past"
    )]
    ShardFatal {
        shard_id: u16,
        namespace_raw: u64,
        op: u64,
    },
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
    // with files left in place) is decided by the `boot/recovery.rs` arms that
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
    #[error("server listeners start on shard 0 only, not on shard {shard_id}")]
    ListenersOffShardZero { shard_id: u16 },
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
    /// A panic no shard thread could surface: compio's `spawn` catches task
    /// panics, so a dead listener or connection task leaves every thread
    /// exiting `Ok`. The panic hook records the first one and the join path
    /// fails the exit on it, so an orchestrator does not read the shutdown
    /// as clean.
    #[error("server shut down after a panic: {description}")]
    Panicked { description: String },
}

/// Why a partition's recovered segments cannot be served.
///
/// Every shape here is structural -- the local files contradict themselves or
/// each other -- but they are distinguished because they point at different
/// causes, and not all of them are at-rest corruption: an empty non-tail
/// segment is a failed rebuild's orphan pairing, a hole is a stray or
/// half-unlinked file, interior damage is bit rot (or a resurrected tail
/// appended over), and offsets that do not continue the chain can be minted
/// into byte-clean files by an upstream crash window as well as by damage.
///
/// An index that contradicts itself is deliberately NOT here, and neither is
/// one the log cannot back UNLESS the topic runs under `enforce_fsync` and the
/// gap is deeper than the single in-flight entry: entries are derived from the
/// log, so recovery drops such an index whole and rebuilds it from a byte-0
/// walk of the log rather than believing any part of it. What `enforce_fsync`
/// adds is evidence from serialized completed flushes: an entry above chunk N
/// means the log fdatasync covering chunk N completed before the later flush
/// began. This is independent of reply timing and turns a deeper gap into
/// evidence about the LOG. Absent that evidence the index only locates data;
/// recovery verifies the log from byte 0.
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
    /// on-disk shape. The index anchor search charges the same verification
    /// budget as it steps back through entries the log cannot back, so an
    /// index packed with claims the log never proves ends here too instead
    /// of paying a verify per entry. Truncation is only ever sound for a
    /// proven torn tail, so giving up keeps the bytes. The residue width is
    /// diagnostic only; it is not a gate.
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
    /// The sparse index of a topic running under `enforce_fsync` outruns its
    /// log by more than the one entry a crash can legitimately strand there.
    /// Persistence writes exactly one entry per flush chunk, chunks never
    /// overlap, and flushes are serialized, so every entry below the last one
    /// names a chunk whose log bytes completed their fdatasync. A completed
    /// chunk can contain batches acknowledged before the flush threshold was
    /// reached, while the in-flight chunk can do so too. Reply timing is not
    /// the proof. Only the chunk in flight when the process died can have an
    /// entry the log never backed. A deeper step-back therefore says the LOG
    /// lost bytes it had already made durable, and rebuilding from what remains
    /// could re-mint offsets, including offsets already returned to clients.
    FsyncedLogLoss {
        start_offset: u64,
        entry_count: u64,
        provable_entries: u64,
        /// Position of the highest entry the log still proves; 0 when it
        /// proves none, which `provable_entries` disambiguates.
        provable_position: u64,
        /// Entries the backward search actually probed, which its own cap
        /// holds below `entry_count` on a long index: `provable_entries == 0`
        /// then means nothing proved in the searched window, not that the log
        /// backs nothing.
        searched_entries: u64,
    },
    /// Under `enforce_fsync`, the byte-0 rebuild after a dropped index proved
    /// the log only through `walked_position`, short of `durable_position`,
    /// the byte the index's own last entry proves the log had already
    /// fdatasynced through (the flush that wrote the entry began only after
    /// the previous chunk's log sync completed). The step-back gate measures
    /// loss at entry granularity; this catches the sub-chunk shape it cannot:
    /// bytes a completed flush made durable are gone mid-chunk, so truncating
    /// to the walked prefix would re-mint their offsets.
    FsyncedRebuildShortfall {
        start_offset: u64,
        entry_count: u64,
        walked_position: u64,
        durable_position: u64,
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
            Self::FsyncedLogLoss {
                start_offset,
                entry_count,
                provable_entries,
                provable_position,
                searched_entries,
            } => write!(
                f,
                "segment {start_offset} runs under enforce_fsync with {entry_count} sparse \
                 index entries, but its log backs only {provable_entries} of the \
                 {searched_entries} searched from the top (up to byte {provable_position}); \
                 every entry below the last describes a log chunk whose fdatasync had \
                 completed, so the log has lost previously durable data rather than the \
                 index having outrun it"
            ),
            Self::FsyncedRebuildShortfall {
                start_offset,
                entry_count,
                walked_position,
                durable_position,
            } => write!(
                f,
                "segment {start_offset} runs under enforce_fsync with {entry_count} sparse \
                 index entries, and the byte-0 rebuild proved its log only through byte \
                 {walked_position}, short of byte {durable_position} which the last \
                 entry's own fdatasync ordering proves the log had already made durable; \
                 the log has lost previously durable bytes mid-chunk, so rebuilding \
                 would re-mint their offsets"
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

/// Per-shard outcome captured by [`crate::boot::ShardHandles::join_all`]
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
