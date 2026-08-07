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
pub enum ServerNgError {
    #[error(transparent)]
    Iggy(Box<iggy_common::IggyError>),
    #[error("failed to load server-ng config")]
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
    #[error("system.sharding.inbox_capacity must be in 1..={max}; got {value}")]
    InvalidInboxCapacity { value: usize, max: usize },
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
    #[error("failed to serialize current server-ng config")]
    CurrentConfigSerialize(#[source] toml::ser::Error),
    #[error("failed to write current server-ng config at {path}")]
    CurrentConfigWrite {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize server-ng logging")]
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
    // Per-partition, not fatal: the boot path fences this one group (quarantines
    // its segment files and materialises it fresh) instead of taking the node
    // down for one damaged local chain. The shapes it reports are exactly what a
    // failed state-transfer quarantine leaves behind, and the rebuild recovers
    // the data from a peer.
    #[error(
        "partition {stream_id}/{topic_id}/{partition_id} at {dir} recovered an \
         unusable segment chain: {reason}"
    )]
    PartitionChainRefused {
        dir: PathBuf,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        reason: PartitionChainRefusal,
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
        "cluster bootstrap with empty metadata requires both {username_env} and {password_env} to be set before server-ng can create the root user deterministically"
    )]
    ClusterRootCredentialsRequired {
        username_env: &'static str,
        password_env: &'static str,
    },
    #[error(
        "recovered segment for stream {stream_id}, topic {topic_id}, partition {partition_id} at start_offset {start_offset} has message/index divergence (messages_size={messages_size_bytes}, indexed_size={indexed_size_bytes}, end_offset={end_offset}); recovery aborted before opening listeners. Restore the partition from a healthy replica or snapshot, or move the segment aside for offline repair before restarting."
    )]
    RecoveredSegmentSizeDivergence {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
        end_offset: u64,
        messages_size_bytes: u64,
        indexed_size_bytes: u64,
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
    #[error(
        "recovered namespace stream {stream_id}, topic {topic_id}, partition {partition_id} exceeds configured limits (max_streams={max_streams}, max_topics={max_topics}, max_partitions={max_partitions})"
    )]
    RecoveredNamespaceOutOfBounds {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        max_streams: usize,
        max_topics: usize,
        max_partitions: usize,
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

/// Why a recovered segment chain cannot be served.
///
/// Both shapes mean the same thing operationally -- the local files do not form
/// a chain this replica can serve -- but they are distinguished because they
/// point at different causes: an empty non-tail segment is a failed rebuild's
/// orphan pairing, a hole is a stray or half-unlinked file.
#[derive(Debug)]
pub enum PartitionChainRefusal {
    EmptyNonTailSegment {
        empty_start: u64,
        next_start: u64,
    },
    Hole {
        previous_start: u64,
        previous_end: u64,
        next_start: u64,
    },
}

impl std::fmt::Display for PartitionChainRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyNonTailSegment {
                empty_start,
                next_start,
            } => write!(
                f,
                "segment {empty_start} is empty yet {next_start} follows it, so the \
                 chain cannot be served past it"
            ),
            Self::Hole {
                previous_start,
                previous_end,
                next_start,
            } => write!(
                f,
                "segment {previous_start} ends at offset {previous_end} but the next \
                 starts at {next_start}, leaving a hole"
            ),
        }
    }
}

/// Per-shard outcome captured by [`crate::bootstrap::ShardHandles::join_all`]
/// when a shard either returned `Err` or panicked.
///
/// Bundled into [`ServerNgError::ShardJoinFailures`] so the operator sees
/// every failing shard rather than only the first one, which previously
/// lived in the trace log alone.
#[derive(Debug)]
pub struct ShardJoinFailure {
    pub shard_id: u16,
    pub kind: ShardJoinFailureKind,
}

#[derive(Debug)]
pub enum ShardJoinFailureKind {
    Error(Box<ServerNgError>),
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

impl From<iggy_common::IggyError> for ServerNgError {
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
                kind: ShardJoinFailureKind::Error(Box::new(ServerNgError::MissingReplicaId)),
            },
            ShardJoinFailure {
                shard_id: 2,
                kind: ShardJoinFailureKind::Panic {
                    message: "boom".to_string(),
                },
            },
        ];
        let rendered = ServerNgError::ShardJoinFailures { failures }.to_string();
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
