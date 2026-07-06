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

use metadata::impls::recovery::RecoveryError;
use server_common::log::LogError;
use shard::ShardCtorError;
use shard_allocator::ShardingError;
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
    #[error("failed to create io_uring runtime for shard {shard_id}")]
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
    #[error("cluster node for replica {replica_id} is missing tcp_replica port")]
    ClusterReplicaPortMissing { replica_id: u8 },
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
        "recovered {consumer_kind} offset {offset} for id {consumer_id} exceeds current_offset {current_offset} in stream {stream_id}, topic {topic_id}, partition {partition_id}"
    )]
    RecoveredConsumerOffsetOutOfBounds {
        consumer_kind: &'static str,
        consumer_id: usize,
        offset: u64,
        current_offset: u64,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
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
    #[error("failed to construct IggyShard from bootstrap inputs")]
    ShardConstruction(#[source] ShardCtorError),
    #[error("{} shard thread(s) failed: {}", failures.len(), format_shard_failures(failures))]
    ShardJoinFailures { failures: Vec<ShardJoinFailure> },
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
    Panic { message: String },
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
