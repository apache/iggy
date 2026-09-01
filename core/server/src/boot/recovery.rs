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

//! Shard construction and the partition recovery it drives.

use crate::boot::topology::{RosterCells, TcpTopology, build_cluster_roster};
use crate::boot::wire_shell_handlers;
use crate::partition_helpers::{
    build_partition_fresh, configure_consumer_offsets, ensure_initial_segment,
    open_partition_superblock,
};
use crate::segment_recovery::{RecoveredSegment, load_persisted_segments};
use crate::server_error::{PartitionRecoveryRefusal, ServerError};
use crate::session_manager::SessionManager;
use crate::shell::{
    ServerMetadata, ServerShard, ShellHandlers, consensus_timers, repair_retry_ticks,
};
use configs::server::ServerConfig;
use consensus::{
    ClientTable, JoinMode, LocalPipeline, PipelineEntry, Sequencer, VsrConsensus, VsrRestore,
    VsrState,
};
use iggy_common::{
    Aes256GcmEncryptor, EncryptorKind, IggyByteSize, IggyError, PartitionStats, TopicRuntimeOptions,
};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use journal::superblock::PingPongSuperblock;
use message_bus::IggyMessageBus;
use metadata::ReplicaIdentity;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::stm::snapshot::Snapshot;
use metadata::stm::stream::Partition;
use partitions::{
    IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionsConfig,
};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::builder::IggyShardBuilder;
use shard::metrics::ShardMetrics;
use shard::shards_table::{PapayaShardsTable, ShardsTable, calculate_shard_assignment};
use shard::{
    CoordinatorConfig, PartitionConsensusConfig, Receiver as ShardReceiver, ShardFrame,
    ShardIdentity, TaggedSender,
};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info, warn};

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(in crate::boot) async fn build_shard_for_thread(
    shard_id: u16,
    total_shards: u16,
    config: &ServerConfig,
    topology: &TcpTopology,
    metadata: ServerMetadata,
    bus: Rc<IggyMessageBus>,
    senders: Vec<TaggedSender>,
    inbox: ShardReceiver<ShardFrame>,
    reply_inbox: ShardReceiver<ShardFrame>,
    metrics: ShardMetrics,
    roster_cells: &RosterCells,
) -> Result<(Rc<ServerShard>, Rc<RefCell<SessionManager>>), ServerError> {
    let shard_local_id = ShardId::new(shard_id);
    let total_partitions = metadata.mux_stm.streams().read(|inner| {
        inner
            .items
            .iter()
            .map(|(_, stream)| {
                stream
                    .topics
                    .iter()
                    .map(|(_, topic)| topic.partitions.len())
                    .sum::<usize>()
            })
            .sum::<usize>()
    });

    // IggyPartitions holds only the partitions owned by this shard
    // (see the filter below at insert time), so the server-wide total
    // is an N-fold overshoot. `ceil(total / shards) * 2` is a coarse
    // upper bound that absorbs hash skew without paying the full
    // multiplier. PapayaShardsTable below stays sized to the server-wide
    // total because every shard routes every namespace.
    let owned_partitions_capacity = total_partitions
        .div_ceil(usize::from(total_shards).max(1))
        .saturating_mul(2);
    // At-rest encryption: built once per shard from the shared config; the
    // ingestion path encrypts on the primary and the poll reply decrypts.
    // A bad key fails the boot rather than silently serving plaintext.
    let encryptor = if config.system.encryption.enabled {
        let aes = Aes256GcmEncryptor::from_base64_key(&config.system.encryption.key)
            .map_err(|error| ServerError::Iggy(Box::new(error)))?;
        Some(Arc::new(EncryptorKind::Aes256Gcm(aes)))
    } else {
        None
    };
    let partitions = IggyPartitions::with_capacity(
        shard_local_id,
        PartitionsConfig {
            messages_required_to_save: iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
            size_of_messages_required_to_save: IggyByteSize::from(
                iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
            ),
            enforce_fsync: iggy_common::DEFAULT_ENFORCE_FSYNC,
            validate_checksum: config.system.partition.validate_checksum,
            segment_size: IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
            preallocate_segments: iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
            encryptor,
            path_layout: partitions::PartitionPathLayout {
                streams_root: config.system.get_streams_path(),
                topics_dir: config.system.topic.path.clone(),
                partitions_dir: config.system.partition.path.clone(),
            },
        },
        owned_partitions_capacity,
    );
    let shards_table = PapayaShardsTable::with_capacity(total_partitions);

    // Stream-filter inside the `read()` closure: only partitions owned by
    // this shard need the heavy (`Arc<TopicStats>` + `Partition`) clones
    // for the async `load_partition` below. Non-owning entries are pushed
    // straight into `shards_table` here, so no Vec scales with the
    // server-wide partition count.
    let owned = metadata.mux_stm.streams().read(|inner| {
        let mut owned = Vec::with_capacity(owned_partitions_capacity);
        for (_, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                for partition in &topic.partitions {
                    let namespace = IggyNamespace::new(stream.id, topic_id, partition.id);
                    let owning_shard =
                        calculate_shard_assignment(&namespace, u32::from(total_shards));
                    if owning_shard == shard_id {
                        // Shared per-partition stats from the registry: the
                        // same `Arc` backs every shard's `get_topic` reply.
                        let stats = inner.stats_registry.partition(
                            stream.id,
                            topic_id,
                            partition.id,
                            topic.stats.clone(),
                        );
                        owned.push((
                            stream.id,
                            topic_id,
                            stats,
                            partition.clone(),
                            TopicRuntimeOptions::from_resource_options(&topic.options),
                        ));
                    } else {
                        shards_table.insert(
                            namespace,
                            PartitionLocation::new(
                                ShardId::new(owning_shard),
                                partition.created_revision,
                            ),
                        );
                    }
                }
            }
        }
        owned
    });

    // Snapshot totals were zeroed once on shard 0 before the factory
    // bundle was broadcast (see `MetadataHandoff::Owner`). All shards
    // here only add their per-partition deltas, so the shared
    // `Arc<TopicStats>` atomics race only against other atomic adds.
    for (stream_id, topic_id, partition_stats, partition_metadata, topic_runtime) in owned {
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_metadata.id);
        let partition = match load_partition(
            config,
            namespace,
            Arc::clone(&partition_stats),
            &partition_metadata,
            topic_runtime,
            topology.cluster_id,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
        )
        .await
        {
            Ok(partition) => partition,
            // ONE damaged local chain must not take the node down. The shapes
            // this refuses are structural -- what a failed state-transfer
            // quarantine leaves behind, or damage the recovery walk proved
            // inside a segment. What follows depends on whether a peer can
            // restore the data. With peers, the segment files are fenced
            // aside (keeping the superblock so the group cannot re-enter
            // view 0), the group is materialised fresh, and the ordinary
            // rejoin path (repair, then state transfer on a refused floor)
            // refills it. Single-replica, only a chain-shape refusal whose
            // planned chain provably holds ZERO recoverable bytes still
            // fences and rebuilds: nothing servable is at stake, so an empty
            // rebuild hides no loss. The verdict variant alone is not that
            // evidence -- a hole and an orphan empty segment both fire over
            // fully populated chains -- which is why the gate reads the byte
            // total the refusal carries. Every other refusal tombstones,
            // leaving its files exactly where they are: a rebuilt empty
            // partition answers polls exactly like a healthy empty one and
            // hides the loss, while an unrouted namespace is a failure an
            // operator can see.
            Err(ServerError::PartitionRecoveryRefused { dir, reason, .. }) => {
                let partition_dir = dir.to_string_lossy().into_owned();
                let rebuild_for_rejoin = topology.replica_count > 1
                    || matches!(
                        reason,
                        PartitionRecoveryRefusal::Hole {
                            recoverable_bytes: 0,
                            ..
                        } | PartitionRecoveryRefusal::EmptyNonTailSegment {
                            recoverable_bytes: 0,
                            ..
                        }
                    );
                error!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    partition_dir,
                    %reason,
                    "refusing the recovered segment chain"
                );
                // A pass-A refusal folded nothing into the stats (recovery
                // counts only accepted chains), but the hydrate-reopen refusal
                // arrives after a fully counted load, so clear them either way.
                partition_stats.zero_out_all();
                if !rebuild_for_rejoin {
                    // No quarantine here, mirroring the superblock arm below:
                    // a tombstone is only durable if its cause is. Fencing the
                    // chain aside would leave the next boot zero segments to
                    // walk, so it would re-seed from the surviving superblock,
                    // plant a fresh segment, and serve the partition empty
                    // with no refusal logged. Left at their real paths, the
                    // same files re-derive this verdict (and this log line)
                    // every boot, and the reconciler's tombstone gate keeps
                    // the namespace away from a fresh build, whose
                    // initial-segment open would truncate the oldest refused
                    // segment in place. The one refusal whose cause is NOT
                    // durable is `StorageSizeMismatch`: it fires from the
                    // reopen right after recovery truncated the same file, so
                    // the next boot re-walks the already-truncated bytes and,
                    // unless the length diverges again, accepts the chain
                    // instead of re-tombstoning -- acceptable for an
                    // assertion that the filesystem lied about a length.
                    // `%reason` repeated on purpose: this is the line an
                    // operator greps to enumerate dark partitions, so it has
                    // to carry the verdict on its own.
                    error!(
                        stream_id,
                        topic_id,
                        partition_id = partition_metadata.id,
                        partition_dir,
                        %reason,
                        "no peer replica holds this partition's data; leaving the refused \
                         segment files in place and tombstoning it instead of serving it \
                         empty"
                    );
                    partitions.tombstone(namespace);
                    continue;
                }
                match partitions::state_transfer::quarantine_segment_files(&partition_dir).await {
                    Ok(fenced_dir) => error!(
                        stream_id,
                        topic_id,
                        partition_id = partition_metadata.id,
                        fenced_dir,
                        "quarantined the refused segment files; they are kept for inspection"
                    ),
                    Err(error) => {
                        // NOT rebuilt: `build_partition_fresh` reaches
                        // `ensure_initial_segment`, which opens segment 0 with
                        // `file_exists = false` and TRUNCATES whatever the
                        // failed quarantine left behind. The likeliest failures
                        // (suffix cap exhausted, `create_dir_all`) move zero
                        // files, so rebuilding would destroy the oldest segment
                        // on the first attempt while the higher-offset survivors
                        // keep refusing every boot -- a loop that never
                        // terminates and eats the chain one segment at a time.
                        // Tombstone instead: the namespace stays unmaterialised
                        // and unrouted, the reconciler backs off, and an
                        // operator still has every byte.
                        error!(
                            stream_id,
                            topic_id,
                            partition_id = partition_metadata.id,
                            partition_dir,
                            %error,
                            "failed to quarantine the refused segment files; leaving this \
                             partition tombstoned rather than rebuilding over them"
                        );
                        partitions.tombstone(namespace);
                        continue;
                    }
                }
                build_partition_fresh(
                    config,
                    namespace,
                    partition_stats,
                    partition_metadata.created_revision,
                    topic_runtime,
                    topology.cluster_id,
                    topology.self_replica_id,
                    topology.replica_count,
                    partition_metadata.created_view,
                    Rc::clone(&bus),
                )
                .await?
            }
            // An untrustworthy superblock fences ONE group, not the node. The
            // segment files stay exactly where they are -- unlike a refused
            // chain, the data on disk is not the thing in doubt -- so there is
            // nothing to quarantine and nothing to rebuild: rebuilding fresh
            // would hand this replica a view-0 identity while a record it
            // cannot read says otherwise. Tombstoned, the namespace stays
            // unmaterialised and unrouted, the reconciler backs off, and an
            // operator has every byte plus a message naming the directory.
            Err(
                error @ (ServerError::PartitionSuperblockIo { .. }
                | ServerError::PartitionSuperblockVersionUnknown { .. }
                | ServerError::PartitionSuperblockUnverifiable { .. }
                | ServerError::PartitionSuperblockUndecodable { .. }
                | ServerError::PartitionSuperblockIdentityMismatch { .. }),
            ) => {
                error!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    %error,
                    "cannot trust this partition's durable consensus state; tombstoning the \
                     partition and continuing to boot the rest of the shard"
                );
                partition_stats.zero_out_all();
                partitions.tombstone(namespace);
                continue;
            }
            Err(error) => return Err(error),
        };
        partitions.insert(namespace, partition);
        shards_table.insert(
            namespace,
            PartitionLocation::new(ShardId::new(shard_id), partition_metadata.created_revision),
        );
    }

    let shard_handle = Rc::new(RefCell::new(None));
    // Same wiring path as the simulator's shell mode: one per-shard
    // SessionManager shared by the client-request handler (binds sessions)
    // and the get_clients handler (reads them). It also carries this shard's
    // cluster roster for the GetClusterMetadata read.
    let ShellHandlers {
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        sessions,
    } = wire_shell_handlers(
        &bus,
        &shard_handle,
        Arc::clone(&config.system),
        config.personal_access_token.max_tokens_per_user,
    );
    sessions
        .borrow_mut()
        .set_cluster_roster(Rc::new(build_cluster_roster(
            shard_id,
            config,
            topology,
            roster_cells,
        )?));
    let shard_name = format!("server-shard-{shard_id}");
    let built = IggyShardBuilder::new(
        ShardIdentity::new(shard_id, shard_name),
        Rc::clone(&bus),
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        metadata,
        partitions,
        senders,
        inbox,
        reply_inbox,
        shards_table,
        PartitionConsensusConfig::new(
            topology.cluster_id,
            shard::ReplicaTopology::new(topology.self_replica_id, topology.replica_count),
            Rc::clone(&bus),
        ),
        CoordinatorConfig {
            skip_shard_zero_for_replicas: config.cluster.coordinator.skip_shard_zero_for_replicas,
            skip_shard_zero_for_clients: config.cluster.coordinator.skip_shard_zero_for_clients,
        },
        metrics,
    )
    .build()
    .map_err(ServerError::ShardConstruction)?;

    let shard = Rc::new(built.shard);
    // Repair pacing is shared by both planes' repair loops, so it is a
    // per-shard tunable set once here rather than per consensus group.
    shard.set_repair_retry_ticks(repair_retry_ticks(config));
    shard.set_superblock_wedged_fatal_failures(superblock_wedged_fatal_failures(config));
    shard.set_served_segment_cache_bytes_max(
        config
            .partition
            .transfer_served_cache_bytes_max
            .as_bytes_u64(),
    );
    shard.set_partition_artifact_len_max(
        config.partition.transfer_artifact_bytes_max.as_bytes_u64(),
    );
    shard.set_repair_chunk_max(config.cluster.repair_chunk_max as u64);
    // Bounds a served state-transfer chunk. A frame above the bus ceiling is
    // rejected by the RECEIVING transport, which tears the replica connection
    // down rather than dropping one message.
    shard.set_bus_max_message_size(
        usize::try_from(config.message_bus.max_message_size.as_bytes_u64()).unwrap_or(usize::MAX),
    );
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    Ok((shard, sessions))
}

// Pin the configs-crate default literals (duplicated there to avoid a
// build-time edge onto the runtime crates) against the runtime constants,
// mirroring the message_bus IOV_MAX pin. A drift on either side fails this
// crate's build until both are reconciled.
const _: () = assert!(
    configs::metadata::DEFAULT_METADATA_PREPARE_QUEUE_DEPTH
        == consensus::PIPELINE_PREPARE_QUEUE_MAX
);
const _: () = assert!(
    configs::metadata::DEFAULT_METADATA_JOURNAL_SLOTS
        == journal::prepare_journal::DEFAULT_SLOT_COUNT
);
const _: () = assert!(
    configs::partition::DEFAULT_PARTITION_PREPARE_QUEUE_DEPTH
        == consensus::PIPELINE_PREPARE_QUEUE_MAX
);
const _: () =
    assert!(configs::metadata::DEFAULT_METADATA_CLIENTS_TABLE_MAX == consensus::CLIENTS_TABLE_MAX);
const _: () =
    assert!(configs::cluster::DEFAULT_VIEW_PROBE_ATTEMPTS_MAX == consensus::PROBE_ATTEMPTS_MAX);
const _: () =
    assert!(configs::partition::DEFAULT_EVICTED_RING_CAPACITY == partitions::EVICTED_RING_CAPACITY);
const _: () = assert!(
    configs::partition::DEFAULT_EVICTED_RING_BYTES_MAX == partitions::EVICTED_RING_BYTES_MAX
);
const _: () = assert!(
    configs::partition::DEFAULT_TRANSFER_ARTIFACT_BYTES_MAX
        == shard::PARTITION_ARTIFACT_LEN_DEFAULT
);
const _: () = assert!(
    configs::partition::DEFAULT_TRANSFER_SERVED_CACHE_BYTES_MAX
        == shard::SERVED_SEGMENT_CACHE_BYTES_DEFAULT
);
const _: () = assert!(configs::cluster::DEFAULT_REPAIR_CHUNK_MAX as u64 == shard::REPAIR_CHUNK_MAX);
const _: () = assert!(
    configs::cluster::STATE_CHUNK_HEADER_LEN
        == size_of::<iggy_binary_protocol::consensus::StateChunkHeader>() as u64
);
// Both prepare-queue ceilings are pinned by the view-change wire, not by memory: a
// `DoViewChange` carries the sender's suffix spanning `commit..=op` with one nack
// bit and one present bit per entry, each bitset a single `u128`. The depth bounds
// `op - commit`, so a depth at or above `DVC_HEADERS_MAX` produces entries the new
// primary can neither adopt nor prove dead. Strictly less than, because the head op
// needs the reserved slot.
const _: () =
    assert!(configs::metadata::MAX_METADATA_PREPARE_QUEUE_DEPTH < consensus::DVC_HEADERS_MAX);
const _: () =
    assert!(configs::partition::MAX_PARTITION_PREPARE_QUEUE_DEPTH < consensus::DVC_HEADERS_MAX);
// `DVC_HEADERS_MAX` is a bare literal in both the wire crate, which sizes the
// bitsets, and the consensus crate, which cannot depend on it the other way around.
// Same u128, so a drift lets one side address entries the other cannot.
const _: () =
    assert!(consensus::DVC_HEADERS_MAX == iggy_binary_protocol::consensus::DVC_HEADERS_MAX);
const _: () = assert!(consensus::DVC_HEADERS_MAX == u128::BITS as usize);

/// `[cluster] superblock_wedged_fatal_timeout` as a consecutive-failure count.
/// Retries pin at the backoff cap after warmup, so the window divided by
/// [`journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS`] bounds how
/// long a wedged replica may limp before it fail-stops. Zero stays zero
/// (fail-stop disabled).
fn superblock_wedged_fatal_failures(config: &ServerConfig) -> u64 {
    superblock_window_to_failures(
        config
            .cluster
            .superblock_wedged_fatal_timeout
            .get_duration(),
    )
}

fn superblock_window_to_failures(window: Duration) -> u64 {
    if window.is_zero() {
        return 0;
    }
    let cap_micros = u128::from(journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS);
    u64::try_from((window.as_micros() / cap_micros).max(1)).unwrap_or(u64::MAX)
}

/// Floor for the post-restart read-recovery deadline (see
/// [`recovery_barrier_deadline`]). At and below the 5s default heartbeat the
/// worst-case recovery is dominated by the heartbeat-independent term - the
/// `ViewChangeStatus` backstop plus election ceremony and suffix recommit,
/// empirically ~7s - so the scaled value must never fall under this or a
/// fast-heartbeat cluster would 503 legitimate reads mid-recovery. The backstop
/// is the configurable `[cluster] view_change_status_timeout`; raising it past
/// its 5s default is why `recovery_barrier_deadline` scales that knob in too
/// rather than leaning on this floor to cover it.
const RECOVERY_BARRIER_DEADLINE_FLOOR: Duration = Duration::from_secs(15);

/// Safety factor applied to each scaled term of the recovery deadline: a slower
/// heartbeat stretches election and suffix recommit proportionally, and a wider
/// status backstop stretches the ceremony it bounds. 3x reproduces the
/// empirically chosen 15s margin at the shared 5s default (3 x 5s = 15s) and
/// holds that factor as either knob grows.
const RECOVERY_BARRIER_MULTIPLIER: u32 = 3;

/// How long the post-restart read path waits for the recovered WAL suffix to
/// re-commit before failing loud (retryable 503): the largest of the fixed
/// floor, a `[cluster] heartbeat_timeout`-scaled window, and a
/// `[cluster] view_change_status_timeout`-scaled window. Both knobs feed it
/// because either, raised far past its default, stretches worst-case recovery
/// past the fixed floor; see `await_recovery_barrier` for the read-side wait.
fn recovery_barrier_deadline(heartbeat: Duration, view_change_status: Duration) -> Duration {
    // saturating: neither timeout has a config ceiling, plain `*` panics
    heartbeat
        .saturating_mul(RECOVERY_BARRIER_MULTIPLIER)
        .max(view_change_status.saturating_mul(RECOVERY_BARRIER_MULTIPLIER))
        .max(RECOVERY_BARRIER_DEADLINE_FLOOR)
}

/// Shard 0's half of a metadata recovery: everything [`metadata::impls::recovery::recover`] produced except the
/// state machine, which every shard receives through the factory bundle.
///
/// Named rather than a positional tuple: the fields are same-typed `Option<u64>`s and
/// `(u64, u128)` pairs that a reorder would silently rebind, and one of them decides
/// what view the replica boots into.
pub(in crate::boot) struct RecoveredOwnerState {
    pub(in crate::boot) journal: PrepareJournal,
    pub(in crate::boot) snapshot: Option<IggySnapshot>,
    pub(in crate::boot) last_applied_op: Option<u64>,
    pub(in crate::boot) last_journaled_op: Option<u64>,
    pub(in crate::boot) client_table: ClientTable,
    pub(in crate::boot) superblock: PingPongSuperblock,
    pub(in crate::boot) recovered_state: Option<VsrState>,
    pub(in crate::boot) snapshot_checkpoint: (u64, u128),
}

/// Rebuild metadata consensus from what recovery read off this replica's own disk.
///
/// Takes the recovery result, topology and config whole rather than the dozen-plus
/// scalars it needs from them: most were `u64` tick counts, where a misordered
/// argument type-checks and mistunes a timeout silently.
pub(in crate::boot) fn restore_metadata_consensus(
    owner: &RecoveredOwnerState,
    topology: &TcpTopology,
    config: &ServerConfig,
    bus: Rc<IggyMessageBus>,
) -> VsrConsensus<Rc<IggyMessageBus>> {
    let journal = &owner.journal;
    let replica_count = topology.replica_count;
    let recovered_state = owner.recovered_state;
    let snapshot_floor = owner
        .snapshot
        .as_ref()
        .map_or(0, IggySnapshot::sequence_number);
    let commit_watermark = owner.last_applied_op.unwrap_or(snapshot_floor);
    let restored_op = owner.last_journaled_op.unwrap_or(snapshot_floor);
    let recovery_deadline = recovery_barrier_deadline(
        config.cluster.heartbeat_timeout.get_duration(),
        config.cluster.view_change_status_timeout.get_duration(),
    );
    let prepare_queue_depth = config.metadata.prepare_queue_depth;

    let last_header = journal
        .last_op()
        .and_then(|op| usize::try_from(op).ok())
        .and_then(|op| journal.header(op).map(|header| *header));
    // On a RESTART in a cluster, rejoin as a quorum-invisible backup and
    // probe for the current view (`RequestStartView`): the view's primary
    // answers with a `StartView`, the replica adopts it as a backup, and
    // journal repair fills any WAL gap. A probing replica never resumes
    // primaryship -- if this replica IS the current primary-by-index, its
    // probe makes the backups elect past it.
    // The probe re-broadcasts on its timeout, so it needs no live mesh at
    // boot. A FRESH boot keeps the plain init: the cluster needs its view-0
    // primary to exist, and a single-replica cluster has no peer to ask.
    //
    // Prior life is EITHER a non-empty WAL or a recovered superblock. A view
    // change persists without touching the WAL, so a replica that changed
    // view before its first metadata write comes back with a non-zero view
    // and an empty journal; gating on the WAL alone would `init()` it into
    // `Status::Normal` as primary for a view the cluster may have moved past,
    // with `ceded_primaryship` false and no probe to correct it.
    //
    // The rejoin also awaits a state transfer: snapshot-shaped metadata state
    // (snapshot + client table) is replaced from the live primary the probe
    // finds, then journal repair fills the tail. If the probe exhausts
    // instead -- full-cluster bootstrap, nobody live to fetch from -- the
    // election fallback clears the stage and this local recovery stands.
    let join = if replica_count > 1 && (restored_op > 0 || recovered_state.is_some()) {
        JoinMode::ProbeAsBackup {
            await_state_transfer: true,
        }
    } else {
        JoinMode::Init
    };
    let timers = consensus_timers(config);
    let consensus = VsrConsensus::restored(
        topology.cluster_id,
        topology.self_replica_id,
        replica_count,
        server_common::sharding::METADATA_GROUP,
        bus,
        // Request queue keeps the stock 2x ratio over the prepare queue
        // (32 -> 64 at defaults): buffered requests are cheap relative to
        // in-flight prepares and drain as prepares commit.
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
        VsrRestore {
            timers: &timers,
            // View and log_view come from the durable superblock when present.
            // A present but unreadable superblock already refused boot in
            // `recover()`, so no durable record means genuinely absent: a
            // fresh node, or one that took writes but never checkpointed or
            // changed view. There, inferring the view from the last WAL
            // prepare is safe, since the persist-before-send gate guarantees
            // this replica never externalized a view beyond what a re-probe
            // re-derives, and it re-probes as a backup.
            durable_view: recovered_state.map(|state| (state.view, state.log_view)),
            view_fallback: last_header.map(|header| header.view),
            // Metadata, not a partition group: it has a journal to infer from
            // and no second plane to line up with.
            seed_view: None,
            // Fresh random incarnation each boot, so a StartView addressed to
            // a previous incarnation still in flight is ignored
            // (`handle_start_view` guard). `| 1` guarantees the non-zero the
            // guard treats as set. The deterministic simulator overrides this
            // with a seed-derived value bumped per restart.
            incarnation: Some(rand::random::<u128>() | 1),
            join,
        },
    );
    consensus.sequencer().set_sequence(restored_op);
    // A SOLO replica's durable journal head IS its commit point: quorum is
    // 1-of-1, so an entry commits the instant it is durable, and the acks
    // the cluster ceremony below would wait on cannot topologically exist.
    // The embedded watermark is structurally one op stale (the commit point
    // is only ever written down inside the NEXT entry), so trusting it solo
    // manufactures an "uncommitted" suffix that provably committed and
    // wedges the recovery barrier forever.
    let commit_watermark = if replica_count == 1 {
        restored_op
    } else {
        commit_watermark
    };
    // The commit point is restored from the WAL's embedded watermark (each
    // journaled prepare carries the primary's commit at send time), NOT from
    // the journal head: journaled does not imply committed, and claiming
    // commit for the un-quorum'd tail both risks split-brain on a later view
    // change and starves the tail of re-replication (it would live in no
    // pipeline). The suffix `(commit_watermark, restored_op]` is re-pipelined
    // below when this replica is the recovered view's primary.
    //
    // TODO(hubcio): the watermark is a lower bound (the last entry stamps
    // the commit point as of its send). Persisting an explicit (view,
    // commit_op) watermark on the commit path would tighten recovery and
    // allow refusing boot on an excessive gap; a backup that recovered a
    // LONGER tail than the cluster's primary still needs uncommitted-suffix
    // truncation when conflicting ops arrive (message repair milestone).
    consensus.restore_commit_state(commit_watermark, commit_watermark);
    if let Some(header) = last_header {
        consensus.set_last_prepare_checksum(header.checksum);
        consensus.observe_prepare_timestamp(header.timestamp);
    }

    // The WAL's tail past the watermark is prepared-but-not-provably-committed
    // state. Until the cluster confirms it (re-pipelined below on a resumed
    // primary; via StartView adoption + the local commit walk on a rejoined
    // backup), serving reads would show pre-restart state that clients already
    // saw acked -- gate them on the barrier regardless of role. If the suffix
    // never re-commits cluster-wide, the read path fails loud with a retryable
    // 503 once the paired deadline expires (`await_recovery_barrier`).
    if commit_watermark < restored_op {
        consensus.set_recovery_barrier(restored_op);
        consensus.set_recovery_deadline(recovery_deadline);
    }

    // Re-pipeline the prepared-but-uncommitted suffix so the primary's
    // retransmit machinery re-replicates it and quorum can (re-)commit it.
    // A backup's suffix stays journal-only: the primary's traffic either
    // confirms it (re-forward + re-ack path) or supersedes it.
    if consensus.is_primary()
        && !consensus.has_ceded_primaryship()
        && commit_watermark < restored_op
    {
        info!(
            commit_watermark,
            restored_op, "re-pipelining recovered uncommitted metadata suffix"
        );
        consensus.with_pipeline_mut(|pipeline| {
            #[allow(clippy::cast_possible_truncation)]
            for op in (commit_watermark + 1)..=restored_op {
                let Some(header) = journal.header(op as usize) else {
                    warn!(
                        op,
                        "recovered journal suffix has a gap; stopping re-pipeline"
                    );
                    break;
                };
                let mut entry = PipelineEntry::new(*header);
                entry.add_ack(topology.self_replica_id);
                pipeline.push(entry);
            }
        });
        // These went in through `Pipeline::push`, not `push_prepare_entry`, and `init`
        // no longer arms the timer: without this the recovered suffix sits in the
        // pipeline with nothing driving its retransmit.
        consensus.sync_prepare_timeout();
    }

    consensus
}

/// Recover this partition's persisted segment chain, stamping each segment
/// with the topic's effective segment size (the per-topic value when the
/// topic was created with one, else the shard-wide configured size).
///
/// The topic's effective `enforce_fsync` goes in for the same reason: it is
/// what tells recovery whether a durable index entry the log cannot back is a
/// benign torn index or previously durable data the log lost.
async fn recover_partition_segments(
    config: &ServerConfig,
    namespace: IggyNamespace,
    runtime_options: TopicRuntimeOptions,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, ServerError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let segment_size = runtime_options
        .segment_size
        .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
    let enforce_fsync = runtime_options
        .enforce_fsync
        .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC);
    load_persisted_segments(
        config,
        stream_id,
        topic_id,
        partition_id,
        segment_size,
        enforce_fsync,
        stats,
    )
    .await
    .map_err(|source| {
        error!(
            stream_id,
            topic_id,
            partition_id,
            error = %source,
            "failed to load partition log during server bootstrap"
        );
        source
    })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::boot) async fn load_partition(
    config: &ServerConfig,
    namespace: IggyNamespace,
    stats: Arc<PartitionStats>,
    partition_metadata: &Partition,
    runtime_options: TopicRuntimeOptions,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    // (view, log_view) come from the group's durable superblock when present;
    // a present but unverifiable record already refused boot inside
    // `open_partition_superblock`.
    let partition_dir = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    let (superblock, recovered_state) = open_partition_superblock(
        &partition_dir,
        ReplicaIdentity {
            cluster: cluster_id,
            replica_id: self_replica_id,
            replica_count,
        },
    )
    .await?;

    // A recovered partition lost its journal state with the process: the
    // partition journal is in-memory and segments carry no op numbers, so
    // this replica cannot know the group's (op, commit) even when the
    // superblock restored its view. In a cluster it boots as a
    // quorum-invisible backup and probes for the current view
    // (`RequestStartView`): the view's primary answers with a `StartView`,
    // journal repair fills the rejoin window, and the commit floor settles
    // at the serving peer's retention point. The probe re-broadcasts on its
    // timeout, so it needs no live mesh at boot. Single-replica groups
    // have no peer to ask and keep the plain init.
    let join = if replica_count > 1 {
        JoinMode::ProbeAsBackup {
            await_state_transfer: false,
        }
    } else {
        JoinMode::Init
    };
    // Request queue holds 2x the prepare depth (buffered requests drain as
    // prepares commit); depth is the per-partition `[partition]` knob.
    let prepare_queue_depth = config.partition.prepare_queue_depth;
    let timers = consensus_timers(config);
    let consensus = VsrConsensus::restored(
        cluster_id,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
        VsrRestore {
            timers: &timers,
            durable_view: recovered_state
                .as_ref()
                .map(|state| (state.view, state.log_view)),
            view_fallback: None,
            seed_view: None,
            incarnation: None,
            join,
        },
    );

    // No prepare-timestamp floor is restored here: the partition consensus
    // journal is non-durable today, so there is no persisted head to observe
    // (unlike `restore_metadata_consensus`, which observes its restored head).
    // When PartitionJournal becomes durable (the milestone named in the
    // multi-shard wiring commit body), observe the restored head and the max
    // recovered message timestamp here, or an NTP rewind across a restart could
    // regress persisted `base_timestamp`.

    let recovered_segments =
        recover_partition_segments(config, namespace, runtime_options, &stats).await?;

    let mut partition = IggyPartition::new(stats.clone(), consensus);
    partition.set_runtime_options(runtime_options);
    partition.set_superblock(superblock, recovered_state.as_ref());
    // Recovered partitions honor the same config-surfaced ring ceilings as the
    // fresh-create path (build_partition_fresh). Retention is already off for
    // single-replica groups, so this only sizes the multi-replica ring.
    partition.log.journal().inner.set_ring_caps(
        config.partition.evicted_ring_capacity,
        config.partition.evicted_ring_bytes_max.as_bytes_u64(),
    );
    partition.set_partition_dir(partition_dir.clone());
    // Before the hydrate: the durable record is keyed by incarnation, so a
    // `purge.gen` left behind by a previous life of this namespace reads 0.
    partition.set_created_revision(partition_metadata.created_revision);
    partition.hydrate_applied_purge_generation().await?;
    hydrate_partition_log(
        &mut partition,
        &partition_dir,
        stream_id,
        topic_id,
        partition_id,
        recovered_segments,
    )
    .await?;

    let sized_end = partition
        .log
        .segments()
        .iter()
        .filter(|segment| segment.size > IggyByteSize::default())
        .map(|segment| segment.end_offset)
        .max();
    // An empty chain whose segment is named for a nonzero offset is the
    // shape a state-transfer install (or its converge) plants at the group
    // frontier after the origin GC'd everything: the file name carries the
    // frontier, and re-minting offsets from 0 here would fork this
    // replica's batch stamps from the rest of the group after a restart.
    let empty_frontier = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .max()
        .filter(|&start| sized_end.is_none() && start > 0);
    let current_offset = sized_end.or_else(|| empty_frontier.map(|start| start - 1));
    partition.created_at = partition_metadata.created_at;
    partition.recovered_durable_offset = sized_end;
    // The OFFSET COUNTER is restored from that file name (above), but the
    // `installed_frontier` CLAIM deliberately is not: the claim says "everything
    // below me is represented here", and `converge_to_empty_after_failed_install`
    // refuses to make it when staged segments were dropped -- yet a converge
    // plants exactly the same empty `{frontier:020}.log` a legitimate empty
    // install does, so boot provably cannot tell them apart. Re-deriving it here
    // would hand the refused claim back: the repair floor stand-in would accept a
    // commit floor over ops this replica holds zero bytes for, and the replica
    // would pass the serve gate and offer that emptiness onward, making a peer
    // unlink its own chain. Leaving it `None` costs one spurious full
    // re-transfer on the legitimate empty-install restart; a false caught-up
    // claim is not recoverable. A durable home for the frontier (the partition
    // superblock already reserves a field) is what would settle it properly.
    let counter = current_offset.unwrap_or(0);
    partition.offset.store(counter, Ordering::Release);
    partition.dirty_offset.store(counter, Ordering::Relaxed);
    partition.should_increment_offset = current_offset.is_some();
    // The durable frontier is a LOWER BOUND on top of what the segments proved:
    // it is the only carrier left when the segments that named the frontier are
    // gone (an all-GC'd origin's install, a crash inside the swap window), and
    // taking the max means real recovered data always wins.
    partition.restore_offset_frontier(recovered_state.as_ref());
    let current_offset = partition.offset.load(Ordering::Acquire);

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

/// Reopen writers over a recovered segment chain.
///
/// Takes no `&ServerConfig`: every knob it needs is the partition's own
/// resolved topic option now, which is the whole point of the per-topic move.
async fn hydrate_partition_log(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    partition_dir: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    recovered_segments: Vec<RecoveredSegment>,
) -> Result<(), ServerError> {
    // The partition's own resolved knobs, not the shard-wide config: a topic
    // created with `enforce_fsync` or a per-topic `segment_size` must get them
    // on the writers reopened over its recovered chain too, or a restart would
    // silently drop back to the node defaults.
    let runtime = partition.runtime_options();
    let enforce_fsync = runtime
        .enforce_fsync
        .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC);
    let segment_size = runtime
        .segment_size
        .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
    let preallocate_segments = runtime
        .preallocate_segments
        .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS);
    for RecoveredSegment { segment, storage } in recovered_segments {
        partition
            .log
            .add_persisted_segment(segment, storage, None, None);
    }

    if let Some(active_index) = partition.log.segments().len().checked_sub(1) {
        let storage = &partition.log.storages()[active_index];
        if let (
            Some(messages_reader),
            Some(index_reader),
            Some(storage_messages_writer),
            Some(storage_index_writer),
        ) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
            storage.messages_writer.as_ref(),
            storage.index_writer.as_ref(),
        ) {
            let index_path = index_reader.path();
            let start_offset = partition.log.segments()[active_index].start_offset;
            // Share the storage's size counters: they are the write cursors.
            // A private counter would let the append position diverge from the
            // segment bookkeeping that index entries and poll bounds rely on.
            let messages_size_counter = storage_messages_writer.size_counter();
            let index_size_counter = storage_index_writer.size_counter();
            partition.log.messages_writers_mut()[active_index] = Some(Rc::new(
                MessagesWriter::new(
                    &messages_reader.path(),
                    messages_size_counter,
                    enforce_fsync,
                    true,
                    preallocate_segments.then_some(segment_size),
                )
                .await
                .map_err(|source| {
                    error!(
                        stream_id,
                        topic_id,
                        partition_id,
                        path = %messages_reader.path(),
                        error = %source,
                        "failed to initialize persisted messages writer"
                    );
                    hydrate_reopen_error(
                        source,
                        partition_dir,
                        stream_id,
                        topic_id,
                        partition_id,
                        start_offset,
                    )
                })?,
            ));
            partition.log.index_writers_mut()[active_index] = Some(Rc::new(
                IggyIndexWriter::new(&index_path, index_size_counter, enforce_fsync, true)
                    .await
                    .map_err(|source| {
                        error!(
                            stream_id,
                            topic_id,
                            partition_id,
                            path = %index_path,
                            error = %source,
                            "failed to initialize persisted sparse index writer"
                        );
                        hydrate_reopen_error(
                            source,
                            partition_dir,
                            stream_id,
                            topic_id,
                            partition_id,
                            start_offset,
                        )
                    })?,
            ));
        }
    }

    Ok(())
}

/// Routes a hydrate-reopen writer failure. The seed-vs-stat divergence guard
/// (`SegmentSizeMismatchAtOpen`) is a post-condition assertion on recovery's
/// own truncation: pass C truncates every file to its recovered size before
/// storage and writers reopen it, so the guard can only fire if the
/// filesystem lied about a length or a change broke that truncate-then-open
/// contract. Kept as defense-in-depth and routed as a structural refusal
/// because a retried boot cannot help. Every other failure here (open, stat,
/// sync) is transient I/O and stays node-fatal: a retried boot can still
/// serve the partition, while fencing would quarantine healthy data (and at
/// `replica_count = 1` tombstone the partition outright).
fn hydrate_reopen_error(
    source: IggyError,
    partition_dir: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    start_offset: u64,
) -> ServerError {
    match source {
        IggyError::SegmentSizeMismatchAtOpen(on_disk_bytes, expected_bytes) => {
            ServerError::PartitionRecoveryRefused {
                dir: PathBuf::from(partition_dir),
                stream_id,
                topic_id,
                partition_id,
                reason: PartitionRecoveryRefusal::StorageSizeMismatch {
                    start_offset,
                    on_disk_bytes,
                    expected_bytes,
                },
            }
        }
        transient => transient.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_fatal_window_converts_to_capped_backoff_retries() {
        assert_eq!(
            superblock_window_to_failures(Duration::ZERO),
            0,
            "zero window must stay the disabled sentinel"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_mins(2)),
            120,
            "past warmup one retry rides each 1s backoff cap"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_micros(500)),
            1,
            "a sub-cap window still needs one failure to fire"
        );
    }

    #[test]
    fn default_cluster_heartbeat_timeout_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .heartbeat_timeout
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::NORMAL_HEARTBEAT_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] heartbeat_timeout default drifted from \
             TimeoutManager::NORMAL_HEARTBEAT_TICKS"
        );
    }

    #[test]
    fn recovery_barrier_deadline_holds_the_floor_for_small_heartbeats() {
        // Below the 5s default the heartbeat-independent recovery term (~7s of
        // ViewChangeStatus backstop plus ceremony) dominates, so the floor
        // governs however small the heartbeat is; 3 x 5s lands exactly on it.
        // A default-sized status backstop stays on the floor, not above it.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(1), Duration::from_secs(5)),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(5), Duration::from_secs(5)),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
    }

    #[test]
    fn recovery_barrier_deadline_scales_past_the_floor_for_large_heartbeats() {
        // Once 3 x heartbeat clears the floor the scaled window governs, so a
        // slow-heartbeat cluster is not failed 503 before its longer recovery
        // can finish. A default-sized status backstop stays under it.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(10), Duration::from_secs(5)),
            Duration::from_secs(30)
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(15), Duration::from_secs(5)),
            Duration::from_secs(45)
        );
    }

    #[test]
    fn recovery_barrier_deadline_scales_with_the_status_backstop() {
        // A raised view-change status backstop stretches worst-case recovery
        // even when the heartbeat stays fast, so the deadline must track it or
        // post-restart reads 503 before a slow election settles.
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(1), Duration::from_secs(10)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn recovery_barrier_deadline_at_config_defaults_matches_the_floor() {
        // Folding the status term in must not move the stock deadline: at the
        // shared 5s defaults each scaled term lands exactly on the 15s floor,
        // so an un-tuned cluster keeps its pre-existing recovery window.
        let cluster = configs::cluster::ClusterConfig::default();
        assert_eq!(
            recovery_barrier_deadline(
                cluster.heartbeat_timeout.get_duration(),
                cluster.view_change_status_timeout.get_duration(),
            ),
            RECOVERY_BARRIER_DEADLINE_FLOOR
        );
    }

    #[test]
    fn recovery_barrier_deadline_saturates_instead_of_panicking() {
        // Neither timeout has a config ceiling, so both multiplies must
        // saturate rather than abort boot on an absurd parseable value.
        assert_eq!(
            recovery_barrier_deadline(Duration::MAX, Duration::from_secs(5)),
            Duration::MAX
        );
        assert_eq!(
            recovery_barrier_deadline(Duration::from_secs(5), Duration::MAX),
            Duration::MAX
        );
    }

    #[test]
    fn default_commit_broadcast_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .commit_broadcast_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::COMMIT_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] commit_broadcast_interval default drifted from \
             TimeoutManager::COMMIT_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_prepare_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string,
        // so no static assert can pin it); keep it in lockstep with the
        // built-in the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .prepare_retransmit_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::PREPARE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] prepare_retransmit_interval default drifted from \
             TimeoutManager::PREPARE_TICKS"
        );
    }

    #[test]
    fn default_partition_prepare_queue_depth_matches_consensus_constant() {
        // The config default lives in core/server/config.toml and flows
        // through PartitionConfig::default(); keep the embedded value in
        // lockstep with the pipeline depth LocalPipeline::new() (the simulator
        // and tests) runs on, so a default deployment is byte-identical.
        let config_default = configs::partition::PartitionConfig::default().prepare_queue_depth;
        assert_eq!(
            config_default,
            consensus::PIPELINE_PREPARE_QUEUE_MAX,
            "[partition] prepare_queue_depth default drifted from \
             consensus::PIPELINE_PREPARE_QUEUE_MAX"
        );
    }

    #[test]
    fn default_view_change_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it). One knob drives both view-change
        // retransmit timers, which are equal by design, so pin it against both.
        let config_default = configs::cluster::ClusterConfig::default()
            .view_change_retransmit_interval
            .get_duration()
            .as_millis();
        let start_view_change =
            u128::from(consensus::TimeoutManager::START_VIEW_CHANGE_MESSAGE_TICKS)
                * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        let do_view_change = u128::from(consensus::TimeoutManager::DO_VIEW_CHANGE_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, start_view_change,
            "[cluster] view_change_retransmit_interval default drifted from \
             TimeoutManager::START_VIEW_CHANGE_MESSAGE_TICKS"
        );
        assert_eq!(
            config_default, do_view_change,
            "[cluster] view_change_retransmit_interval default drifted from \
             TimeoutManager::DO_VIEW_CHANGE_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_view_change_status_timeout_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .view_change_status_timeout
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::VIEW_CHANGE_STATUS_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] view_change_status_timeout default drifted from \
             TimeoutManager::VIEW_CHANGE_STATUS_TICKS"
        );
    }

    #[test]
    fn default_request_start_view_retransmit_interval_matches_consensus_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .request_start_view_retransmit_interval
            .get_duration()
            .as_millis();
        let built_in = u128::from(consensus::TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS)
            * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] request_start_view_retransmit_interval default drifted from \
             TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS"
        );
    }

    #[test]
    fn default_view_probe_attempts_max_matches_consensus_constant() {
        // Belt and suspenders with the static assert above: that pins the
        // duplicated configs-crate literal, this pins the shipped config.toml
        // value the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default().view_probe_attempts_max;
        assert_eq!(
            config_default,
            consensus::PROBE_ATTEMPTS_MAX,
            "[cluster] view_probe_attempts_max default drifted from \
             consensus::PROBE_ATTEMPTS_MAX"
        );
    }

    #[test]
    fn default_repair_retry_interval_matches_partitions_constant() {
        // The config default lives in core/server/config.toml (a string, so
        // no static assert can pin it); keep it in lockstep with the built-in
        // the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default()
            .repair_retry_interval
            .get_duration()
            .as_millis();
        let built_in =
            u128::from(partitions::REPAIR_RETRY_TICKS) * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] repair_retry_interval default drifted from \
             partitions::REPAIR_RETRY_TICKS"
        );
    }

    #[test]
    fn default_repair_chunk_max_matches_shard_constant() {
        // Belt and suspenders with the static assert above: that pins the
        // duplicated configs-crate literal, this pins the shipped config.toml
        // value the simulator and un-configured replicas run on.
        let config_default = configs::cluster::ClusterConfig::default().repair_chunk_max;
        assert_eq!(
            config_default as u64,
            shard::REPAIR_CHUNK_MAX,
            "[cluster] repair_chunk_max default drifted from shard::REPAIR_CHUNK_MAX"
        );
    }

    #[test]
    fn default_evicted_ring_capacity_matches_partitions_constant() {
        // Belt and suspenders with the static assert above; this pins the
        // shipped config.toml value.
        let config_default = configs::partition::PartitionConfig::default().evicted_ring_capacity;
        assert_eq!(
            config_default,
            partitions::EVICTED_RING_CAPACITY,
            "[partition] evicted_ring_capacity default drifted from \
             partitions::EVICTED_RING_CAPACITY"
        );
    }

    #[test]
    fn default_evicted_ring_bytes_max_matches_partitions_constant() {
        // Belt and suspenders with the static assert above; this pins the
        // shipped config.toml value.
        let config_default = configs::partition::PartitionConfig::default()
            .evicted_ring_bytes_max
            .as_bytes_u64();
        assert_eq!(
            config_default,
            partitions::EVICTED_RING_BYTES_MAX,
            "[partition] evicted_ring_bytes_max default drifted from \
             partitions::EVICTED_RING_BYTES_MAX"
        );
    }
}
