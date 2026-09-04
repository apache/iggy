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
use crate::partition_helpers::load_partition_or_fence;
use crate::server_error::ServerError;
use crate::session_manager::SessionManager;
use crate::shell::{
    ServerMetadata, ServerShard, ShellHandlers, ShellShardHandle, consensus_timers,
    repair_gap_debounce_ticks, repair_retry_ticks,
};
use configs::server::ServerConfig;
use consensus::{
    ClientTable, JoinMode, LocalPipeline, PipelineEntry, Sequencer, VsrConsensus, VsrRestore,
    VsrState,
};
use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyByteSize, TopicRuntimeOptions};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use journal::superblock::PingPongSuperblock;
use message_bus::IggyMessageBus;
use message_bus::client_listener::RequestHandler;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::stm::snapshot::Snapshot;
use partitions::{IggyPartitions, PartitionsConfig};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use shard::builder::IggyShardBuilder;
use shard::metrics::ShardMetrics;
use shard::shards_table::{PapayaShardsTable, ShardsTable, calculate_shard_assignment};
use shard::{
    CoordinatorConfig, PartitionConsensusConfig, Receiver as ShardReceiver, ShardFrame,
    ShardIdentity, TaggedSender,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// A shard built for its thread, with what `shard_main` wires after the
/// build: the session manager its request plane shares, the shard's one
/// client-request handler (shard 0 hands the same instance to its local
/// transports), and the weak self-reference the deferred handlers
/// upgrade per frame, already backfilled.
pub(in crate::boot) struct ShardBuild {
    pub shard: Rc<ServerShard>,
    pub sessions: Rc<RefCell<SessionManager>>,
    pub on_client_request: RequestHandler,
    pub shard_handle: ShellShardHandle<Rc<IggyMessageBus>, PrepareJournal, IggySnapshot>,
}

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
) -> Result<ShardBuild, ServerError> {
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
        let loaded = load_partition_or_fence(
            config,
            namespace,
            partition_stats,
            &partition_metadata,
            topic_runtime,
            topology.cluster_id,
            topology.self_replica_id,
            topology.replica_count,
            Rc::clone(&bus),
            &partitions,
        )
        .await;
        let partition = match loaded {
            Ok(Some(partition)) => partition,
            Ok(None) => continue,
            // A refused claim is a failed superblock write, not damage: the
            // namespace stays materialisable, so skipping it here costs one
            // partition its start instead of the whole shard, and the
            // reconciler's addition pass retries it within a tick.
            Err(error @ ServerError::PartitionOffsetReservationClaim { .. }) => {
                error!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    %error,
                    "skipping this partition at boot; the reconciler retries its first \
                     offset reservation claim"
                );
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
        Rc::clone(&on_client_request),
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
    shard.set_partition_gap_debounce_ticks(repair_gap_debounce_ticks(config));
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
    Ok(ShardBuild {
        shard,
        sessions,
        on_client_request,
        shard_handle,
    })
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
const _: () = assert!(
    configs::partition::PARTITION_DEDUP_CLIENTS_DEFAULT == consensus::PARTITION_DEDUP_CLIENTS_MAX
);
const _: () = assert!(
    configs::partition::PARTITION_CONSUMER_OFFSETS_DEFAULT
        == partitions::DEFAULT_CONSUMER_OFFSETS_MAX
);
const _: () = assert!(
    2 * configs::partition::PARTITION_CONSUMER_OFFSETS_CEILING
        <= partitions::CONSUMER_OFFSETS_ENTRIES_MAX as usize
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

/// `[cluster] superblock_wedged_fatal_timeout` as a consecutive-failure count,
/// which is the only shape the shard's `superblock_wedged` can compare. Zero
/// stays zero (fail-stop disabled).
fn superblock_wedged_fatal_failures(config: &ServerConfig) -> u64 {
    superblock_window_to_failures(
        config
            .cluster
            .superblock_wedged_fatal_timeout
            .get_duration(),
    )
}

/// The failure count whose arrival time is the first at or past `window`.
///
/// Walks the real retry schedule rather than dividing by the backoff cap. Only
/// the retries past warmup pin at the cap: the first six wait 20, 40, 80, 160,
/// 320 and 640 ms, so they spend 1.26 s of the window where a flat division
/// charges them six. The default 2 m window came out as 120 failures, which
/// arrive after about 114.26 s -- the fail-stop firing almost six seconds before
/// the window the operator configured.
fn superblock_window_to_failures(window: Duration) -> u64 {
    if window.is_zero() {
        return 0;
    }
    // `write_superblock_inner` records failure N and only then arms the wait
    // that follows it, so failure N ARRIVES at the sum of the N-1 waits before
    // it -- the first arrives at zero. The loop sums forward until the window is
    // covered, and the count that satisfies it is one past the last wait summed.
    let window_micros = window.as_micros();
    let mut elapsed = 0u128;
    let mut waits = 0u64;
    while elapsed < window_micros {
        waits += 1;
        elapsed += u128::from(superblock_retry_backoff_micros(waits));
    }
    // A window shorter than the very first retry lands here with one wait
    // summed, giving two: a threshold of one would fail-stop on the first
    // failure, before any of the window had elapsed at all.
    waits.saturating_add(1)
}

/// The wait `IggyPartition`'s superblock writer arms after its `failures`-th
/// consecutive failure.
///
/// Mirrors that arithmetic exactly. A divergence here does not fail a test, it
/// moves the fail-stop to a time no operator asked for.
fn superblock_retry_backoff_micros(failures: u64) -> u64 {
    journal::superblock::SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS
        .saturating_mul(1 << failures.min(journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT))
        .min(journal::superblock::SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS)
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
            126,
            "the six warmup retries spend 1.26s, not 6s: 120 would fire at ~114.26s"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_secs(30)),
            36,
            "the configured floor for a nonzero window"
        );
        assert_eq!(
            superblock_window_to_failures(Duration::from_micros(500)),
            2,
            "a window shorter than the first retry must not fail-stop on the \
             very first failure, before any of it elapsed"
        );
    }

    /// The threshold is a floor on elapsed time, never a ceiling: the failure it
    /// names must arrive at or after the configured window, and its predecessor
    /// must arrive before it. Walked against the writer's own schedule.
    #[test]
    fn given_a_fatal_window_when_converted_should_never_fire_before_it_elapses() {
        for window in [
            Duration::from_secs(30),
            Duration::from_secs(45),
            Duration::from_mins(2),
            Duration::from_mins(10),
        ] {
            let threshold = superblock_window_to_failures(window);
            // Failure N arrives at the sum of the N-1 waits before it.
            let arrival = |count: u64| -> u128 {
                (1..count)
                    .map(|wait| u128::from(superblock_retry_backoff_micros(wait)))
                    .sum()
            };
            assert!(
                arrival(threshold) >= window.as_micros(),
                "{window:?}: failure {threshold} arrives at {}us, inside the window",
                arrival(threshold)
            );
            assert!(
                arrival(threshold - 1) < window.as_micros(),
                "{window:?}: failure {} already covers the window, so {threshold} is late",
                threshold - 1
            );
        }
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

    /// The floor is prose in `config.toml` ("50 consensus ticks (500ms at the
    /// 10ms tick)"), which is the number an operator sizes
    /// `repair_gap_debounce_interval` against. Nothing else would notice it
    /// drifting.
    #[test]
    fn documented_gap_debounce_floor_matches_the_shard_constant() {
        assert_eq!(
            shard::PARTITION_GAP_DEBOUNCE_TICKS_MIN,
            50,
            "the gap debounce floor moved; core/server/config.toml states it in \
             ticks and milliseconds under [cluster] repair_gap_debounce_interval"
        );
        assert_eq!(
            u128::from(shard::PARTITION_GAP_DEBOUNCE_TICKS_MIN)
                * shard::CONSENSUS_TICK_INTERVAL.as_millis(),
            500,
            "the floor is no longer 500ms; core/server/config.toml states that \
             figure under [cluster] repair_gap_debounce_interval"
        );
    }

    #[test]
    fn default_repair_gap_debounce_interval_matches_partitions_constant() {
        // Same lockstep the retry interval keeps: the shipped config.toml value
        // is what an un-configured replica and the simulator run on, and the
        // shard's own default is the compile-time constant.
        let config_default = configs::cluster::ClusterConfig::default()
            .repair_gap_debounce_interval
            .get_duration()
            .as_millis();
        let built_in =
            u128::from(partitions::REPAIR_RETRY_TICKS) * shard::CONSENSUS_TICK_INTERVAL.as_millis();
        assert_eq!(
            config_default, built_in,
            "[cluster] repair_gap_debounce_interval default drifted from the shard's \
             compile-time debounce"
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
    fn default_offset_reservation_lease_matches_partitions_constant() {
        // `IggyPartition::new` falls back to the partitions constant (simulator,
        // unit tests) while boot installs this one, so drift would have the
        // fence write at a different rate in the simulator than in production.
        let config_default =
            configs::partition::PartitionConfig::default().offset_reservation_lease;
        assert_eq!(
            config_default.get(),
            partitions::DEFAULT_OFFSET_RESERVATION_LEASE,
            "[partition] offset_reservation_lease default drifted from \
             partitions::DEFAULT_OFFSET_RESERVATION_LEASE"
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
