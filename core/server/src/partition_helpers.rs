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

//! Partition materialisation shared by the boot path and the runtime
//! reconciliation loop.
//!
//! [`load_partition_or_fence`] hydrates an [`IggyPartition`] from its
//! on-disk state and rules on a segment chain recovery refused, and on a
//! directory that belongs to another incarnation;
//! [`build_partition_fresh`] materialises one for a namespace that has no
//! directory yet. Both sit on the same namespace-bounds validation, and on
//! the consumer-offset configuration and initial-segment provisioning in
//! [`partitions::configure_consumer_offsets`] and
//! [`partitions::ensure_initial_segment`].
//!
//! Boot runs the loader over every owned namespace. The reconciler picks
//! whichever builder the partition directory calls for when a committed
//! `CreateTopic` / `CreatePartitions` event has no matching local
//! partition yet.

use crate::server_error::ServerError;
use crate::shell::consensus_timers;
use compio::fs::create_dir_all;
use configs::server::ServerConfig;
use consensus::{
    FreshGroupStart, JoinMode, LocalPipeline, VsrConsensus, VsrRestore, VsrState, fresh_group_start,
};
use iggy_common::{IggyByteSize, IggyError, IggyTimestamp, PartitionStats, TopicRuntimeOptions};
use journal::durable_storage::{DiskStorage, DurableStorage};
use journal::partition_journal::SegmentPosition;
use journal::superblock::{PingPongSuperblock, SuperblockContents};
use message_bus::IggyMessageBus;
use metadata::stm::stream::Partition;
use metadata::{IdentityField, ReplicaIdentity};
use partitions::{
    IggyPartition, IggyPartitions, PartitionPersistence, PartitionRecoveryError,
    PartitionRecoveryRefusal, PartitionsConfig, RecoveredSegment, configure_consumer_offsets,
    create_partition_file_hierarchy, ensure_initial_segment, hydrate_partition_log,
    load_persisted_segments, load_persisted_segments_with_checkpoint,
};
use server_common::fatal::is_descriptor_exhaustion;
use server_common::sharding::IggyNamespace;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{error, warn};

const PARTITION_INITIALIZATION_DIR: &str = "partition-initialization";
const RETIREMENT_FENCE_FILE: &str = "retirement.fence";

/// Open the durable superblock for one partition's consensus group and read
/// back the last recorded VSR state.
///
/// Mirrors the metadata plane's recovery contract: an EMPTY superblock is a
/// genuinely fresh group (or one that never changed view) and yields `None`;
/// a present record must decode and match this replica's identity; a present
/// but unverifiable record is an error, because treating it as fresh would
/// let this replica re-enter a view it already acted in. The boot path
/// tombstones just that partition rather than refusing the whole node.
///
/// The returned store is the ONE open instance for this group: the partition
/// keeps writing through it, and re-opening later would fork the ping-pong
/// sequence counter.
///
/// # Errors
///
/// [`ServerError::PartitionSuperblockIo`] when the directory or a slot
/// cannot be read; the `VersionUnknown` / `Unverifiable` / `Undecodable` /
/// `IdentityMismatch` / `PartitionViewBelowCreation` variants when a record
/// exists but cannot be trusted.
pub async fn open_partition_superblock(
    partition_dir: &str,
    identity: ReplicaIdentity,
    created_view: u32,
) -> Result<(Rc<PingPongSuperblock>, Option<VsrState>), ServerError> {
    let io_error = |source| ServerError::PartitionSuperblockIo {
        dir: PathBuf::from(partition_dir),
        source,
    };
    // The load path can reach a partition whose directory was never
    // materialized on this replica (a committed create it missed); the
    // superblock lives inside that directory either way.
    create_dir_all(partition_dir).await.map_err(io_error)?;
    let (superblock, latest) = PingPongSuperblock::open_with_latest(partition_dir)
        .await
        .map_err(io_error)?;
    let recovered_state = match latest {
        SuperblockContents::Present(bytes) => {
            Some(VsrState::try_from(bytes.as_slice()).map_err(|source| {
                ServerError::PartitionSuperblockUndecodable {
                    dir: PathBuf::from(partition_dir),
                    source,
                }
            })?)
        }
        SuperblockContents::Unreadable {
            version: Some(version),
        } => {
            return Err(ServerError::PartitionSuperblockVersionUnknown {
                dir: PathBuf::from(partition_dir),
                version,
            });
        }
        SuperblockContents::Unreadable { version: None } => {
            return Err(ServerError::PartitionSuperblockUnverifiable {
                dir: PathBuf::from(partition_dir),
            });
        }
        SuperblockContents::Empty => None,
    };
    if let Some(state) = recovered_state.as_ref() {
        let mismatch = |field, expected: u128, found: u128| {
            Err(ServerError::PartitionSuperblockIdentityMismatch {
                dir: PathBuf::from(partition_dir),
                field,
                expected,
                found,
            })
        };
        if state.cluster != identity.cluster {
            return mismatch(IdentityField::Cluster, identity.cluster, state.cluster);
        }
        if state.replica_id != identity.replica_id {
            return mismatch(
                IdentityField::ReplicaId,
                identity.replica_id.into(),
                state.replica_id.into(),
            );
        }
        if state.replica_count != identity.replica_count {
            return mismatch(
                IdentityField::ReplicaCount,
                identity.replica_count.into(),
                state.replica_count.into(),
            );
        }
        if state.log_view != 0 && state.log_view < created_view {
            return Err(ServerError::PartitionViewBelowCreation {
                dir: PathBuf::from(partition_dir),
                view: state.view,
                log_view: state.log_view,
                created_view,
            });
        }
    }
    Ok((Rc::new(superblock), recovered_state))
}

/// Recover an owned partition from its on-disk state.
///
/// Shared by boot and the reconciler so a partition this replica committed
/// before a crash but re-learns only after restart (its WAL watermark trails
/// the commit by one op) is hydrated from its segments like any other, not
/// rebuilt over them. A directory whose [`partitions::CREATED_REVISION_FILE`]
/// names an older incarnation is deleted first, and the partition loads as if
/// it had none. `Ok(None)` means the
/// namespace was tombstoned here, or that its directory belongs to a newer
/// incarnation than local metadata knows; the arms below say when. Errors are
/// transient I/O, left to the caller.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn load_partition_or_fence(
    config: &ServerConfig,
    namespace: IggyNamespace,
    partition_stats: Arc<PartitionStats>,
    partition_metadata: &Partition,
    topic_runtime: TopicRuntimeOptions,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
    partitions: &IggyPartitions<Rc<IggyMessageBus>, PingPongSuperblock>,
) -> Result<Option<IggyPartition<Rc<IggyMessageBus>>>, ServerError> {
    let created_revision = partition_metadata.created_revision;
    match partition_retirement_fence(&config.get_system_path(), namespace).await {
        Ok(Some(recorded)) if recorded > created_revision => return Ok(None),
        Ok(Some(recorded)) if recorded == created_revision => {
            warn!(
                namespace_raw = namespace.inner(),
                created_revision, "partition remains offline under its durable retirement fence"
            );
            partitions.fence(namespace, created_revision);
            return Ok(None);
        }
        Err(ref error @ ServerError::PartitionRetirementFenceIo { ref source, .. })
            if source.kind() == std::io::ErrorKind::InvalidData =>
        {
            error!(%error, namespace_raw = namespace.inner(), "unreadable retirement fence; refusing partition");
            partitions.fence(namespace, created_revision);
            return Ok(None);
        }
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    let initialization_dir = partition_initialization_path(&config.get_system_path(), namespace);
    match partitions::read_created_revision(&initialization_dir).await {
        Ok(Some(recorded)) if recorded > created_revision => return Ok(None),
        Err(source) if source.kind() == std::io::ErrorKind::InvalidData => {
            error!(%source, namespace_raw = namespace.inner(), "unreadable initialization record; refusing partition");
            partitions.fence(namespace, created_revision);
            return Ok(None);
        }
        Err(source) => {
            return Err(ServerError::PartitionCreatedRevisionIo {
                dir: PathBuf::from(initialization_dir),
                source,
            });
        }
        Ok(_) => {}
    }
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let directory = config.get_partition_path(stream_id, topic_id, namespace.partition_id());
    partitions::install_backup::recover(Path::new(&directory))
        .await
        .map_err(|source| ServerError::PartitionSuperblockIo {
            dir: PathBuf::from(&directory),
            source,
        })?;
    let revision_io = |source| ServerError::PartitionCreatedRevisionIo {
        dir: PathBuf::from(&directory),
        source,
    };
    let unmarked = match partitions::read_created_revision(&directory).await {
        // A delete that did not finish before a restart left this directory,
        // and a later create reused its ids. Nothing in it belongs to the
        // committed partition, so it goes, marker last. The load below then
        // joins as a probing backup like any other load, not as the `Init`
        // member of a fresh build, which counts toward quorums at once and is
        // safe only if this replica never served the group.
        Ok(Some(recorded)) if recorded < created_revision => {
            warn!(
                stream_id,
                topic_id,
                partition_id = partition_metadata.id,
                partition_dir = directory,
                recorded_revision = recorded,
                created_revision,
                "deleting a partition directory left behind by a deleted incarnation"
            );
            partitions::delete_partitions_from_disk(
                stream_id,
                topic_id,
                namespace.partition_id(),
                partitions.config(),
            )
            .await?;
            true
        }
        // Local metadata trails the create that made this directory. Not a
        // tombstone: the reconciler retries once a commit moves metadata on.
        Ok(Some(recorded)) if recorded > created_revision => {
            warn!(
                stream_id,
                topic_id,
                partition_id = partition_metadata.id,
                partition_dir = directory,
                recorded_revision = recorded,
                created_revision,
                "partition directory belongs to a newer incarnation than local metadata; \
                 deferring its load"
            );
            return Ok(None);
        }
        Ok(Some(_)) => false,
        Ok(None) => {
            match std::fs::read_dir(&directory).and_then(|mut entries| entries.next().transpose()) {
                Ok(Some(_)) => warn!(
                    stream_id,
                    topic_id,
                    partition_id = partition_metadata.id,
                    partition_dir = directory,
                    created_revision,
                    "adopting a nonempty partition directory without created.revision; \
                     if this namespace was deleted and recreated, delete and recreate it again \
                     only if its messages can be discarded; see core/server/README.md"
                ),
                Ok(None) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(revision_io(source)),
            }
            true
        }
        // Same policy as an undecodable superblock: one group goes dark, the
        // node stays up, and every byte stays where it is.
        Err(source) if source.kind() == std::io::ErrorKind::InvalidData => {
            error!(
                stream_id,
                topic_id,
                partition_id = partition_metadata.id,
                partition_dir = directory,
                %source,
                "cannot tell which incarnation this partition directory belongs to; \
                 tombstoning the partition instead of serving it"
            );
            partitions.fence(namespace, created_revision);
            return Ok(None);
        }
        Err(source) => return Err(revision_io(source)),
    };
    // Both durability policies persist consensus files. Fence the incarnation
    // before loading; only Persisted requires the hierarchy barrier below.
    if unmarked {
        create_partition_file_hierarchy(
            stream_id,
            topic_id,
            namespace.partition_id(),
            partitions.config(),
        )
        .await?;
        if topic_runtime.durability.is_persisted()
            || topic_runtime.consumer_offset_durability.is_persisted()
        {
            persist_partition_hierarchy(&directory, &config.get_system_path()).await?;
        }
        record_created_revision(&directory, created_revision).await?;
    }
    // Heap-pinned: the loader's and the rebuilder's futures side by side
    // outgrow clippy's `large_futures` cap, and this runs once per partition.
    match Box::pin(load_partition(
        config,
        partitions.config(),
        namespace,
        Arc::clone(&partition_stats),
        partition_metadata,
        topic_runtime,
        cluster_id,
        self_replica_id,
        replica_count,
        Rc::clone(&bus),
    ))
    .await
    {
        Ok(partition) => Ok(Some(partition)),
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
        Err(ServerError::PartitionRecovery(PartitionRecoveryError::Refused {
            dir,
            reason,
            ..
        })) => {
            let partition_dir = dir.to_string_lossy().into_owned();
            let rebuild_for_rejoin = replica_count > 1
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
                partitions.fence(namespace, created_revision);
                return Ok(None);
            }
            if replica_count > 1 {
                partitions::state_transfer::mark_materialization_missing(
                    &partition_dir,
                    partition_metadata.created_revision,
                )
                .await
                .map_err(|error| {
                    error!(%error, "cannot persist missing-materialization fence");
                    ServerError::Iggy(Box::new(IggyError::CannotSyncFile))
                })?;
            }
            match partitions::state_transfer::quarantine_partition_files(
                &partition_dir,
                (replica_count > 1).then_some(partition_metadata.created_revision),
            )
            .await
            {
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
                    partitions.fence(namespace, created_revision);
                    return Ok(None);
                }
            }
            Box::pin(build_partition_fresh(
                config,
                partitions.config(),
                namespace,
                Arc::clone(&partition_stats),
                partition_metadata.created_revision,
                topic_runtime,
                cluster_id,
                self_replica_id,
                replica_count,
                partition_metadata.created_view,
                Rc::clone(&bus),
            ))
            .await
            .map(Some)
        }
        // No free file descriptor says nothing about the record, so the load
        // fails like any other, and a tombstone would keep the group dark after
        // descriptors free up. At boot the error stops the server with exit
        // status 4, because the superblock read notes it. Later, the reconciler
        // retries the load with backoff.
        Err(error)
            if matches!(&error, ServerError::PartitionSuperblockIo { source, .. }
                if is_descriptor_exhaustion(source)) =>
        {
            Err(error)
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
            | ServerError::PartitionSuperblockIdentityMismatch { .. }
            | ServerError::PartitionViewBelowCreation { .. }
            | ServerError::PartitionWalViewBelowCreation { .. }),
        ) => {
            error!(
                stream_id,
                topic_id,
                partition_id = partition_metadata.id,
                %error,
                "cannot trust this partition's durable consensus state; tombstoning the \
                 partition instead of serving it"
            );
            partition_stats.zero_out_all();
            partitions.fence(namespace, created_revision);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn load_partition(
    config: &ServerConfig,
    partitions_config: &PartitionsConfig,
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
    let first_initialization =
        partition_needs_initialization(config, namespace, partition_metadata.created_revision)
            .await?;
    // (view, log_view) come from the group's durable superblock when present;
    // a present but unverifiable record already refused boot inside
    // `open_partition_superblock`.
    let partition_dir = config.get_partition_path(stream_id, topic_id, partition_id);
    let (superblock, recovered_state) = open_partition_superblock(
        &partition_dir,
        ReplicaIdentity {
            cluster: cluster_id,
            replica_id: self_replica_id,
            replica_count,
        },
        partition_metadata.created_view,
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
            view_fallback: Some(partition_metadata.created_view),
            seed_view: None,
            incarnation: None,
            join,
        },
    );
    consensus.set_recovery_election_allowed(first_initialization);

    // No prepare-timestamp floor is restored here: the partition consensus
    // journal is non-durable today, so there is no persisted head to observe
    // (unlike `restore_metadata_consensus`, which observes its restored head).
    // When PartitionJournal becomes durable (the milestone named in the
    // multi-shard wiring commit body), observe the restored head and the max
    // recovered message timestamp here, or an NTP rewind across a restart could
    // regress persisted `base_timestamp`.

    let wal_directory =
        Path::new(&partition_dir).join(format!("prepares-{}", partition_metadata.created_revision));
    let wal_error = |source: std::io::Error| match source.kind() {
        std::io::ErrorKind::InvalidData
        | std::io::ErrorKind::UnexpectedEof
        | std::io::ErrorKind::NotFound => {
            ServerError::PartitionRecovery(PartitionRecoveryError::Refused {
                dir: PathBuf::from(&partition_dir),
                stream_id: namespace.stream_id(),
                topic_id: namespace.topic_id(),
                partition_id: namespace.partition_id(),
                reason: PartitionRecoveryRefusal::PrepareWal {
                    directory: wal_directory.clone(),
                    source,
                },
            })
        }
        _ => ServerError::PartitionPrepareWalIo {
            dir: wal_directory.clone(),
            source,
        },
    };
    let recovered_persistence = {
        let directory = wal_directory.clone();
        if replica_count == 1
            && !first_initialization
            && !journal::PartitionPrepareJournal::has_published_frontier(&directory)
                .await
                .map_err(&wal_error)?
        {
            return Err(wal_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "existing singleton partition is missing its prepare WAL frontier",
            )));
        }
        if replica_count > 1 && !first_initialization {
            PartitionPersistence::fence_missing_history(
                &directory,
                partition_metadata.created_revision,
            )
            .await
            .map_err(&wal_error)?;
        }
        let (persistence, prepares) = PartitionPersistence::open_with_capacity(
            &directory,
            namespace.inner(),
            partition_metadata.created_revision,
            DiskStorage,
            config.partition.wal_bytes_max.as_bytes_u64(),
            runtime_options
                .preallocate_segments
                .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS),
        )
        .await
        .map_err(&wal_error)?;
        if let Some(log_view) = persistence.certified_log_view()
            && log_view != 0
            && log_view < partition_metadata.created_view
        {
            return Err(ServerError::PartitionWalViewBelowCreation {
                dir: directory,
                log_view,
                created_view: partition_metadata.created_view,
            });
        }
        (persistence, prepares)
    };
    let segment_checkpoint = recovered_persistence.0.segment_checkpoint();
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
    partition.set_dedup_clients_max(config.partition.dedup_clients_max);
    partition.set_consumer_offsets_max(config.partition.consumer_offsets_max);
    partition.set_offset_reservation_lease(config.partition.offset_reservation_lease);
    partition.set_partition_dir(partition_dir.clone());
    // Before the hydrate: the durable record is keyed by incarnation, so a
    // `purge.gen` left behind by a previous life of this namespace reads 0.
    partition.set_created_revision(partition_metadata.created_revision);
    partition
        .restore_retry_checkpoint(recovered_persistence.0.checkpoint_op())
        .await
        .map_err(wal_error)?;
    let recovered_segments = recover_partition_segments(
        partitions_config,
        namespace,
        runtime_options,
        &stats,
        segment_checkpoint,
    )
    .await?;
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

    partition.created_at = partition_metadata.created_at;
    restore_partition_offsets(&mut partition, recovered_state.as_ref(), segment_checkpoint);
    let current_offset = partition.offset.load(Ordering::Acquire);

    configure_consumer_offsets(&mut partition, partitions_config, namespace, current_offset)
        .await?;
    if !partition.log.has_segments() {
        ensure_initial_segment(
            &mut partition,
            partitions_config,
            namespace,
            segment_checkpoint.is_some(),
        )
        .await?;
    }

    open_partition_persistence(&mut partition, config, Some(recovered_persistence)).await?;
    ensure_initial_segment(&mut partition, partitions_config, namespace, true).await?;
    if replica_count == 1 && !partition.durability().is_persisted() {
        partition
            .reanchor_to_offset_frontier(partitions_config)
            .await
            .map_err(|error| ServerError::Iggy(Box::new(error)))?;
    }
    if let Err(error) =
        record_partition_initialization(config, namespace, partition_metadata.created_revision)
            .await
    {
        partition.stats.zero_out_all();
        return Err(error);
    }
    Ok(partition)
}

/// Restore the offset counter of a recovered partition from what boot could
/// prove about its offset space.
///
/// Three carriers, weakest last: the sized segments' end offset, an empty
/// chain's file name (a state-transfer install at the group frontier), and the
/// superblock's durable frontier as a lower bound over both.
fn restore_partition_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    recovered_state: Option<&VsrState>,
    segment_checkpoint: Option<SegmentPosition>,
) {
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
    //
    // Bounded by the durable frontier: an install writes it at the group
    // frontier, so the name is corroborated, while the boot re-anchor and
    // `ensure_initial_segment` plant at `mint_frontier()`, a RESERVATION that
    // names no data and leaves the frontier far below. Without the bound two
    // crashes under the flush threshold promote 65537 to committed on a
    // partition holding nothing, and `store_consumer_offset` admits the hole.
    let durable_frontier = recovered_state.map_or(0, |state| state.offset_frontier);
    let empty_frontier = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .max()
        .map(|start| start.min(durable_frontier))
        .filter(|&start| sized_end.is_none() && start > 0);
    let checkpoint_end =
        segment_checkpoint.and_then(|checkpoint| checkpoint.next_offset.checked_sub(1));
    let current_offset = sized_end
        .or_else(|| empty_frontier.map(|start| start - 1))
        .max(checkpoint_end);
    partition.recovered_durable_offset = sized_end.max(checkpoint_end);
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
    partition.set_offset_space_used(current_offset.is_some());
    // The durable frontier is a LOWER BOUND on top of what the segments proved:
    // it is the only carrier left when the segments that named the frontier are
    // gone (an all-GC'd origin's install, a crash inside the swap window), and
    // taking the max means real recovered data always wins.
    partition.restore_offset_frontier(recovered_state);
}

/// Recover this partition's persisted segment chain, stamping each segment
/// with the topic's effective segment size (the per-topic value when the
/// topic was created with one, else the shared segment-size default).
///
/// The topic's effective `persisted` goes in for the same reason: it is
/// what tells recovery whether a durable index entry the log cannot back is a
/// benign torn index or previously durable data the log lost.
async fn recover_partition_segments(
    partitions_config: &PartitionsConfig,
    namespace: IggyNamespace,
    runtime_options: TopicRuntimeOptions,
    stats: &PartitionStats,
    checkpoint: Option<SegmentPosition>,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let segment_size = runtime_options.effective_segment_size();
    let persisted = runtime_options.durability.is_persisted();
    let recovered = if checkpoint.is_some() {
        load_persisted_segments_with_checkpoint(
            partitions_config,
            namespace,
            segment_size,
            persisted,
            stats,
            checkpoint,
        )
        .await
    } else {
        load_persisted_segments(partitions_config, namespace, segment_size, persisted, stats).await
    };
    recovered.map_err(|source| {
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

/// Materialise a brand-new [`IggyPartition`] for a namespace that has no on-disk state yet.
///
/// Counterpart to [`load_partition_or_fence`], which hydrates from
/// on-disk state; this builder is the runtime path invoked by the
/// reconciliation loop when a committed `CreateTopic` /
/// `CreatePartitions` metadata event names a partition the local shard
/// has not yet materialised and has no directory for. A directory
/// already on disk is routed through the loader instead, so a prior
/// life's segments are hydrated rather than built over.
///
/// Steps performed. 1 to 4 are idempotent on retry after a partial failure; the
/// claim is last precisely because it is not (see its own comment):
/// 1. Create directory hierarchy on disk.
/// 2. Build per-partition VSR consensus group, resuming any superblock-recorded view.
/// 3. Configure empty consumer-offset storage with the on-disk paths set.
/// 4. Provision the initial segment + writers (offset 0).
/// 5. Claim the group's first offset-reservation block (solo groups with a store).
///
/// The namespace arrives packed, so its components are in range by
/// construction. Metadata admission is what bounds them.
///
/// `created_view` is the view a group with no durable record of its own starts
/// in: the metadata plane's view when it committed the create, recorded on
/// the committed partition so every replica seeds the same value. See the
/// `seed_view` comment below for why a group left at view 0 is unreachable. A
/// restart keeps it as a view floor while probing for the live view, without
/// assigning it to the recovered log.
///
/// The returned partition's `offset` / `dirty_offset` are `0` and its
/// `OffsetSpace` is unused, mirroring a clean append starting at the empty
/// segment.
///
/// # Errors
///
/// Returns [`ServerError`] when directory creation, superblock recovery,
/// segment provisioning, or the first offset-reservation claim fails.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn build_partition_fresh(
    config: &ServerConfig,
    partitions_config: &PartitionsConfig,
    namespace: IggyNamespace,
    stats: Arc<PartitionStats>,
    created_revision: u64,
    runtime_options: TopicRuntimeOptions,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    created_view: u32,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerError> {
    if partition_retirement_fence(&config.get_system_path(), namespace)
        .await?
        .is_some_and(|recorded| recorded >= created_revision)
    {
        return Err(ServerError::PartitionRetirementFenced {
            namespace_raw: namespace.inner(),
            created_revision,
        });
    }
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let first_initialization =
        partition_needs_initialization(config, namespace, created_revision).await?;

    let partition_dir = config.get_partition_path(stream_id, topic_id, partition_id);
    let restarted = replica_count > 1 && !first_initialization;
    if replica_count == 1 && !first_initialization {
        let directory = Path::new(&partition_dir).join(format!("prepares-{created_revision}"));
        if !journal::PartitionPrepareJournal::has_published_frontier(&directory)
            .await
            .map_err(|source| ServerError::PartitionPrepareWalIo {
                dir: directory.clone(),
                source,
            })?
        {
            return Err(ServerError::PartitionPrepareWalIo {
                dir: directory,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "initialized singleton partition is missing its prepare WAL frontier",
                ),
            });
        }
    }
    create_partition_file_hierarchy(stream_id, topic_id, partition_id, partitions_config)
        .await
        .map_err(|source| {
            error!(
                stream_id,
                topic_id,
                partition_id,
                error = %source,
                "failed to create partition file hierarchy for fresh partition"
            );
            source
        })?;
    if runtime_options.durability.is_persisted()
        || runtime_options.consumer_offset_durability.is_persisted()
    {
        persist_partition_hierarchy(&partition_dir, &config.get_system_path()).await?;
    }
    // Replicated also persists consensus files, so the incarnation fence is
    // required even when the hierarchy barrier above is skipped.
    record_created_revision(&partition_dir, created_revision).await?;

    // The hierarchy create above guarantees the directory exists; recover this
    // group's durable (view, log_view) before choosing how to join, so a
    // restart materialization resumes from the view it last recorded instead
    // of re-entering an older one.
    let (superblock, recovered_state) = open_partition_superblock(
        &partition_dir,
        ReplicaIdentity {
            cluster: cluster_id,
            replica_id: self_replica_id,
            replica_count,
        },
        created_view,
    )
    .await?;

    // The external initialization record, including after directory loss,
    // distinguishes a prior participant from a fresh group member. Prior
    // participants probe before counting toward quorums.
    let durable_view = recovered_state
        .as_ref()
        .map(|state| (state.view, state.log_view));
    // Shared with the simulator's `init_partition`, which cannot call this
    // builder; see `fresh_group_start`.
    let FreshGroupStart {
        view_fallback,
        seed_view,
        join,
    } = fresh_group_start(restarted, durable_view, created_view);
    // Request queue holds 2x the prepare depth (buffered requests drain as
    // prepares commit); depth is the per-partition `[partition]` knob.
    let prepare_queue_depth = config.partition.prepare_queue_depth;
    let timers = crate::shell::consensus_timers(config);
    let consensus = VsrConsensus::restored(
        cluster_id,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
        VsrRestore {
            timers: &timers,
            durable_view,
            view_fallback,
            // Both planes pick their primary as `view % replica_count` from
            // their OWN view counter. A group left at view 0 while the
            // metadata plane sits elsewhere therefore names a different node
            // than the roster advertises as leader, and nothing routes a
            // partition write across that gap: the client is sent to the
            // metadata leader and refused there for the whole budget. Seeding
            // from the view the create was admitted in keeps the two congruent
            // for a group born after a metadata election.
            //
            // The seed comes off the committed partition, not this replica's
            // live metadata view: the two differ once the metadata plane
            // elects again, and a seed above the group's real view is an
            // empty log that outranks committed history in the next DVC
            // merge. The value rides the create's request body (a header view
            // is restamped per delivery), so every replica commits the same
            // one and a late materialiser lands at or below its peers.
            seed_view,
            incarnation: None,
            join,
        },
    );

    let mut partition = IggyPartition::new(stats, consensus);
    partition
        .consensus()
        .set_recovery_election_allowed(first_initialization);
    partition.set_runtime_options(runtime_options);
    partition.set_superblock(superblock, recovered_state.as_ref());
    // Surface the evicted-ring ceilings from config onto the fresh journal.
    // IggyPartition::new has already disabled retention for single-replica
    // groups (nobody to serve), so this only sizes the multi-replica ring; the
    // caps are inert while retention is off.
    partition.log.journal().inner.set_ring_caps(
        config.partition.evicted_ring_capacity,
        config.partition.evicted_ring_bytes_max.as_bytes_u64(),
    );
    partition.set_dedup_clients_max(config.partition.dedup_clients_max);
    partition.set_consumer_offsets_max(config.partition.consumer_offsets_max);
    partition.set_offset_reservation_lease(config.partition.offset_reservation_lease);
    partition.set_partition_dir(partition_dir);
    // Fresh dirs read generation 0; a dir surviving from a crashed process
    // (this "fresh" build races repair re-materialization) reads the last
    // durably-applied purge so the reconciler does not re-wipe messages
    // appended after it. Keyed by incarnation, so a dir left behind by a failed
    // delete does not fence the recreated partition's purges: set the revision
    // first.
    partition.set_created_revision(created_revision);
    partition.hydrate_applied_purge_generation().await?;
    partition.created_at = IggyTimestamp::now();
    partition.offset.store(0, Ordering::Release);
    partition.dirty_offset.store(0, Ordering::Relaxed);
    partition.set_offset_space_used(false);
    debug_assert!(
        !partition.log.has_segments(),
        "fresh partition must not carry recovered segments"
    );

    // A "fresh" build is also how a FENCED partition comes back (the shard
    // tombstones it and the reconciler rebuilds through here), and the fence
    // deliberately leaves the superblock in place, so the recorded frontier is
    // the rebuild's only anchor.
    //
    // It is a LOWER BOUND, not a guarantee: the record is written on view
    // changes and transfer installs, so it lags the counter arbitrarily -- a
    // fresh joiner that adopted a view while empty and then filled via repair
    // has a record still reading 0, and this rebuild would re-seed at 0. For
    // ordinary crash recovery that staleness is harmless (segments survive and
    // win the max); it is the fence paths that promote the stale bound to sole
    // source of truth. Closing it needs the runtime fence to persist the
    // frontier before quarantining, and the boot-path chain refusal to carry
    // the refused chain's max `end_offset` on its error.
    partition.restore_offset_frontier(recovered_state.as_ref());

    let current_offset = partition.offset.load(Ordering::Acquire);

    configure_consumer_offsets(&mut partition, partitions_config, namespace, current_offset)
        .await?;
    ensure_initial_segment(&mut partition, partitions_config, namespace, false).await?;

    // Claim the first offset-reservation block HERE so no send ever pays the
    // create, write, file fsync, rename and directory fsync of a first claim
    // inline in the shard's request pump, where the consensus tick is a sibling
    // arm. It is a NEW write on a path that otherwise only READS the superblock:
    // one atomic replace per created partition, serialised with its siblings in
    // the reconciler's addition loop, so it lengthens the window a produce
    // arriving with the create spends parked.
    //
    // LAST of the steps, because a claim written before a step that then fails
    // outlives the create. The reconciler routes any namespace whose directory
    // exists to `load_partition_or_fence`, and step 1 made that directory, so
    // the retry comes back through the loader: `restore_offset_frontier` there
    // resumes the append point at the recorded reservation and holes every
    // offset below it on a partition that never took a write.
    //
    // `0`, not `mint_frontier()`: a rebuild recovers its append point exactly ON
    // the reservation it recorded, so asking to cover the frontier would fail
    // the callee's strict `>` and rewrite the record on every rebuild. Asking
    // only for offset 0 leaves that same check to skip every partition already
    // carrying a reservation, which pays one inline fence on its first send
    // instead, the cost a graceful stop and boot already carries. No-op above
    // one replica and with no store attached, where nothing is reserved.
    //
    // The shard tick takes over from the first mint onward
    // (`needs_offset_reservation_extension`), which stays gated on a partition
    // that has minted so boot cannot write a superblock per idle partition.
    if !partition.reserve_offsets_through(0).await {
        // Not degraded-but-live: the failed write armed the group's superblock
        // retry backoff, and `reserve_offsets_through_retryable` refuses every
        // send arriving inside it with a transient the HTTP plane does not
        // replay. Neither caller escalates: the reconciler backs the namespace
        // off and its retry materialises through the loader, whose partition
        // carries a clear backoff cell, and boot skips the partition, leaving it
        // to that same retry.
        //
        // `ensure_initial_segment` folded its segment into the parent topic
        // before the claim ran, and the retry counts the same file again while
        // recovering it.
        partition.stats.zero_out_all();
        return Err(ServerError::PartitionOffsetReservationClaim {
            stream_id,
            topic_id,
            partition_id,
            namespace_raw: namespace.inner(),
        });
    }

    open_partition_persistence(&mut partition, config, None).await?;
    if let Err(error) = record_partition_initialization(config, namespace, created_revision).await {
        partition.stats.zero_out_all();
        return Err(error);
    }
    Ok(partition)
}

fn partition_initialization_path(system_path: &str, namespace: IggyNamespace) -> String {
    format!(
        "{system_path}/{PARTITION_INITIALIZATION_DIR}/{}",
        namespace.inner()
    )
}

pub async fn partition_retirement_fence(
    system_path: &str,
    namespace: IggyNamespace,
) -> Result<Option<u64>, ServerError> {
    let directory = partition_initialization_path(system_path, namespace);
    partitions::read_revision_record(&directory, RETIREMENT_FENCE_FILE)
        .await
        .map_err(|source| ServerError::PartitionRetirementFenceIo {
            dir: PathBuf::from(directory),
            source,
        })
}

/// A failed incarnation may stop retaining session receipts only once it can
/// no longer rejoin with those receipts missing after a restart.
pub async fn record_partition_retirement_fence(
    system_path: &str,
    namespace: IggyNamespace,
    created_revision: u64,
) -> Result<(), ServerError> {
    if partition_retirement_fence(system_path, namespace)
        .await?
        .is_some_and(|recorded| recorded >= created_revision)
    {
        let directory = partition_initialization_path(system_path, namespace);
        return persist_partition_hierarchy(&directory, system_path).await;
    }
    let directory = partition_initialization_path(system_path, namespace);
    create_dir_all(&directory)
        .await
        .map_err(|source| ServerError::PartitionRetirementFenceIo {
            dir: PathBuf::from(&directory),
            source,
        })?;
    persist_partition_hierarchy(&directory, system_path).await?;
    partitions::write_revision_record(&directory, RETIREMENT_FENCE_FILE, created_revision)
        .await
        .map_err(|source| ServerError::PartitionRetirementFenceIo {
            dir: PathBuf::from(directory),
            source,
        })
}

async fn partition_needs_initialization(
    config: &ServerConfig,
    namespace: IggyNamespace,
    created_revision: u64,
) -> Result<bool, ServerError> {
    let directory = partition_initialization_path(&config.get_system_path(), namespace);
    let recorded = partitions::read_created_revision(&directory)
        .await
        .map_err(|source| ServerError::PartitionCreatedRevisionIo {
            dir: PathBuf::from(&directory),
            source,
        })?;
    if recorded.is_some_and(|revision| revision > created_revision) {
        return Err(ServerError::PartitionCreatedRevisionIo {
            dir: PathBuf::from(directory),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "partition initialization belongs to a newer metadata incarnation",
            ),
        });
    }
    Ok(recorded != Some(created_revision))
}

/// Publish outside the partition directory before this incarnation can serve.
/// An absent directory can then be distinguished from lost initialized history.
async fn record_partition_initialization(
    config: &ServerConfig,
    namespace: IggyNamespace,
    created_revision: u64,
) -> Result<(), ServerError> {
    if !partition_needs_initialization(config, namespace, created_revision).await? {
        let directory = partition_initialization_path(&config.get_system_path(), namespace);
        return persist_partition_hierarchy(&directory, &config.get_system_path()).await;
    }
    let directory = partition_initialization_path(&config.get_system_path(), namespace);
    create_dir_all(&directory)
        .await
        .map_err(|source| ServerError::PartitionCreatedRevisionIo {
            dir: PathBuf::from(&directory),
            source,
        })?;
    persist_partition_hierarchy(&directory, &config.get_system_path()).await?;
    record_created_revision(&directory, created_revision).await
}

/// Record which incarnation `partition_dir` belongs to; see
/// [`partitions::CREATED_REVISION_FILE`].
async fn record_created_revision(
    partition_dir: &str,
    created_revision: u64,
) -> Result<(), ServerError> {
    partitions::write_created_revision(partition_dir, created_revision)
        .await
        .map_err(|source| ServerError::PartitionCreatedRevisionIo {
            dir: PathBuf::from(partition_dir),
            source,
        })
}

/// Open a partition's prepare WAL with the budget and the group-commit delay
/// this server was configured with.
async fn open_partition_persistence(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerConfig,
    recovered: Option<(
        Rc<PartitionPersistence>,
        Vec<server_common::Message<iggy_binary_protocol::PrepareHeader>>,
    )>,
) -> Result<(), ServerError> {
    partition
        .open_persistence_with_recovered(
            config.partition.wal_bytes_max.as_bytes_u64(),
            std::time::Duration::from_micros(config.partition.wal_group_commit_delay_micros),
            recovered,
        )
        .await
        .map_err(|error| ServerError::Iggy(Box::new(error)))
}

async fn persist_partition_hierarchy(
    partition_path: &str,
    data_root: &str,
) -> Result<(), ServerError> {
    let root = Path::new(data_root);
    let mut current = Path::new(partition_path);
    loop {
        DiskStorage
            .sync_directory(current)
            .await
            .map_err(|_| ServerError::Iggy(Box::new(IggyError::CannotSyncFile)))?;
        if current == root {
            break;
        }
        let Some(parent) = current
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            break;
        };
        current = parent;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use configs::server::ServerConfig;
    use consensus::Sequencer;
    use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
    use iggy_binary_protocol::requests::system::SessionIdentity;
    use iggy_binary_protocol::{Command, Operation, PrepareHeader, WireEncode};
    use journal::DurableAppend;
    use journal::superblock::SuperblockStore;
    use partitions::PartitionPathLayout;
    use server_common::Message;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::ShardId;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    const CLUSTER: u128 = 7;
    const REPLICA: u8 = 1;
    const REPLICAS: u8 = 3;
    const OFFSETS_MAGIC: &[u8; 4] = b"ICO1";
    const OFFSETS_VERSION: u8 = 3;
    const RETRY_CHECKPOINT_MAGIC: &[u8; 4] = b"IRP2";

    #[compio::test]
    async fn given_uninitialized_singleton_when_booting_should_finish_initialization() {
        assert_uninitialized_partition_recovers(1).await;
    }

    #[compio::test]
    async fn given_uninitialized_replica_when_booting_should_finish_initialization() {
        assert_uninitialized_partition_recovers(REPLICAS).await;
    }

    async fn assert_uninitialized_partition_recovers(replica_count: u8) {
        const REVISION: u64 = 7;
        let namespace = IggyNamespace::new(1, 1, 0);
        for interrupted in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let partitions = solo_partitions(&config);
            let directory = config.get_partition_path(1, 1, 0);
            if interrupted {
                create_partition_file_hierarchy(1, 1, 0, partitions.config())
                    .await
                    .unwrap();
                record_created_revision(&directory, REVISION).await.unwrap();
                open_partition_superblock(
                    &directory,
                    ReplicaIdentity {
                        cluster: CLUSTER,
                        replica_id: 0,
                        replica_count,
                    },
                    0,
                )
                .await
                .unwrap();
            }
            let recovered = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), REVISION, 0),
                TopicRuntimeOptions::default(),
                CLUSTER,
                0,
                replica_count,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap()
            .expect("a partition that never initialized must finish its committed creation");
            assert!(
                !recovered.requires_state_transfer(),
                "first initialization has no missing history to transfer; interrupted={interrupted}"
            );
            assert!(
                journal::PartitionPrepareJournal::has_published_frontier(
                    &Path::new(&directory).join(format!("prepares-{REVISION}"))
                )
                .await
                .unwrap()
            );
        }
    }

    #[compio::test]
    async fn given_initialized_partition_when_directory_lost_should_preserve_recovery_fence() {
        const REVISION: u64 = 7;
        let namespace = IggyNamespace::new(1, 1, 0);
        for replica_count in [1, REPLICAS] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let partitions = solo_partitions(&config);
            let initialized = build_partition_fresh(
                &config,
                partitions.config(),
                namespace,
                Arc::new(PartitionStats::default()),
                REVISION,
                TopicRuntimeOptions::default(),
                CLUSTER,
                0,
                replica_count,
                0,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap();
            drop(initialized);
            std::fs::remove_dir_all(config.get_partition_path(1, 1, 0)).unwrap();
            let recovered = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), REVISION, 0),
                TopicRuntimeOptions::default(),
                CLUSTER,
                0,
                replica_count,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap();
            if replica_count == 1 {
                assert!(
                    recovered.is_none(),
                    "a singleton must not hide lost history"
                );
            } else {
                assert!(recovered.unwrap().requires_state_transfer());
            }
        }
    }

    #[compio::test]
    async fn given_initialized_singleton_when_rebuilding_lost_directory_should_refuse() {
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        drop(build_solo_partition(&config).await.unwrap());
        std::fs::remove_dir_all(config.get_partition_path(1, 1, 0)).unwrap();
        assert!(
            build_solo_partition(&config).await.is_err(),
            "reconciliation must not bypass the initialized partition's missing-history fence"
        );
    }

    #[compio::test]
    async fn given_uncertified_view_when_loading_or_rebuilding_should_persist_creation_floor() {
        const CREATED_VIEW: u32 = 2;
        let namespace = IggyNamespace::new(1, 1, 0);
        for (use_loader, old_view) in [
            (true, None),
            (false, None),
            (true, Some(CREATED_VIEW - 1)),
            (false, Some(CREATED_VIEW - 1)),
        ] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let directory = config.get_partition_path(1, 1, 0);
            let partitions_config = solo_partitions_config(&config);
            let fresh = build_partition_fresh(
                &config,
                &partitions_config,
                namespace,
                Arc::new(PartitionStats::default()),
                0,
                TopicRuntimeOptions::default(),
                CLUSTER,
                REPLICA,
                REPLICAS,
                CREATED_VIEW,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap();
            assert_eq!(fresh.consensus().view(), CREATED_VIEW);
            assert!(fresh.consensus().needs_superblock_persist());
            drop(fresh);

            std::fs::remove_dir_all(Path::new(&directory).join("prepares-0")).unwrap();

            if let Some(view) = old_view {
                let (store, _) = open_partition_superblock(&directory, test_identity(), 0)
                    .await
                    .unwrap();
                let state = VsrState {
                    commit_max: 0,
                    ..recorded_state(view, 0)
                };
                store.write(&state.to_bytes()).await.unwrap();
            }

            let recovered = if use_loader {
                load_partition(
                    &config,
                    &partitions_config,
                    namespace,
                    Arc::new(PartitionStats::default()),
                    &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, CREATED_VIEW),
                    TopicRuntimeOptions::default(),
                    CLUSTER,
                    REPLICA,
                    REPLICAS,
                    Rc::new(IggyMessageBus::new(0)),
                )
                .await
            } else {
                build_partition_fresh(
                    &config,
                    &partitions_config,
                    namespace,
                    Arc::new(PartitionStats::default()),
                    0,
                    TopicRuntimeOptions::default(),
                    CLUSTER,
                    REPLICA,
                    REPLICAS,
                    CREATED_VIEW,
                    Rc::new(IggyMessageBus::new(0)),
                )
                .await
            }
            .unwrap();
            assert_eq!(
                recovered.consensus().status(),
                consensus::Status::Recovering
            );
            assert_eq!(
                recovered.consensus().log_view(),
                0,
                "creation metadata must not certify the recovered log"
            );
            assert!(recovered.consensus().needs_superblock_persist());
            assert_eq!(recovered.consensus().view(), CREATED_VIEW);
            assert!(recovered.persist_superblock_if_needed().await);
            assert!(!recovered.consensus().needs_superblock_persist());
            drop(recovered);
            let (_, recorded) =
                open_partition_superblock(&directory, test_identity(), CREATED_VIEW)
                    .await
                    .unwrap();
            let recorded = recorded.unwrap();
            assert_eq!(recorded.view, CREATED_VIEW);
            assert_eq!(recorded.log_view, 0);
        }
    }

    #[compio::test]
    async fn given_durable_state_below_creation_view_when_loading_should_refuse_unchanged() {
        const CREATED_VIEW: u32 = 2;
        let namespace = IggyNamespace::new(1, 1, 0);
        for state in [recorded_state(1, 1), recorded_state(3, 1)] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let directory = config.get_partition_path(1, 1, 0);
            let (store, _) = open_partition_superblock(&directory, test_identity(), 0)
                .await
                .unwrap();
            store.write(&state.to_bytes()).await.unwrap();
            drop(store);
            let partitions = solo_partitions(&config);
            let loaded = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, CREATED_VIEW),
                TopicRuntimeOptions::default(),
                CLUSTER,
                REPLICA,
                REPLICAS,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap();
            assert!(loaded.is_none());
            assert!(partitions.is_tombstoned(&namespace));
            let (_, recovered) = open_partition_superblock(&directory, test_identity(), 0)
                .await
                .unwrap();
            assert_eq!(
                recovered,
                Some(state),
                "refusal must preserve contradictory durable state"
            );
            assert!(!Path::new(&format!("{directory}.fenced.0")).exists());
            assert!(matches!(
                build_partition_fresh(
                    &config,
                    partitions.config(),
                    namespace,
                    Arc::new(PartitionStats::default()),
                    0,
                    TopicRuntimeOptions::default(),
                    CLUSTER,
                    REPLICA,
                    REPLICAS,
                    CREATED_VIEW,
                    Rc::new(IggyMessageBus::new(0)),
                )
                .await,
                Err(ServerError::PartitionViewBelowCreation { .. }),
            ));
        }
    }

    #[compio::test]
    async fn given_certified_wal_below_creation_view_when_loading_should_refuse_unchanged() {
        const CREATED_VIEW: u32 = 2;
        const LOG_VIEW: u32 = 1;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        let directory = config.get_partition_path(1, 1, 0);
        let wal = Path::new(&directory).join("prepares-0");
        create_dir_all(&directory).await.unwrap();
        let mut journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
            .await
            .unwrap();
        journal.certify_log_view(LOG_VIEW, 0, 0).await.unwrap();
        drop(journal);
        let partitions = solo_partitions(&config);
        let metadata = Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, CREATED_VIEW);
        let runtime = TopicRuntimeOptions {
            durability: iggy_common::Durability::Persisted,
            preallocate_segments: Some(false),
            ..Default::default()
        };
        let result = load_partition(
            &config,
            partitions.config(),
            namespace,
            Arc::new(PartitionStats::default()),
            &metadata,
            runtime,
            CLUSTER,
            REPLICA,
            REPLICAS,
            Rc::new(IggyMessageBus::new(0)),
        )
        .await;
        assert!(matches!(
            result,
            Err(ServerError::PartitionWalViewBelowCreation {
                dir,
                log_view: LOG_VIEW,
                created_view: CREATED_VIEW,
            }) if dir == wal
        ));
        let loaded = load_partition_or_fence(
            &config,
            namespace,
            Arc::new(PartitionStats::default()),
            &metadata,
            runtime,
            CLUSTER,
            REPLICA,
            REPLICAS,
            Rc::new(IggyMessageBus::new(0)),
            &partitions,
        )
        .await
        .unwrap();
        assert!(loaded.is_none());
        assert!(partitions.is_tombstoned(&namespace));
        assert!(!Path::new(&format!("{directory}.fenced.0")).exists());
        let journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
            .await
            .unwrap();
        assert_eq!(journal.certified_log_view(), Some(LOG_VIEW));
    }

    #[compio::test]
    async fn given_directory_of_an_older_incarnation_when_loading_should_delete_it_and_probe() {
        const LEFTOVER_REVISION: u64 = 1;
        const CREATED_REVISION: u64 = 2;
        const CREATED_VIEW: u32 = 2;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let directory = config.get_partition_path(1, 1, 0);
        drop(build_incarnation(&config, LEFTOVER_REVISION, CREATED_VIEW).await);
        // The deleted incarnation had moved past its creation view, so its
        // superblock passes the creation floor and only the marker tells.
        let leftover_view = CREATED_VIEW + 3;
        let (store, _) = open_partition_superblock(&directory, test_identity(), 0)
            .await
            .unwrap();
        store
            .write(&recorded_state(leftover_view, leftover_view).to_bytes())
            .await
            .unwrap();
        drop(store);
        let partitions = solo_partitions(&config);

        let partition = load_incarnation(&config, &partitions, CREATED_REVISION, CREATED_VIEW)
            .await
            .expect("a dead incarnation's directory must not fence the new one");
        assert_eq!(
            partition.consensus().view(),
            CREATED_VIEW,
            "the dead incarnation's view must not be adopted"
        );
        assert_eq!(
            partition.consensus().status(),
            consensus::Status::Recovering,
            "a replica that found a leftover must not count toward quorums before a peer answers"
        );
        assert_eq!(partition.consensus().log_view(), 0);
        drop(partition);
        assert!(!partitions.is_tombstoned(&IggyNamespace::new(1, 1, 0)));
        assert_eq!(
            partitions::read_created_revision(&directory).await.unwrap(),
            Some(CREATED_REVISION)
        );
        let (_, recorded) = open_partition_superblock(&directory, test_identity(), 0)
            .await
            .unwrap();
        assert!(recorded.is_none_or(|state| state.view == CREATED_VIEW));
    }

    #[compio::test]
    async fn given_directory_of_a_newer_incarnation_when_loading_should_defer_until_metadata_catches_up()
     {
        const COMMITTED_REVISION: u64 = 2;
        const DIRECTORY_REVISION: u64 = 3;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let directory = config.get_partition_path(1, 1, 0);
        drop(build_incarnation(&config, DIRECTORY_REVISION, 0).await);
        let segment = solo_partitions_config(&config).get_messages_path(1, 1, 0, 0);
        let partitions = solo_partitions(&config);

        assert!(
            load_incarnation(&config, &partitions, COMMITTED_REVISION, 0)
                .await
                .is_none()
        );
        assert!(
            !partitions.is_tombstoned(&IggyNamespace::new(1, 1, 0)),
            "a deferral must leave the namespace for a later pass"
        );
        assert_eq!(
            partitions::read_created_revision(&directory).await.unwrap(),
            Some(DIRECTORY_REVISION)
        );
        assert!(Path::new(&segment).exists());

        assert!(
            load_incarnation(&config, &partitions, DIRECTORY_REVISION, 0)
                .await
                .is_some(),
            "once metadata names the directory's incarnation, it loads"
        );
    }

    #[compio::test]
    async fn given_retirement_fence_when_restarting_should_require_a_new_incarnation() {
        const REVISION: u64 = 7;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        drop(build_incarnation(&config, REVISION, 0).await);
        record_partition_retirement_fence(&config.get_system_path(), namespace, REVISION)
            .await
            .unwrap();
        for directory_lost in [false, true] {
            if directory_lost {
                std::fs::remove_dir_all(config.get_partition_path(1, 1, 0)).unwrap();
            }
            let partitions = solo_partitions(&config);
            assert!(
                load_incarnation(&config, &partitions, REVISION, 0)
                    .await
                    .is_none()
            );
            assert_eq!(partitions.failed_revision(&namespace), Some(REVISION));
            assert!(matches!(
                build_partition_fresh(
                    &config,
                    &solo_partitions_config(&config),
                    namespace,
                    Arc::new(PartitionStats::default()),
                    REVISION,
                    TopicRuntimeOptions::default(),
                    CLUSTER,
                    REPLICA,
                    REPLICAS,
                    0,
                    Rc::new(IggyMessageBus::new(0)),
                )
                .await,
                Err(ServerError::PartitionRetirementFenced { .. })
            ));
        }
        drop(build_incarnation(&config, REVISION + 1, 0).await);
        assert!(
            load_incarnation(&config, &solo_partitions(&config), REVISION + 1, 0)
                .await
                .is_some()
        );
    }

    #[compio::test]
    async fn given_corrupt_retirement_fence_when_restarting_should_refuse_partition() {
        const REVISION: u64 = 7;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        drop(build_incarnation(&config, REVISION, 0).await);
        let directory = partition_initialization_path(&config.get_system_path(), namespace);
        std::fs::write(Path::new(&directory).join(RETIREMENT_FENCE_FILE), b"torn").unwrap();
        let partitions = solo_partitions(&config);
        assert!(
            load_incarnation(&config, &partitions, REVISION, 0)
                .await
                .is_none()
        );
        assert!(
            record_partition_retirement_fence(&config.get_system_path(), namespace, REVISION)
                .await
                .is_err()
        );
    }

    #[compio::test]
    async fn given_no_recorded_revision_when_loading_should_record_the_committed_one() {
        const CREATED_REVISION: u64 = 4;
        for directory_exists in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let directory = config.get_partition_path(1, 1, 0);
            if directory_exists {
                // What an older server leaves: the same files, no marker.
                drop(build_incarnation(&config, CREATED_REVISION, 0).await);
                std::fs::remove_file(Path::new(&directory).join(partitions::CREATED_REVISION_FILE))
                    .unwrap();
            }
            let partitions = solo_partitions(&config);

            assert!(
                load_incarnation(&config, &partitions, CREATED_REVISION, 0)
                    .await
                    .is_some()
            );
            assert_eq!(
                partitions::read_created_revision(&directory).await.unwrap(),
                Some(CREATED_REVISION)
            );
        }
    }

    #[compio::test]
    async fn given_unreadable_recorded_revision_when_loading_should_tombstone_in_place() {
        const CREATED_REVISION: u64 = 1;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let directory = config.get_partition_path(1, 1, 0);
        drop(build_incarnation(&config, CREATED_REVISION, 0).await);
        let marker = Path::new(&directory).join(partitions::CREATED_REVISION_FILE);
        std::fs::write(&marker, [0u8; 3]).unwrap();
        let partitions = solo_partitions(&config);

        assert!(
            load_incarnation(&config, &partitions, CREATED_REVISION, 0)
                .await
                .is_none()
        );
        assert!(partitions.is_tombstoned(&IggyNamespace::new(1, 1, 0)));
        assert_eq!(std::fs::read(&marker).unwrap(), [0u8; 3]);
        assert!(Path::new(&solo_partitions_config(&config).get_messages_path(1, 1, 0, 0)).exists());
    }

    #[compio::test]
    async fn given_a_failed_hierarchy_sync_when_loading_should_leave_the_directory_unmarked() {
        const CREATED_REVISION: u64 = 1;
        const NOT_LISTABLE: u32 = 0o300;
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let directory = config.get_partition_path(1, 1, 0);
        let parent = Path::new(&directory)
            .parent()
            .expect("a partition directory has a parent");
        std::fs::create_dir_all(parent).unwrap();
        let mode = std::fs::metadata(parent).unwrap().permissions().mode();
        std::fs::set_permissions(parent, Permissions::from_mode(NOT_LISTABLE)).unwrap();
        // Root opens the directory anyway, so nothing would fail its sync.
        let blocked = std::fs::read_dir(parent).is_err();
        let partitions = solo_partitions(&config);

        let failed = try_load_incarnation(&config, &partitions, CREATED_REVISION, 0).await;
        let marker = partitions::read_created_revision(&directory).await.unwrap();
        std::fs::set_permissions(parent, Permissions::from_mode(mode)).unwrap();
        if !blocked {
            return;
        }

        assert!(failed.is_err(), "the parent directory cannot be synced");
        assert_eq!(
            marker, None,
            "a marker must prove that the hierarchy sync finished"
        );
        assert!(
            load_incarnation(&config, &partitions, CREATED_REVISION, 0)
                .await
                .is_some(),
            "the retry repeats the sync"
        );
        assert_eq!(
            partitions::read_created_revision(&directory).await.unwrap(),
            Some(CREATED_REVISION)
        );
    }

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_creation_floor_when_replaying_certified_wal_should_preserve_history_and_view() {
        const CREATED_VIEW: u32 = 2;
        const LOG_VIEW: u32 = 5;
        const PAYLOAD: &[u8] = b"certified-history";
        let namespace = IggyNamespace::new(1, 1, 0);
        let runtime = TopicRuntimeOptions {
            durability: iggy_common::Durability::Persisted,
            preallocate_segments: Some(false),
            ..Default::default()
        };
        for durable_view in [None, Some(LOG_VIEW + 1)] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            let partitions_config = solo_partitions_config(&config);
            let fresh = build_partition_fresh(
                &config,
                &partitions_config,
                namespace,
                Arc::new(PartitionStats::default()),
                0,
                runtime,
                CLUSTER,
                REPLICA,
                REPLICAS,
                CREATED_VIEW,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap();
            assert!(!fresh.requires_state_transfer());
            assert_eq!(fresh.consensus().log_view(), CREATED_VIEW);
            drop(fresh);
            let directory = config.get_partition_path(1, 1, 0);
            let mut messages = IggyMessages::with_capacity(1);
            messages.push(IggyMessage {
                header: IggyMessageHeader::default(),
                payload: Bytes::from_static(PAYLOAD),
                user_headers: None,
            });
            let batch = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
            let total = size_of::<PrepareHeader>() + batch.header.total_size();
            let mut prepare = Message::<PrepareHeader>::new(total);
            let body = &mut prepare.as_mut_slice()[size_of::<PrepareHeader>()..];
            batch.header.encode_into(body);
            body[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
            let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::SendMessages;
                header.cluster = CLUSTER;
                header.group = namespace.inner();
                header.view = LOG_VIEW;
                header.op = 1;
                header.commit = 1;
                header.size = u32::try_from(total).unwrap();
                header.checksum = header.identity_checksum();
            });
            let checksum = prepare.header().checksum;
            let wal = Path::new(&directory).join("prepares-0");
            let mut journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
                .await
                .unwrap();
            journal.append(prepare.into_frozen()).await.unwrap();
            journal
                .certify_log_view(LOG_VIEW, 1, checksum)
                .await
                .unwrap();
            drop(journal);
            if let Some(view) = durable_view {
                let (store, _) =
                    open_partition_superblock(&directory, test_identity(), CREATED_VIEW)
                        .await
                        .unwrap();
                let state = VsrState {
                    commit_max: 1,
                    ..recorded_state(view, LOG_VIEW)
                };
                store.write(&state.to_bytes()).await.unwrap();
            }
            let recovered = load_partition(
                &config,
                &partitions_config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, CREATED_VIEW),
                runtime,
                CLUSTER,
                REPLICA,
                REPLICAS,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap();
            assert_eq!(
                recovered.consensus().view(),
                durable_view.unwrap_or(LOG_VIEW)
            );
            assert_eq!(recovered.consensus().log_view(), LOG_VIEW);
            assert_eq!(recovered.consensus().sequencer().current_sequence(), 1);
            assert_eq!(recovered.consensus().commit_max(), 1);
            assert!(!recovered.requires_state_transfer());
            let stored = recovered.log.journal().inner.repair_entry(1).unwrap();
            assert!(
                stored
                    .as_slice()
                    .windows(PAYLOAD.len())
                    .any(|bytes| bytes == PAYLOAD)
            );
            drop(recovered);
            let journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
                .await
                .unwrap();
            assert_eq!(journal.certified_log_view(), Some(LOG_VIEW));
            assert_eq!(journal.head(), 1);
        }
    }

    #[compio::test]
    async fn missing_or_corrupt_retry_checkpoints_fence_only_the_partition() {
        let namespace = IggyNamespace::new(1, 1, 0);
        for checkpoint in [None, Some(b"corrupt receipt checkpoint".as_slice())] {
            let root = tempfile::tempdir().unwrap();
            let config = solo_config(&root);
            drop(build_solo_partition(&config).await.unwrap());
            let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
            let wal = directory.join("prepares-0");
            let mut journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
                .await
                .unwrap();
            let identity = SessionIdentity {
                client_id: 1,
                session: 1,
                metadata_watermark: 2,
            };
            let body = identity.to_bytes();
            let total = size_of::<PrepareHeader>() + body.len();
            let mut prepare = Message::<PrepareHeader>::new(total);
            prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
            let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::RetireSession;
                header.cluster = CLUSTER;
                header.group = namespace.inner();
                header.op = 1;
                header.retry_capacity = u32::try_from(config.partition.dedup_clients_max).unwrap();
                header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
                header.session = 1;
                header.request = identity.metadata_watermark;
                header.size = u32::try_from(total).unwrap();
                header.checksum = header.identity_checksum();
            });
            journal.append(prepare.into_frozen()).await.unwrap();
            journal.checkpoint(1).await.unwrap();
            drop(journal);
            if let Some(checkpoint) = checkpoint {
                std::fs::write(wal.join("receipts-1.checkpoint"), checkpoint).unwrap();
            }
            let partitions = solo_partitions(&config);
            let outcome = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, 0),
                TopicRuntimeOptions::default(),
                CLUSTER,
                0,
                1,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap();
            assert!(
                outcome.is_none(),
                "unusable retry protection must refuse this partition"
            );
            assert!(wal.exists(), "refusal must retain the WAL evidence");
            if let Some(checkpoint) = checkpoint {
                assert_eq!(
                    std::fs::read(wal.join("receipts-1.checkpoint")).unwrap(),
                    checkpoint
                );
            }
        }
    }

    #[compio::test]
    async fn given_solo_segments_without_wal_when_reloaded_should_refuse_and_preserve_bytes() {
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        drop(build_solo_partition(&config).await.unwrap());
        let directory = PathBuf::from(config.get_partition_path(1, 1, 0));
        let log_path = directory.join("00000000000000000000.log");
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader::default(),
            payload: Bytes::from_static(b"retained-without-retry-history"),
            user_headers: None,
        });
        let batch = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
        let mut bytes = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut bytes);
        bytes[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
        std::fs::write(&log_path, &bytes).unwrap();
        std::fs::remove_dir_all(directory.join("prepares-0")).unwrap();

        for _ in 0..2 {
            let partitions = solo_partitions(&config);
            let recovered = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, 0),
                TopicRuntimeOptions::default(),
                CLUSTER,
                0,
                1,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap();
            assert!(
                recovered.is_none(),
                "missing retry history must fence the partition"
            );
            assert_eq!(std::fs::read(&log_path).unwrap(), bytes);
            assert!(!directory.join("prepares-0").exists());
        }
    }

    #[compio::test]
    async fn loading_after_retention_preserves_the_wal_owned_tail_when_the_logical_chain_is_empty()
    {
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader::default(),
            payload: Bytes::from_static(b"retained-tail"),
            user_headers: None,
        });
        let mut batch = SendMessagesOwned::from_messages(namespace, &messages).unwrap();
        let segment_size = IggyByteSize::from(batch.header.total_size() as u64);
        let runtime = TopicRuntimeOptions {
            durability: iggy_common::Durability::Persisted,
            segment_size: Some(segment_size),
            preallocate_segments: Some(false),
            ..Default::default()
        };
        drop(
            build_partition_fresh(
                &config,
                &solo_partitions_config(&config),
                namespace,
                Arc::new(PartitionStats::default()),
                0,
                runtime,
                CLUSTER,
                REPLICA,
                REPLICAS,
                0,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap(),
        );
        let wal = Path::new(&config.get_partition_path(1, 1, 0)).join("prepares-0");
        let mut journal = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
            .await
            .unwrap();
        let mut parent = 0;
        let mut tail_body = Vec::new();
        for offset in 0..2 {
            batch.header.base_offset = offset;
            batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
            let total = size_of::<PrepareHeader>() + batch.header.total_size();
            let mut prepare = Message::<PrepareHeader>::new(total);
            let body = &mut prepare.as_mut_slice()[size_of::<PrepareHeader>()..];
            batch.header.encode_into(body);
            body[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
            tail_body = body.to_vec();
            let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::SendMessages;
                header.cluster = CLUSTER;
                header.group = namespace.inner();
                header.op = offset + 1;
                header.retry_capacity = u32::try_from(config.partition.dedup_clients_max).unwrap();
                header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
                header.session = 1;
                header.request = offset + 1;
                header.parent = parent;
                header.size = u32::try_from(total).unwrap();
                header.checksum = header.identity_checksum();
            });
            parent = prepare.header().checksum;
            journal.append(prepare.into_frozen()).await.unwrap();
        }
        let mut protection = OFFSETS_MAGIC.to_vec();
        protection.push(OFFSETS_VERSION);
        for value in [0_u64, 1, 0] {
            protection.extend_from_slice(&value.to_le_bytes());
        }
        protection.extend_from_slice(
            &u32::try_from(config.partition.dedup_clients_max)
                .unwrap()
                .to_le_bytes(),
        );
        protection.extend_from_slice(&[0; 3 * size_of::<u32>()]);
        protection.push(0);
        protection.extend_from_slice(&0_u128.to_le_bytes());
        protection.extend_from_slice(&0_u32.to_le_bytes());
        protection
            .extend_from_slice(&consensus::state_artifact_checksum(&protection).to_le_bytes());
        let mut checkpoint = RETRY_CHECKPOINT_MAGIC.to_vec();
        checkpoint.extend_from_slice(&namespace.inner().to_le_bytes());
        checkpoint.extend_from_slice(&0_u64.to_le_bytes());
        checkpoint.extend_from_slice(&1_u64.to_le_bytes());
        checkpoint.extend_from_slice(&protection);
        checkpoint
            .extend_from_slice(&consensus::state_artifact_checksum(&checkpoint).to_le_bytes());
        std::fs::write(wal.join("receipts-1.checkpoint"), checkpoint).unwrap();
        journal.checkpoint(1).await.unwrap();
        drop(journal);
        let partitions_config = solo_partitions_config(&config);
        std::fs::remove_file(partitions_config.get_messages_path(1, 1, 0, 0)).unwrap();
        std::fs::remove_file(partitions_config.get_index_path(1, 1, 0, 0)).unwrap();
        let partitions = solo_partitions(&config);
        let partition = load_partition(
            &config,
            partitions.config(),
            namespace,
            Arc::new(PartitionStats::default()),
            &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, 0),
            runtime,
            CLUSTER,
            REPLICA,
            REPLICAS,
            Rc::new(IggyMessageBus::new(0)),
        )
        .await
        .unwrap();
        assert_eq!(partition.log.active_segment().start_offset, 1);
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
        assert!(partition.log.messages_writers().last().unwrap().is_none());
        assert_eq!(
            std::fs::read(partitions_config.get_messages_path(1, 1, 0, 1)).unwrap(),
            tail_body
        );
        drop(partition);
        let recovered = journal::PartitionPrepareJournal::open(&wal, namespace.inner(), 0)
            .await
            .unwrap();
        assert_eq!(recovered.head(), 2);
        assert_eq!(
            recovered.prepares().await.unwrap()[1].as_slice()[size_of::<PrepareHeader>()..],
            tail_body
        );
    }

    #[compio::test]
    async fn corrupt_prepare_wal_is_quarantined_without_losing_the_recovery_fence() {
        let root = tempfile::tempdir().unwrap();
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        let runtime = TopicRuntimeOptions {
            durability: iggy_common::Durability::Persisted,
            preallocate_segments: Some(false),
            ..Default::default()
        };
        drop(
            build_partition_fresh(
                &config,
                &solo_partitions_config(&config),
                namespace,
                Arc::new(PartitionStats::default()),
                0,
                runtime,
                CLUSTER,
                REPLICA,
                REPLICAS,
                0,
                Rc::new(IggyMessageBus::new(0)),
            )
            .await
            .unwrap(),
        );
        let directory = config.get_partition_path(1, 1, 0);
        let (store, _) = open_partition_superblock(&directory, test_identity(), 0)
            .await
            .unwrap();
        let state = recorded_state(3, 2);
        store.write(&state.to_bytes()).await.unwrap();
        drop(store);
        let frontier = Path::new(&directory).join("prepares-0/frontier");
        let mut corrupt = std::fs::read(&frontier).unwrap();
        // Every slot: the frontier alternates between two of them, and one
        // damaged copy is recoverable by design, so damaging a single slot
        // would open the partition instead of fencing it.
        for slot in corrupt.chunks_mut(journal::partition_journal::PARTITION_WAL_BLOCK_SIZE) {
            slot[0] ^= u8::MAX;
        }
        std::fs::write(&frontier, &corrupt).unwrap();
        let partitions = solo_partitions(&config);
        let metadata = Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, 0);

        for _ in 0..2 {
            let partition = load_partition_or_fence(
                &config,
                namespace,
                Arc::new(PartitionStats::default()),
                &metadata,
                runtime,
                CLUSTER,
                REPLICA,
                REPLICAS,
                Rc::new(IggyMessageBus::new(0)),
                &partitions,
            )
            .await
            .unwrap()
            .unwrap();
            assert!(partition.requires_state_transfer());
            assert!(partition.consensus().view() >= state.view);
            let (_, recovered) = open_partition_superblock(&directory, test_identity(), 0)
                .await
                .unwrap();
            assert_eq!(recovered, Some(state));
        }
        assert_eq!(
            std::fs::read(format!("{directory}.fenced.0/prepares-0/frontier")).unwrap(),
            corrupt,
        );
    }

    fn recorded_state(view: u32, log_view: u32) -> VsrState {
        VsrState {
            cluster: CLUSTER,
            replica_id: REPLICA,
            replica_count: REPLICAS,
            view,
            log_view,
            commit_max: 42,
            checkpoint_op: 0,
            checkpoint_checksum: 0,
            offset_frontier: 0,
            offset_reserved: 0,
        }
    }

    /// The solo shape the reservation is scoped to, with a claim already
    /// recorded and nothing flushed behind it.
    fn reserved_solo_state(reserved: u64) -> VsrState {
        VsrState {
            replica_id: 0,
            replica_count: 1,
            commit_max: 0,
            offset_reserved: reserved,
            ..recorded_state(0, 0)
        }
    }

    fn partition_dir(root: &tempfile::TempDir) -> String {
        root.path().join("partition").to_string_lossy().into_owned()
    }

    const fn test_identity() -> ReplicaIdentity {
        ReplicaIdentity {
            cluster: CLUSTER,
            replica_id: REPLICA,
            replica_count: REPLICAS,
        }
    }

    /// The offset reservation is solo-only, so every test that touches it builds
    /// under this identity.
    const fn solo_identity() -> ReplicaIdentity {
        ReplicaIdentity {
            cluster: CLUSTER,
            replica_id: 0,
            replica_count: 1,
        }
    }

    fn solo_config(root: &tempfile::TempDir) -> ServerConfig {
        ServerConfig {
            path: root.path().to_string_lossy().into_owned(),
            ..ServerConfig::default()
        }
    }

    async fn build_solo_partition(
        config: &ServerConfig,
    ) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerError> {
        build_partition_fresh(
            config,
            &solo_partitions_config(config),
            IggyNamespace::new(1, 1, 0),
            Arc::new(PartitionStats::default()),
            0,
            TopicRuntimeOptions::default(),
            CLUSTER,
            0,
            1,
            0,
            Rc::new(IggyMessageBus::new(0)),
        )
        .await
    }

    /// Rooted where `solo_config` puts the server's own paths, so the builder's
    /// segment and offset directories land beside the superblock.
    fn solo_partitions_config(config: &ServerConfig) -> PartitionsConfig {
        PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: IggyByteSize::from(1024_u64),
            validate_checksum: true,
            segment_size: IggyByteSize::from(1_048_576_u64),
            preallocate_segments: false,
            encryptor: None,
            path_layout: PartitionPathLayout {
                streams_root: config.get_streams_path(),
            },
        }
    }

    /// The container the loader tombstones into.
    fn solo_partitions(config: &ServerConfig) -> IggyPartitions<Rc<IggyMessageBus>> {
        IggyPartitions::new(ShardId::new(0), solo_partitions_config(config))
    }

    /// Builds partition 1/1/0 of a three-replica group fresh for the
    /// incarnation created at `created_revision`.
    async fn build_incarnation(
        config: &ServerConfig,
        created_revision: u64,
        created_view: u32,
    ) -> IggyPartition<Rc<IggyMessageBus>> {
        build_partition_fresh(
            config,
            &solo_partitions_config(config),
            IggyNamespace::new(1, 1, 0),
            Arc::new(PartitionStats::default()),
            created_revision,
            TopicRuntimeOptions::default(),
            CLUSTER,
            REPLICA,
            REPLICAS,
            created_view,
            Rc::new(IggyMessageBus::new(0)),
        )
        .await
        .unwrap()
    }

    /// Loads partition 1/1/0 the way boot does, with committed metadata naming
    /// the incarnation created at `created_revision`.
    async fn load_incarnation(
        config: &ServerConfig,
        partitions: &IggyPartitions<Rc<IggyMessageBus>>,
        created_revision: u64,
        created_view: u32,
    ) -> Option<IggyPartition<Rc<IggyMessageBus>>> {
        try_load_incarnation(config, partitions, created_revision, created_view)
            .await
            .unwrap()
    }

    async fn try_load_incarnation(
        config: &ServerConfig,
        partitions: &IggyPartitions<Rc<IggyMessageBus>>,
        created_revision: u64,
        created_view: u32,
    ) -> Result<Option<IggyPartition<Rc<IggyMessageBus>>>, ServerError> {
        let namespace = IggyNamespace::new(1, 1, 0);
        load_partition_or_fence(
            config,
            namespace,
            Arc::new(PartitionStats::default()),
            &Partition::new(
                0,
                namespace.inner(),
                IggyTimestamp::now(),
                created_revision,
                created_view,
            ),
            TopicRuntimeOptions {
                durability: iggy_common::Durability::Persisted,
                consumer_offset_durability: iggy_common::Durability::Persisted,
                ..TopicRuntimeOptions::default()
            },
            CLUSTER,
            REPLICA,
            REPLICAS,
            Rc::new(IggyMessageBus::new(0)),
            partitions,
        )
        .await
    }

    /// The reservation the partition left on disk, which is the only copy a
    /// restart or a first send can read.
    async fn recorded_reservation(dir: &str) -> u64 {
        let (_store, recorded) = open_partition_superblock(dir, solo_identity(), 0)
            .await
            .expect("reopen the partition superblock");
        recorded
            .expect("a partition that recorded a reservation")
            .offset_reserved
    }

    /// The create claims the first lease block, so the DURABLE record covers the
    /// first send before it arrives. Asserting on the returned partition alone
    /// would pass with no claim at all: the inline fence at the mint writes the
    /// same block on the first send, which is exactly what this moves off the
    /// append path.
    #[compio::test]
    async fn given_a_fresh_solo_partition_when_building_should_record_its_first_claim() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = solo_config(&root);
        let dir = config.get_partition_path(1, 1, 0);

        let partition = build_solo_partition(&config)
            .await
            .expect("build a fresh partition");
        drop(partition);

        assert_eq!(
            recorded_reservation(&dir).await,
            1 + u64::from(config.partition.offset_reservation_lease.get()),
            "the create must leave a full lease block covering offset 0 on disk"
        );
    }

    /// A rebuild that reads a reservation back must NAME its planted segment for
    /// the append point. Named 0, the segment takes the first append's
    /// `base_offset` of N instead, `rposition(|s| s.start_offset <= offset)`
    /// routes every poll for `0..N-1` into it, and the next boot makes that
    /// durable.
    #[compio::test]
    async fn given_a_recorded_reservation_when_building_fresh_should_plant_at_the_append_point() {
        const RESERVED: u64 = 65_537;
        let root = tempfile::tempdir().expect("tempdir");
        let config = solo_config(&root);
        let dir = config.get_partition_path(1, 1, 0);

        let (store, recovered) = open_partition_superblock(&dir, solo_identity(), 0)
            .await
            .expect("open a fresh partition superblock");
        assert!(recovered.is_none());
        store
            .write(&reserved_solo_state(RESERVED).to_bytes())
            .await
            .expect("record the reservation");
        drop(store);

        let partition = build_solo_partition(&config)
            .await
            .expect("rebuild the partition over its recorded reservation");

        assert_eq!(
            partition.mint_frontier(),
            RESERVED,
            "the append point must resume above every offset the reservation covered"
        );
        assert_eq!(
            partition.offset_frontier(),
            0,
            "nothing was flushed, so the committed frontier names no data"
        );

        let planted: Vec<String> = std::fs::read_dir(&dir)
            .expect("list the partition dir")
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                (path.extension()? == "log")
                    .then(|| path.file_name()?.to_str().map(str::to_owned))
                    .flatten()
            })
            .collect();
        assert_eq!(
            planted,
            vec![format!("{RESERVED:0>20}.log")],
            "the initial segment must be named for the append point, not offset 0"
        );

        drop(partition);
        assert_eq!(
            recorded_reservation(&dir).await,
            RESERVED,
            "a rebuild resumes ON its recorded reservation, so re-claiming here would \
             burn a lease block and two fsyncs per rebuild"
        );
    }

    /// The loader's fence-and-rebuild arm is the claim's SECOND caller. A
    /// refused claim is a failed superblock write, not damage, so it has to come
    /// back as an error every caller can retry: absorbing it into a tombstone
    /// here would darken the namespace for the life of the process over a fault
    /// the next attempt may not even hit.
    #[compio::test]
    async fn given_a_failing_claim_when_rebuilding_a_fenced_chain_should_refuse_not_tombstone() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        let dir = config.get_partition_path(1, 1, 0);
        std::fs::create_dir_all(&dir).expect("partition dir");
        let partitions_config = solo_partitions_config(&config);
        // Two empty segments make the first a NON-tail empty, the refusal a solo
        // group rebuilds through (zero recoverable bytes) instead of tombstoning
        // where it stands.
        for start_offset in [0, 1] {
            std::fs::File::create(partitions_config.get_messages_path(1, 1, 0, start_offset))
                .expect("empty segment log");
        }
        // The rebuild's claim is this group's first superblock write, so it
        // targets slot A. A directory where its temp file goes fails the atomic
        // replace and nothing else: the slot reads still find the store empty,
        // and the quarantine moves segment files only.
        std::fs::create_dir(Path::new(&dir).join("superblock.a.tmp")).expect("block slot A");
        drop(
            journal::PartitionPrepareJournal::open(
                &Path::new(&dir).join("prepares-0"),
                namespace.inner(),
                0,
            )
            .await
            .unwrap(),
        );

        let stats = Arc::new(PartitionStats::default());
        let partitions = solo_partitions(&config);
        let loaded = load_partition_or_fence(
            &config,
            namespace,
            Arc::clone(&stats),
            &Partition::new(0, namespace.inner(), IggyTimestamp::now(), 0, 0),
            TopicRuntimeOptions::default(),
            CLUSTER,
            0,
            1,
            Rc::new(IggyMessageBus::new(0)),
            &partitions,
        )
        .await;

        match loaded {
            Err(ServerError::PartitionOffsetReservationClaim { .. }) => {}
            Err(other) => panic!("expected the claim's own refusal, got {other}"),
            Ok(Some(_)) => panic!("the planted directory must fail the rebuild's claim"),
            Ok(None) => panic!("a refused claim must reach the caller, not be absorbed here"),
        }
        assert!(
            std::fs::metadata(format!("{dir}.fenced.0")).is_ok(),
            "the quarantine must have run, or this asserts on the wrong arm"
        );
        assert!(
            !partitions.is_tombstoned(&namespace),
            "a transient write failure must leave the namespace materialisable"
        );
        assert_eq!(
            stats.segments_count_inconsistent(),
            0,
            "the rebuild counted its initial segment before the claim refused, and the \
             retry counts the same file again"
        );
    }

    #[compio::test]
    async fn given_fresh_partition_dir_when_superblock_opened_should_yield_no_state() {
        let root = tempfile::tempdir().expect("tempdir");
        // A not-yet-materialized directory must open as fresh, not error: the
        // helper creates it, since a follower can reach load before its first
        // segment write.
        let (_store, recovered) =
            open_partition_superblock(&partition_dir(&root), test_identity(), 0)
                .await
                .expect("open a fresh partition superblock");
        assert!(
            recovered.is_none(),
            "an empty superblock is a fresh group, never an error"
        );
    }

    #[compio::test]
    async fn given_recorded_view_when_superblock_reopened_should_recover_state() {
        for (view, log_view, created_view) in [(3, 2, 2), (2, 0, 2), (0, 0, 0)] {
            let root = tempfile::tempdir().expect("tempdir");
            let dir = partition_dir(&root);
            let (store, recovered) = open_partition_superblock(&dir, test_identity(), created_view)
                .await
                .expect("first open");
            assert!(recovered.is_none());
            let state = recorded_state(view, log_view);
            store
                .write(&state.to_bytes())
                .await
                .expect("record the advanced view");
            drop(store);

            let (_store, recovered) =
                open_partition_superblock(&dir, test_identity(), created_view)
                    .await
                    .expect("reopen after a restart");
            assert_eq!(
                recovered,
                Some(state),
                "a restarted partition must recover exactly its recorded state"
            );
        }
    }

    #[compio::test]
    async fn given_foreign_cluster_record_when_superblock_opened_should_refuse_boot() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = partition_dir(&root);
        let (store, _) = open_partition_superblock(&dir, test_identity(), 0)
            .await
            .expect("first open");
        let foreign = VsrState {
            cluster: CLUSTER + 1,
            ..recorded_state(1, 1)
        };
        store
            .write(&foreign.to_bytes())
            .await
            .expect("record a foreign identity");
        drop(store);

        let refused = open_partition_superblock(&dir, test_identity(), 0).await;

        match refused {
            Err(ServerError::PartitionSuperblockIdentityMismatch { field, .. }) => {
                assert_eq!(field, IdentityField::Cluster);
            }
            Err(other) => panic!("expected an identity mismatch, got {other}"),
            Ok(_) => panic!("a copied or misplaced partition directory must refuse boot"),
        }
    }
}
