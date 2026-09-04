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
//! on-disk state and rules on a segment chain recovery refused;
//! [`build_partition_fresh`] materialises one for a namespace that has no
//! directory yet. Both sit on the same namespace-bounds validation,
//! consumer-offset configuration, and initial-segment provisioning.
//!
//! Boot runs the loader over every owned namespace. The reconciler picks
//! whichever builder the partition directory calls for when a committed
//! `CreateTopic` / `CreatePartitions` event has no matching local
//! partition yet.

use crate::offset_recovery::{load_consumer_group_offsets, load_consumer_offsets};
use crate::segment_recovery::{RecoveredSegment, load_persisted_segments};
use crate::server_error::{PartitionRecoveryRefusal, ServerError};
use crate::shell::consensus_timers;
use compio::fs::create_dir_all;
use configs::server::ServerConfig;
use consensus::{
    FreshGroupStart, JoinMode, LocalPipeline, VsrConsensus, VsrRestore, VsrState, fresh_group_start,
};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, IggyError, IggyTimestamp, PartitionStats,
    TopicRuntimeOptions,
};
use journal::superblock::{PingPongSuperblock, SuperblockContents};
use message_bus::IggyMessageBus;
use metadata::stm::stream::Partition;
use metadata::{IdentityField, ReplicaIdentity};
use partitions::{
    IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionsConfig, Segment,
};
use server_common::SegmentStorage;
use server_common::fs_utils::remove_dir_all;
use server_common::sharding::IggyNamespace;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{error, warn};

/// Create the on-disk directory hierarchy for a partition.
///
/// Builds the partition root, offsets, consumer offsets, and consumer
/// group offsets directories. Idempotent: every step short-circuits when
/// the directory already exists, so a reconciler retry after a partial
/// failure is safe.
///
/// # Errors
///
/// Returns [`IggyError::CannotCreatePartitionDirectory`] or
/// [`IggyError::CannotCreatePartition`] on directory creation failure.
pub async fn create_partition_file_hierarchy(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &ServerConfig,
) -> Result<(), IggyError> {
    let partition_path = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    if !Path::new(&partition_path).exists() && create_dir_all(&partition_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionDirectory(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let offset_path = config
        .system
        .get_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&offset_path).exists() && create_dir_all(&offset_path).await.is_err() {
        error!(
            stream_id,
            topic_id, partition_id, "Failed to create offsets directory for partition"
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let consumer_offset_path =
        config
            .system
            .get_consumer_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_offset_path).exists()
        && create_dir_all(&consumer_offset_path).await.is_err()
    {
        error!(
            stream_id,
            topic_id, partition_id, "Failed to create consumer offsets directory for partition"
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let consumer_group_offsets_path =
        config
            .system
            .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_group_offsets_path).exists()
        && create_dir_all(&consumer_group_offsets_path).await.is_err()
    {
        error!(
            stream_id,
            topic_id,
            partition_id,
            "Failed to create consumer group offsets directory for partition"
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    Ok(())
}

/// Populate `partition` with consumer-offset / consumer-group-offset storage.
///
/// Hydrates from on-disk state if files exist (recovery path) or
/// configures empty maps (fresh partition path). Recovered offsets are bounded
/// so a partition that lost its tail does not surface consumer offsets ahead of
/// an offset it never handed out, and `current_offset` is where a bounded one
/// lands.
///
/// # Errors
///
/// Returns [`ServerError::ConsumerOffsetsLoad`] when the on-disk files
/// exist but fail to decode. A stored offset past the offset space is clamped
/// to `current_offset` (with a warning), not an error.
pub fn configure_consumer_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), ServerError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let consumer_offsets_path =
        config
            .system
            .get_consumer_offsets_path(stream_id, topic_id, partition_id);
    let consumer_group_offsets_path =
        config
            .system
            .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    // The bound is the offset space this replica could have MINTED, not the data
    // it can still serve. A boot re-anchor leaves the append point a lease block
    // above the recovered chain, so on the restart after a crash that took
    // acked-but-unflushed messages, a position stored before that crash names a
    // real offset sitting under an empty chain -- confirmed to a client, and not
    // "past the log" the way a torn offset file is. Bounding it by the data head
    // instead walks a committed consumer position BACKWARD across the restart,
    // which is the silent re-read the reservation exists to prevent.
    // `mint_frontier` is one past the next mint, and reads 0 on the fresh-build
    // path, where the max leaves `current_offset` in charge as before.
    let offset_space_ceiling = current_offset.max(partition.mint_frontier().saturating_sub(1));

    let loaded_consumer_offsets = load_partition_consumer_offsets(
        &consumer_offsets_path,
        "consumer",
        stream_id,
        topic_id,
        partition_id,
    )?;
    let consumer_offsets = ConsumerOffsets::with_capacity(loaded_consumer_offsets.len());
    {
        let guard = consumer_offsets.pin();
        for offset in loaded_consumer_offsets {
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > offset_space_ceiling {
                // A crash can persist an offset ahead of the flushed data
                // (offsets are stored eagerly, messages flush later). Clamp to
                // the recovered head so the consumer resumes instead of being
                // stuck polling past the log; mirrors the legacy contract.
                warn!(
                    consumer_id = offset.consumer_id,
                    recovered_offset,
                    current_offset,
                    offset_space_ceiling,
                    stream_id,
                    topic_id,
                    partition_id,
                    "recovered consumer offset ahead of partition data; clamping"
                );
                offset.offset.store(current_offset, Ordering::Relaxed);
            }
            guard.insert(offset.consumer_id as usize, offset);
        }
    }

    let loaded_group_offsets = load_partition_consumer_group_offsets(
        &consumer_group_offsets_path,
        stream_id,
        topic_id,
        partition_id,
    )?;
    let consumer_group_offsets = ConsumerGroupOffsets::with_capacity(loaded_group_offsets.len());
    {
        let guard = consumer_group_offsets.pin();
        for (group_id, offset) in loaded_group_offsets {
            let recovered_offset = offset.offset.load(Ordering::Relaxed);
            if recovered_offset > offset_space_ceiling {
                warn!(
                    consumer_group_id = group_id.0,
                    recovered_offset,
                    current_offset,
                    offset_space_ceiling,
                    stream_id,
                    topic_id,
                    partition_id,
                    "recovered consumer group offset ahead of partition data; clamping"
                );
                offset.offset.store(current_offset, Ordering::Relaxed);
            }
            guard.insert(group_id, offset);
        }
    }

    // Offset files follow the topic's own `enforce_fsync`: they are part of the
    // same partition's durability story, and the global knob they used to read
    // is gone.
    let enforce_fsync = partition
        .runtime_options()
        .enforce_fsync
        .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC);
    partition.configure_consumer_offset_storage(
        consumer_offsets_path,
        consumer_group_offsets_path,
        consumer_offsets,
        consumer_group_offsets,
        enforce_fsync,
    );
    Ok(())
}

fn load_partition_consumer_offsets(
    path: &str,
    consumer_kind: &'static str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Vec<iggy_common::ConsumerOffset>, ServerError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerError::ConsumerOffsetsLoad {
            consumer_kind,
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        })
    })
}

fn load_partition_consumer_group_offsets(
    path: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Vec<(iggy_common::ConsumerGroupId, iggy_common::ConsumerOffset)>, ServerError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_group_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerError::ConsumerOffsetsLoad {
            consumer_kind: "consumer group",
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        })
    })
}

/// Provision an initial segment + writers for a partition that has none.
///
/// No-op when `partition.log.has_segments()` already returns `true`
/// (recovery hydrated existing segments), so callers can invoke this
/// unconditionally.
///
/// # Errors
///
/// Returns [`ServerError`] on segment-storage creation failure or
/// writer initialisation failure.
pub async fn ensure_initial_segment(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerError> {
    if partition.log.has_segments() {
        return Ok(());
    }

    // At the RESTORED FRONTIER, not always 0: after a crash inside the install's
    // swap window the chain is empty while the recorded frontier is N, and a
    // segment named 0 would then take the first append's `base_offset = N` --
    // `rposition(|s| s.start_offset <= offset)` routes every poll for `0..N-1`
    // into it, the next boot makes that shape durable, and this replica starts
    // offering peers a segment that claims `[0..N]`.
    let start_offset = partition.mint_frontier();
    let messages_path =
        config
            .system
            .get_messages_file_path(stream_id, topic_id, partition_id, start_offset);
    let index_path = config
        .system
        .get_index_path(stream_id, topic_id, partition_id, start_offset);
    let runtime = partition.runtime_options();
    let segment_size = runtime
        .segment_size
        .unwrap_or_else(|| IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE));
    let enforce_fsync = runtime
        .enforce_fsync
        .unwrap_or(iggy_common::DEFAULT_ENFORCE_FSYNC);
    let preallocate_segments = runtime
        .preallocate_segments
        .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS);
    // `file_exists = false` TRUNCATES both files, which is load-bearing here: a
    // fenced-and-rebuilt partition (or one whose quarantine failed) can reach
    // this with a stale `.index` at offset 0 on disk. The `partitions`-side
    // writers with the same names do NOT truncate, so opening them directly
    // instead would read index entries from a previous generation.
    let storage = SegmentStorage::new(&messages_path, &index_path, 0, 0, false)
        .await
        .map_err(|source| {
            error!(
                stream_id,
                topic_id,
                partition_id,
                error = %source,
                "failed to create initial segment storage"
            );
            source
        })?;
    // Share the storage's size counters: they are the write cursors. A private
    // counter would let the append position diverge from the segment
    // bookkeeping that index entries and poll bounds rely on.
    let messages_size_counter = storage
        .messages_writer
        .as_ref()
        .map(|writer| writer.size_counter())
        .unwrap_or_default();
    let index_size_counter = storage
        .index_writer
        .as_ref()
        .map(|writer| writer.size_counter())
        .unwrap_or_default();
    partition.log.add_persisted_segment(
        Segment::new(start_offset, segment_size),
        storage,
        Some(Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_counter,
                enforce_fsync,
                false,
                preallocate_segments.then_some(segment_size),
            )
            .await
            .map_err(|source| {
                error!(
                    stream_id,
                    topic_id,
                    partition_id,
                    path = %messages_path,
                    error = %source,
                    "failed to initialize initial messages writer"
                );
                source
            })?,
        )),
        Some(Rc::new(
            IggyIndexWriter::new(&index_path, index_size_counter, enforce_fsync, false)
                .await
                .map_err(|source| {
                    error!(
                        stream_id,
                        topic_id,
                        partition_id,
                        path = %index_path,
                        error = %source,
                        "failed to initialize initial sparse index writer"
                    );
                    source
                })?,
        )),
    );
    partition.stats.increment_segments_count(1);

    Ok(())
}

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
/// `IdentityMismatch` variants when a record exists but cannot be trusted.
pub async fn open_partition_superblock(
    partition_dir: &str,
    identity: ReplicaIdentity,
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
    }
    Ok((Rc::new(superblock), recovered_state))
}

/// Recover an owned partition from its on-disk state.
///
/// Shared by boot and the reconciler so a partition this replica committed
/// before a crash but re-learns only after restart (its WAL watermark trails
/// the commit by one op) is hydrated from its segments like any other, not
/// rebuilt over them. `Ok(None)` means the namespace was tombstoned here; the
/// arms below say when. Errors are transient I/O, left to the caller.
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
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
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
        Err(ServerError::PartitionRecoveryRefused { dir, reason, .. }) => {
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
                partitions.tombstone(namespace);
                return Ok(None);
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
                    return Ok(None);
                }
            }
            match Box::pin(build_partition_fresh(
                config,
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
            {
                Ok(partition) => Ok(Some(partition)),
                // Boot propagates whatever this returns, so an `Err` here costs a
                // whole shard its start over ONE partition's failed write.
                // Tombstoning is also the only safe answer: the failed build
                // already quarantined the chain and planted an empty segment 0,
                // which the next load would accept and serve as a healthy empty
                // partition. The stats lose the segment that build counted before
                // the claim refused.
                Err(error @ ServerError::PartitionOffsetReservationClaim { .. }) => {
                    error!(
                        stream_id,
                        topic_id,
                        partition_id = partition_metadata.id,
                        partition_dir,
                        %error,
                        "failed to claim the rebuilt partition's first offset reservation; \
                         leaving it tombstoned rather than serving it unreserved"
                    );
                    partition_stats.zero_out_all();
                    partitions.tombstone(namespace);
                    Ok(None)
                }
                Err(error) => Err(error),
            }
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
                 partition instead of serving it"
            );
            partition_stats.zero_out_all();
            partitions.tombstone(namespace);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
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
    partition.set_dedup_clients_max(config.partition.dedup_clients_max);
    partition.set_offset_reservation_lease(config.partition.offset_reservation_lease);
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

    partition.created_at = partition_metadata.created_at;
    restore_partition_offsets(&mut partition, partitions_config, recovered_state.as_ref()).await?;
    let current_offset = partition.offset.load(Ordering::Acquire);

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

    Ok(partition)
}

/// Restore the offset counter of a recovered partition from what boot could
/// prove about its offset space, then put the next append point where the
/// recovery walk can read it back.
///
/// Three carriers, weakest last: the sized segments' end offset, an empty
/// chain's file name (a state-transfer install at the group frontier), and the
/// superblock's durable frontier as a lower bound over both.
async fn restore_partition_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    partitions_config: &PartitionsConfig,
    recovered_state: Option<&VsrState>,
) -> Result<(), ServerError> {
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
    let current_offset = sized_end.or_else(|| empty_frontier.map(|start| start - 1));
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
    partition.set_offset_space_used(current_offset.is_some());
    // The durable frontier is a LOWER BOUND on top of what the segments proved:
    // it is the only carrier left when the segments that named the frontier are
    // gone (an all-GC'd origin's install, a crash inside the swap window), and
    // taking the max means real recovered data always wins.
    partition.restore_offset_frontier(recovered_state);
    // Minting from the reservation leaves a hole between the recovered chain
    // and the new append point, and the recovery walk REFUSES a hole inside a
    // segment (tombstoning the partition on the solo arm), so put it on a
    // segment boundary instead.
    //
    // Solo only, in step with the reservation itself: a replicated group's
    // segment boundaries must be a function of the batches alone or the
    // reconciler's offset-keyed segment GC never converges.
    if partition.consensus().replica_count() == 1 {
        partition
            .reanchor_to_offset_frontier(partitions_config)
            .await
            .map_err(|error| ServerError::Iggy(Box::new(error)))?;
    }
    Ok(())
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
    load_persisted_segments(config, namespace, segment_size, enforce_fsync, stats)
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
/// restart materialization ignores it and probes for the live view instead.
///
/// The returned partition's `offset` / `dirty_offset` are `0` and its
/// `OffsetSpace` is unused, mirroring a clean append starting at the empty
/// segment.
///
/// # Errors
///
/// Returns [`ServerError`] when directory creation, superblock recovery,
/// segment provisioning, or the first offset-reservation claim fails.
#[allow(clippy::too_many_arguments)]
pub async fn build_partition_fresh(
    config: &ServerConfig,
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
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();

    // Sampled BEFORE the hierarchy create: a pre-existing partition directory
    // is the marker of a prior life (the .log inside may legitimately be
    // empty -- committed-but-unflushed data dies with the journal), while a
    // genuinely fresh create finds nothing.
    let restarted = replica_count > 1
        && std::fs::metadata(
            config
                .system
                .get_partition_path(stream_id, topic_id, partition_id),
        )
        .is_ok();
    create_partition_file_hierarchy(stream_id, topic_id, partition_id, config)
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

    // The hierarchy create above guarantees the directory exists; recover this
    // group's durable (view, log_view) before choosing how to join, so a
    // restart materialization resumes from the view it last recorded instead
    // of re-entering an older one.
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

    // A partition directory that already exists here is a rebuild over a
    // fenced chain (`load_partition_or_fence` quarantined the refused segment
    // files and kept the superblock; every other prior life is hydrated by
    // that loader before reaching this builder), not a fresh create: this
    // replica's group state died with the process, so claiming view-0
    // primaryship would heartbeat
    // commit_min=0 at peers that hold the committed log (racing their
    // election). Join as a quorum-invisible backup and probe for the
    // current view instead; journal repair re-materializes the data from a
    // peer, byte-identical by the deterministic-roll/replicated-ciphertext
    // design. A truly fresh create keeps the plain init: every group needs
    // its view-0 primary to exist.
    let durable_view = recovered_state
        .as_ref()
        .map(|state| (state.view, state.log_view));
    // Shared with the simulator's `init_partition`, which cannot call this
    // builder; see `fresh_group_start`.
    let FreshGroupStart { join, seed_view } =
        fresh_group_start(restarted, durable_view, created_view);
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
            view_fallback: None,
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

    configure_consumer_offsets(&mut partition, config, namespace, current_offset)?;
    ensure_initial_segment(&mut partition, config, stream_id, topic_id, partition_id).await?;

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
        // replay. Both callers absorb this without escalating: the reconciler
        // backs the namespace off and its retry materialises through the loader,
        // whose partition carries a clear backoff cell, while the loader's own
        // fence-and-rebuild arm tombstones the partition, since boot propagates
        // anything it returns.
        return Err(ServerError::PartitionOffsetReservationClaim {
            stream_id,
            topic_id,
            partition_id,
            namespace_raw: namespace.inner(),
        });
    }

    Ok(partition)
}

/// Recursive delete of partition root. Idempotent: `NotFound` is treated
/// as success so a prior crashed pass cannot arm perpetual backoff.
///
/// # Errors
///
/// [`IggyError::CannotDeletePartitionDirectory`] on any non-`NotFound`
/// OS error.
pub async fn delete_partitions_from_disk(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &ServerConfig,
) -> Result<(), IggyError> {
    let partition_path = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    match remove_dir_all(&partition_path).await {
        Ok(()) => {
            tracing::info!(
                stream_id,
                topic_id,
                partition_id,
                path = %partition_path,
                "deleted partition directory"
            );
            Ok(())
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(
                stream_id,
                topic_id,
                partition_id,
                path = %partition_path,
                "partition directory already absent"
            );
            Ok(())
        }
        Err(source) => {
            error!(
                stream_id,
                topic_id,
                partition_id,
                path = %partition_path,
                error = %source,
                "failed to delete partition directory"
            );
            // Variant format: {0}=partition_id, {1}=stream_id, {2}=topic_id.
            Err(IggyError::CannotDeletePartitionDirectory(
                partition_id,
                stream_id,
                topic_id,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use configs::server::ServerSystemConfig;
    use journal::superblock::SuperblockStore;
    use partitions::PartitionPathLayout;
    use server_common::sharding::ShardId;

    const CLUSTER: u128 = 7;
    const REPLICA: u8 = 1;
    const REPLICAS: u8 = 3;

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
            system: Arc::new(ServerSystemConfig {
                path: root.path().to_string_lossy().into_owned(),
                ..ServerSystemConfig::default()
            }),
            ..ServerConfig::default()
        }
    }

    async fn build_solo_partition(
        config: &ServerConfig,
    ) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerError> {
        build_partition_fresh(
            config,
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

    /// The container the loader tombstones into. Its config is never read on the
    /// paths under test, which stop before `restore_partition_offsets`.
    fn solo_partitions() -> IggyPartitions<Rc<IggyMessageBus>> {
        IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                validate_checksum: true,
                segment_size: IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        )
    }

    /// The reservation the partition left on disk, which is the only copy a
    /// restart or a first send can read.
    async fn recorded_reservation(dir: &str) -> u64 {
        let (_store, recorded) = open_partition_superblock(dir, solo_identity())
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
        let dir = config.system.get_partition_path(1, 1, 0);

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
        let dir = config.system.get_partition_path(1, 1, 0);

        let (store, recovered) = open_partition_superblock(&dir, solo_identity())
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

    /// The loader's fence-and-rebuild arm is the claim's SECOND caller, and boot
    /// propagates whatever the loader returns: an `Err` here costs the shard its
    /// whole start over one partition's failed write.
    #[compio::test]
    async fn given_a_failing_claim_when_rebuilding_a_fenced_chain_should_tombstone_the_partition() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = solo_config(&root);
        let namespace = IggyNamespace::new(1, 1, 0);
        let dir = config.system.get_partition_path(1, 1, 0);
        std::fs::create_dir_all(&dir).expect("partition dir");
        // Two empty segments make the first a NON-tail empty, the refusal a solo
        // group rebuilds through (zero recoverable bytes) instead of tombstoning
        // where it stands.
        for start_offset in [0, 1] {
            std::fs::File::create(config.system.get_messages_file_path(1, 1, 0, start_offset))
                .expect("empty segment log");
        }
        // The rebuild's claim is this group's first superblock write, so it
        // targets slot A. A directory where its temp file goes fails the atomic
        // replace and nothing else: the slot reads still find the store empty,
        // and the quarantine moves segment files only.
        std::fs::create_dir(Path::new(&dir).join("superblock.a.tmp")).expect("block slot A");

        let stats = Arc::new(PartitionStats::default());
        let partitions = solo_partitions();
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
            Ok(None) => {}
            Ok(Some(_)) => panic!("the planted directory must fail the rebuild's claim"),
            Err(error) => panic!("a refused claim must fence one partition, not boot: {error}"),
        }
        assert!(
            std::fs::metadata(format!("{dir}.fenced.0")).is_ok(),
            "the quarantine must have run, or this asserts on the wrong arm"
        );
        assert!(
            partitions.is_tombstoned(&namespace),
            "an unreserved partition must stay unrouted"
        );
        assert_eq!(
            stats.segments_count_inconsistent(),
            0,
            "the rebuild counted its initial segment before the claim refused"
        );
    }

    #[compio::test]
    async fn given_fresh_partition_dir_when_superblock_opened_should_yield_no_state() {
        let root = tempfile::tempdir().expect("tempdir");
        // A not-yet-materialized directory must open as fresh, not error: the
        // helper creates it, since a follower can reach load before its first
        // segment write.
        let (_store, recovered) = open_partition_superblock(&partition_dir(&root), test_identity())
            .await
            .expect("open a fresh partition superblock");
        assert!(
            recovered.is_none(),
            "an empty superblock is a fresh group, never an error"
        );
    }

    #[compio::test]
    async fn given_recorded_view_when_superblock_reopened_should_recover_state() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = partition_dir(&root);
        let (store, recovered) = open_partition_superblock(&dir, test_identity())
            .await
            .expect("first open");
        assert!(recovered.is_none());
        let state = recorded_state(3, 2);
        store
            .write(&state.to_bytes())
            .await
            .expect("record the advanced view");
        drop(store);

        let (_store, recovered) = open_partition_superblock(&dir, test_identity())
            .await
            .expect("reopen after a restart");

        assert_eq!(
            recovered,
            Some(state),
            "a restarted partition must recover exactly the state it recorded"
        );
    }

    #[compio::test]
    async fn given_foreign_cluster_record_when_superblock_opened_should_refuse_boot() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = partition_dir(&root);
        let (store, _) = open_partition_superblock(&dir, test_identity())
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

        let refused = open_partition_superblock(&dir, test_identity()).await;

        match refused {
            Err(ServerError::PartitionSuperblockIdentityMismatch { field, .. }) => {
                assert_eq!(field, IdentityField::Cluster);
            }
            Err(other) => panic!("expected an identity mismatch, got {other}"),
            Ok(_) => panic!("a copied or misplaced partition directory must refuse boot"),
        }
    }
}
