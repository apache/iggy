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

//! Helpers shared between the recovery path in [`crate::bootstrap`] and
//! the runtime partition reconciliation loop.
//!
//! Recovery hydrates an [`IggyPartition`] from on-disk state; the
//! reconciler builds one from scratch when a committed
//! `CreateTopic` / `CreatePartitions` metadata event has no matching
//! local partition yet. The two paths share namespace-bounds validation,
//! consumer-offset configuration, and initial-segment provisioning.

use crate::offset_recovery::{load_consumer_group_offsets, load_consumer_offsets};
use crate::server_error::ServerError;
use compio::fs::create_dir_all;
use configs::server::ServerConfig;
use consensus::{JoinMode, LocalPipeline, VsrConsensus, VsrRestore, VsrState};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, IggyError, IggyTimestamp, PartitionStats,
    TopicRuntimeOptions,
};
use journal::superblock::{PingPongSuperblock, SuperblockContents};
use message_bus::IggyMessageBus;
use metadata::{IdentityField, ReplicaIdentity};
use partitions::{IggyIndexWriter, IggyPartition, MessagesWriter, Segment};
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
/// configures empty maps (fresh partition path). `current_offset` bounds
/// recovered offsets so a partition that lost its tail does not surface
/// consumer offsets ahead of its current log head.
///
/// # Errors
///
/// Returns [`ServerError::ConsumerOffsetsLoad`] when the on-disk files
/// exist but fail to decode. A stored offset ahead of `current_offset` is
/// clamped (with a warning), not an error.
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
            if recovered_offset > current_offset {
                // A crash can persist an offset ahead of the flushed data
                // (offsets are stored eagerly, messages flush later). Clamp to
                // the recovered head so the consumer resumes instead of being
                // stuck polling past the log; mirrors the legacy contract.
                warn!(
                    consumer_id = offset.consumer_id,
                    recovered_offset,
                    current_offset,
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
            if recovered_offset > current_offset {
                warn!(
                    consumer_group_id = group_id.0,
                    recovered_offset,
                    current_offset,
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
    let start_offset = partition.offset_frontier();
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
pub(crate) async fn open_partition_superblock(
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

/// Materialise a brand-new [`IggyPartition`] for a namespace that has no on-disk state yet.
///
/// Counterpart to bootstrap's `load_partition`, which hydrates from
/// on-disk state during recovery; this builder is the runtime path
/// invoked by the reconciliation loop when a committed
/// `CreateTopic` / `CreatePartitions` metadata event names a partition
/// the local shard has not yet materialised.
///
/// Steps performed (all idempotent on retry after a partial failure):
/// 1. Create directory hierarchy on disk.
/// 2. Build per-partition VSR consensus group, resuming any superblock-recorded view.
/// 3. Configure empty consumer-offset storage with the on-disk paths set.
/// 4. Provision the initial segment + writers (offset 0).
///
/// The namespace arrives packed, so its components are in range by
/// construction. Metadata admission is what bounds them.
///
/// `view_seed` is the view a group with no durable record of its own starts
/// in, and it is the metadata plane's current view: see the `seed_view`
/// comment below for why a group left at view 0 is unreachable. `None` keeps
/// the historical view-0 start, and is also what a restart materialization
/// gets, since it probes for the live view instead.
///
/// The returned partition's `offset` / `dirty_offset` are `0` and
/// `should_increment_offset` is `false`, mirroring a clean append starting
/// at the empty segment.
///
/// # Errors
///
/// Returns [`ServerError`] when directory creation, superblock recovery, or
/// segment provisioning fails.
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
    view_seed: Option<u32>,
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

    // A partition directory that already holds segment bytes is a RESTART
    // materialization, not a fresh create: this replica's group state died
    // with the process, so claiming view-0 primaryship would heartbeat
    // commit_min=0 at peers that hold the committed log (racing their
    // election). Join as a quorum-invisible backup and probe for the
    // current view instead; journal repair re-materializes the data from a
    // peer, byte-identical by the deterministic-roll/replicated-ciphertext
    // design. A truly fresh create keeps the plain init: every group needs
    // its view-0 primary to exist.
    let join = if restarted {
        JoinMode::ProbeAsBackup {
            await_state_transfer: false,
        }
    } else {
        JoinMode::Init
    };
    // Only a plain `Init` may be seeded. A probing backup must sit at or below
    // the group's real view for the primary's `StartView` to move it forward;
    // seeded above it, the probe reply reads as stale and is dropped, and the
    // replica never rejoins. The probe exists precisely to learn the view, so
    // there is nothing for a seed to add there.
    let seed_view = match join {
        JoinMode::Init => view_seed,
        JoinMode::ProbeAsBackup { .. } => None,
    };
    // Request queue holds 2x the prepare depth (buffered requests drain as
    // prepares commit); depth is the per-partition `[partition]` knob.
    let prepare_queue_depth = config.partition.prepare_queue_depth;
    let timers = crate::bootstrap::consensus_timers(config);
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
            // Both planes pick their primary as `view % replica_count` from
            // their OWN view counter. A group left at view 0 while the
            // metadata plane sits elsewhere therefore names a different node
            // than the roster advertises as leader, and nothing routes a
            // partition write across that gap: the client is sent to the
            // metadata leader and refused there for the whole budget. Seeding
            // from the metadata view keeps the two congruent for a group born
            // after a metadata election.
            //
            // Replicas can still disagree on the seed: each publishes its own
            // metadata view on a 100ms poll, so one may read V while another
            // has yet to see the election. That resolves the way any view
            // disagreement does, the higher view winning through `StartView` -
            // but only because the seed sets `log_view` too. The caller is
            // what keeps a replica from seeding a view it has no opinion on;
            // see `ReconcilerCtx::partition_view_seed`.
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
    partition.should_increment_offset = false;
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
    use journal::superblock::SuperblockStore;

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
