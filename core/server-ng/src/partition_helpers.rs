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
use crate::server_error::ServerNgError;
use compio::fs::create_dir_all;
use configs::server_ng::ServerNgConfig;
use consensus::{LocalPipeline, VsrConsensus};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyError, IggyTimestamp, PartitionStats,
};
use message_bus::IggyMessageBus;
use partitions::{IggyIndexWriter, IggyPartition, MessagesWriter, Segment};
use server_common::SegmentStorage;
use server_common::fs_utils::remove_dir_all;
use server_common::sharding::IggyNamespace;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{error, warn};

/// Validate that a namespace fits within the static caps declared in
/// `config.extra.namespace`.
///
/// Bootstrap calls this for every recovered namespace; the reconciler
/// calls this before materialising a freshly committed partition. Same
/// error variant either way so operators see one root cause label.
///
/// # Errors
///
/// Returns [`ServerNgError::RecoveredNamespaceOutOfBounds`] if any of
/// `stream_id`, `topic_id`, or `partition_id` exceed the configured
/// maxima.
pub const fn validate_namespace_bounds(
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    let namespace = &config.extra.namespace;
    if stream_id < namespace.max_streams
        && topic_id < namespace.max_topics
        && partition_id < namespace.max_partitions
    {
        return Ok(());
    }

    Err(ServerNgError::RecoveredNamespaceOutOfBounds {
        stream_id,
        topic_id,
        partition_id,
        max_streams: namespace.max_streams,
        max_topics: namespace.max_topics,
        max_partitions: namespace.max_partitions,
    })
}

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
    config: &ServerNgConfig,
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
/// Returns [`ServerNgError::ConsumerOffsetsLoad`] when the on-disk files
/// exist but fail to decode. A stored offset ahead of `current_offset` is
/// clamped (with a warning), not an error.
pub fn configure_consumer_offsets(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), ServerNgError> {
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

    partition.configure_consumer_offset_storage(
        consumer_offsets_path,
        consumer_group_offsets_path,
        consumer_offsets,
        consumer_group_offsets,
        config.system.partition.enforce_fsync,
    );
    Ok(())
}

fn load_partition_consumer_offsets(
    path: &str,
    consumer_kind: &'static str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Vec<iggy_common::ConsumerOffset>, ServerNgError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerNgError::ConsumerOffsetsLoad {
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
) -> Result<Vec<(iggy_common::ConsumerGroupId, iggy_common::ConsumerOffset)>, ServerNgError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    load_consumer_group_offsets(path).or_else(|source| {
        if matches!(&source, IggyError::CannotReadConsumerOffsets(missing_path) if !Path::new(missing_path).exists())
        {
            return Ok(Vec::new());
        }

        Err(ServerNgError::ConsumerOffsetsLoad {
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
/// Returns [`ServerNgError`] on segment-storage creation failure or
/// writer initialisation failure.
pub async fn ensure_initial_segment(
    partition: &mut IggyPartition<Rc<IggyMessageBus>>,
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerNgError> {
    if partition.log.has_segments() {
        return Ok(());
    }

    let messages_path = config
        .system
        .get_messages_file_path(stream_id, topic_id, partition_id, 0);
    let index_path = config
        .system
        .get_index_path(stream_id, topic_id, partition_id, 0);
    let enforce_fsync = config.system.partition.enforce_fsync;
    let storage = SegmentStorage::new(
        &messages_path,
        &index_path,
        0,
        0,
        enforce_fsync,
        enforce_fsync,
        false,
    )
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
    // Share the storage's size counters so reads observe persisted bytes;
    // a writer with a private counter grows the file invisibly to readers.
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
        Segment::new(0, config.system.segment.size),
        storage,
        Some(Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_counter,
                config.system.partition.enforce_fsync,
                false,
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
            IggyIndexWriter::new(
                &index_path,
                index_size_counter,
                config.system.partition.enforce_fsync,
                false,
            )
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

/// Materialise a brand-new [`IggyPartition`] for a namespace that has no on-disk state yet.
///
/// Counterpart to bootstrap's `load_partition`, which hydrates from
/// on-disk state during recovery; this builder is the runtime path
/// invoked by the reconciliation loop when a committed
/// `CreateTopic` / `CreatePartitions` metadata event names a partition
/// the local shard has not yet materialised.
///
/// Steps performed (all idempotent on retry after a partial failure):
/// 1. Validate namespace fits within the configured caps.
/// 2. Create directory hierarchy on disk.
/// 3. Build per-partition VSR consensus group at view 0.
/// 4. Configure empty consumer-offset storage with the on-disk paths set.
/// 5. Provision the initial segment + writers (offset 0).
///
/// The returned partition's `offset` / `dirty_offset` are `0` and
/// `should_increment_offset` is `false`, mirroring a clean append starting
/// at the empty segment.
///
/// # Errors
///
/// Returns [`ServerNgError`] when bounds validation, directory creation,
/// or segment provisioning fails.
pub async fn build_partition_fresh(
    config: &ServerNgConfig,
    namespace: IggyNamespace,
    stats: Arc<PartitionStats>,
    cluster_id: u128,
    self_replica_id: u8,
    replica_count: u8,
    bus: Rc<IggyMessageBus>,
) -> Result<IggyPartition<Rc<IggyMessageBus>>, ServerNgError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();

    validate_namespace_bounds(config, stream_id, topic_id, partition_id)?;
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

    // Request queue holds 2x the prepare depth (buffered requests drain as
    // prepares commit); depth is the per-partition `[partition]` knob.
    let prepare_queue_depth = config.partition.prepare_queue_depth;
    let consensus = VsrConsensus::new(
        cluster_id,
        self_replica_id,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::with_capacities(prepare_queue_depth, prepare_queue_depth * 2),
    );
    consensus.set_normal_heartbeat_ticks(crate::bootstrap::cluster_heartbeat_ticks(config));
    consensus.set_commit_message_ticks(crate::bootstrap::commit_broadcast_ticks(config));
    consensus.set_prepare_ticks(crate::bootstrap::prepare_retransmit_ticks(config));
    consensus
        .set_view_change_retransmit_ticks(crate::bootstrap::view_change_retransmit_ticks(config));
    consensus.set_view_change_status_ticks(crate::bootstrap::view_change_status_ticks(config));
    consensus.set_request_start_view_ticks(crate::bootstrap::request_start_view_ticks(config));
    consensus.set_probe_attempts_max(config.cluster.view_probe_attempts_max);
    // A partition directory that already holds segment bytes is a RESTART
    // materialization, not a fresh create: this replica's group state died
    // with the process, so claiming view-0 primaryship would heartbeat
    // commit_min=0 at peers that hold the committed log (racing their
    // election). Join as a quorum-invisible backup and probe for the
    // current view instead; journal repair re-materializes the data from a
    // peer, byte-identical by the deterministic-roll/replicated-ciphertext
    // design. A truly fresh create keeps the plain init: every group needs
    // its view-0 primary to exist.
    if restarted {
        consensus.init_as_backup();
        consensus.begin_view_probe();
    } else {
        consensus.init();
    }

    let mut partition = IggyPartition::new(stats, consensus);
    partition.set_partition_dir(config.system.get_partition_path(
        stream_id,
        topic_id,
        partition_id,
    ));
    partition.created_at = IggyTimestamp::now();
    partition.offset.store(0, Ordering::Release);
    partition.dirty_offset.store(0, Ordering::Relaxed);
    partition.should_increment_offset = false;
    partition.stats.set_current_offset(0);
    debug_assert!(
        !partition.log.has_segments(),
        "fresh partition must not carry recovered segments"
    );

    configure_consumer_offsets(&mut partition, config, namespace, 0)?;
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
    config: &ServerNgConfig,
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
