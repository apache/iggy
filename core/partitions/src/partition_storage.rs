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

//! Partition storage lifecycle.
//!
//! Create the directory hierarchy and the first segment, seed the persisted
//! consumer offsets and reopen the recovered log's writers on a partition, and
//! delete a partition from disk.

use crate::offset_recovery::{
    RecoveredOffsets, load_consumer_offsets_with_storage, load_group_offsets_with_storage,
};
use crate::segment_recovery::{PartitionRecoveryError, PartitionRecoveryRefusal, RecoveredSegment};
use crate::{IggyIndexWriter, IggyPartition, MessagesWriter, PartitionsConfig, Segment};
use compio::fs::create_dir_all;
use compio::io::AsyncWriteAtExt;
use iggy_common::{ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets, IggyError};
use journal::durable_storage::{DiskStorage, DurableStorage};
use message_bus::MessageBus;
use server_common::SegmentStorage;
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::fs_utils::walk_dir;
use server_common::sharding::IggyNamespace;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use tracing::{error, warn};

/// Create the on-disk directory hierarchy for a partition.
///
/// Builds the partition root, offsets, consumer offsets, and consumer
/// group offsets directories. Idempotent: an existing directory is
/// accepted, so a reconciler retry after a partial failure is safe. The
/// external group offsets directory comes from
/// [`configure_consumer_offsets_with_storage`], which a partition made
/// before that kind existed reaches too.
///
/// # Errors
///
/// Returns [`IggyError::CannotCreatePartitionDirectory`] or
/// [`IggyError::CannotCreatePartition`] on directory creation failure.
pub async fn create_partition_file_hierarchy(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &PartitionsConfig,
) -> Result<(), IggyError> {
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    if !Path::new(&partition_path).exists() && create_dir_all(&partition_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionDirectory(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    // `create_dir_all` also creates the shared `offsets/` parent.
    for path in [
        config.get_consumer_offsets_path(stream_id, topic_id, partition_id),
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id),
    ] {
        if create_dir_all(&path).await.is_err() {
            error!(
                stream_id,
                topic_id, partition_id, path, "Failed to create offsets directory for partition"
            );
            return Err(IggyError::CannotCreatePartition(
                partition_id,
                stream_id,
                topic_id,
            ));
        }
    }

    Ok(())
}

/// File in a partition directory that names the incarnation the directory
/// belongs to: the partition's `created_revision`, as 8 little-endian bytes.
///
/// Ids are reused after a delete, and a delete that did not finish before a
/// restart leaves the directory behind, so the path alone does not say whose
/// segments it holds. The file is written before any other content and removed
/// after all of it ([`delete_partitions_from_disk`]).
pub const CREATED_REVISION_FILE: &str = "created.revision";

/// Durably record that `partition_dir` belongs to the incarnation created at
/// `created_revision`. The directory must exist.
///
/// # Errors
///
/// The underlying I/O error.
pub async fn write_created_revision(
    partition_dir: &str,
    created_revision: u64,
) -> std::io::Result<()> {
    write_revision_record(partition_dir, CREATED_REVISION_FILE, created_revision).await
}

/// The incarnation `partition_dir` belongs to, or `None` when nothing recorded
/// one: a missing directory, or one an older server made.
///
/// # Errors
///
/// The underlying I/O error, and `InvalidData` for a record that is not 8 bytes.
pub async fn read_created_revision(partition_dir: &str) -> std::io::Result<Option<u64>> {
    read_revision_record(partition_dir, CREATED_REVISION_FILE).await
}

/// Durably replace the 8-byte little-endian record `name` in `directory`: write
/// a temporary sibling, sync it, rename it over the record, sync the directory.
pub async fn write_revision_record(
    directory: &str,
    name: &str,
    revision: u64,
) -> std::io::Result<()> {
    let path = Path::new(directory).join(name);
    let temporary = Path::new(directory).join(format!("{name}.tmp"));
    let mut file = compio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .await
        .note_descriptor_exhaustion(|| format!("opening {}", temporary.display()))?;
    file.write_all_at(revision.to_le_bytes(), 0).await.0?;
    file.sync_all().await?;
    if let Err(error) = compio::fs::rename(&temporary, path).await {
        let _ = compio::fs::remove_file(&temporary).await;
        return Err(error);
    }
    DiskStorage.sync_directory(Path::new(directory)).await
}

/// Read the 8-byte little-endian record `name` in `directory`, `None` when it
/// is absent.
pub async fn read_revision_record(directory: &str, name: &str) -> std::io::Result<Option<u64>> {
    let path = Path::new(directory).join(name);
    match compio::fs::read(&path)
        .await
        .note_descriptor_exhaustion(|| format!("reading {}", path.display()))
    {
        Ok(bytes) => {
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{name} in {directory} is not an 8-byte record"),
                )
            })?;
            Ok(Some(u64::from_le_bytes(bytes)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Populate `partition` with consumer and consumer group offset storage from disk.
///
/// Hydrates from state on disk if files exist (recovery path) or
/// configures empty maps (fresh partition path). Recovered offsets are bounded
/// so a partition that lost its tail does not surface consumer offsets ahead of
/// an offset it never handed out, and `current_offset` is where a bounded one
/// lands.
///
/// # Errors
///
/// Returns [`PartitionRecoveryError::ConsumerOffsetsLoad`] when an existing offset
/// directory cannot be enumerated. A stored offset past the offset space is clamped
/// to `current_offset` (with a warning), not an error. External group offsets are
/// never clamped.
pub async fn configure_consumer_offsets<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), PartitionRecoveryError> {
    configure_consumer_offsets_with_storage(
        &DiskStorage,
        partition,
        config,
        namespace,
        current_offset,
    )
    .await
}

/// Recover consumer and group offsets from `storage` into a new partition.
///
/// Restore the partition's message offset and reservation frontier before
/// calling this, and pass its restored offset counter as `current_offset`.
/// These values bound which saved consumer positions are plausible.
///
/// Missing directories produce empty maps. Valid records seed the visible
/// offsets and their persistence state. Unreadable records and invalid records
/// whose removal cannot be made durable retain their admission capacity slots.
/// Offsets beyond the space reserved by the partition are clamped to
/// `current_offset`, as during server boot. Clamping changes the visible position
/// without rewriting its file; persistence tracking retains the original value
/// read from storage. External group offsets are never clamped.
///
/// # Errors
/// Returns [`PartitionRecoveryError::ConsumerOffsetsLoad`] if an existing offset directory
/// cannot be enumerated. Consumer recovery may already have seeded the partition
/// when group recovery fails, so callers must discard a failed recovery.
#[allow(clippy::too_many_lines)]
pub async fn configure_consumer_offsets_with_storage<S: DurableStorage, B: MessageBus>(
    storage: &S,
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    current_offset: u64,
) -> Result<(), PartitionRecoveryError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let consumer_offsets_path = config.get_consumer_offsets_path(stream_id, topic_id, partition_id);
    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    let external_group_offsets_path =
        config.get_external_group_offsets_path(stream_id, topic_id, partition_id);
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

    let recovered_consumers = load_partition_consumer_offsets(
        storage,
        &consumer_offsets_path,
        ConsumerKind::Consumer.as_str(),
        stream_id,
        topic_id,
        partition_id,
    )
    .await?;
    let consumer_offsets = ConsumerOffsets::with_capacity(recovered_consumers.entries.len());
    {
        let guard = consumer_offsets.pin();
        for offset in recovered_consumers.entries {
            seed_recovered_offset(
                partition,
                namespace,
                ConsumerKind::Consumer,
                &offset,
                Some(offset_space_ceiling),
                current_offset,
            );
            guard.insert(offset.consumer_id as usize, offset);
        }
    }

    let recovered_groups = load_partition_group_offsets(
        storage,
        &consumer_group_offsets_path,
        ConsumerKind::ConsumerGroup,
        stream_id,
        topic_id,
        partition_id,
    )
    .await?;
    let consumer_group_offsets =
        ConsumerGroupOffsets::with_capacity(recovered_groups.entries.len());
    {
        let guard = consumer_group_offsets.pin();
        for (group_id, offset) in recovered_groups.entries {
            seed_recovered_offset(
                partition,
                namespace,
                ConsumerKind::ConsumerGroup,
                &offset,
                Some(offset_space_ceiling),
                current_offset,
            );
            guard.insert(group_id, offset);
        }
    }

    // A partition made before this kind existed has no directory for it. The checkpoint syncs
    // every offset directory, so it has to exist before the first one.
    storage
        .create_directories(Path::new(&external_group_offsets_path))
        .await
        .map_err(|_| PartitionRecoveryError::ConsumerOffsetsLoad {
            consumer_kind: ConsumerKind::ExternalGroup.as_str(),
            stream_id,
            topic_id,
            partition_id,
            path: external_group_offsets_path.clone(),
            source: Box::new(IggyError::CannotCreateConsumerOffsetsDirectory(
                external_group_offsets_path.clone(),
            )),
        })?;
    let recovered_external = load_partition_group_offsets(
        storage,
        &external_group_offsets_path,
        ConsumerKind::ExternalGroup,
        stream_id,
        topic_id,
        partition_id,
    )
    .await?;
    // Never clamped: the value belongs to the external system, and a Kafka commit sits one past
    // the last message. The durable table is the only store of an external offset.
    for (_, offset) in &recovered_external.entries {
        seed_recovered_offset(
            partition,
            namespace,
            ConsumerKind::ExternalGroup,
            offset,
            None,
            current_offset,
        );
    }

    // Offset files have their own knob, not the topic's `persisted`: that
    // one gates message and index writes, and syncing a 16-byte cursor on every
    // commit costs milliseconds per commit for a file whose loss is a redelivery.
    partition.configure_consumer_offset_storage(
        [
            consumer_offsets_path.clone(),
            consumer_group_offsets_path.clone(),
            external_group_offsets_path.clone(),
        ],
        consumer_offsets,
        consumer_group_offsets,
    );
    for (kind, path, stranded_ids) in [
        (
            ConsumerKind::Consumer,
            &consumer_offsets_path,
            recovered_consumers.stranded_ids,
        ),
        (
            ConsumerKind::ConsumerGroup,
            &consumer_group_offsets_path,
            recovered_groups.stranded_ids,
        ),
        (
            ConsumerKind::ExternalGroup,
            &external_group_offsets_path,
            recovered_external.stranded_ids,
        ),
    ] {
        for consumer_id in stranded_ids {
            if partition.seed_stranded_consumer_offset(kind, consumer_id) {
                warn!(stream_id, topic_id, partition_id, %kind, consumer_id, path = %path,
                    "unloaded offset file retains its capacity slot until it is updated, deleted, or reclaimed");
            }
        }
    }
    for kind in ConsumerKind::ALL {
        let count = partition.occupied_consumer_offset_count(kind);
        let limit = partition.consumer_offset_capacity_for(kind).limit();
        if count > limit {
            warn!(
                stream_id,
                topic_id,
                partition_id,
                ?kind,
                count,
                limit,
                "recovered consumer offsets exceed the configured admission limit"
            );
        }
    }
    Ok(())
}

/// Seed the durable state of one recovered offset record. A record past
/// `ceiling` is clamped to `current_offset` first, and `None` skips the clamp.
/// The file high-water keeps the value read from storage.
fn seed_recovered_offset<B: MessageBus>(
    partition: &IggyPartition<B>,
    namespace: IggyNamespace,
    kind: ConsumerKind,
    offset: &ConsumerOffset,
    ceiling: Option<u64>,
    current_offset: u64,
) {
    let recovered_offset = offset.offset.load(Ordering::Relaxed);
    if let Some(ceiling) = ceiling
        && recovered_offset > ceiling
    {
        // A crash can persist an offset ahead of the flushed data (offsets are
        // stored eagerly, messages flush later). Clamp to the recovered head so
        // the consumer resumes instead of being stuck polling past the log;
        // mirrors the legacy contract.
        warn!(
            stream_id = namespace.stream_id(),
            topic_id = namespace.topic_id(),
            partition_id = namespace.partition_id(),
            %kind,
            consumer_id = offset.consumer_id,
            recovered_offset,
            current_offset,
            offset_space_ceiling = ceiling,
            "recovered offset ahead of partition data; clamping"
        );
        offset.offset.store(current_offset, Ordering::Relaxed);
    }
    partition.seed_recovered_consumer_offset(
        kind,
        offset.consumer_id,
        offset.offset.load(Ordering::Relaxed),
        recovered_offset,
    );
}

/// Provision an initial segment + writers for a partition that has none.
///
/// No-op when `partition.log.has_segments()` already returns `true`
/// (recovery hydrated existing segments), so callers can invoke this
/// unconditionally.
///
/// # Errors
///
/// Returns [`PartitionRecoveryError::Iggy`] on segment-storage creation
/// failure or writer initialisation failure.
pub async fn ensure_initial_segment<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    wal_owned_messages: bool,
) -> Result<(), PartitionRecoveryError> {
    if partition.log.has_segments() {
        return Ok(());
    }
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();

    // At the RESTORED FRONTIER, not always 0: after a crash inside the install's
    // swap window the chain is empty while the recorded frontier is N, and a
    // segment named 0 would then take the first append's `base_offset = N` --
    // `rposition(|s| s.start_offset <= offset)` routes every poll for `0..N-1`
    // into it, the next boot makes that shape durable, and this replica starts
    // offering peers a segment that claims `[0..N]`.
    let start_offset = partition.mint_frontier();
    let messages_path = config.get_messages_path(stream_id, topic_id, partition_id, start_offset);
    let index_path = config.get_index_path(stream_id, topic_id, partition_id, start_offset);
    let runtime = partition.runtime_options();
    let segment_size = runtime.effective_segment_size();
    let persisted = runtime.durability.is_persisted();
    let preallocate_segments = runtime
        .preallocate_segments
        .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS);
    // Recreate stale indexes, but preserve any physical tail retained by the WAL.
    let storage = if wal_owned_messages {
        SegmentStorage::with_read_only_messages(
            &messages_path,
            &index_path,
            0,
            false,
            preallocate_segments.then_some(segment_size.as_bytes_u64()),
        )
        .await
    } else {
        SegmentStorage::new(&messages_path, &index_path, 0, 0, false).await
    }
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
    let messages_size_counter = storage.messages_size.clone().unwrap_or_default();
    let index_size_counter = storage.index_size.clone().unwrap_or_default();
    let messages_writer = if wal_owned_messages {
        None
    } else {
        Some(Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_counter,
                persisted,
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
        ))
    };
    partition.log.add_persisted_segment(
        Segment::new(start_offset, segment_size),
        storage,
        messages_writer,
        Some(Rc::new(
            IggyIndexWriter::new(&index_path, index_size_counter, persisted, false)
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

/// Recursive delete of partition root. Idempotent: `NotFound` is treated
/// as success so a prior crashed pass cannot arm perpetual backoff.
///
/// The [`CREATED_REVISION_FILE`] goes last, so a delete cut short by a crash
/// leaves a directory the loader still knows as a dead incarnation, never
/// unmarked segments that the next incarnation with the same ids would adopt.
///
/// # Errors
///
/// [`IggyError::CannotDeletePartitionDirectory`] on any non-`NotFound`
/// OS error.
pub async fn delete_partitions_from_disk(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &PartitionsConfig,
) -> Result<(), IggyError> {
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    match remove_partition_dir(&partition_path).await {
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

async fn remove_partition_dir(partition_path: &str) -> std::io::Result<()> {
    let directory = Path::new(partition_path);
    let result = async {
        let marker = directory.join(CREATED_REVISION_FILE);
        for entry in walk_dir(directory).await? {
            if entry.path == marker || entry.path == directory {
                continue;
            }
            if entry.is_dir {
                compio::fs::remove_dir(&entry.path).await?;
            } else {
                compio::fs::remove_file(&entry.path).await?;
            }
        }
        // The removals above must be durable before the marker's own is.
        DiskStorage.sync_directory(directory).await?;
        DiskStorage.remove_tree(directory).await
    }
    .await;
    if let Err(error) = &result
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return result;
    }

    // Retrying an interrupted delete can find the root or its parent already gone.
    // Sync the nearest surviving ancestor before acknowledging that absence.
    let current_directory = Path::new(".");
    let mut parent = directory
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(current_directory);
    loop {
        match DiskStorage.sync_directory(parent).await {
            Ok(()) => return result,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && parent != current_directory =>
            {
                parent = parent
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(current_directory);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Reopen writers over a recovered segment chain.
///
/// Takes no `&PartitionsConfig`: every knob it needs is the partition's own
/// resolved topic option now, which is the whole point of the per-topic move.
///
/// # Errors
///
/// [`PartitionRecoveryError::Refused`] with
/// [`PartitionRecoveryRefusal::StorageSizeMismatch`] when a messages or index
/// writer reopened over the recovered bounds finds that the file length
/// diverged from them. Every other reopen failure is transient I/O, returned
/// as [`PartitionRecoveryError::Iggy`]. That includes a size mismatch on a
/// WAL-owned tail, which reopens only its index writer.
pub async fn hydrate_partition_log<B: MessageBus>(
    partition: &mut IggyPartition<B>,
    partition_dir: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    recovered_segments: Vec<RecoveredSegment>,
) -> Result<(), PartitionRecoveryError> {
    // The partition's own resolved knobs, not the shard-wide config: a topic
    // created with `persisted` or a per-topic `segment_size` must get them
    // on the writers reopened over its recovered chain too, or a restart would
    // silently drop back to the node defaults.
    let runtime = partition.runtime_options();
    let persisted = runtime.durability.is_persisted();
    let segment_size = runtime.effective_segment_size();
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
        if storage.messages_size.is_none()
            && let (Some(index_reader), Some(index_size)) =
                (&storage.index_reader, &storage.index_size)
        {
            partition.log.index_writers_mut()[active_index] = Some(Rc::new(
                IggyIndexWriter::new(&index_reader.path(), Rc::clone(index_size), persisted, true)
                    .await?,
            ));
            return Ok(());
        }
        if let (
            Some(messages_reader),
            Some(index_reader),
            Some(storage_messages_size),
            Some(storage_index_size),
        ) = (
            storage.messages_reader.as_ref(),
            storage.index_reader.as_ref(),
            storage.messages_size.as_ref(),
            storage.index_size.as_ref(),
        ) {
            let index_path = index_reader.path();
            let start_offset = partition.log.segments()[active_index].start_offset;
            // Share the storage's size counters: they are the write cursors.
            // A private counter would let the append position diverge from the
            // segment bookkeeping that index entries and poll bounds rely on.
            let messages_size_counter = Rc::clone(storage_messages_size);
            let index_size_counter = Rc::clone(storage_index_size);
            partition.log.messages_writers_mut()[active_index] = Some(Rc::new(
                MessagesWriter::new(
                    &messages_reader.path(),
                    messages_size_counter,
                    persisted,
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
                IggyIndexWriter::new(&index_path, index_size_counter, persisted, true)
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

async fn load_partition_consumer_offsets<S: DurableStorage>(
    storage: &S,
    path: &str,
    consumer_kind: &'static str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<RecoveredOffsets<iggy_common::ConsumerOffset>, PartitionRecoveryError> {
    if !storage
        .exists_following_links(Path::new(path))
        .await
        .unwrap_or(false)
    {
        return Ok(RecoveredOffsets::default());
    }

    match load_consumer_offsets_with_storage(storage, path).await {
        Ok(offsets) => Ok(offsets),
        Err(IggyError::CannotReadConsumerOffsets(_))
            if !storage
                .exists_following_links(Path::new(path))
                .await
                .unwrap_or(false) =>
        {
            Ok(RecoveredOffsets::default())
        }
        Err(source) => Err(PartitionRecoveryError::ConsumerOffsetsLoad {
            consumer_kind,
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        }),
    }
}

async fn load_partition_group_offsets<S: DurableStorage>(
    storage: &S,
    path: &str,
    kind: ConsumerKind,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<
    RecoveredOffsets<(iggy_common::ConsumerGroupId, iggy_common::ConsumerOffset)>,
    PartitionRecoveryError,
> {
    if !storage
        .exists_following_links(Path::new(path))
        .await
        .unwrap_or(false)
    {
        return Ok(RecoveredOffsets::default());
    }

    match load_group_offsets_with_storage(storage, path, kind).await {
        Ok(offsets) => Ok(offsets),
        Err(IggyError::CannotReadConsumerOffsets(_))
            if !storage
                .exists_following_links(Path::new(path))
                .await
                .unwrap_or(false) =>
        {
            Ok(RecoveredOffsets::default())
        }
        Err(source) => Err(PartitionRecoveryError::ConsumerOffsetsLoad {
            consumer_kind: kind.as_str(),
            stream_id,
            topic_id,
            partition_id,
            path: path.to_string(),
            source: Box::new(source),
        }),
    }
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
) -> PartitionRecoveryError {
    match source {
        IggyError::SegmentSizeMismatchAtOpen(on_disk_bytes, expected_bytes) => {
            PartitionRecoveryError::Refused {
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
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    #[compio::test]
    async fn revision_record_rename_failure_removes_the_temporary_file() {
        const CREATED_REVISION: u64 = 7;
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join(CREATED_REVISION_FILE);
        let temporary = root.path().join(format!("{CREATED_REVISION_FILE}.tmp"));
        std::fs::create_dir(&record).unwrap();

        let error = write_created_revision(root.path().to_str().unwrap(), CREATED_REVISION)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::IsADirectory);
        assert!(
            record.is_dir(),
            "the failed rename must preserve its destination"
        );
        assert!(
            !temporary.exists(),
            "the failed rename must clean up its temporary file"
        );
    }

    #[compio::test]
    async fn removing_a_partition_dir_requires_syncing_its_parent() {
        const CREATED_REVISION: u64 = 7;
        const NO_PARENT_READ: u32 = 0o300;
        let root = tempfile::tempdir().unwrap();
        let partition_dir = root.path().join("0");
        std::fs::create_dir(&partition_dir).unwrap();
        write_created_revision(partition_dir.to_str().unwrap(), CREATED_REVISION)
            .await
            .unwrap();
        let mode = std::fs::metadata(root.path()).unwrap().permissions().mode();
        std::fs::set_permissions(root.path(), Permissions::from_mode(NO_PARENT_READ)).unwrap();
        let parent_read_denied = std::fs::File::open(root.path()).is_err();

        let result = remove_partition_dir(partition_dir.to_str().unwrap()).await;
        let retry = remove_partition_dir(partition_dir.to_str().unwrap()).await;
        std::fs::set_permissions(root.path(), Permissions::from_mode(mode)).unwrap();
        // Root ignores the mode, so the parent sync cannot be made to fail.
        if !parent_read_denied {
            return;
        }

        assert!(!partition_dir.exists());
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "a removed partition cannot be acknowledged before its parent sync succeeds"
        );
        assert_eq!(
            retry.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied,
            "a retry must repeat the failed parent sync even when the partition is already absent"
        );
    }

    #[compio::test]
    async fn removing_a_partition_dir_takes_the_marker_with_every_other_entry() {
        const CREATED_REVISION: u64 = 7;
        let root = tempfile::tempdir().unwrap();
        let partition_dir = root.path().join("0");
        std::fs::create_dir_all(partition_dir.join("offsets")).unwrap();
        std::fs::write(partition_dir.join("offsets/1"), b"offset").unwrap();
        std::fs::write(partition_dir.join("00000000000000000000.log"), b"segment").unwrap();
        let moved_segment = root.path().join("moved.log");
        std::fs::write(&moved_segment, b"segment").unwrap();
        std::os::unix::fs::symlink(
            &moved_segment,
            partition_dir.join("00000000000000000001.log"),
        )
        .unwrap();
        let partition_dir = partition_dir.to_str().unwrap();
        write_created_revision(partition_dir, CREATED_REVISION)
            .await
            .unwrap();
        assert_eq!(
            read_created_revision(partition_dir).await.unwrap(),
            Some(CREATED_REVISION)
        );

        remove_partition_dir(partition_dir).await.unwrap();
        assert!(!Path::new(partition_dir).exists());
        assert!(
            moved_segment.exists(),
            "a symlink is unlinked, never followed"
        );
        assert_eq!(read_created_revision(partition_dir).await.unwrap(), None);
        assert_eq!(
            remove_partition_dir(partition_dir)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound,
            "the caller maps a missing directory to an already finished delete"
        );
    }

    #[compio::test]
    async fn a_removal_cut_short_leaves_the_marker_in_place() {
        const CREATED_REVISION: u64 = 7;
        const NO_REMOVALS: u32 = 0o500;
        let root = tempfile::tempdir().unwrap();
        let partition_dir = root.path().join("0");
        let offsets = partition_dir.join("offsets");
        std::fs::create_dir_all(&offsets).unwrap();
        std::fs::write(offsets.join("1"), b"offset").unwrap();
        let segment = partition_dir.join("00000000000000000000.log");
        std::fs::write(&segment, b"segment").unwrap();
        let partition_dir = partition_dir.to_str().unwrap();
        write_created_revision(partition_dir, CREATED_REVISION)
            .await
            .unwrap();
        let mode = std::fs::metadata(&offsets).unwrap().permissions().mode();
        std::fs::set_permissions(&offsets, Permissions::from_mode(NO_REMOVALS)).unwrap();
        // Root ignores the mode, so nothing would cut the removal short.
        let blocked = std::fs::remove_file(offsets.join("1")).is_err();

        let result = remove_partition_dir(partition_dir).await;
        std::fs::set_permissions(&offsets, Permissions::from_mode(mode)).unwrap();
        if !blocked {
            return;
        }

        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert!(
            !segment.exists(),
            "the walk reached the inner entry after the top-level files"
        );
        assert_eq!(
            read_created_revision(partition_dir).await.unwrap(),
            Some(CREATED_REVISION),
            "a delete cut short must leave a directory the loader still knows as dead"
        );
    }
}
