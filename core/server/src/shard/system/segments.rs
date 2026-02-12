/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::configs::cache_indexes::CacheIndexesConfig;
use crate::shard::IggyShard;
use crate::streaming::segments::Segment;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize};

impl IggyShard {
    /// Performs all cleanup for a topic's partitions: time-based expiry then size-based trimming.
    ///
    /// Runs entirely inside the message pump's serialized loop — reads partition state and
    /// deletes segments atomically with no TOCTOU window.
    pub(crate) async fn clean_topic_messages(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_ids: &[usize],
    ) -> Result<(u64, u64), IggyError> {
        let (expiry, max_topic_size) = self
            .metadata
            .get_topic_config(stream_id, topic_id)
            .unwrap_or((
                self.config.system.topic.message_expiry,
                MaxTopicSize::Unlimited,
            ));

        let mut total_segments = 0u64;
        let mut total_messages = 0u64;

        // Phase 1: time-based expiry
        if !matches!(expiry, IggyExpiry::NeverExpire) {
            let now = IggyTimestamp::now();
            for &partition_id in partition_ids {
                let (s, m) = self
                    .delete_expired_segments_for_partition(
                        stream_id,
                        topic_id,
                        partition_id,
                        now,
                        expiry,
                    )
                    .await?;
                total_segments += s;
                total_messages += m;
            }
        }

        // Phase 2: size-based trimming
        if !matches!(max_topic_size, MaxTopicSize::Unlimited) {
            let max_bytes = max_topic_size.as_bytes_u64();
            let threshold = max_bytes * 9 / 10;

            loop {
                let current_size = self
                    .metadata
                    .with_metadata(|m| {
                        m.streams
                            .get(stream_id)
                            .and_then(|s| s.topics.get(topic_id))
                            .map(|t| t.stats.size_bytes_inconsistent())
                    })
                    .unwrap_or(0);

                if current_size < threshold {
                    break;
                }

                let Some((target_partition_id, target_offset)) =
                    self.find_oldest_sealed_segment(stream_id, topic_id, partition_ids)
                else {
                    break;
                };

                let (s, m) = self
                    .remove_segment_by_offset(
                        stream_id,
                        topic_id,
                        target_partition_id,
                        target_offset,
                    )
                    .await?;
                if s == 0 {
                    break;
                }
                total_segments += s;
                total_messages += m;
            }
        }

        Ok((total_segments, total_messages))
    }

    /// Deletes all expired sealed segments from a single partition.
    async fn delete_expired_segments_for_partition(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        now: IggyTimestamp,
        expiry: IggyExpiry,
    ) -> Result<(u64, u64), IggyError> {
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

        let expired_offsets: Vec<u64> = {
            let partitions = self.local_partitions.borrow();
            let Some(partition) = partitions.get(&ns) else {
                return Ok((0, 0));
            };
            let segments = partition.log.segments();
            let last_idx = segments.len().saturating_sub(1);

            segments
                .iter()
                .enumerate()
                .filter(|(idx, seg)| *idx != last_idx && seg.is_expired(now, expiry))
                .map(|(_, seg)| seg.start_offset)
                .collect()
        };

        let mut total_segments = 0u64;
        let mut total_messages = 0u64;
        for offset in expired_offsets {
            let (s, m) = self
                .remove_segment_by_offset(stream_id, topic_id, partition_id, offset)
                .await?;
            total_segments += s;
            total_messages += m;
        }
        Ok((total_segments, total_messages))
    }

    /// Finds the oldest sealed segment across the given partitions, comparing by timestamp.
    /// Returns `(partition_id, start_offset)` or `None` if no deletable segments exist.
    fn find_oldest_sealed_segment(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_ids: &[usize],
    ) -> Option<(usize, u64)> {
        let partitions = self.local_partitions.borrow();
        let mut oldest: Option<(usize, u64, u64)> = None;

        for &partition_id in partition_ids {
            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
            let Some(partition) = partitions.get(&ns) else {
                continue;
            };

            let segments = partition.log.segments();
            if segments.len() <= 1 {
                continue;
            }

            let first = &segments[0];
            if !first.sealed {
                continue;
            }

            match &oldest {
                None => oldest = Some((partition_id, first.start_offset, first.start_timestamp)),
                Some((_, _, ts)) if first.start_timestamp < *ts => {
                    oldest = Some((partition_id, first.start_offset, first.start_timestamp));
                }
                _ => {}
            }
        }

        oldest.map(|(pid, offset, _)| (pid, offset))
    }

    /// Removes a single segment identified by its start_offset from the given partition.
    /// Skips if the segment no longer exists or is the active (last) segment.
    async fn remove_segment_by_offset(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> Result<(u64, u64), IggyError> {
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

        let removed = {
            let mut partitions = self.local_partitions.borrow_mut();
            let Some(partition) = partitions.get_mut(&ns) else {
                return Ok((0, 0));
            };

            let log = &mut partition.log;
            let last_idx = log.segments().len().saturating_sub(1);

            let Some(idx) = log
                .segments()
                .iter()
                .position(|s| s.start_offset == start_offset)
            else {
                return Ok((0, 0));
            };

            if idx == last_idx {
                tracing::warn!(
                    "Refusing to delete active segment (start_offset: {start_offset}) \
                     for partition ID: {partition_id}"
                );
                return Ok((0, 0));
            }

            let segment = log.segments_mut().remove(idx);
            let storage = log.storages_mut().remove(idx);
            log.indexes_mut().remove(idx);

            Some((segment, storage, partition.stats.clone()))
        };

        let Some((segment, mut storage, stats)) = removed else {
            return Ok((0, 0));
        };

        let segment_size = segment.size.as_bytes_u64();
        let end_offset = segment.end_offset;
        let messages_in_segment = if start_offset == end_offset {
            0
        } else {
            (end_offset - start_offset) + 1
        };

        let _ = storage.shutdown();
        let (messages_path, index_path) = storage.segment_and_index_paths();

        if let Some(path) = messages_path
            && let Err(e) = compio::fs::remove_file(&path).await
        {
            tracing::error!("Failed to delete messages file {}: {}", path, e);
        }

        if let Some(path) = index_path
            && let Err(e) = compio::fs::remove_file(&path).await
        {
            tracing::error!("Failed to delete index file {}: {}", path, e);
        }

        stats.decrement_size_bytes(segment_size);
        stats.decrement_segments_count(1);
        stats.decrement_messages_count(messages_in_segment);

        tracing::info!(
            "Deleted segment (start: {}, end: {}, size: {}, messages: {}) from partition {}",
            start_offset,
            end_offset,
            segment_size,
            messages_in_segment,
            partition_id
        );

        Ok((1, messages_in_segment))
    }

    /// Deletes the N oldest **sealed** segments from a partition, preserving the active segment
    /// and partition offset. Reuses `remove_segment_by_offset` — same logic as the message cleaner.
    ///
    /// Segments containing unconsumed messages are protected: a segment is only eligible for
    /// deletion if `end_offset <= min_committed_offset` across all consumers and consumer groups.
    /// If no consumers exist, there is no barrier.
    pub(crate) async fn delete_oldest_segments(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        segments_count: u32,
    ) -> Result<(u64, u64), IggyError> {
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

        let sealed_offsets: Vec<u64> = {
            let partitions = self.local_partitions.borrow();
            let Some(partition) = partitions.get(&ns) else {
                return Ok((0, 0));
            };

            let min_committed = Self::min_committed_offset(
                &partition.consumer_offsets,
                &partition.consumer_group_offsets,
            );

            let segments = partition.log.segments();
            let last_idx = segments.len().saturating_sub(1);

            segments
                .iter()
                .enumerate()
                .filter(|(idx, seg)| {
                    *idx != last_idx
                        && seg.sealed
                        && min_committed.is_none_or(|barrier| seg.end_offset <= barrier)
                })
                .map(|(_, seg)| seg.start_offset)
                .take(segments_count as usize)
                .collect()
        };

        let mut total_segments = 0u64;
        let mut total_messages = 0u64;
        for offset in sealed_offsets {
            let (s, m) = self
                .remove_segment_by_offset(stream_id, topic_id, partition_id, offset)
                .await?;
            total_segments += s;
            total_messages += m;
        }
        Ok((total_segments, total_messages))
    }

    /// Returns the minimum committed offset across all consumers and consumer groups,
    /// or `None` if no consumers exist (no barrier).
    fn min_committed_offset(
        consumer_offsets: &crate::streaming::partitions::consumer_offsets::ConsumerOffsets,
        consumer_group_offsets: &crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets,
    ) -> Option<u64> {
        let co_guard = consumer_offsets.pin();
        let cg_guard = consumer_group_offsets.pin();
        let consumers = co_guard
            .iter()
            .map(|(_, co)| co.offset.load(std::sync::atomic::Ordering::Relaxed));
        let groups = cg_guard
            .iter()
            .map(|(_, co)| co.offset.load(std::sync::atomic::Ordering::Relaxed));
        consumers.chain(groups).min()
    }

    /// Drains all segments, deletes their files, and re-initializes the partition log at offset 0.
    /// Used exclusively by `purge_topic_inner` — a destructive full reset.
    pub(crate) async fn purge_all_segments(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let namespace = IggyNamespace::new(stream_id, topic_id, partition_id);

        let (segments, storages, stats) = {
            let mut partitions = self.local_partitions.borrow_mut();
            let partition = partitions
                .get_mut(&namespace)
                .expect("purge_all_segments: partition must exist in local_partitions");

            let upperbound = partition.log.segments().len();
            let segments = partition
                .log
                .segments_mut()
                .drain(..upperbound)
                .collect::<Vec<_>>();
            let storages = partition
                .log
                .storages_mut()
                .drain(..upperbound)
                .collect::<Vec<_>>();
            let _ = partition
                .log
                .indexes_mut()
                .drain(..upperbound)
                .collect::<Vec<_>>();
            (segments, storages, partition.stats.clone())
        };

        for (mut storage, segment) in storages.into_iter().zip(segments.into_iter()) {
            let (msg_writer, index_writer) = storage.shutdown();
            let start_offset = segment.start_offset;

            let log_path = if let Some(msg_writer) = msg_writer {
                let path = msg_writer.path();
                drop(msg_writer);
                path
            } else {
                self.config.system.get_messages_file_path(
                    stream_id,
                    topic_id,
                    partition_id,
                    start_offset,
                )
            };
            drop(index_writer);

            let index_path =
                self.config
                    .system
                    .get_index_path(stream_id, topic_id, partition_id, start_offset);

            for path in [&log_path, &index_path] {
                match compio::fs::remove_file(path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!("File already gone at path: {path}");
                    }
                    Err(e) => {
                        tracing::error!("Failed to delete file at path: {path}, err: {e}");
                        return Err(IggyError::CannotDeleteFile);
                    }
                }
            }
        }

        self.init_log_in_local_partitions(&namespace).await?;
        stats.increment_segments_count(1);
        Ok(())
    }

    /// Creates a fresh segment at offset 0 after all segments have been drained.
    ///
    /// The log is momentarily empty between `delete_segments`' drain and this call, which is
    /// safe because the message pump serializes all handlers — no concurrent operation can
    /// observe the empty state.
    async fn init_log_in_local_partitions(
        &self,
        namespace: &IggyNamespace,
    ) -> Result<(), IggyError> {
        use crate::streaming::segments::storage::create_segment_storage;

        let start_offset = 0;
        let segment = Segment::new(start_offset, self.config.system.segment.size);

        let storage = create_segment_storage(
            &self.config.system,
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            0, // messages_size
            0, // indexes_size
            start_offset,
        )
        .await?;

        let mut partitions = self.local_partitions.borrow_mut();
        if let Some(partition) = partitions.get_mut(namespace) {
            partition.log.add_persisted_segment(segment, storage);
            // Reset offset when starting fresh with a new segment at offset 0
            partition
                .offset
                .store(start_offset, std::sync::atomic::Ordering::SeqCst);
            partition.should_increment_offset = false;
        }
        Ok(())
    }

    /// Rotate to a new segment when the current segment is full.
    /// The new segment starts at the next offset after the current segment's end.
    /// Seals the old segment so it becomes eligible for expiry-based cleanup.
    ///
    /// Safety: called exclusively from the message pump (via append handler) — the captured
    /// `old_segment_index` remains valid across the `create_segment_storage` await because
    /// no other handler can modify the segment vec while this frame is in progress.
    pub(crate) async fn rotate_segment_in_local_partitions(
        &self,
        namespace: &IggyNamespace,
    ) -> Result<(), IggyError> {
        use crate::streaming::segments::storage::create_segment_storage;

        let (start_offset, old_segment_index) = {
            let mut partitions = self.local_partitions.borrow_mut();
            let partition = partitions
                .get_mut(namespace)
                .expect("rotate_segment: partition must exist");
            let old_segment_index = partition.log.segments().len() - 1;
            let active_segment = partition.log.active_segment_mut();
            active_segment.sealed = true;
            (active_segment.end_offset + 1, old_segment_index)
        };

        let segment = Segment::new(start_offset, self.config.system.segment.size);

        let storage = create_segment_storage(
            &self.config.system,
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            0, // messages_size
            0, // indexes_size
            start_offset,
        )
        .await?;

        let mut partitions = self.local_partitions.borrow_mut();
        if let Some(partition) = partitions.get_mut(namespace) {
            // Clear old segment's indexes if cache_indexes is not set to All.
            // This prevents memory accumulation from keeping index buffers for sealed segments.
            if !matches!(
                self.config.system.segment.cache_indexes,
                CacheIndexesConfig::All
            ) {
                partition.log.indexes_mut()[old_segment_index] = None;
            }

            // Close writers for the sealed segment - they're never needed after sealing.
            // This releases file handles and associated kernel/io_uring resources.
            let old_storage = &mut partition.log.storages_mut()[old_segment_index];
            let _ = old_storage.shutdown();

            partition.log.add_persisted_segment(segment, storage);
            partition.stats.increment_segments_count(1);
            tracing::info!(
                "Rotated to new segment at offset {} for partition {} (stream {}, topic {})",
                start_offset,
                namespace.partition_id(),
                namespace.stream_id(),
                namespace.topic_id()
            );
        }
        Ok(())
    }
}
