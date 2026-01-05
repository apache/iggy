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

use crate::shard::IggyShard;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, IggyTimestamp};
use std::rc::Rc;
use tracing::{debug, error, info, trace, warn};

pub fn spawn_message_cleaner(shard: Rc<IggyShard>) {
    if !shard.config.data_maintenance.messages.cleaner_enabled {
        info!("Message cleaner is disabled.");
        return;
    }

    let period = shard
        .config
        .data_maintenance
        .messages
        .interval
        .get_duration();
    info!(
        "Message cleaner is enabled, expired segments will be automatically deleted every: {:?}",
        period
    );
    let shard_clone = shard.clone();
    shard
        .task_registry
        .periodic("clean_messages")
        .every(period)
        .tick(move |_shutdown| clean_expired_messages(shard_clone.clone()))
        .spawn();
}

async fn clean_expired_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Cleaning expired messages...");

    let namespaces = shard.get_current_shard_namespaces();
    let now = IggyTimestamp::now();
    let delete_oldest_segments = shard.config.system.topic.delete_oldest_segments;

    let mut topics: std::collections::HashMap<(usize, usize), Vec<usize>> =
        std::collections::HashMap::new();

    for ns in namespaces {
        let stream_id = ns.stream_id();
        let topic_id = ns.topic_id();
        topics
            .entry((stream_id, topic_id))
            .or_default()
            .push(ns.partition_id());
    }

    let mut total_deleted_segments = 0u64;
    let mut total_deleted_messages = 0u64;

    for ((stream_id, topic_id), partition_ids) in topics {
        let mut topic_deleted_segments = 0u64;
        let mut topic_deleted_messages = 0u64;

        for partition_id in partition_ids {
            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

            // Handle expired segments
            let expired_result = handle_expired_segments(&shard, &ns, now).await;

            match expired_result {
                Ok(deleted) => {
                    topic_deleted_segments += deleted.segments_count;
                    topic_deleted_messages += deleted.messages_count;
                }
                Err(err) => {
                    error!(
                        "Failed to clean expired segments for {:?}. Error: {}",
                        ns, err
                    );
                }
            }

            // Handle oldest segments if topic size management is enabled
            if delete_oldest_segments {
                let oldest_result = handle_oldest_segments(&shard, &ns, stream_id, topic_id).await;

                match oldest_result {
                    Ok(deleted) => {
                        topic_deleted_segments += deleted.segments_count;
                        topic_deleted_messages += deleted.messages_count;
                    }
                    Err(err) => {
                        error!(
                            "Failed to clean oldest segments for {:?}. Error: {}",
                            ns, err
                        );
                    }
                }
            }
        }

        if topic_deleted_segments > 0 {
            info!(
                "Deleted {} segments and {} messages for stream ID: {}, topic ID: {}",
                topic_deleted_segments, topic_deleted_messages, stream_id, topic_id
            );
            total_deleted_segments += topic_deleted_segments;
            total_deleted_messages += topic_deleted_messages;

            // Update metrics
            shard
                .metrics
                .decrement_segments(topic_deleted_segments as u32);
            shard.metrics.decrement_messages(topic_deleted_messages);
        } else {
            trace!(
                "No segments were deleted for stream ID: {}, topic ID: {}",
                stream_id, topic_id
            );
        }
    }

    if total_deleted_segments > 0 {
        info!(
            "Total cleaned: {} segments and {} messages",
            total_deleted_segments, total_deleted_messages
        );
    }

    Ok(())
}

#[derive(Debug, Default)]
struct DeletedSegments {
    pub segments_count: u64,
    pub messages_count: u64,
}

async fn handle_expired_segments(
    shard: &Rc<IggyShard>,
    ns: &IggyNamespace,
    now: IggyTimestamp,
) -> Result<DeletedSegments, IggyError> {
    // Check if partition exists in partition_store
    let rc_partition = match shard.partition_store.borrow().get_rc(ns) {
        Some(rc) => rc,
        None => return Ok(DeletedSegments::default()),
    };

    // Get expired segment offsets
    let expired_segment_offsets = {
        let partition_data = rc_partition.borrow();
        let mut expired = Vec::new();
        for segment in partition_data.log.segments() {
            if segment.is_expired(now) {
                expired.push(segment.start_offset);
            }
        }
        expired
    };

    if expired_segment_offsets.is_empty() {
        return Ok(DeletedSegments::default());
    }

    debug!(
        "Found {} expired segments for {:?}",
        expired_segment_offsets.len(),
        ns
    );

    delete_segments(shard, ns, &expired_segment_offsets).await
}

async fn handle_oldest_segments(
    shard: &Rc<IggyShard>,
    ns: &IggyNamespace,
    stream_id: usize,
    topic_id: usize,
) -> Result<DeletedSegments, IggyError> {
    // Get topic info from SharedMetadata via snapshot
    let snapshot = shard.shared_metadata.load();
    let stream_meta = snapshot.streams.get(&stream_id).ok_or_else(|| {
        IggyError::StreamIdNotFound(iggy_common::Identifier::numeric(stream_id as u32).unwrap())
    })?;
    let topic_meta = stream_meta.get_topic(topic_id).ok_or_else(|| {
        IggyError::TopicIdNotFound(
            iggy_common::Identifier::numeric(topic_id as u32).unwrap(),
            iggy_common::Identifier::numeric(stream_id as u32).unwrap(),
        )
    })?;

    use iggy_common::MaxTopicSize;

    // Check if topic has a size limit
    let max_size_bytes: Option<u64> = match topic_meta.max_topic_size {
        MaxTopicSize::ServerDefault => {
            // Use server default
            match shard.config.system.topic.max_size {
                MaxTopicSize::Custom(size) => Some(size.as_bytes_u64()),
                MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => None,
            }
        }
        MaxTopicSize::Custom(size) => Some(size.as_bytes_u64()),
        MaxTopicSize::Unlimited => None,
    };

    if max_size_bytes.is_none() {
        debug!(
            "Topic is unlimited, oldest segments will not be deleted for {:?}",
            ns
        );
        return Ok(DeletedSegments::default());
    }

    // Get topic stats to check if almost full
    let topic_stats = shard.shared_stats.get_topic_stats(stream_id, topic_id);
    drop(snapshot); // Release the guard before await

    let is_almost_full = if let (Some(max_size), Some(stats)) = (max_size_bytes, &topic_stats) {
        let current_size = stats.size_bytes_inconsistent();
        // Consider "almost full" when at 90% capacity
        current_size >= (max_size as f64 * 0.9) as u64
    } else {
        false
    };

    if !is_almost_full {
        debug!(
            "Topic is not almost full, oldest segments will not be deleted for {:?}",
            ns
        );
        return Ok(DeletedSegments::default());
    }

    // Check if partition exists in partition_store
    let rc_partition = match shard.partition_store.borrow().get_rc(ns) {
        Some(rc) => rc,
        None => return Ok(DeletedSegments::default()),
    };

    // Get oldest segment offset (first closed segment, not the active one)
    let oldest_segment_offset = {
        let partition_data = rc_partition.borrow();
        let segments = partition_data.log.segments();
        if segments.len() > 1 {
            segments.first().map(|s| s.start_offset)
        } else {
            None
        }
    };

    if let Some(start_offset) = oldest_segment_offset {
        info!(
            "Deleting oldest segment with start offset {} for {:?}",
            start_offset, ns
        );

        delete_segments(shard, ns, &[start_offset]).await
    } else {
        debug!("No closed segments found to delete for {:?}", ns);
        Ok(DeletedSegments::default())
    }
}

async fn delete_segments(
    shard: &Rc<IggyShard>,
    ns: &IggyNamespace,
    segment_offsets: &[u64],
) -> Result<DeletedSegments, IggyError> {
    if segment_offsets.is_empty() {
        return Ok(DeletedSegments::default());
    }

    info!(
        "Deleting {} segments for {:?}...",
        segment_offsets.len(),
        ns
    );

    // Get partition from partition_store
    let rc_partition = match shard.partition_store.borrow().get_rc(ns) {
        Some(rc) => rc,
        None => return Ok(DeletedSegments::default()),
    };

    let mut segments_count = 0u64;
    let mut messages_count = 0u64;

    // Extract segments and storages to delete
    let (stats, segments_to_delete, mut storages_to_delete) = {
        let mut partition_data = rc_partition.borrow_mut();
        let mut segments_to_remove = Vec::new();
        let mut storages_to_remove = Vec::new();

        let mut indices_to_remove: Vec<usize> = Vec::new();
        for &start_offset in segment_offsets {
            if let Some(idx) = partition_data
                .log
                .segments()
                .iter()
                .position(|s| s.start_offset == start_offset)
            {
                indices_to_remove.push(idx);
            }
        }

        indices_to_remove.sort_by(|a, b| b.cmp(a));
        for idx in indices_to_remove {
            let segment = partition_data.log.segments_mut().remove(idx);
            let storage = partition_data.log.storages_mut().remove(idx);
            partition_data.log.indexes_mut().remove(idx);

            segments_to_remove.push(segment);
            storages_to_remove.push(storage);
        }

        (
            partition_data.stats.clone(),
            segments_to_remove,
            storages_to_remove,
        )
    };

    for (segment, storage) in segments_to_delete
        .into_iter()
        .zip(storages_to_delete.iter_mut())
    {
        let segment_size = segment.size.as_bytes_u64();
        let start_offset = segment.start_offset;
        let end_offset = segment.end_offset;

        let approx_messages = if (end_offset - start_offset) == 0 {
            0
        } else {
            (end_offset - start_offset) + 1
        };

        let _ = storage.shutdown();
        let (messages_path, index_path) = storage.segment_and_index_paths();

        if let Some(path) = messages_path {
            if let Err(e) = compio::fs::remove_file(&path).await {
                error!("Failed to delete messages file {}: {}", path, e);
            } else {
                trace!("Deleted messages file: {}", path);
            }
        } else {
            warn!(
                "Messages writer path not found for segment starting at offset {}",
                start_offset
            );
        }

        if let Some(path) = index_path {
            if let Err(e) = compio::fs::remove_file(&path).await {
                error!("Failed to delete index file {}: {}", path, e);
            } else {
                trace!("Deleted index file: {}", path);
            }

            let time_index_path = path.replace(".index", ".timeindex");
            if let Err(e) = compio::fs::remove_file(&time_index_path).await {
                trace!(
                    "Could not delete time index file {}: {}",
                    time_index_path, e
                );
            }
        } else {
            warn!(
                "Index writer path not found for segment starting at offset {}",
                start_offset
            );
        }

        stats.decrement_size_bytes(segment_size);
        stats.decrement_segments_count(1);
        stats.decrement_messages_count(approx_messages);

        info!(
            "Deleted segment with start offset {} (end: {}, size: {}, messages: {}) for {:?}",
            start_offset, end_offset, segment_size, approx_messages, ns
        );

        segments_count += 1;
        messages_count += approx_messages;
    }

    Ok(DeletedSegments {
        segments_count,
        messages_count,
    })
}
