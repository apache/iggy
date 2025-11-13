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
use crate::streaming::topics::helpers as topics_helpers;
use iggy_common::{Identifier, IggyError, IggyTimestamp};
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
        let stream_identifier = Identifier::numeric(stream_id as u32).unwrap();
        let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();

        let mut topic_deleted_segments = 0u64;
        let mut topic_deleted_messages = 0u64;

        for partition_id in partition_ids {
            // Handle expired segments
            let expired_result = handle_expired_segments(
                &shard,
                &stream_identifier,
                &topic_identifier,
                partition_id,
                now,
            )
            .await;

            match expired_result {
                Ok(deleted) => {
                    topic_deleted_segments += deleted.segments_count;
                    topic_deleted_messages += deleted.messages_count;
                }
                Err(err) => {
                    error!(
                        "Failed to clean expired segments for stream ID: {}, topic ID: {}, partition ID: {}. Error: {}",
                        stream_id, topic_id, partition_id, err
                    );
                }
            }

            // Handle oldest segments if topic size management is enabled
            if delete_oldest_segments {
                let oldest_result = handle_oldest_segments(
                    &shard,
                    &stream_identifier,
                    &topic_identifier,
                    partition_id,
                )
                .await;

                match oldest_result {
                    Ok(deleted) => {
                        topic_deleted_segments += deleted.segments_count;
                        topic_deleted_messages += deleted.messages_count;
                    }
                    Err(err) => {
                        error!(
                            "Failed to clean oldest segments for stream ID: {}, topic ID: {}, partition ID: {}. Error: {}",
                            stream_id, topic_id, partition_id, err
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
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: usize,
    now: IggyTimestamp,
) -> Result<DeletedSegments, IggyError> {
    // Get expired segments
    let expired_segment_offsets =
        shard
            .streams
            .with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                let mut expired = Vec::new();
                for segment in log.segments() {
                    if segment.is_expired(now) {
                        expired.push(segment.start_offset);
                    }
                }
                expired
            });

    if expired_segment_offsets.is_empty() {
        return Ok(DeletedSegments::default());
    }

    debug!(
        "Found {} expired segments for stream ID: {}, topic ID: {}, partition ID: {}",
        expired_segment_offsets.len(),
        stream_id,
        topic_id,
        partition_id
    );

    delete_segments(
        shard,
        stream_id,
        topic_id,
        partition_id,
        &expired_segment_offsets,
    )
    .await
}

async fn handle_oldest_segments(
    shard: &Rc<IggyShard>,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: usize,
) -> Result<DeletedSegments, IggyError> {
    let topic_info =
        shard
            .streams
            .with_topic_by_id(stream_id, topic_id, topics_helpers::get_topic_size_info());

    let (is_unlimited, is_almost_full) = topic_info;

    if is_unlimited {
        debug!(
            "Topic is unlimited, oldest segments will not be deleted for stream ID: {}, topic ID: {}, partition ID: {}",
            stream_id, topic_id, partition_id
        );
        return Ok(DeletedSegments::default());
    }

    if !is_almost_full {
        debug!(
            "Topic is not almost full, oldest segments will not be deleted for stream ID: {}, topic ID: {}, partition ID: {}",
            stream_id, topic_id, partition_id
        );
        return Ok(DeletedSegments::default());
    }

    let oldest_segment_offset =
        shard
            .streams
            .with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                let segments = log.segments();
                // Find the first closed segment (not the active one)
                if segments.len() > 1 {
                    // The last segment is always active, so we look at earlier ones
                    segments.first().map(|s| s.start_offset)
                } else {
                    None
                }
            });

    if let Some(start_offset) = oldest_segment_offset {
        info!(
            "Deleting oldest segment with start offset {} for stream ID: {}, topic ID: {}, partition ID: {}",
            start_offset, stream_id, topic_id, partition_id
        );

        delete_segments(shard, stream_id, topic_id, partition_id, &[start_offset]).await
    } else {
        debug!(
            "No closed segments found to delete for stream ID: {}, topic ID: {}, partition ID: {}",
            stream_id, topic_id, partition_id
        );
        Ok(DeletedSegments::default())
    }
}

async fn delete_segments(
    shard: &Rc<IggyShard>,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: usize,
    segment_offsets: &[u64],
) -> Result<DeletedSegments, IggyError> {
    if segment_offsets.is_empty() {
        return Ok(DeletedSegments::default());
    }

    info!(
        "Deleting {} segments for stream ID: {}, topic ID: {}, partition ID: {}...",
        segment_offsets.len(),
        stream_id,
        topic_id,
        partition_id
    );

    let mut segments_count = 0u64;
    let mut messages_count = 0u64;

    let to_delete = shard.streams.with_partition_by_id_mut(
        stream_id,
        topic_id,
        partition_id,
        |(_, stats, .., log)| {
            let mut segments_to_remove = Vec::new();
            let mut storages_to_remove = Vec::new();

            let mut indices_to_remove: Vec<usize> = Vec::new();
            for &start_offset in segment_offsets {
                if let Some(idx) = log
                    .segments()
                    .iter()
                    .position(|s| s.start_offset == start_offset)
                {
                    indices_to_remove.push(idx);
                }
            }

            indices_to_remove.sort_by(|a, b| b.cmp(a));
            for idx in indices_to_remove {
                let segment = log.segments_mut().remove(idx);
                let storage = log.storages_mut().remove(idx);
                log.indexes_mut().remove(idx);

                segments_to_remove.push(segment);
                storages_to_remove.push(storage);
            }

            (stats.clone(), segments_to_remove, storages_to_remove)
        },
    );

    let (stats, segments_to_delete, mut storages_to_delete) = to_delete;

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
        stats.decrement_messages_count(messages_count);

        info!(
            "Deleted segment with start offset {} (end: {}, size: {}, messages: {}) from partition ID: {}",
            start_offset, end_offset, segment_size, approx_messages, partition_id
        );

        segments_count += 1;
        messages_count += approx_messages;
    }

    Ok(DeletedSegments {
        segments_count,
        messages_count,
    })
}
