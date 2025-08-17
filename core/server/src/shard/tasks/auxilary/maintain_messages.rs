use crate::{
    archiver::ArchiverKind, map_toggle_str, shard::IggyShard, shard_debug, shard_error, shard_info,
    shard_trace, streaming::topics::topic::Topic,
};
use compio::time;
use error_set::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, IggyTimestamp, locking::IggyRwLockFn};
use std::{rc::Rc, time::Duration};

pub async fn spawn_message_maintainance_task(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    let config = &shard.config.data_maintenance.messages;
    if !config.cleaner_enabled && !config.archiver_enabled {
        shard_info!(shard.id, "Messages maintainer is disabled.");
        return Ok(());
    }

    let interval = config.interval;
    shard_info!(
        shard.id,
        "Message maintainer, cleaner is {}, archiver is {}, interval: {interval}",
        map_toggle_str(config.cleaner_enabled),
        map_toggle_str(config.archiver_enabled)
    );
    let clean_messages = config.cleaner_enabled;
    let mut interval_timer = time::interval(interval.get_duration());
    loop {
        let shutdown_check = async {
            loop {
                if shard.is_shutting_down() {
                    return;
                }
                compio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let archiver = shard.archiver.clone();
        let fut = interval_timer.tick();

        futures::select! {
            _ = shutdown_check.fuse() => {
                shard_info!(shard.id, "Message maintainer shutting down");
                break;
            }
            _ = fut.fuse() => {
                let streams = shard.get_streams();
                for stream in streams {
                    let topics = stream.get_topics();
                    for topic in topics {
                        let expired_segments = handle_expired_segments(
                            shard.id,
                            topic,
                            archiver.clone(),
                            shard.config.system.segment.archive_expired,
                            clean_messages,
                        )
                        .await;
                        if expired_segments.is_err() {
                            shard_error!(shard.id, "Failed to get expired segments for stream ID: {}, topic ID: {}", topic.stream_id, topic.topic_id);
                            continue;
                        }

                        let stream_id = stream.stream_id;
                        let oldest_segments = handle_oldest_segments(
                            shard.id,
                            stream_id,
                            topic,
                            archiver.clone(),
                            shard.config.system.topic.delete_oldest_segments,
                        )
                        .await;
                        if oldest_segments.is_err() {
                            shard_error!(shard.id, "Failed to get oldest segments for stream ID: {}, topic ID: {}", topic.stream_id, topic.topic_id);
                            continue;
                        }

                        let deleted_expired_segments = expired_segments.unwrap();
                        let deleted_oldest_segments = oldest_segments.unwrap();
                        let deleted_segments = HandledSegments {
                            segments_count: deleted_expired_segments.segments_count
                                + deleted_oldest_segments.segments_count,
                            messages_count: deleted_expired_segments.messages_count
                                + deleted_oldest_segments.messages_count,
                        };

                        if deleted_segments.segments_count == 0 {
                            shard_trace!(shard.id,
                                "No segments were deleted for stream ID: {}, topic ID: {}",
                                topic.stream_id, topic.topic_id
                            );
                            continue;
                        }

                        shard_info!(shard.id,
                            "Deleted {} segments and {} messages for stream ID: {}, topic ID: {}",
                            deleted_segments.segments_count,
                            deleted_segments.messages_count,
                            topic.stream_id,
                            topic.topic_id
                        );

                        shard.metrics.decrement_segments(deleted_segments.segments_count);
                        shard.metrics.decrement_messages(deleted_segments.messages_count);
                    }
            }
        }
        }
    }
    Ok(())
}

async fn handle_expired_segments(
    shard_id: u16,
    topic: &Topic,
    archiver: Option<Rc<ArchiverKind>>,
    archive: bool,
    clean: bool,
) -> Result<HandledSegments, IggyError> {
    let expired_segments = get_expired_segments(shard_id, topic, IggyTimestamp::now()).await;
    if expired_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    if archive {
        if let Some(archiver) = archiver {
            shard_info!(
                shard_id,
                "Archiving expired segments for stream ID: {}, topic ID: {}",
                topic.stream_id,
                topic.topic_id
            );
            archive_segments(shard_id, topic, &expired_segments, archiver.clone()).await.with_error_context(|error| {
                format!("CHANNEL_COMMAND - failed to archive expired segments for stream ID: {}, topic ID: {}. {error}", topic.stream_id, topic.topic_id)
            })?;
        } else {
            shard_error!(
                shard_id,
                "Archiver is not enabled, yet archive_expired is set to true. Cannot archive expired segments for stream ID: {}, topic ID: {}",
                topic.stream_id,
                topic.topic_id
            );
            return Ok(HandledSegments::none());
        }
    }

    if clean {
        shard_info!(
            shard_id,
            "Deleting expired segments for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        delete_segments(shard_id, topic, &expired_segments).await
    } else {
        shard_info!(
            shard_id,
            "Deleting expired segments is disabled for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        Ok(HandledSegments::none())
    }
}

async fn get_expired_segments(
    shard_id: u16,
    topic: &Topic,
    now: IggyTimestamp,
) -> Vec<SegmentsToHandle> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        shard_debug!(
            shard_id,
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        return Vec::new();
    }

    shard_debug!(
        shard_id,
        "Found {} expired segments for stream ID: {}, topic ID: {}",
        expired_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    expired_segments
        .into_iter()
        .map(|(partition_id, start_offsets)| SegmentsToHandle {
            partition_id,
            start_offsets,
        })
        .collect()
}

async fn handle_oldest_segments(
    shard_id: u16,
    stream_id: u32,
    topic: &Topic,
    archiver: Option<Rc<ArchiverKind>>,
    delete_oldest_segments: bool,
) -> Result<HandledSegments, IggyError> {
    if let Some(archiver) = archiver {
        let mut segments_to_archive = Vec::new();
        for partition in topic.partitions.values() {
            let mut start_offsets = Vec::new();
            let partition = partition.read().await;
            for segment in partition.get_segments().iter().filter(|s| s.is_closed()) {
                let is_archived = archiver.is_archived(segment.index_file_path(), None).await;
                if is_archived.is_err() {
                    shard_error!(
                        shard_id,
                        "Failed to check if segment with start offset: {} is archived for stream ID: {}, topic ID: {}, partition ID: {}. Error: {}",
                        segment.start_offset(),
                        topic.stream_id,
                        topic.topic_id,
                        partition.partition_id,
                        is_archived.err().unwrap()
                    );
                    continue;
                }

                if !is_archived.unwrap() {
                    shard_debug!(
                        shard_id,
                        "Segment with start offset: {} is not archived for stream ID: {}, topic ID: {}, partition ID: {}",
                        segment.start_offset(),
                        topic.stream_id,
                        topic.topic_id,
                        partition.partition_id
                    );
                    start_offsets.push(segment.start_offset());
                }
            }

            if !start_offsets.is_empty() {
                shard_info!(
                    shard_id,
                    "Found {} segments to archive for stream ID: {}, topic ID: {}, partition ID: {}",
                    start_offsets.len(),
                    topic.stream_id,
                    topic.topic_id,
                    partition.partition_id
                );
                segments_to_archive.push(SegmentsToHandle {
                    partition_id: partition.partition_id,
                    start_offsets,
                });
            }
        }

        let segments_count = segments_to_archive
            .iter()
            .map(|s| s.start_offsets.len())
            .sum::<usize>();
        shard_info!(
            shard_id,
            "Archiving {} oldest segments for stream ID: {}, topic ID: {}...",
            segments_count,
            topic.stream_id,
            topic.topic_id
        );
        archive_segments(shard_id, topic, &segments_to_archive, archiver.clone())
            .await
            .with_error_context(|error| {
                format!(
                    "CHANNEL_COMMAND - failed to archive segments for stream ID: {}, topic ID: {}. {error}",
                    topic.stream_id, topic.topic_id
                )
            })?;
    }

    if topic.is_unlimited() {
        shard_debug!(
            shard_id,
            "Topic is unlimited, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !delete_oldest_segments {
        shard_debug!(
            shard_id,
            "Delete oldest segments is disabled, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !topic.is_almost_full() {
        shard_debug!(
            shard_id,
            "Topic is not almost full, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    let oldest_segments = get_oldest_segments(shard_id, topic).await;
    if oldest_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    delete_segments(shard_id, topic, &oldest_segments).await
}

async fn get_oldest_segments(shard_id: u16, topic: &Topic) -> Vec<SegmentsToHandle> {
    let mut oldest_segments = Vec::new();
    for partition in topic.partitions.values() {
        let partition = partition.read().await;
        if let Some(segment) = partition.get_segments().first() {
            if !segment.is_closed() {
                continue;
            }

            oldest_segments.push(SegmentsToHandle {
                partition_id: partition.partition_id,
                start_offsets: vec![segment.start_offset()],
            });
        }
    }

    if oldest_segments.is_empty() {
        shard_debug!(
            shard_id,
            "No oldest segments found for stream ID: {}, topic ID: {}",
            topic.stream_id,
            topic.topic_id
        );
        return oldest_segments;
    }

    shard_info!(
        shard_id,
        "Found {} oldest segments for stream ID: {}, topic ID: {}.",
        oldest_segments.len(),
        topic.stream_id,
        topic.topic_id
    );

    oldest_segments
}

#[derive()]
struct SegmentsToHandle {
    partition_id: u32,
    start_offsets: Vec<u64>,
}

#[derive(Debug)]
struct HandledSegments {
    pub segments_count: u32,
    pub messages_count: u64,
}

impl HandledSegments {
    pub fn none() -> Self {
        Self {
            segments_count: 0,
            messages_count: 0,
        }
    }
}

async fn archive_segments(
    shard_id: u16,
    topic: &Topic,
    segments_to_archive: &[SegmentsToHandle],
    archiver: Rc<ArchiverKind>,
) -> Result<u64, IggyError> {
    if segments_to_archive.is_empty() {
        return Ok(0);
    }

    let segments_count = segments_to_archive
        .iter()
        .map(|s| s.start_offsets.len())
        .sum::<usize>();
    shard_info!(
        shard_id,
        "Found {} segments to archive for stream ID: {}, topic ID: {}, archiving...",
        segments_count,
        topic.stream_id,
        topic.topic_id
    );

    let mut archived_segments = 0;
    for segment_to_archive in segments_to_archive {
        match topic.get_partition(segment_to_archive.partition_id) {
            Ok(partition) => {
                let partition = partition.read().await;
                for start_offset in &segment_to_archive.start_offsets {
                    let segment = partition.get_segment(*start_offset);
                    if segment.is_none() {
                        shard_error!(
                            shard_id,
                            "Segment with start offset: {} not found for stream ID: {}, topic ID: {}, partition ID: {}",
                            start_offset,
                            topic.stream_id,
                            topic.topic_id,
                            partition.partition_id
                        );
                        continue;
                    }

                    let segment = segment.unwrap();
                    let files = [segment.index_file_path(), segment.messages_file_path()];
                    if let Err(error) = archiver.archive(&files, None).await {
                        shard_error!(
                            shard_id,
                            "Failed to archive segment with start offset: {} for stream ID: {}, topic ID: {}, partition ID: {}. Error: {}",
                            start_offset,
                            topic.stream_id,
                            topic.topic_id,
                            partition.partition_id,
                            error
                        );
                        continue;
                    }
                    shard_info!(
                        shard_id,
                        "Archived Segment with start offset: {}, for stream ID: {}, topic ID: {}, partition ID: {}",
                        start_offset,
                        topic.stream_id,
                        topic.topic_id,
                        partition.partition_id
                    );
                    archived_segments += 1;
                }
            }
            Err(error) => {
                shard_error!(
                    shard_id,
                    "Partition with ID: {} was not found for stream ID: {}, topic ID: {}. Error: {}",
                    segment_to_archive.partition_id,
                    topic.stream_id,
                    topic.topic_id,
                    error
                );
                continue;
            }
        }
    }

    Ok(archived_segments)
}

async fn delete_segments(
    shard_id: u16,
    topic: &Topic,
    segments_to_delete: &[SegmentsToHandle],
) -> Result<HandledSegments, IggyError> {
    shard_info!(
        shard_id,
        "Deleting {} segments for stream ID: {}, topic ID: {}...",
        segments_to_delete.len(),
        topic.stream_id,
        topic.topic_id
    );

    let mut segments_count = 0;
    let mut messages_count = 0;
    for segment_to_delete in segments_to_delete {
        match topic.get_partition(segment_to_delete.partition_id) {
            Ok(partition) => {
                let mut partition = partition.write().await;
                let mut last_end_offset = 0;
                for start_offset in &segment_to_delete.start_offsets {
                    let deleted_segment = partition.delete_segment(*start_offset).await.with_error_context(|error| {
                        format!("CHANNEL_COMMAND - failed to delete segment for stream with ID: {}, topic with ID: {}. {error}", topic.stream_id, topic.topic_id)
                    })?;
                    last_end_offset = deleted_segment.end_offset;
                    segments_count += 1;
                    messages_count += deleted_segment.messages_count as u64;
                }

                if partition.get_segments().is_empty() {
                    let start_offset = last_end_offset + 1;
                    partition.add_persisted_segment(start_offset).await.with_error_context(|error| {
                        format!("CHANNEL_COMMAND - failed to add persisted segment for stream with ID: {}, topic with ID: {}. {error}", topic.stream_id, topic.topic_id)
                    })?;
                }
            }
            Err(error) => {
                shard_error!(
                    shard_id,
                    "Partition with ID: {} was not found for stream with ID: {}, topic with ID: {}. {error}",
                    segment_to_delete.partition_id,
                    topic.stream_id,
                    topic.topic_id
                );
                continue;
            }
        }
    }

    Ok(HandledSegments {
        segments_count,
        messages_count,
    })
}
