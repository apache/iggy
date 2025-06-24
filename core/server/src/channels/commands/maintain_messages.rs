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

//TODO: Fixme
/*

use crate::archiver::ArchiverKind;
use crate::channels::server_command::BackgroundServerCommand;
use crate::configs::server::MessagesMaintenanceConfig;
use crate::map_toggle_str;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use flume::Sender;
use iggy_common::IggyDuration;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::locking::IggySharedMutFn;
use std::sync::Arc;
use tokio::time;
use tracing::{debug, error, info, instrument, trace};

pub struct MessagesMaintainer {
    cleaner_enabled: bool,
    archiver_enabled: bool,
    interval: IggyDuration,
    sender: Sender<MaintainMessagesCommand>,
}

#[derive(Debug, Default, Clone)]
pub struct MaintainMessagesCommand {
    clean_messages: bool,
    archive_messages: bool,
}

#[derive(Debug, Default, Clone)]
pub struct MaintainMessagesExecutor;

impl MessagesMaintainer {
    pub fn new(
        config: &MessagesMaintenanceConfig,
        sender: Sender<MaintainMessagesCommand>,
    ) -> Self {
        Self {
            cleaner_enabled: config.cleaner_enabled,
            archiver_enabled: config.archiver_enabled,
            interval: config.interval,
            sender,
        }
    }

    pub fn start(&self) {
        if !self.cleaner_enabled && !self.archiver_enabled {
            info!("Messages maintainer is disabled.");
            return;
        }

        let interval = self.interval;
        let sender = self.sender.clone();
        info!(
            "Message maintainer, cleaner is {}, archiver is {}, interval: {interval}",
            map_toggle_str(self.cleaner_enabled),
            map_toggle_str(self.archiver_enabled)
        );
        let clean_messages = self.cleaner_enabled;
        let archive_messages = self.archiver_enabled;
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                sender
                    .send(MaintainMessagesCommand {
                        clean_messages,
                        archive_messages,
                    })
                    .unwrap_or_else(|err| {
                        error!("Failed to send MaintainMessagesCommand. Error: {}", err);
                    });
            }
        });
    }
}

impl BackgroundServerCommand<MaintainMessagesCommand> for MaintainMessagesExecutor {
    #[instrument(skip_all, name = "trace_maintain_messages")]
    async fn execute(&mut self, system: &SharedSystem, command: MaintainMessagesCommand) {
        let system = system.read().await;
        let streams = system.get_streams();
        for stream in streams {
            let topics = stream.get_topics();
            for topic in topics {
                let archiver = if command.archive_messages {
                    system.archiver.clone()
                } else {
                    None
                };
                let expired_segments = handle_expired_segments(
                    topic,
                    archiver.clone(),
                    system.config.segment.archive_expired,
                    command.clean_messages,
                )
                .await;
                if expired_segments.is_err() {
                    error!(
                        "Failed to get expired segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                let oldest_segments = handle_oldest_segments(
                    topic,
                    archiver.clone(),
                    system.config.topic.delete_oldest_segments,
                )
                .await;
                if oldest_segments.is_err() {
                    error!(
                        "Failed to get oldest segments for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
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
                    trace!(
                        "No segments were deleted for stream ID: {}, topic ID: {}",
                        topic.stream_id, topic.topic_id
                    );
                    continue;
                }

                info!(
                    "Deleted {} segments and {} messages for stream ID: {}, topic ID: {}",
                    deleted_segments.segments_count,
                    deleted_segments.messages_count,
                    topic.stream_id,
                    topic.topic_id
                );

                system
                    .metrics
                    .decrement_segments(deleted_segments.segments_count);
                system
                    .metrics
                    .decrement_messages(deleted_segments.messages_count);
            }
        }
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        sender: Sender<MaintainMessagesCommand>,
    ) {
        if (!config.data_maintenance.archiver.enabled
            || !config.data_maintenance.messages.archiver_enabled)
            && !config.data_maintenance.messages.cleaner_enabled
        {
            return;
        }

        let messages_maintainer =
            MessagesMaintainer::new(&config.data_maintenance.messages, sender);
        messages_maintainer.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        config: &crate::configs::server::ServerConfig,
        receiver: flume::Receiver<MaintainMessagesCommand>,
    ) {
        if (!config.data_maintenance.archiver.enabled
            || !config.data_maintenance.messages.archiver_enabled)
            && !config.data_maintenance.messages.cleaner_enabled
        {
            return;
        }

        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            info!("Messages maintainer receiver stopped.");
        });
    }
}

async fn handle_expired_segments(
    topic: &Topic,
    archiver: Option<Arc<ArchiverKind>>,
    archive: bool,
    clean: bool,
) -> Result<HandledSegments, IggyError> {
    let expired_segments = get_expired_segments(topic, IggyTimestamp::now()).await;
    if expired_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    if archive {
        if let Some(archiver) = archiver {
            info!(
                "Archiving expired segments for stream ID: {}, topic ID: {}",
                topic.stream_id, topic.topic_id
            );
            archive_segments(topic, &expired_segments, archiver.clone()).await.with_error_context(|error| {
                format!("CHANNEL_COMMAND - failed to archive expired segments for stream ID: {}, topic ID: {}. {error}", topic.stream_id, topic.topic_id)
            })?;
        } else {
            error!(
                "Archiver is not enabled, yet archive_expired is set to true. Cannot archive expired segments for stream ID: {}, topic ID: {}",
                topic.stream_id, topic.topic_id
            );
            return Ok(HandledSegments::none());
        }
    }

    if clean {
        info!(
            "Deleting expired segments for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        delete_segments(topic, &expired_segments).await
    } else {
        info!(
            "Deleting expired segments is disabled for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        Ok(HandledSegments::none())
    }
}

async fn get_expired_segments(topic: &Topic, now: IggyTimestamp) -> Vec<SegmentsToHandle> {
    let expired_segments = topic
        .get_expired_segments_start_offsets_per_partition(now)
        .await;
    if expired_segments.is_empty() {
        debug!(
            "No expired segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Vec::new();
    }

    debug!(
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
    topic: &Topic,
    archiver: Option<Arc<ArchiverKind>>,
    delete_oldest_segments: bool,
) -> Result<HandledSegments, IggyError> {
    if let Some(archiver) = archiver {
        let mut segments_to_archive = Vec::new();
        for partition in topic.partitions.values() {
            let mut start_offsets = Vec::new();
            let partition = partition.read().await;
            for segment in partition.get_segments() {
                if !segment.is_closed() {
                    continue;
                }

                let is_archived = archiver.is_archived(segment.index_file_path(), None).await;
                if is_archived.is_err() {
                    error!(
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
                    debug!(
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
                info!(
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

        info!(
            "Archiving {} oldest segments for stream ID: {}, topic ID: {}...",
            segments_to_archive.len(),
            topic.stream_id,
            topic.topic_id,
        );
        archive_segments(topic, &segments_to_archive, archiver.clone())
            .await
            .with_error_context(|error| {
                format!(
                    "CHANNEL_COMMAND - failed to archive segments for stream ID: {}, topic ID: {}. {error}",
                    topic.stream_id, topic.topic_id
                )
            })?;
    }

    if topic.is_unlimited() {
        debug!(
            "Topic is unlimited, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !delete_oldest_segments {
        debug!(
            "Delete oldest segments is disabled, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    if !topic.is_almost_full() {
        debug!(
            "Topic is not almost full, oldest segments will not be deleted for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return Ok(HandledSegments::none());
    }

    let oldest_segments = get_oldest_segments(topic).await;
    if oldest_segments.is_empty() {
        return Ok(HandledSegments::none());
    }

    delete_segments(topic, &oldest_segments).await
}

async fn get_oldest_segments(topic: &Topic) -> Vec<SegmentsToHandle> {
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
        debug!(
            "No oldest segments found for stream ID: {}, topic ID: {}",
            topic.stream_id, topic.topic_id
        );
        return oldest_segments;
    }

    info!(
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
    topic: &Topic,
    segments_to_archive: &[SegmentsToHandle],
    archiver: Arc<ArchiverKind>,
) -> Result<u64, IggyError> {
    if segments_to_archive.is_empty() {
        return Ok(0);
    }

    info!(
        "Found {} segments to archive for stream ID: {}, topic ID: {}, archiving...",
        segments_to_archive.len(),
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
                        error!(
                            "Segment with start offset: {} not found for stream ID: {}, topic ID: {}, partition ID: {}",
                            start_offset, topic.stream_id, topic.topic_id, partition.partition_id
                        );
                        continue;
                    }

                    let segment = segment.unwrap();
                    let files = [segment.index_file_path(), segment.messages_file_path()];
                    if let Err(error) = archiver.archive(&files, None).await {
                        error!(
                            "Failed to archive segment with start offset: {} for stream ID: {}, topic ID: {}, partition ID: {}. Error: {}",
                            start_offset,
                            topic.stream_id,
                            topic.topic_id,
                            partition.partition_id,
                            error
                        );
                        continue;
                    }
                    info!(
                        "Archived Segment with start offset: {}, for stream ID: {}, topic ID: {}, partition ID: {}",
                        start_offset, topic.stream_id, topic.topic_id, partition.partition_id
                    );
                    archived_segments += 1;
                }
            }
            Err(error) => {
                error!(
                    "Partition with ID: {} was not found for stream ID: {}, topic ID: {}. Error: {}",
                    segment_to_archive.partition_id, topic.stream_id, topic.topic_id, error
                );
                continue;
            }
        }
    }

    Ok(archived_segments)
}

async fn delete_segments(
    topic: &Topic,
    segments_to_delete: &[SegmentsToHandle],
) -> Result<HandledSegments, IggyError> {
    info!(
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
                error!(
                    "Partition with ID: {} was not found for stream with ID: {}, topic with ID: {}. {error}",
                    segment_to_delete.partition_id, topic.stream_id, topic.topic_id
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

*/
