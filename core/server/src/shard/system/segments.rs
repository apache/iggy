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
use crate::streaming::segments::Segment;
use iggy_common::IggyError;
use iggy_common::sharding::IggyNamespace;

impl IggyShard {
    pub(crate) async fn delete_segments_base(
        &self,
        stream: usize,
        topic: usize,
        partition_id: usize,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        let namespace = IggyNamespace::new(stream, topic, partition_id);

        // Drain segments from local_partitions
        let (segments, storages, stats) = {
            let mut partitions = self.local_partitions.borrow_mut();
            let partition = partitions
                .get_mut(&namespace)
                .expect("delete_segments_base: partition must exist in local_partitions");

            let upperbound = partition.log.segments().len();
            let begin = upperbound.saturating_sub(segments_count as usize);
            let segments = partition
                .log
                .segments_mut()
                .drain(begin..upperbound)
                .collect::<Vec<_>>();
            let storages = partition
                .log
                .storages_mut()
                .drain(begin..upperbound)
                .collect::<Vec<_>>();
            let _ = partition
                .log
                .indexes_mut()
                .drain(begin..upperbound)
                .collect::<Vec<_>>();
            (segments, storages, partition.stats.clone())
        };

        for (mut storage, segment) in storages.into_iter().zip(segments.into_iter()) {
            let (msg_writer, index_writer) = storage.shutdown();
            if let Some(msg_writer) = msg_writer
                && let Some(index_writer) = index_writer
            {
                // We need to fsync before closing to ensure all data is written to disk.
                msg_writer.fsync().await?;
                index_writer.fsync().await?;
                let path = msg_writer.path();
                drop(msg_writer);
                drop(index_writer);
                // File might not exist if never actually written to disk (lazy creation)
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!(
                            "Segment file already gone or never created at path: {}",
                            path
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to delete segment file at path: {}, err: {}",
                            path,
                            e
                        );
                        return Err(IggyError::CannotDeleteFile);
                    }
                }
            } else {
                let start_offset = segment.start_offset;
                let path = self.config.system.get_messages_file_path(
                    stream,
                    topic,
                    partition_id,
                    start_offset,
                );
                // File might not exist if segment was never written to (lazy creation)
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        tracing::debug!(
                            "Segment file already gone or never created at path: {}",
                            path
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to delete segment file at path: {}, err: {}",
                            path,
                            e
                        );
                        return Err(IggyError::CannotDeleteFile);
                    }
                }
            }
        }

        // Add segment directly to local_partitions
        self.init_log_in_local_partitions(&namespace).await?;
        stats.increment_segments_count(1);
        Ok(())
    }

    /// Initialize a new segment in local_partitions.
    /// Used when partition data is in local_partitions (not slabs).
    async fn init_log_in_local_partitions(
        &self,
        namespace: &IggyNamespace,
    ) -> Result<(), IggyError> {
        use crate::streaming::segments::storage::create_segment_storage;

        let start_offset = 0;
        let segment = Segment::new(
            start_offset,
            self.config.system.segment.size,
            self.config.system.segment.message_expiry,
        );

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
    pub(crate) async fn rotate_segment_in_local_partitions(
        &self,
        namespace: &IggyNamespace,
    ) -> Result<(), IggyError> {
        use crate::streaming::segments::storage::create_segment_storage;

        let start_offset = {
            let partitions = self.local_partitions.borrow();
            let partition = partitions
                .get(namespace)
                .expect("rotate_segment: partition must exist");
            partition.log.active_segment().end_offset + 1
        };

        let segment = Segment::new(
            start_offset,
            self.config.system.segment.size,
            self.config.system.segment.message_expiry,
        );

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
            partition.stats.increment_segments_count(1);
            tracing::info!(
                "Rotated to new segment at offset {} for partition {:?}",
                start_offset,
                namespace
            );
        }
        Ok(())
    }
}
