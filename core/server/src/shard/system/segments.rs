use crate::shard::IggyShard;
use crate::streaming;
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
use iggy_common::Identifier;
use iggy_common::IggyError;

impl IggyShard {
    pub async fn delete_segments_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        self.delete_segments_base(stream_id, topic_id, partition_id, segments_count)
            .await
    }

    pub async fn delete_segments_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        let (segments, storages, stats) = self.streams.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            |(_, stats, .., log)| {
                let upperbound = log.segments().len();
                let begin = upperbound.saturating_sub(segments_count as usize);
                let segments = log
                    .segments_mut()
                    .drain(begin..upperbound)
                    .collect::<Vec<_>>();
                let storages = log
                    .storages_mut()
                    .drain(begin..upperbound)
                    .collect::<Vec<_>>();
                let _ = log
                    .indexes_mut()
                    .drain(begin..upperbound)
                    .collect::<Vec<_>>();
                (segments, storages, stats.clone())
            },
        );
        let numeric_stream_id = self
            .streams
            .with_stream_by_id(stream_id, streaming::streams::helpers::get_stream_id());
        let numeric_topic_id = self.streams.with_topic_by_id(
            stream_id,
            topic_id,
            streaming::topics::helpers::get_topic_id(),
        );

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
                compio::fs::remove_file(&path).await.map_err(|e| {
                    tracing::error!(
                        "Failed to delete segment file at path: {}, err: {}",
                        path,
                        e
                    );
                    IggyError::CannotDeleteFile
                })?;
            } else {
                let start_offset = segment.start_offset;
                let path = self.config.system.get_messages_file_path(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                    start_offset,
                );
                compio::fs::remove_file(&path).await.map_err(|e| {
                    tracing::error!(
                        "Failed to delete segment file at path: {}, err: {}",
                        path,
                        e
                    );
                    IggyError::CannotDeleteFile
                })?;
            }
        }
        self.init_log(stream_id, topic_id, partition_id).await?;
        // TODO: Tech debt. make the increment seg count be part of init_log.
        stats.increment_segments_count(1);
        Ok(())
    }
}
