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

use crate::metadata::StreamMeta;
use crate::shard::IggyShard;
use crate::shard::transmission::message::{ResolvedStream, ResolvedTopic};
use crate::streaming::streams::storage::{create_stream_file_hierarchy, delete_stream_directory};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, IggyTimestamp};
use std::sync::Arc;

/// Info returned when a stream is deleted - contains what callers need for logging/events.
pub struct DeletedStreamInfo {
    pub id: usize,
    pub name: String,
}

impl IggyShard {
    pub async fn create_stream(&self, name: String) -> Result<usize, IggyError> {
        let name_arc = Arc::from(name.as_str());
        if self.metadata.stream_name_exists(&name_arc) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        let stream_id = self.metadata.next_stream_id();
        create_stream_file_hierarchy(stream_id, &self.config.system).await?;

        let created_at = IggyTimestamp::now();
        let stats = Arc::new(crate::streaming::stats::StreamStats::default());
        let meta = StreamMeta::with_stats(0, name_arc, created_at, stats);
        let assigned_id = self.writer().add_stream(meta);
        debug_assert_eq!(
            assigned_id, stream_id,
            "Stream ID mismatch: expected {stream_id}, got {assigned_id}"
        );

        self.metrics.increment_streams(1);
        Ok(stream_id)
    }

    pub fn update_stream(&self, stream: ResolvedStream, name: String) -> Result<(), IggyError> {
        self.writer()
            .try_update_stream(&self.metadata, stream.id(), Arc::from(name.as_str()))
    }

    pub async fn delete_stream(
        &self,
        stream: ResolvedStream,
    ) -> Result<DeletedStreamInfo, IggyError> {
        let stream_id = stream.id();

        let (topics_with_partitions, stream_name, stats, topics_count, partitions_count) =
            self.metadata.with_metadata(|m| {
                let stream_meta = m
                    .streams
                    .get(stream_id)
                    .expect("Stream metadata must exist");
                let twp: Vec<_> = stream_meta
                    .topics
                    .iter()
                    .map(|(topic_id, topic)| {
                        let partition_ids: Vec<usize> = (0..topic.partitions.len()).collect();
                        (topic_id, partition_ids)
                    })
                    .collect();
                let partitions_count: usize = stream_meta
                    .topics
                    .iter()
                    .map(|(_, t)| t.partitions.len())
                    .sum();
                (
                    twp,
                    stream_meta.name.to_string(),
                    stream_meta.stats.clone(),
                    stream_meta.topics.len(),
                    partitions_count,
                )
            });

        {
            let namespaces: Vec<_> = topics_with_partitions
                .iter()
                .flat_map(|(topic_id, partition_ids)| {
                    partition_ids
                        .iter()
                        .map(|&partition_id| IggyNamespace::new(stream_id, *topic_id, partition_id))
                })
                .collect();
            let mut partitions = self.local_partitions.borrow_mut();
            for ns in namespaces {
                partitions.remove(&ns);
            }
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(topics_count as u32);
        self.metrics.decrement_partitions(partitions_count as u32);
        self.metrics
            .decrement_messages(stats.messages_count_inconsistent());
        self.metrics
            .decrement_segments(stats.segments_count_inconsistent());

        self.writer().delete_stream(stream_id);

        let stream_info = DeletedStreamInfo {
            id: stream_id,
            name: stream_name,
        };

        self.client_manager
            .delete_consumer_groups_for_stream(stream_id);

        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == stream_id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        delete_stream_directory(stream_id, &topics_with_partitions, &self.config.system).await?;
        Ok(stream_info)
    }

    /// Clears in-memory state for all topics in a stream.
    pub async fn purge_stream(&self, stream: ResolvedStream) -> Result<(), IggyError> {
        let stream_id = stream.id();
        let topic_ids = self.metadata.get_topic_ids(stream_id);

        for topic_id in topic_ids {
            let topic = ResolvedTopic {
                stream_id,
                topic_id,
            };
            self.purge_topic(topic).await?;
        }

        Ok(())
    }

    /// Disk cleanup for local partitions across all topics in a stream.
    pub(crate) async fn purge_stream_local(&self, stream: ResolvedStream) -> Result<(), IggyError> {
        let stream_id = stream.id();
        let topic_ids = self.metadata.get_topic_ids(stream_id);

        for topic_id in topic_ids {
            let topic = ResolvedTopic {
                stream_id,
                topic_id,
            };
            self.purge_topic_local(topic).await?;
        }

        Ok(())
    }
}
