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

use crate::{
    metadata::{resolve_consumer_group_id_inner, resolve_stream_id_inner, resolve_topic_id_inner},
    shard::{
        IggyShard,
        transmission::message::{
            ResolvedConsumerGroup, ResolvedPartition, ResolvedStream, ResolvedTopic,
        },
    },
    streaming::polling_consumer::PollingConsumer,
};
use iggy_common::{Consumer, ConsumerKind, Identifier, IggyError};

impl IggyShard {
    /// Resolves stream identifier to typed `ResolvedStream`.
    pub fn resolve_stream(&self, stream_id: &Identifier) -> Result<ResolvedStream, IggyError> {
        self.metadata
            .with_metadata(|m| resolve_stream_inner(m, stream_id))
    }

    /// Resolves topic from identifiers. Returns StreamIdNotFound if stream doesn't exist,
    /// TopicIdNotFound if topic doesn't exist.
    pub fn resolve_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.metadata.with_metadata(|m| {
            let stream = resolve_stream_inner(m, stream_id)?;
            let id = resolve_topic_id_inner(m, stream.0, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
            Ok(ResolvedTopic {
                stream_id: stream.0,
                topic_id: id,
            })
        })
    }

    /// Resolves partition from identifiers. Returns appropriate error at each level.
    pub fn resolve_partition(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<ResolvedPartition, IggyError> {
        self.metadata.with_metadata(|m| {
            let stream = resolve_stream_inner(m, stream_id)?;
            let tid = resolve_topic_id_inner(m, stream.0, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;

            let exists = m
                .streams
                .get(stream.0)
                .and_then(|s| s.topics.get(tid))
                .and_then(|t| t.partitions.get(partition_id))
                .is_some();

            if !exists {
                return Err(IggyError::PartitionNotFound(
                    partition_id,
                    topic_id.clone(),
                    stream_id.clone(),
                ));
            }

            Ok(ResolvedPartition {
                stream_id: stream.0,
                topic_id: tid,
                partition_id,
            })
        })
    }

    /// Resolves consumer group from identifiers. Returns appropriate error at each level.
    pub fn resolve_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ResolvedConsumerGroup, IggyError> {
        self.metadata.with_metadata(|m| {
            let stream = resolve_stream_inner(m, stream_id)?;
            let tid = resolve_topic_id_inner(m, stream.0, topic_id)
                .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;

            let gid =
                resolve_consumer_group_id_inner(m, stream.0, tid, group_id).ok_or_else(|| {
                    IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
                })?;

            Ok(ResolvedConsumerGroup {
                stream_id: stream.0,
                topic_id: tid,
                group_id: gid,
            })
        })
    }

    /// Validates that partitions_count does not exceed actual partition count.
    pub fn validate_partitions_count(
        &self,
        topic: ResolvedTopic,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        let actual = self
            .metadata
            .partitions_count(topic.stream_id, topic.topic_id);
        if partitions_count > actual as u32 {
            return Err(IggyError::InvalidPartitionsCount);
        }
        Ok(())
    }

    /// Resolves consumer with partition ID for polling/offset operations.
    /// For consumer groups, all lookups happen under a single metadata read guard.
    pub fn resolve_consumer_with_partition_id(
        &self,
        topic: ResolvedTopic,
        consumer: &Consumer,
        client_id: u32,
        partition_id: Option<u32>,
        calculate_partition_id: bool,
    ) -> Result<Option<(PollingConsumer, usize)>, IggyError> {
        match consumer.kind {
            ConsumerKind::Consumer => {
                let partition_id = partition_id.unwrap_or(0);
                Ok(Some((
                    PollingConsumer::consumer(&consumer.id, partition_id as usize),
                    partition_id as usize,
                )))
            }
            ConsumerKind::ConsumerGroup => {
                if self.client_manager.try_get_client(client_id).is_none() {
                    return Err(IggyError::StaleClient);
                }

                self.metadata.resolve_consumer_group_partition(
                    topic.stream_id,
                    topic.topic_id,
                    &consumer.id,
                    client_id,
                    partition_id,
                    calculate_partition_id,
                )
            }
        }
    }

    /// Resolves topic and verifies user has append permission atomically.
    pub fn resolve_topic_for_append(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.metadata
            .resolve_for_append(user_id, stream_id, topic_id)
    }

    /// Resolves topic and verifies user has poll permission atomically.
    pub fn resolve_topic_for_poll(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.metadata.resolve_for_poll(user_id, stream_id, topic_id)
    }

    /// Resolves topic and verifies user has permission to store consumer offset atomically.
    pub fn resolve_topic_for_store_consumer_offset(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.metadata
            .resolve_for_store_consumer_offset(user_id, stream_id, topic_id)
    }

    /// Resolves topic and verifies user has permission to delete consumer offset atomically.
    pub fn resolve_topic_for_delete_consumer_offset(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<ResolvedTopic, IggyError> {
        self.metadata
            .resolve_for_delete_consumer_offset(user_id, stream_id, topic_id)
    }

    /// Resolves partition and verifies user has permission to delete segments atomically.
    pub fn resolve_partition_for_delete_segments(
        &self,
        user_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<ResolvedPartition, IggyError> {
        self.metadata
            .resolve_for_delete_segments(user_id, stream_id, topic_id, partition_id)
    }
}

fn resolve_stream_inner(
    m: &crate::metadata::InnerMetadata,
    stream_id: &Identifier,
) -> Result<ResolvedStream, IggyError> {
    resolve_stream_id_inner(m, stream_id)
        .map(ResolvedStream)
        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))
}
