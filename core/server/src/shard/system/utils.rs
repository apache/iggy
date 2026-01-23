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

use crate::{shard::IggyShard, streaming::polling_consumer::PollingConsumer};
use iggy_common::{Consumer, ConsumerKind, Identifier, IggyError};

impl IggyShard {
    /// Resolves stream identifier to numeric ID, returning error if not found.
    pub fn resolve_stream_id(&self, stream_id: &Identifier) -> Result<usize, IggyError> {
        self.metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))
    }

    /// Resolves topic identifier to (stream_id, topic_id), returning error if not found.
    pub fn resolve_topic_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(usize, usize), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;
        let topic = self
            .metadata
            .get_topic_id(stream, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        Ok((stream, topic))
    }

    /// Resolves partition identifier to (stream_id, topic_id, partition_id), returning error if not found.
    pub fn resolve_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(usize, usize, usize), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;
        if !self.metadata.partition_exists(stream, topic, partition_id) {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                topic_id.clone(),
                stream_id.clone(),
            ));
        }
        Ok((stream, topic, partition_id))
    }

    /// Resolves consumer group identifier to (stream_id, topic_id, group_id), returning error if not found.
    pub fn resolve_consumer_group_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(usize, usize, usize), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;
        let group = self
            .metadata
            .get_consumer_group_id(stream, topic, group_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
            })?;
        Ok((stream, topic, group))
    }

    pub fn ensure_topic_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.resolve_topic_id(stream_id, topic_id)?;
        Ok(())
    }

    pub fn ensure_consumer_group_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;
        Ok(())
    }

    pub fn ensure_partitions_exist(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;
        let actual_partitions_count = self.metadata.partitions_count(stream, topic);

        if partitions_count > actual_partitions_count as u32 {
            return Err(IggyError::InvalidPartitionsCount);
        }

        Ok(())
    }

    pub fn ensure_partition_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        self.resolve_partition_id(stream_id, topic_id, partition_id)?;
        Ok(())
    }

    pub fn resolve_consumer_with_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
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
                // Client may have been removed by heartbeat verifier while request was in-flight
                if self.client_manager.try_get_client(client_id).is_none() {
                    return Err(IggyError::StaleClient);
                }

                let (stream, topic, cg_id) =
                    self.resolve_consumer_group_id(stream_id, topic_id, &consumer.id)?;

                if !self.metadata.consumer_group_exists(stream, topic, cg_id) {
                    return Err(IggyError::ConsumerGroupIdNotFound(
                        consumer.id.clone(),
                        topic_id.clone(),
                    ));
                }

                let member_id = self
                    .metadata
                    .get_consumer_group_member_id(stream, topic, cg_id, client_id)
                    .ok_or_else(|| {
                        if self.client_manager.try_get_client(client_id).is_none() {
                            return IggyError::StaleClient;
                        }
                        IggyError::ConsumerGroupMemberNotFound(
                            client_id,
                            consumer.id.clone(),
                            topic_id.clone(),
                        )
                    })?;

                if let Some(partition_id) = partition_id {
                    return Ok(Some((
                        PollingConsumer::consumer_group(cg_id, member_id),
                        partition_id as usize,
                    )));
                }

                let partition_id = self.metadata.get_next_member_partition_id(
                    stream,
                    topic,
                    cg_id,
                    member_id,
                    calculate_partition_id,
                );

                match partition_id {
                    Some(partition_id) => Ok(Some((
                        PollingConsumer::consumer_group(cg_id, member_id),
                        partition_id,
                    ))),
                    None => Ok(None),
                }
            }
        }
    }
}
