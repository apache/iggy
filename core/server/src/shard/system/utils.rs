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

use iggy_common::{Consumer, ConsumerKind, Identifier, IggyError};

use crate::{
    shard::IggyShard,
    streaming::{
        polling_consumer::PollingConsumer,
        topics::{self},
    },
};

impl IggyShard {
    pub fn ensure_stream_exists(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        if !self.streams.exists(stream_id) {
            return Err(IggyError::StreamIdNotFound(stream_id.clone()));
        }
        Ok(())
    }

    pub fn ensure_topic_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_stream_exists(stream_id)?;
        let exists = self
            .streams
            .with_topics(stream_id, topics::helpers::exists(topic_id));
        if !exists {
            return Err(IggyError::TopicIdNotFound(
                stream_id.clone(),
                topic_id.clone(),
            ));
        }
        Ok(())
    }

    pub fn ensure_consumer_group_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_stream_exists(stream_id)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let exists = self.streams.with_topic_by_id(
            stream_id,
            topic_id,
            topics::helpers::cg_exists(group_id),
        );
        if !exists {
            return Err(IggyError::ConsumerGroupIdNotFound(
                group_id.clone(),
                topic_id.clone(),
            ));
        }
        Ok(())
    }

    pub fn ensure_partitions_exist(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_topic_exists(stream_id, topic_id)?;
        let actual_partitions_count =
            self.streams
                .with_partitions(stream_id, topic_id, |partitions| partitions.len());

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
        self.ensure_topic_exists(stream_id, topic_id)?;
        let partition_exists = self
            .streams
            .with_topic_by_id(stream_id, topic_id, |(root, ..)| {
                root.partitions().exists(partition_id)
            });

        if !partition_exists {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                topic_id.clone(),
                stream_id.clone(),
            ));
        }

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
                self.ensure_consumer_group_exists(stream_id, topic_id, &consumer.id)?;
                let cg_id = self.streams.with_consumer_group_by_id(
                    stream_id,
                    topic_id,
                    &consumer.id,
                    topics::helpers::get_consumer_group_id(),
                );
                let Some(member_id) = self.streams.with_consumer_group_by_id(
                    stream_id,
                    topic_id,
                    &consumer.id,
                    topics::helpers::get_consumer_group_member_id(client_id),
                ) else {
                    return Err(IggyError::ConsumerGroupMemberNotFound(
                        client_id,
                        consumer.id.clone(),
                        topic_id.clone(),
                    ));
                };
                if let Some(partition_id) = partition_id {
                    return Ok(Some((
                        PollingConsumer::consumer_group(cg_id, member_id),
                        partition_id as usize,
                    )));
                }

                let partition_id = if calculate_partition_id {
                    self.streams.with_consumer_group_by_id(
                        stream_id,
                        topic_id,
                        &consumer.id,
                        topics::helpers::calculate_partition_id_unchecked(member_id),
                    )
                } else {
                    self.streams.with_consumer_group_by_id(
                        stream_id,
                        topic_id,
                        &consumer.id,
                        topics::helpers::get_current_partition_id_unchecked(member_id),
                    )
                };
                let Some(partition_id) = partition_id else {
                    return Ok(None);
                };

                Ok(Some((
                    PollingConsumer::consumer_group(cg_id, member_id),
                    partition_id,
                )))
            }
        }
    }
}
