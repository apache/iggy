/* Licensed to the Apache Software Foundation (ASF) under one
        polling_consumer: &PollingConsumer,
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

use super::COMPONENT;
use crate::{
    shard::IggyShard,
    streaming::{partitions, polling_consumer::PollingConsumer, session::Session, streams, topics},
};
use error_set::ErrContext;
use iggy_common::{Consumer, ConsumerOffsetInfo, Identifier, IggyError};

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().store_consumer_offset(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to store consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                    session.get_user_id(),
                )
            })?;
        }
        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        ) else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };

        self.store_consumer_offset_base(
            stream_id,
            topic_id,
            &polling_consumer,
            partition_id,
            offset,
        );
        self.persist_consumer_offset_to_disk(
            stream_id,
            topic_id,
            &polling_consumer,
            partition_id,
        ).await?;
        Ok((polling_consumer, partition_id))
    }

    pub async fn get_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().get_consumer_offset(
                session.get_user_id(),
                stream_id as u32,   
                topic_id as u32
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                    session.get_user_id()
                )
            })?;
        }
        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        ) else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };

        let offset = match polling_consumer {
            PollingConsumer::Consumer(id, _) => self.streams2.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                partitions::helpers::get_consumer_offset(id),
            ),
            PollingConsumer::ConsumerGroup(_, id) => self.streams2.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                partitions::helpers::get_consumer_group_member_offset(id),
            ),
        };
        Ok(offset)
    }

    pub async fn delete_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().delete_consumer_offset(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32
            ).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - permission denied to delete consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                session.get_user_id(),
            )
        })?;
        }
        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        ) else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };

        self.delete_consumer_offset_base(stream_id, topic_id, &polling_consumer, partition_id)?;
        self.delete_consumer_offset_from_disk(stream_id, topic_id, &polling_consumer, partition_id).await?;
        Ok((polling_consumer, partition_id))
    }

    fn store_consumer_offset_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
        offset: u64,
    ) {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                self.streams2.with_stream_by_id(
                    stream_id,
                    streams::helpers::store_consumer_offset(
                        *id,
                        topic_id,
                        partition_id,
                        offset,
                        &self.config.system,
                    ),
                );
            }
            PollingConsumer::ConsumerGroup(_, id) => {
                self.streams2.with_stream_by_id(
                    stream_id,
                    streams::helpers::store_consumer_group_member_offset(
                        *id,
                        topic_id,
                        partition_id,
                        offset,
                        &self.config.system,
                    ),
                );
            }
        }
    }

    fn delete_consumer_offset_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                self.streams2
                    .with_partition_by_id(stream_id, topic_id, partition_id, partitions::helpers::delete_consumer_offset(*id)).with_error_context(|error| {
                        format!(
                            "{COMPONENT} (error: {error}) - failed to delete consumer offset for consumer with ID: {id} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                        )
                    })?;
            }
            PollingConsumer::ConsumerGroup(_, id) => {
                self.streams2
                    .with_partition_by_id(stream_id, topic_id, partition_id, partitions::helpers::delete_consumer_group_member_offset(*id)).with_error_context(|error| {
                        format!(
                            "{COMPONENT} (error: {error}) - failed to delete consumer group member offset for member with ID: {id} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                        )
                    })?;
            }
        }
        Ok(())
    }

    async fn persist_consumer_offset_to_disk(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                self.streams2.with_partition_by_id_async(
                    stream_id,
                    topic_id,
                    partition_id,
                    partitions::helpers::persist_consumer_offset_to_disk(self.id, *id)
                ).await
            }
            PollingConsumer::ConsumerGroup(_, id) => {
                self.streams2.with_partition_by_id_async(
                    stream_id,
                    topic_id,
                    partition_id,
                    partitions::helpers::persist_consumer_group_member_offset_to_disk(self.id, *id)
                ).await
            }
        }
    }

    async fn delete_consumer_offset_from_disk(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                self.streams2
                    .with_partition_by_id_async(
                        stream_id,
                        topic_id,
                        partition_id,
                        partitions::helpers::delete_consumer_offset_from_disk(self.id, *id),
                    )
                    .await
            }
            PollingConsumer::ConsumerGroup(_, id) => {
                self.streams2
                    .with_partition_by_id_async(
                        stream_id,
                        topic_id,
                        partition_id,
                        partitions::helpers::delete_consumer_group_member_offset_from_disk(self.id, *id),
                    )
                    .await
            }
        }
    }

    pub fn store_consumer_offset_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
        offset: u64,
    ) {
        self.store_consumer_offset_base(
            stream_id,
            topic_id,
            polling_consumer,
            partition_id,
            offset,
        );
    }

    pub fn delete_consumer_offset_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        self.delete_consumer_offset_base(stream_id, topic_id, polling_consumer, partition_id)
    }
}
