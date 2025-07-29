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

use super::COMPONENT;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::{IggyShard, ShardInfo};
use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use tokio_util::io::StreamReader;
use tracing::info;

impl IggyShard {
    pub fn find_topic<'topic, 'stream>(
        &self,
        session: &Session,
        stream: &'stream Stream,
        topic_id: &Identifier,
    ) -> Result<&'topic Topic, IggyError>
    where
        'stream: 'topic,
    {
        self.ensure_authenticated(session)?;
        let stream_id = stream.stream_id;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .borrow()
                .get_topic(session.get_user_id(), stream.stream_id, topic.topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        Ok(topic)
    }

    pub fn find_topics<'stream, 'topic>(
        &self,
        session: &Session,
        stream: &'stream Stream,
    ) -> Result<Vec<&'topic Topic>, IggyError>
    where
        'stream: 'topic,
    {
        self.ensure_authenticated(session)?;
        let stream_id = stream.stream_id;
        self.permissioner
        .borrow()
            .get_topics(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get topics in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(stream.get_topics())
    }

    pub fn try_find_topic<'stream, 'topic>(
        &self,
        session: &Session,
        stream: &'stream Stream,
        topic_id: &Identifier,
    ) -> Result<Option<&'topic Topic>, IggyError>
    where
        'stream: 'topic,
    {
        self.ensure_authenticated(session)?;
        let stream_id = stream.stream_id;
        let Some(topic) = stream.try_get_topic(topic_id)? else {
            return Ok(None);
        };

        self.permissioner
        .borrow()
            .get_topic(session.get_user_id(), stream_id, topic.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(Some(topic))
    }

    pub fn create_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.create_topic_base(
            stream_id,
            topic_id,
            name,
            partitions_count,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )?;

        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(Identifier, Vec<u32>), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            self.permissioner
            .borrow()
                .create_topic(session.get_user_id(), stream.stream_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }

        let (topic_id, partition_ids) = self.create_topic_base(
            stream_id,
            topic_id,
            name,
            partitions_count,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )?;

        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let numeric_stream_id = stream.stream_id;
        let topic = stream
                .get_topic(&topic_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
                })?;
        topic.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
        })?;

        let event = ShardEvent::CreatedTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            name: name.to_string(),
            partitions_count,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor: Some(replication_factor.unwrap_or(1)),
        };
        let _responses = self.broadcast_event_to_all_shards(event.into()).await;

        let numeric_topic_id = topic.topic_id;
        let records = self
            .create_shard_table_records(&partition_ids, numeric_stream_id, numeric_topic_id)
            .collect::<Vec<_>>();

        for (ns, shard_info) in records.iter() {
            let partition = topic.get_partition(ns.partition_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get partition with ID: {} in topic with ID: {topic_id}", ns.partition_id)
            })?;

            let mut partition = partition.write().await;
            partition.persist().await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to persist partition: {partition}")
            })?;

            if shard_info.id() == self.id {
                let partition_id = ns.partition_id;
                partition.open().await.with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to open partition with ID: {partition_id} in topic with ID: {topic_id} for stream with ID: {stream_id}"
                    )
                })?;
            }
        }

        self.insert_shard_table_records(records);

        let event = ShardEvent::CreatedShardTableRecords {
            stream_id: numeric_stream_id,
            topic_id: numeric_topic_id,
            partition_ids: partition_ids.clone(),
        };

        let _responses = self.broadcast_event_to_all_shards(event.into()).await;

        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        Ok((topic_id, partition_ids))
    }

    fn create_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(Identifier, Vec<u32>), IggyError> {
        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}")
        })?;
        let stream_id = stream.stream_id;
        let (topic_id, partition_ids) = stream
            .create_topic(
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor.unwrap_or(1),
            )
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to create topic with name: {name} in stream ID: {stream_id}")
            })?;
        Ok((topic_id, partition_ids))
    }

    pub async fn update_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.update_topic_base(
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let topic = self
                .find_topic(session, &stream, topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id}"
                    )
                })?;
            self.permissioner.borrow().update_topic(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to update topic for user with id: {}, stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    topic.stream_id,
                    topic.topic_id,
                )
            })?;
        }

        self.update_topic_base(
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to update topic with ID: {topic_id} in stream with ID: {stream_id}",
            )
        })?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        topic.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
        })?;

        // TODO: if message_expiry is changed, we need to check if we need to purge messages based on the new expiry
        // TODO: if max_size_bytes is changed, we need to check if we need to purge messages based on the new size
        // TODO: if replication_factor is changed, we need to do `something`
        Ok(())
    }

    async fn update_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.get_stream_mut(stream_id)?
            .update_topic(
                topic_id,
                name,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor.unwrap_or(1),
            )
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update topic with ID: {topic_id} in stream with ID: {stream_id}",
                )
            })?;
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream
            .get_topic(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}",
                )
            })?;
        for partition in topic.partitions.values() {
            let mut partition = partition.write().await;
            partition.message_expiry = message_expiry;
            for segment in partition.segments.iter_mut() {
                segment.update_message_expiry(message_expiry);
            }
        }

        info!("Updated topic: {topic}");
        Ok(())
    }

    pub async fn delete_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Topic, IggyError> {
        let topic = self.delete_topic_base(stream_id, topic_id).await?;
        Ok(topic)
    }

    pub async fn delete_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Topic, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let topic = self
                .find_topic(session, &stream, topic_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id} in stream with ID: {stream_id}")
                })?;
            self.permissioner.borrow().delete_topic(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to delete topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        }

        let topic = self.delete_topic_base(stream_id, topic_id)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}")
            })?;
        Ok(topic)
    }

    async fn delete_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Topic, IggyError> {
        let mut stream = self.get_stream_mut(stream_id)?;
        let stream_id_value = stream.stream_id;
        let topic = stream
            .delete_topic(topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}"))?;
        drop(stream);
        self.metrics.decrement_topics(1);
        self.metrics
            .decrement_partitions(topic.get_partitions_count());
        self.metrics.decrement_messages(topic.get_messages_count());
        self.metrics
            .decrement_segments(topic.get_segments_count().await);
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id);
        Ok(topic)
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = self
            .find_topic(session, &stream, topic_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id} in stream with ID: {stream_id}")
            })?;
        self.permissioner
            .borrow()
            .purge_topic(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;

        self.purge_topic_base(topic.stream_id, topic.topic_id).await
    }

    pub async fn purge_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream
            .get_topic(topic_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
            })?;

        self.purge_topic_base(stream.stream_id, topic.topic_id)
            .await
    }

    async fn purge_topic_base(&self, stream_id: u32, topic_id: u32) -> Result<(), IggyError> {
        let stream = self.get_stream(&Identifier::numeric(stream_id)?)?;
        let topic = stream.get_topic(&Identifier::numeric(topic_id)?)?;

        for partition in topic.get_partitions() {
            let mut partition = partition.write().await;
            let partition_id = partition.partition_id;
            let namespace = IggyNamespace::new(stream_id, topic_id, partition_id);
            let shard_info = self.find_shard_table_record(&namespace).unwrap();
            if shard_info.id() == self.id {
                partition.purge().await?;
            }
        }
        Ok(())
    }
}
