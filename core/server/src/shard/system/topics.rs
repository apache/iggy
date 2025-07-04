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
use crate::shard::{IggyShard, ShardInfo};
use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use iggy_common::locking::IggySharedMutFn;
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

    pub async fn create_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
        shards_assignment: Vec<(IggyNamespace, ShardInfo)>,
    ) -> Result<(), IggyError> {
        let (topic_id, _) = self.create_topic_base(
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
        let stream_id = stream.stream_id;
        let topic = stream
            .get_topic(&Identifier::numeric(topic_id)?)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
            })?;

        for (_, shard_info) in &shards_assignment {
            if shard_info.id() == self.id {
                topic.persist().await.with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
                })?;
            }
        }

        self.insert_shard_table_records(shards_assignment);

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
    ) -> Result<(Vec<(IggyNamespace, ShardInfo)>, Identifier), IggyError> {
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
        let stream_id = stream.stream_id;
        let topic = stream
                .get_topic(&Identifier::numeric(topic_id)?)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
                })?;
        topic.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
        })?;

        // TODO: Refactor
        let records = partition_ids.into_iter().map(|partition_id| {
            let namespace = IggyNamespace::new(stream_id, topic_id, partition_id);
            let hash = namespace.generate_hash();
            let shard_id = hash % self.get_available_shards_count();
            let shard_info = ShardInfo::new(shard_id as u16);
            (namespace, shard_info)
        });
        let records = records.collect::<Vec<_>>();
        self.insert_shard_table_records(records.clone());

        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        Ok((records, Identifier::numeric(topic_id)?))
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
    ) -> Result<(u32, Vec<u32>), IggyError> {
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
        topic.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
        })?;
        info!("Updated topic: {topic}");

        // TODO: if message_expiry is changed, we need to check if we need to purge messages based on the new expiry
        // TODO: if max_size_bytes is changed, we need to check if we need to purge messages based on the new size
        // TODO: if replication_factor is changed, we need to do `something`
        Ok(())
    }

    pub async fn delete_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
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
            stream_id_value = topic.stream_id;
        }

        let topic = self
            .get_stream_mut(stream_id)?
            .delete_topic(topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}"))?;
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}")
        })?;
        topic
            .delete()
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete topic: {topic}")
            })
            .map_err(|_| IggyError::CannotDeleteTopic(topic.topic_id, stream.stream_id))?;

        self.metrics.decrement_topics(1);
        self.metrics
            .decrement_partitions(topic.get_partitions_count());
        self.metrics.decrement_messages(topic.get_messages_count());
        self.metrics
            .decrement_segments(topic.get_segments_count().await);
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id);
        Ok(())
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
        topic.purge().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to purge topic with ID: {topic_id} in stream with ID: {stream_id}")
        })
    }
}
