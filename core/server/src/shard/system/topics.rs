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

use crate::streaming::session::Session;
use crate::streaming::systems::COMPONENT;
use crate::streaming::systems::system::System;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use iggy_common::locking::IggySharedMutFn;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};

impl System {
    pub fn find_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<&Topic, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self
            .find_stream(session, stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to find stream with ID: {stream_id}")
            })?;
        let topic = stream.get_topic(topic_id);
        if let Ok(topic) = topic {
            self.permissioner
                .get_topic(session.get_user_id(), stream.stream_id, topic.topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
            return Ok(topic);
        }

        topic
    }

    pub fn find_topics(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<Vec<&Topic>, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        self.permissioner
            .get_topics(session.get_user_id(), stream.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get topics in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(stream.get_topics())
    }

    pub fn try_find_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<&Topic>, IggyError> {
        self.ensure_authenticated(session)?;
        let Some(stream) = self
            .try_find_stream(session, stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to find stream with ID: {stream_id}")
            })?
        else {
            return Ok(None);
        };

        let Some(topic) = stream.try_get_topic(topic_id)? else {
            return Ok(None);
        };

        self.permissioner
            .get_topic(session.get_user_id(), stream.stream_id, topic.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        Ok(Some(topic))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: &str,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<&Topic, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            self.permissioner
                .create_topic(session.get_user_id(), stream.stream_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }

        let created_topic_id = self
            .get_stream_mut(stream_id)?
            .create_topic(
                topic_id,
                name,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor.unwrap_or(1),
            )
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to create topic with name: {name} in stream ID: {stream_id}")
            })?;

        self.metrics.increment_topics(1);
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        self.get_stream(stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?
            .get_topic(&created_topic_id.try_into()?)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get created topic with ID: {created_topic_id} in stream with ID: {stream_id}",
                )
            })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<&Topic, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self
                .find_topic(session, stream_id, topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id}"
                    )
                })?;
            self.permissioner.update_topic(
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
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update topic with ID: {topic_id} in stream with ID: {stream_id}",
                )
            })?;

        // TODO: if message_expiry is changed, we need to check if we need to purge messages based on the new expiry
        // TODO: if max_size_bytes is changed, we need to check if we need to purge messages based on the new size
        // TODO: if replication_factor is changed, we need to do `something`
        self.get_stream(stream_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?
            .get_topic(topic_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
            })
    }

    pub async fn delete_topic(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        {
            let topic = self
                .find_topic(session, stream_id, topic_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id} in stream with ID: {stream_id}")
                })?;
            self.permissioner.delete_topic(
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
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}"))?;

        self.metrics.decrement_topics(1);
        self.metrics
            .decrement_partitions(topic.get_partitions_count());
        self.metrics.decrement_messages(topic.get_messages_count());
        self.metrics
            .decrement_segments(topic.get_segments_count().await);
        let client_manager = self.client_manager.read().await;
        client_manager
            .delete_consumer_groups_for_topic(stream_id_value, topic.topic_id)
            .await;
        Ok(())
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let topic = self
            .find_topic(session, stream_id, topic_id)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to find topic with ID: {topic_id} in stream with ID: {stream_id}")
            })?;
        self.permissioner
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
