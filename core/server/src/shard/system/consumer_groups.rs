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

use std::cell::Ref;

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;

impl IggyShard {
    pub fn get_consumer_group<'cg, 'stream>(
        &self,
        session: &Session,
        stream: &'stream Stream,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<Ref<'cg, ConsumerGroup>>, IggyError>
    where
        'stream: 'cg,
    {
        self.ensure_authenticated(session)?;
        let stream_id = stream.stream_id;
        let topic = stream.get_topic(topic_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - topic with ID: {topic_id} was not found in stream with ID: {stream_id}",
            )
        })?;

        self.permissioner
        .borrow()
            .get_consumer_group(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get consumer group with ID: {group_id} for user with ID: {} in topic with ID: {topic_id} and stream with ID: {stream_id}",
                    session.get_user_id(),
                )
            })?;

        topic.try_get_consumer_group(group_id)
    }

    pub fn get_consumer_groups(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - stream with ID: {stream_id} was not found")
        })?;
        let topic = self.find_topic(session, &stream, topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic with ID: {topic_id} was not found in stream with ID: {stream_id}"))?;

        self.permissioner
        .borrow()
            .get_consumer_groups(session.get_user_id(), topic.stream_id, topic.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get consumer groups in topic with ID: {topic_id} and stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;

        Ok(topic.get_consumer_groups())
    }

    pub fn create_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<Identifier, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id)
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.borrow().create_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;
        }

        let mut stream = self.get_stream_mut(stream_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}"))?;
        let topic = stream.get_topic_mut(topic_id)
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;

        topic
            .create_consumer_group(group_id, name)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to create consumer group with name: {name}")
            })
    }

    pub async fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id)
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;

            self.permissioner.borrow().delete_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;

            stream_id_value = topic.stream_id;
            topic_id_value = topic.topic_id;
        }

        let consumer_group;
        {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let topic = stream.get_topic_mut(topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;

            consumer_group = topic.delete_consumer_group(consumer_group_id)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {consumer_group_id}"))?
        }

        for member in consumer_group.get_members() {
            self.client_manager.borrow_mut().leave_consumer_group(
                    member.id,
                    stream_id_value,
                    topic_id_value,
                    consumer_group.group_id,
                )
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to make client leave consumer group for client ID: {}, group ID: {}", member.id, consumer_group.group_id))?;
        }

        Ok(())
    }

    pub fn join_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self
                .find_topic(session, &stream, topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}",
                    )
                })?;

            self.permissioner.borrow().join_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to join consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;

            stream_id_value = topic.stream_id;
            topic_id_value = topic.topic_id;
        }

        let group_id;
        {
            let mut stream = self.get_stream_mut(stream_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to get mutable reference to stream with ID: {stream_id}")
                })?;
            let topic = stream.get_topic_mut(topic_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to get mutable reference to topic with ID: {topic_id}")
                })?;

            {
                let consumer_group = topic
                    .get_consumer_group(consumer_group_id)
                    .with_error_context(|error| {
                        format!(
                            "{COMPONENT} (error: {error}) - consumer group not found for group_id: {consumer_group_id:?}"
                        )
                    })?;

                group_id = consumer_group.group_id;
            }

            topic
                .join_consumer_group(consumer_group_id, session.client_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to join consumer group for group ID: {group_id}"
                    )
                })?;
        }

        self.client_manager.borrow_mut().join_consumer_group(session.client_id, stream_id_value, topic_id_value, group_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to make client join consumer group for client ID: {}",
                    session.client_id
                )
            })?;

        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
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
                        "{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id:?}, topic_id: {topic_id:?}"
                    )
                })?;

            self.permissioner.borrow().leave_consumer_group(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to leave consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), topic.stream_id, topic.topic_id))?;
        }

        self.leave_consumer_group_by_client(
            stream_id,
            topic_id,
            consumer_group_id,
            session.client_id,
        )
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to leave consumer group for client ID: {}",
                session.client_id
            )
        })
    }

    pub fn leave_consumer_group_by_client(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let stream_id_value;
        let topic_id_value;
        let group_id;

        {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let stream_id = stream.stream_id;
            let topic = stream.get_topic_mut(topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}",
                    )
                })?;
            let topic_id = topic.topic_id;
            {
                let consumer_group = topic
                    .get_consumer_group(consumer_group_id)
                    .with_error_context(|error| {
                        format!(
                        "{COMPONENT} (error: {error}) - consumer group not found for group_id: {consumer_group_id}",
                    )
                    })?;
                group_id = consumer_group.group_id;
            }

            stream_id_value = stream_id;
            topic_id_value = topic_id;
            topic
                .leave_consumer_group(consumer_group_id, client_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed leave consumer group, client ID {client_id}",)
                })?;
        }

        self.client_manager.borrow_mut().leave_consumer_group(
            client_id,
            stream_id_value,
            topic_id_value,
            group_id,
        )
    }
}
