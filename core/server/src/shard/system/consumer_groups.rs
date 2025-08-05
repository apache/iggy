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
use crate::streaming::topics::consumer_group2;
use crate::streaming::topics::consumer_group2::MEMBERS_CAPACITY;
use crate::streaming::topics::consumer_group2::Member;
use ahash::AHashMap;
use arcshift::ArcShift;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::locking::IggyRwLockFn;
use slab::Slab;

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

    pub fn create_consumer_group_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<(), IggyError> {
        self.create_consumer_group_base(stream_id, topic_id, group_id, name)?;
        Ok(())
    }

    pub fn create_consumer_group2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        members: ArcShift<Slab<Member>>,
        group_id: Option<u32>,
        name: String,
    ) -> Result<usize, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self
                .streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, |stream| stream.id());
            self.permissioner.borrow().create_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        self.create_consumer_group_base2(stream_id, topic_id, members, name)
    }

    pub fn create_consumer_group_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        members: ArcShift<Slab<Member>>,
        name: String,
    ) -> Result<usize, IggyError> {
        self.create_consumer_group_base2(stream_id, topic_id, members, name)
    }

    fn create_consumer_group_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        members: ArcShift<Slab<Member>>,
        name: String,
    ) -> Result<usize, IggyError> {
        let id = self
            .streams2
            .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                let partitions = topic.partitions().with(|partitions| {
                    partitions
                        .iter()
                        .map(|(_, partition)| partition.id())
                        .collect::<Vec<_>>()
                });
                topic.consumer_groups_mut().with_mut(|container| {
                    let cg = consumer_group2::ConsumerGroup::new(name, members.clone(), partitions);
                    cg.insert_into(container)
                })
            });
        Ok(id)
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
        self.create_consumer_group_base(stream_id, topic_id, group_id, name)
    }

    fn create_consumer_group_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: Option<u32>,
        name: &str,
    ) -> Result<Identifier, IggyError> {
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

    pub fn delete_consumer_group_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<ConsumerGroup, IggyError> {
        self.delete_consumer_group_base(stream_id, topic_id, consumer_group_id)
    }

    pub fn delete_consumer_group2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<consumer_group2::ConsumerGroup, IggyError> {
        self.ensure_authenticated(session)?;
        //self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;
        {
            let topic_id = self
                .streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, |stream| stream.id());
            self.permissioner.borrow().delete_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        self.delete_consumer_group_base2(stream_id, topic_id, group_id)
    }

    pub fn delete_consumer_group_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<consumer_group2::ConsumerGroup, IggyError> {
        self.delete_consumer_group_base2(stream_id, topic_id, group_id)
    }

    fn delete_consumer_group_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<consumer_group2::ConsumerGroup, IggyError> {
        let cg = self
            .streams2
            .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                topic.consumer_groups_mut().with_mut(|container| {
                    match group_id.kind {
                        iggy_common::IdKind::Numeric => {
                            container.try_remove(group_id.get_u32_value().unwrap() as usize)
                        }
                        iggy_common::IdKind::String => {
                            let key = group_id.get_cow_str_value().unwrap().to_string();
                            container.try_remove_by_key(&key)
                        }
                    }
                    .ok_or_else(|| {
                        //TODO: Fix the not found erros to accept Identifier instead of u32
                        IggyError::ConsumerGroupIdNotFound(0, 0)
                    })
                })
            })?;
        Ok(cg)
    }

    pub async fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
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
        }
        let cg = self.delete_consumer_group_base(stream_id, topic_id, consumer_group_id)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;

        for (_, partition) in topic.partitions.iter() {
            let partition = partition.read().await;
            if let Some((_, offset)) = partition.consumer_group_offsets.remove(&cg.group_id) {
                self.storage
                    .partition
                    .delete_consumer_offset(&offset.path)
                    .await?;
            }
        }

        Ok(())
    }

    fn delete_consumer_group_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: &Identifier,
    ) -> Result<ConsumerGroup, IggyError> {
        let stream_id_value;
        let topic_id_value;
        let consumer_group;
        {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let topic = stream.get_topic_mut(topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;

            consumer_group = topic.delete_consumer_group(consumer_group_id)
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {consumer_group_id}"))?;
            stream_id_value = topic.stream_id;
            topic_id_value = topic.topic_id;
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
        Ok(consumer_group)
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
