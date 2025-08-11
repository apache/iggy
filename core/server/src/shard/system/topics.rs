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

use std::str::FromStr;
use std::sync::Arc;

use super::COMPONENT;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::{IggyShard, ShardInfo};
use crate::shard_info;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::TopicStats;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::storage2::create_topic_file_hierarchy;
use crate::streaming::topics::topic::Topic;
use crate::streaming::topics::topic2;
use clap::Id;
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
    pub async fn create_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: Option<u32>,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
        stats: Arc<TopicStats>,
    ) -> Result<usize, IggyError> {
        self.ensure_authenticated(session)?;
        //self.ensure_stream_exists(stream_id)?;
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, |stream| stream.id());
        {
            self.permissioner
            .borrow()
                .create_topic(session.get_user_id(), numeric_stream_id as u32)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }
        let topic_id = self.create_topic2_base(
            stream_id,
            name,
            replication_factor.unwrap_or(1),
            message_expiry,
            compression,
            max_topic_size,
            stats,
        )?;

        self.streams2.with_topic_by_id(
            stream_id,
            &Identifier::numeric(topic_id as u32).unwrap(),
            |topic| {
                let message_expiry = match topic.message_expiry() {
                    IggyExpiry::ServerDefault => self.config.system.segment.message_expiry,
                    _ => message_expiry,
                };
                shard_info!(self.id, "Topic message expiry: {}", message_expiry);
            },
        );

        // Create file hierarchy for the topic.
        create_topic_file_hierarchy(self.id, numeric_stream_id, topic_id, &self.config.system)
            .await?;
        Ok(topic_id)
    }

    pub fn create_topic2_bypass_auth(
        &self,
        stream_id: &Identifier,
        name: String,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        stats: Arc<TopicStats>,
    ) -> Result<usize, IggyError> {
        let topic_id = self.create_topic2_base(
            stream_id,
            name,
            replication_factor.unwrap_or(1),
            message_expiry,
            compression,
            max_topic_size,
            stats,
        )?;
        Ok(topic_id)
    }

    fn create_topic2_base(
        &self,
        stream_id: &Identifier,
        name: String,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        stats: Arc<TopicStats>,
    ) -> Result<usize, IggyError> {
        let topic_id = self.streams2.with_stream_by_id(stream_id, |stream| {
            let exists = stream.topics().exists(&Identifier::named(&name).unwrap());
            if exists {
                // TODO: Fixme, replace the second argument with identifier, rather than numeric ID.
                return Err(IggyError::TopicNameAlreadyExists(name.to_owned(), 0));
            }
            let topic = topic2::Topic::new(
                name,
                replication_factor,
                message_expiry,
                compression,
                max_topic_size,
            );
            let name = topic.name().clone();
            let topic_id = stream.topics().with_mut(|topics| {
                let topic_id = topic.insert_into(topics);
                topic_id
            });
            stream.topics().with_mut_index(|index| {
                index.insert(name, topic_id);
            });

            stream.topics().with_stats_mut(|container| {
                container.insert(stats);
            });
            Ok(topic_id)
        })?;

        Ok(topic_id)
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
                    format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {numeric_stream_id}")
                })?;
        topic.persist().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to persist topic: {topic}")
        })?;

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

    pub fn update_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        //self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self
                .streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, |stream| stream.id());
            self.permissioner.borrow().update_topic(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to update topic for user with id: {}, stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream_id,
                    topic_id,
                )
            })?;
        }
        self.update_topic_base2(
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor.unwrap_or(1),
        );
        Ok(())
    }

    pub fn update_topic_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.update_topic_base2(
            stream_id,
            topic_id,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor.unwrap_or(1),
        );
        Ok(())
    }

    pub fn update_topic_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) {
        let (old_name, new_name) =
            self.streams2
                .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                    let old_name = topic.name().clone();
                    topic.set_name(name.clone());
                    topic.set_message_expiry(message_expiry);
                    topic.set_compression(compression_algorithm);
                    topic.set_max_topic_size(max_topic_size);
                    topic.set_replication_factor(replication_factor);
                    (old_name, name)
                    // TODO: Set message expiry for all partitions and segments.
                });
        if old_name != new_name {
            self.streams2.with_topics(stream_id, |topics| {
                topics.with_mut_index(|index| {
                    // Rename the key inside of hashmap
                    let idx = index.remove(&old_name).expect("Rename key: key not found");
                    index.insert(new_name, idx);
                })
            });
        }
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
        let new_name = self.get_stream_mut(stream_id)?
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
            .get_topic(&Identifier::from_str(new_name.as_str()).unwrap())
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

    pub async fn delete_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<topic2::Topic, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self
                .streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, |stream| stream.id());
            self.permissioner
            .borrow()
                .delete_topic(session.get_user_id(), stream_id as u32, topic_id as u32)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to delete topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }
        let topic = self.delete_topic_base2(stream_id, topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;
        // TODO: Remove partitions.
        Ok(topic)
    }

    pub async fn delete_topic_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<topic2::Topic, IggyError> {
        let topic = self.delete_topic_base2(stream_id, topic_id)?;
        Ok(topic)
    }

    pub fn delete_topic_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<topic2::Topic, IggyError> {
        let topic = self.streams2.with_stream_by_id(stream_id, |stream| {
            let id = stream
                .topics()
                .with_topic_by_id(topic_id, |topic| topic.id());
            stream.topics().with_mut(|container| {
                container.try_remove(id).ok_or_else(|| {
                    let topic_name = stream
                        .topics()
                        .with_topic_by_id(topic_id, |topic| topic.name().clone());
                    IggyError::TopicNameNotFound(topic_name, stream.name().clone())
                })
            })
        })?;
        let id = topic.id();
        self.streams2.with_topics(stream_id, |topics| {
            topics.with_stats_mut(|container| {
                container
                    .try_remove(id)
                    .expect("Topic delete: topic stats not found");
            });
        });
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
    pub async fn purge_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic_id = self
                .streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, |stream| stream.id());
            self.permissioner.borrow().purge_topic(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        }

        Ok(())
    }

    async fn purge_topic_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.streams2
            .with_partitions(stream_id, topic_id, |partitions| {
                //partitions
            });

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
