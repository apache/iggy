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
use crate::shard::IggyShard;
use crate::slab::traits_ext::{EntityComponentSystem, EntityMarker, InsertCell, IntoComponents};
use crate::streaming::session::Session;
use crate::streaming::topics::storage::{create_topic_file_hierarchy, delete_topic_from_disk};
use crate::streaming::topics::topic::{self};
use crate::streaming::{partitions, streams, topics};
use err_trail::ErrContext;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use std::str::FromStr;
use tracing::info;

impl IggyShard {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<topic::Topic, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        let numeric_stream_id = self.streams.get_index(stream_id);
        {
            self.permissioner
            .borrow()
                .create_topic(session.get_user_id(), numeric_stream_id)
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }
        let exists = self.streams.with_topics(
            stream_id,
            topics::helpers::exists(&Identifier::from_str(&name).unwrap()),
        );
        if exists {
            return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
        }

        let config = &self.config.system;
        let parent_stats = self
            .streams
            .with_stream_by_id(stream_id, |(_, stats)| stats.clone());
        let message_expiry = config.resolve_message_expiry(message_expiry);
        info!("Topic message expiry: {}", message_expiry);
        let max_topic_size = config.resolve_max_topic_size(max_topic_size)?;
        let topic = topic::create_and_insert_topics_mem(
            &self.streams,
            stream_id,
            name,
            replication_factor.unwrap_or(1),
            message_expiry,
            compression,
            max_topic_size,
            parent_stats,
        );
        self.metrics.increment_topics(1);

        // Create file hierarchy for the topic.
        create_topic_file_hierarchy(numeric_stream_id, topic.id(), &self.config.system).await?;
        Ok(topic)
    }

    pub fn create_topic_bypass_auth(&self, stream_id: &Identifier, topic: topic::Topic) -> usize {
        self.streams
            .with_topics(stream_id, |topics| topics.insert(topic))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
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
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id_val =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id_val = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().update_topic(
                session.get_user_id(),
                stream_id_val,
                topic_id_val
            ).with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to update topic for user with id: {}, stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream_id_val,
                    topic_id_val,
                )
            })?;
        }

        let exists = self.streams.with_topics(
            stream_id,
            topics::helpers::exists(&Identifier::from_str(&name).unwrap()),
        );
        if exists {
            return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
        }

        self.update_topic_base(
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

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
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
            replication_factor.unwrap_or(1),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) {
        let update_topic_closure = topics::helpers::update_topic(
            name.clone(),
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        );
        let (old_name, new_name) =
            self.streams
                .with_topic_by_id_mut(stream_id, topic_id, update_topic_closure);
        if old_name != new_name {
            let rename_closure = topics::helpers::rename_index(&old_name, new_name);
            self.streams.with_topics(stream_id, rename_closure);
        }
    }

    pub async fn delete_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<topic::Topic, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let numeric_topic_id =
            self.streams
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let numeric_stream_id = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        self.permissioner
            .borrow()
                .delete_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to delete topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        let mut topic = self.delete_topic_base(stream_id, topic_id);
        let topic_id_numeric = topic.id();

        // Clean up consumer groups from ClientManager for this topic
        self.client_manager
            .delete_consumer_groups_for_topic(numeric_stream_id, topic_id_numeric);

        // Remove all partition entries from shards_table for this topic
        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == numeric_stream_id && ns.topic_id() == topic_id_numeric {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        let parent = topic.stats().parent().clone();
        // We need to borrow topic as mutable, as we are extracting partitions out of it, in order to close them.
        let (messages_count, size_bytes, segments_count) =
            delete_topic_from_disk(numeric_stream_id, &mut topic, &self.config.system).await?;
        parent.decrement_messages_count(messages_count);
        parent.decrement_size_bytes(size_bytes);
        parent.decrement_segments_count(segments_count);
        self.metrics.decrement_topics(1);
        Ok(topic)
    }

    pub fn delete_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> topic::Topic {
        self.delete_topic_base(stream_id, topic_id)
    }

    pub fn delete_topic_base(&self, stream_id: &Identifier, topic_id: &Identifier) -> topic::Topic {
        self.streams
            .with_topics(stream_id, topics::helpers::delete_topic(topic_id))
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().purge_topic(
                session.get_user_id(),
                stream_id,
                topic_id
            ).with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        }

        let (consumer_offset_paths, consumer_group_offset_paths) = self.streams.with_partitions(
            stream_id,
            topic_id,
            partitions::helpers::purge_consumer_offsets(),
        );
        for path in consumer_offset_paths {
            self.delete_consumer_offset_from_disk(&path).await?;
        }
        for path in consumer_group_offset_paths {
            self.delete_consumer_offset_from_disk(&path).await?;
        }

        self.purge_topic_base(stream_id, topic_id).await?;
        Ok(())
    }

    pub async fn purge_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.purge_topic_base(stream_id, topic_id).await?;
        Ok(())
    }

    async fn purge_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let part_ids = self
            .streams
            .with_partitions(stream_id, topic_id, |partitions| {
                partitions.with_components(|components| {
                    let (roots, ..) = components.into_components();
                    roots.iter().map(|(_, root)| root.id()).collect::<Vec<_>>()
                })
            });

        self.streams.with_partitions(
            stream_id,
            topic_id,
            partitions::helpers::purge_partitions_mem(),
        );

        for part_id in part_ids {
            self.delete_segments_bypass_auth(stream_id, topic_id, part_id, u32::MAX)
                .await?;
        }

        // Zero out topic stats after purging all partitions
        self.streams
            .with_topic_by_id(stream_id, topic_id, |(_root, _aux, stats)| {
                stats.zero_out_all();
            });

        Ok(())
    }
}
