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
use crate::shard_info;
use crate::slab::traits_ext::{EntityComponentSystem, EntityMarker, InsertCell, IntoComponents};
use crate::streaming::session::Session;
use crate::streaming::stats::stats::{StreamStats, TopicStats};
use crate::streaming::topics::storage2::{create_topic_file_hierarchy, delete_topic_from_disk};
use crate::streaming::topics::topic2::{self};
use crate::streaming::{partitions, streams, topics};
use error_set::ErrContext;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
};
use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;
use std::u32;

impl IggyShard {
    pub async fn create_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<topic2::Topic, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;
        let numeric_stream_id = self.streams2.get_index(stream_id);
        {
            self.permissioner
            .borrow()
                .create_topic(session.get_user_id(), numeric_stream_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        }
        let exists = self.streams2.with_topics(
            stream_id,
            topics::helpers::exists(&Identifier::from_str(&name).unwrap()),
        );
        if exists {
            return Err(IggyError::TopicNameAlreadyExists(
                name,
                numeric_stream_id as u32,
            ));
        }

        let config = &self.config.system;
        let parent_stats = self
            .streams2
            .with_stream_by_id(stream_id, |(_, stats)| stats.clone());
        let message_expiry = config.resolve_message_expiry(message_expiry);
        shard_info!(self.id, "Topic message expiry: {}", message_expiry);
        let max_topic_size = config.resolve_max_topic_size(max_topic_size)?;
        let topic = topic2::create_and_insert_topics_mem(
            &self.streams2,
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
        create_topic_file_hierarchy(self.id, numeric_stream_id, topic.id(), &self.config.system)
            .await?;
        Ok(topic)
    }

    fn create_and_insert_topics_mem(
        &self,
        stream_id: &Identifier,
        name: String,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        parent_stats: Arc<StreamStats>,
    ) -> topic2::Topic {
        let stats = Arc::new(TopicStats::new(parent_stats));
        let now = IggyTimestamp::now();
        let mut topic = topic2::Topic::new(
            name,
            stats,
            now,
            replication_factor,
            message_expiry,
            compression,
            max_topic_size,
        );

        let id = self
            .streams2
            .with_topics(stream_id, |topics| topics.insert(topic.clone()));
        topic.update_id(id);
        topic
    }

    pub fn create_topic2_bypass_auth(&self, stream_id: &Identifier, topic: topic2::Topic) -> usize {
        self.streams2
            .with_topics(stream_id, |topics| topics.insert(topic))
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
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id_val = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id_val = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().update_topic(
                session.get_user_id(),
                stream_id_val,
                topic_id_val
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to update topic for user with id: {}, stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream_id_val,
                    topic_id_val,
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
        let update_topic_closure = topics::helpers::update_topic(
            name.clone(),
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        );
        let (old_name, new_name) =
            self.streams2
                .with_topic_by_id_mut(stream_id, topic_id, update_topic_closure);
        if old_name != new_name {
            let rename_closure = topics::helpers::rename_index(&old_name, new_name);
            self.streams2.with_topics(stream_id, rename_closure);
        }
    }

    pub async fn delete_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<topic2::Topic, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        self.permissioner
            .borrow()
                .delete_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - permission denied to delete topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                        session.get_user_id(),
                    )
                })?;
        let mut topic = self.delete_topic_base2(stream_id, topic_id);
        // Clean up consumer groups from ClientManager for this topic
        self.client_manager
            .borrow_mut()
            .delete_consumer_groups_for_topic(numeric_stream_id, topic.id());
        let parent = topic.stats().parent().clone();
        // We need to borrow topic as mutable, as we are extracting partitions out of it, in order to close them.
        let (messages_count, size_bytes, segments_count) =
            delete_topic_from_disk(self.id, numeric_stream_id, &mut topic, &self.config.system)
                .await?;
        parent.decrement_messages_count(messages_count);
        parent.decrement_size_bytes(size_bytes);
        parent.decrement_segments_count(segments_count);
        self.metrics.decrement_topics(1);
        Ok(topic)
    }

    pub fn delete_topic_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> topic2::Topic {
        self.delete_topic_base2(stream_id, topic_id)
    }

    pub fn delete_topic_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> topic2::Topic {
        self.streams2
            .with_topics(stream_id, topics::helpers::delete_topic(topic_id))
    }

    pub async fn purge_topic2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().purge_topic(
                session.get_user_id(),
                stream_id,
                topic_id
            ).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to purge topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;
        }

        self.streams2.with_partitions(
            stream_id,
            topic_id,
            partitions::helpers::purge_partitions_mem(),
        );

        let (consumer_offset_paths, consumer_group_offset_paths) = self.streams2.with_partitions(
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

        self.purge_topic_base2(stream_id, topic_id).await?;
        Ok(())
    }

    pub async fn purge_topic2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.purge_topic_base2(stream_id, topic_id).await?;
        Ok(())
    }

    async fn purge_topic_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let part_ids = self
            .streams2
            .with_partitions(stream_id, topic_id, |partitions| {
                partitions.with_components(|components| {
                    let (roots, ..) = components.into_components();
                    roots.iter().map(|(_, root)| root.id()).collect::<Vec<_>>()
                })
            });
        for part_id in part_ids {
            self.delete_segments_bypass_auth(stream_id, topic_id, part_id, u32::MAX)
                .await?;
        }
        Ok(())
    }
}
