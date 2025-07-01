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
use crate::shard::ShardInfo;
use crate::shard::namespace::IggyNamespace;
use crate::streaming::session::Session;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::locking::IggySharedMutFn;

// TODO: MAJOR REFACTOR!!!!!!!!!!!!!!!!!
impl IggyShard {
    pub async fn create_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        // This whole method is yeah....
        // I don't admit to writing it.
        // Sorry, not sorry.
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic ID: {topic_id}"))?;
            self.permissioner.borrow().create_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to create partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let partition_ids = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let stream_id = stream.stream_id;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let partition_ids = topic
            .add_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add persisted partitions, topic: {topic}")
            })?;
            partition_ids
        };

        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let stream_id = stream.stream_id;
            let topic = stream.get_topic(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;
            let topic_id = topic.topic_id;
            for partition_id in &partition_ids {
                let partition = topic.partitions.get(partition_id).unwrap();
                let mut partition = partition.write().await;
                partition.persist().await.with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to persist partition with id: {}",
                        partition.partition_id
                    )
                })?;
            }
            let records = partition_ids.into_iter().map(|partition_id| {
                let namespace = IggyNamespace::new(stream_id, topic_id, partition_id);
                let hash = namespace.generate_hash();
                let shard_id = hash % self.get_available_shards_count();
                let shard_info = ShardInfo::new(shard_id as u16);
                (namespace, shard_info)
            });
            self.insert_shard_table_records(records);
        }

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;

        topic.reassign_consumer_groups();
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
            self.permissioner.borrow().delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let (numeric_topic_id, partitions) = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;

            let partitions = topic
            .delete_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete persisted partitions for topic: {topic}")
            })?;
            (topic.topic_id, partitions)
        };

        let mut segments_count = 0;
        let mut messages_count = 0;
        for partition in &partitions {
            let mut partition = partition.write().await;
            let partition_id = partition.partition_id;
            let partition_messages_count = partition.get_messages_count();
            segments_count += partition.get_segments_count();
            messages_count += partition_messages_count;
            partition.delete().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete partition with ID: {} in topic with ID: {}",
                    partition_id,
                    numeric_topic_id
                )
            })?;
        }

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id}")
        })?;
        topic.reassign_consumer_groups();
        if partitions.len() > 0 {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(segments_count);
            self.metrics.decrement_messages(messages_count);
        }
        Ok(())
    }
}
