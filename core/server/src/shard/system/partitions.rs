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
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;

impl IggyShard {
    pub async fn create_partitions(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
            self.permissioner.create_partitions(
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

        let topic = self
            .get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
        topic
            .add_persisted_partitions(partitions_count)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add persisted partitions, topic: {topic}")
            })?;
        topic.reassign_consumer_groups().await;
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(())
    }

    pub async fn delete_partitions(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let topic = self.find_topic(session, stream_id, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
            self.permissioner.delete_partitions(
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

        let topic = self
            .get_stream_mut(stream_id)?
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
        let partitions = topic
            .delete_persisted_partitions(partitions_count)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete persisted partitions for topic: {topic}")
            })?;
        topic.reassign_consumer_groups().await;
        if let Some(partitions) = partitions {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(partitions.segments_count);
            self.metrics.decrement_messages(partitions.messages_count);
        }
        Ok(())
    }
}
