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
use crate::metadata::ConsumerGroupMeta;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;

impl IggyShard {
    pub fn create_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Check if consumer group already exists
        if self
            .shared_metadata
            .get_consumer_group(stream_id, topic_id, &name.clone().try_into().unwrap())
            .is_some()
        {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name,
                topic_id.clone(),
            ));
        }

        self.permissioner
            .create_consumer_group(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                    numeric_topic_id
                )
            })?;

        // Get partition IDs from SharedMetadata (sorted for deterministic assignment)
        let snapshot = self.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .get_topic(numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        let mut partition_ids: Vec<usize> = topic_meta.partitions.keys().copied().collect();
        partition_ids.sort_unstable();
        drop(snapshot);

        // Create consumer group in SharedMetadata (single source of truth)
        let cg =
            self.shared_metadata
                .create_consumer_group(stream_id, topic_id, name, partition_ids)?;

        Ok(cg)
    }

    pub fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .delete_consumer_group(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                    numeric_topic_id
                )
            })?;

        // Get group ID for ClientManager cleanup
        let group_meta = self
            .shared_metadata
            .get_consumer_group(stream_id, topic_id, group_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
            })?;
        let group_id_value = group_meta.id;

        // Delete from SharedMetadata (single source of truth)
        let cg = self
            .shared_metadata
            .delete_consumer_group(stream_id, topic_id, group_id)?;

        // Clean up ClientManager state
        self.client_manager.delete_consumer_group(
            numeric_stream_id,
            numeric_topic_id,
            group_id_value,
        );

        Ok(cg)
    }

    pub fn join_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .join_consumer_group(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to join consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                    numeric_topic_id
                )
            })?;

        let client_id = session.client_id;

        // Join in SharedMetadata (single source of truth)
        let _ = self
            .shared_metadata
            .join_consumer_group(stream_id, topic_id, group_id, client_id)?;

        // Get group ID for ClientManager
        let group_meta = self
            .shared_metadata
            .get_consumer_group(stream_id, topic_id, group_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
            })?;
        let group_id_value = group_meta.id;

        // Update ClientManager state
        self.client_manager
            .join_consumer_group(client_id, numeric_stream_id, numeric_topic_id, group_id_value)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to make client join consumer group for client ID: {}",
                    client_id
                )
            })?;

        Ok(())
    }

    pub fn leave_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .leave_consumer_group(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to leave consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                    numeric_topic_id
                )
            })?;

        self.leave_consumer_group_base(stream_id, topic_id, group_id, session.client_id)
    }

    pub fn leave_consumer_group_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Get group ID before leaving
        let group_meta = self
            .shared_metadata
            .get_consumer_group(stream_id, topic_id, group_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(group_id.clone(), topic_id.clone())
            })?;

        // Check if member exists
        if !group_meta.has_member(client_id) {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                client_id,
                group_id.clone(),
                topic_id.clone(),
            ));
        }

        let group_id_value = group_meta.id;

        // Leave in SharedMetadata (single source of truth) - this also rebalances
        self.shared_metadata
            .leave_consumer_group(stream_id, topic_id, group_id, client_id)?;

        // Update ClientManager state
        self.client_manager
            .leave_consumer_group(client_id, numeric_stream_id, numeric_topic_id, group_id_value)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to make client leave consumer group for client ID: {}",
                    client_id
                )
            })?;

        Ok(())
    }
}
