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
use iggy_common::collections::SegmentedSlab;
use std::sync::Arc;

impl IggyShard {
    pub fn create_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> Result<usize, IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        self.permissioner
            .create_consumer_group(session.get_user_id(), stream, topic)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream,
                    topic
                )
            })?;

        let mut partitions: Vec<usize> = {
            let metadata = self.metadata.load();
            metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .map(|t| t.partitions.keys().collect())
                .unwrap_or_default()
        };
        partitions.sort_unstable();

        let id = self
            .metadata
            .try_register_consumer_group(stream, topic, Arc::from(name.as_str()), partitions)
            .map_err(|e| {
                if let IggyError::ConsumerGroupNameAlreadyExists(_, _) = &e {
                    // Re-wrap to use caller's topic_id for the error message
                    IggyError::ConsumerGroupNameAlreadyExists(name.clone(), topic_id.clone())
                } else {
                    e
                }
            })?;

        Ok(id)
    }

    pub fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        self.permissioner
            .delete_consumer_group(session.get_user_id(), stream, topic)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream,
                    topic
                )
            })?;

        let cg = self.delete_consumer_group_base(stream, topic, group);
        Ok(cg)
    }

    pub fn delete_consumer_group_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;
        Ok(self.delete_consumer_group_base(stream, topic, group))
    }

    fn delete_consumer_group_base(
        &self,
        stream: usize,
        topic: usize,
        group: usize,
    ) -> ConsumerGroupMeta {
        let cg_meta = {
            let metadata = self.metadata.load();
            metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .and_then(|t| t.consumer_groups.get(group))
                .cloned()
                .unwrap_or_else(|| ConsumerGroupMeta {
                    id: group,
                    name: Arc::from(""),
                    partitions: Vec::new(),
                    members: SegmentedSlab::new(),
                })
        };

        self.client_manager
            .delete_consumer_group(stream, topic, group);

        self.metadata.delete_consumer_group(stream, topic, group);

        cg_meta
    }

    pub fn join_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        self.permissioner
            .join_consumer_group(session.get_user_id(), stream, topic)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to join consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream,
                    topic
                )
            })?;

        let client_id = session.client_id;

        self.metadata
            .join_consumer_group(stream, topic, group, client_id);

        self.client_manager
            .join_consumer_group(client_id, stream, topic, group)
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
        let (stream, topic, _group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        self.permissioner
            .leave_consumer_group(session.get_user_id(), stream, topic)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to leave consumer group for user {} on stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    stream_id,
                    topic_id
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
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        let member_id = self
            .metadata
            .leave_consumer_group(stream, topic, group, client_id);

        if member_id.is_none() {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                client_id,
                group_id.clone(),
                topic_id.clone(),
            ));
        }

        self.client_manager
            .leave_consumer_group(client_id, stream, topic, group)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to make client leave consumer group for client ID: {}",
                    client_id
                )
            })?;

        Ok(())
    }

    pub fn get_consumer_group_from_shared_metadata(
        &self,
        stream_id: usize,
        topic_id: usize,
        group_id: usize,
    ) -> bytes::Bytes {
        use bytes::{BufMut, BytesMut};

        let metadata = self.metadata.load();

        let Some(cg_meta) = metadata
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.consumer_groups.get(group_id))
        else {
            return bytes::Bytes::new();
        };

        let mut bytes = BytesMut::new();

        bytes.put_u32_le(cg_meta.id as u32);
        bytes.put_u32_le(cg_meta.partitions.len() as u32);
        bytes.put_u32_le(cg_meta.members.len() as u32);
        bytes.put_u8(cg_meta.name.len() as u8);
        bytes.put_slice(cg_meta.name.as_bytes());

        for (_, member) in cg_meta.members.iter() {
            bytes.put_u32_le(member.id as u32);
            bytes.put_u32_le(member.partitions.len() as u32);
            for &partition_id in &member.partitions {
                bytes.put_u32_le(partition_id as u32);
            }
        }

        bytes.freeze()
    }
}
