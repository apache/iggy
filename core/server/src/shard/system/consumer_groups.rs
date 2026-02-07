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
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use slab::Slab;
use std::sync::Arc;

impl IggyShard {
    pub fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> Result<usize, IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let partitions_count = self.metadata.partitions_count(stream, topic) as u32;

        let id = self
            .writer()
            .create_consumer_group(
                &self.metadata,
                stream,
                topic,
                Arc::from(name.as_str()),
                partitions_count,
            )
            .map_err(|e| {
                if let IggyError::ConsumerGroupNameAlreadyExists(_, _) = &e {
                    IggyError::ConsumerGroupNameAlreadyExists(name.clone(), topic_id.clone())
                } else {
                    e
                }
            })?;

        Ok(id)
    }

    pub fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupMeta, IggyError> {
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        let cg = self.delete_consumer_group_base(stream, topic, group);
        Ok(cg)
    }

    fn delete_consumer_group_base(
        &self,
        stream: usize,
        topic: usize,
        group: usize,
    ) -> ConsumerGroupMeta {
        let cg_meta = self
            .metadata
            .get_consumer_group(stream, topic, group)
            .unwrap_or_else(|| ConsumerGroupMeta {
                id: group,
                name: Arc::from(""),
                partitions: Vec::new(),
                members: Slab::new(),
            });

        self.client_manager
            .delete_consumer_group(stream, topic, group);

        self.writer().delete_consumer_group(stream, topic, group);

        cg_meta
    }

    pub fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let (stream, topic, group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        let valid_client_ids: Vec<u32> = self
            .client_manager
            .get_clients()
            .iter()
            .map(|c| c.session.client_id)
            .collect();

        self.writer()
            .join_consumer_group(stream, topic, group, client_id, Some(valid_client_ids));

        if let Some(cg) = self.metadata.get_consumer_group(stream, topic, group)
            && let Some((_, member)) = cg.members.iter().find(|(_, m)| m.client_id == client_id)
            && member.partitions.is_empty()
            && !cg.partitions.is_empty()
        {
            let current_valid_ids: Vec<u32> = self
                .client_manager
                .get_clients()
                .iter()
                .map(|c| c.session.client_id)
                .collect();

            let potentially_stale: Vec<u32> = cg
                .members
                .iter()
                .filter(|(_, m)| {
                    !m.partitions.is_empty() && !current_valid_ids.contains(&m.client_id)
                })
                .map(|(_, m)| m.client_id)
                .collect();

            if !potentially_stale.is_empty() {
                tracing::info!(
                    "join_consumer_group: new member {client_id} has no partitions, found stale members: {potentially_stale:?}, forcing leave"
                );

                for stale_client_id in potentially_stale {
                    let _ =
                        self.writer()
                            .leave_consumer_group(stream, topic, group, stale_client_id);
                }
            }
        }

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
        client_id: u32,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let (_stream, _topic, _group) =
            self.resolve_consumer_group_id(stream_id, topic_id, group_id)?;

        self.leave_consumer_group_base(stream_id, topic_id, group_id, client_id)
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
            .writer()
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
}
