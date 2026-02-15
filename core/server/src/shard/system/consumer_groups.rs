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
use crate::shard::transmission::message::{ResolvedConsumerGroup, ResolvedTopic};
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use std::sync::Arc;

pub struct DeletedConsumerGroup {
    pub group_id: usize,
    pub partition_ids: Vec<usize>,
}

impl IggyShard {
    pub fn create_consumer_group(
        &self,
        topic: ResolvedTopic,
        name: String,
    ) -> Result<usize, IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;

        let partitions_count = self.metadata.partitions_count(stream, topic_id) as u32;

        let id = self
            .writer()
            .create_consumer_group(
                &self.metadata,
                stream,
                topic_id,
                Arc::from(name.as_str()),
                partitions_count,
            )
            .map_err(|e| {
                if let IggyError::ConsumerGroupNameAlreadyExists(_, _) = &e {
                    IggyError::ConsumerGroupNameAlreadyExists(
                        name.clone(),
                        Identifier::numeric(topic_id as u32).unwrap(),
                    )
                } else {
                    e
                }
            })?;

        Ok(id)
    }

    pub fn delete_consumer_group(
        &self,
        group: ResolvedConsumerGroup,
    ) -> Result<DeletedConsumerGroup, IggyError> {
        let stream = group.stream_id;
        let topic = group.topic_id;
        let group_id = group.group_id;

        let partition_ids = self
            .metadata
            .get_consumer_group(stream, topic, group_id)
            .map(|cg| cg.partitions.clone())
            .unwrap_or_default();

        self.client_manager
            .delete_consumer_group(stream, topic, group_id);

        self.writer().delete_consumer_group(stream, topic, group_id);

        Ok(DeletedConsumerGroup {
            group_id,
            partition_ids,
        })
    }

    /// Join runs on shard 0 (control plane), single-threaded — no concurrent joins.
    pub fn join_consumer_group(
        &self,
        client_id: u32,
        group: ResolvedConsumerGroup,
    ) -> Result<(), IggyError> {
        let valid_client_ids: Vec<u32> = self
            .client_manager
            .get_clients()
            .iter()
            .map(|c| c.session.client_id)
            .collect();

        let (_, completable) = self.writer().join_consumer_group(
            group.stream_id,
            group.topic_id,
            group.group_id,
            client_id,
            Some(valid_client_ids),
        );

        for revocation in completable {
            self.writer().complete_partition_revocation(
                group.stream_id,
                group.topic_id,
                group.group_id,
                revocation.slab_id,
                revocation.member_id,
                revocation.partition_id,
                false,
            );
        }

        if let Some(cg) =
            self.metadata
                .get_consumer_group(group.stream_id, group.topic_id, group.group_id)
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
                    let _ = self.writer().leave_consumer_group(
                        group.stream_id,
                        group.topic_id,
                        group.group_id,
                        stale_client_id,
                    );
                }
            }
        }

        self.client_manager
            .join_consumer_group(client_id, group.stream_id, group.topic_id, group.group_id)
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
        group: ResolvedConsumerGroup,
    ) -> Result<(), IggyError> {
        let member_id = self.writer().leave_consumer_group(
            group.stream_id,
            group.topic_id,
            group.group_id,
            client_id,
        );

        if member_id.is_none() {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                client_id,
                Identifier::numeric(group.group_id as u32).unwrap(),
                Identifier::numeric(group.topic_id as u32).unwrap(),
            ));
        }

        self.client_manager
            .leave_consumer_group(client_id, group.stream_id, group.topic_id, group.group_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to make client leave consumer group for client ID: {}",
                    client_id
                )
            })?;

        Ok(())
    }
}
