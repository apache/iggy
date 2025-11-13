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
use crate::slab::consumer_groups;
use crate::slab::traits_ext::EntityMarker;
use crate::slab::traits_ext::Insert;
use crate::streaming::partitions;
use crate::streaming::session::Session;
use crate::streaming::streams;
use crate::streaming::topics;
use crate::streaming::topics::consumer_group;
use crate::streaming::topics::consumer_group::MEMBERS_CAPACITY;
use arcshift::ArcShift;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use slab::Slab;

impl IggyShard {
    pub fn create_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> Result<consumer_group::ConsumerGroup, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let exists = self
            .streams
            .with_topic_by_id(stream_id, topic_id, |(root, ..)| {
                root.consumer_groups()
                    .exists(&name.clone().try_into().unwrap())
            });
        if exists {
            return Err(IggyError::ConsumerGroupNameAlreadyExists(
                name,
                topic_id.clone(),
            ));
        }

        {
            let topic_id =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().create_consumer_group(
                session.get_user_id(),
                stream_id,
                topic_id,
            ).with_error(|error| format!("{COMPONENT} (error: {error}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let cg = self.create_and_insert_consumer_group_mem(stream_id, topic_id, name);
        Ok(cg)
    }

    fn create_and_insert_consumer_group_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> consumer_group::ConsumerGroup {
        let partitions = self.streams.with_topics(stream_id, |topics| {
            topics.with_partitions(topic_id, partitions::helpers::get_partition_ids())
        });
        let members = ArcShift::new(Slab::with_capacity(MEMBERS_CAPACITY));
        let mut cg = consumer_group::ConsumerGroup::new(name, members, partitions);
        let id = self.insert_consumer_group_mem(stream_id, topic_id, cg.clone());
        cg.update_id(id);
        cg
    }

    fn insert_consumer_group_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        cg: consumer_group::ConsumerGroup,
    ) -> consumer_groups::ContainerId {
        self.streams
            .with_consumer_groups_mut(stream_id, topic_id, |container| container.insert(cg))
    }

    pub fn create_consumer_group_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        cg: consumer_group::ConsumerGroup,
    ) -> usize {
        self.insert_consumer_group_mem(stream_id, topic_id, cg)
    }

    pub fn delete_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<consumer_group::ConsumerGroup, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;
        {
            let topic_id =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().delete_consumer_group(
                session.get_user_id(),
                stream_id,
                topic_id,
            ).with_error(|error| format!("{COMPONENT} (error: {error}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let cg = self.delete_consumer_group_base(stream_id, topic_id, group_id);
        Ok(cg)
    }

    pub fn delete_consumer_group_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> consumer_group::ConsumerGroup {
        self.delete_consumer_group_base(stream_id, topic_id, group_id)
    }

    fn delete_consumer_group_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> consumer_group::ConsumerGroup {
        // Get numeric IDs before deletion for ClientManager cleanup
        let stream_id_value = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let topic_id_value =
            self.streams
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let group_id_value = self.streams.with_consumer_group_by_id(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::get_consumer_group_id(),
        );

        let cg = self.streams.with_consumer_groups_mut(
            stream_id,
            topic_id,
            topics::helpers::delete_consumer_group(group_id),
        );

        // Clean up ClientManager state
        self.client_manager
            .delete_consumer_group(stream_id_value, topic_id_value, group_id_value);

        cg
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
        {
            let topic_id =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().join_consumer_group(
                session.get_user_id(),
                stream_id,
                topic_id,
            ).with_error(|error| format!("{COMPONENT} (error: {error}) - permission denied to join consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let client_id = session.client_id;
        self.streams.with_consumer_group_by_id_mut(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::join_consumer_group(client_id),
        );

        // Update ClientManager state
        let stream_id_value = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let topic_id_value =
            self.streams
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let group_id_value = self.streams.with_consumer_group_by_id(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::get_consumer_group_id(),
        );

        self.client_manager.join_consumer_group(
            session.client_id,
            stream_id_value,
            topic_id_value,
            group_id_value,
        )
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to make client join consumer group for client ID: {}",
                session.client_id
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
        {
            let topic_id =
                self.streams
                    .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
            let stream_id = self
                .streams
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().leave_consumer_group(
                session.get_user_id(),
                stream_id,
                topic_id,
            ).with_error(|error| format!("{COMPONENT} (error: {error}) - permission denied to leave consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        self.leave_consumer_group_base(stream_id, topic_id, group_id, session.client_id)
    }

    pub fn leave_consumer_group_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        let Some(_) = self.streams.with_consumer_group_by_id_mut(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::leave_consumer_group(client_id),
        ) else {
            return Err(IggyError::ConsumerGroupMemberNotFound(
                client_id,
                group_id.clone(),
                topic_id.clone(),
            ));
        };

        self.streams.with_consumer_group_by_id_mut(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::rebalance_consumer_group(),
        );

        // Update ClientManager state
        let stream_id_value = self
            .streams
            .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let topic_id_value =
            self.streams
                .with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());
        let group_id_value = self.streams.with_consumer_group_by_id(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::get_consumer_group_id(),
        );

        self.client_manager.leave_consumer_group(
            client_id,
            stream_id_value,
            topic_id_value,
            group_id_value,
        ).with_error(|error| format!("{COMPONENT} (error: {error}) - failed to make client leave consumer group for client ID: {}", client_id))?;
        Ok(())
    }
}
