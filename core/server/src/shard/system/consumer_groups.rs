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
use crate::streaming::topics::consumer_group2;
use crate::streaming::topics::consumer_group2::MEMBERS_CAPACITY;
use arcshift::ArcShift;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use slab::Slab;

impl IggyShard {
    pub fn create_consumer_group2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> Result<consumer_group2::ConsumerGroup, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().create_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to create consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let cg = self.create_and_insert_consumer_group_mem(stream_id, topic_id, name);
        Ok(cg)
    }

    fn create_and_insert_consumer_group_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
    ) -> consumer_group2::ConsumerGroup {
        let partitions = self.streams2.with_topics(stream_id, |topics| {
            topics.with_partitions(topic_id, partitions::helpers::get_partition_ids())
        });
        let members = ArcShift::new(Slab::with_capacity(MEMBERS_CAPACITY));
        let mut cg = consumer_group2::ConsumerGroup::new(name, members, partitions);
        let id = self.insert_consumer_group_mem(stream_id, topic_id, cg.clone());
        cg.update_id(id);
        cg
    }

    fn insert_consumer_group_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        cg: consumer_group2::ConsumerGroup,
    ) -> consumer_groups::ContainerId {
        self.streams2
            .with_consumer_groups_mut(stream_id, topic_id, |container| container.insert(cg))
    }

    pub fn create_consumer_group_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        cg: consumer_group2::ConsumerGroup,
    ) -> usize {
        self.insert_consumer_group_mem(stream_id, topic_id, cg)
    }

    pub fn delete_consumer_group2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<consumer_group2::ConsumerGroup, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_consumer_group_exists(stream_id, topic_id, group_id)?;
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().delete_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to delete consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let cg = self.delete_consumer_group_base2(stream_id, topic_id, group_id);
        Ok(cg)
    }

    pub fn delete_consumer_group_bypass_auth2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> consumer_group2::ConsumerGroup {
        self.delete_consumer_group_base2(stream_id, topic_id, group_id)
    }

    fn delete_consumer_group_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> consumer_group2::ConsumerGroup {
        self.streams2.with_consumer_groups_mut(
            stream_id,
            topic_id,
            topics::helpers::delete_consumer_group(group_id),
        )
        // TODO: remove from the consumer_group
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
            let topic_id = self.streams2.with_topic_by_id(
                &stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().join_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to join consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        let client_id = session.client_id;
        self.streams2.with_consumer_group_by_id_mut(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::join_consumer_group(self.id, client_id),
        );

        // TODO:
        /*
        self.client_manager.borrow_mut().join_consumer_group(session.client_id, stream_id_value, topic_id_value, group_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to make client join consumer group for client ID: {}",
                    session.client_id
                )
            })?;
            */
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
        {
            let topic_id = self.streams2.with_topic_by_id(
                stream_id,
                topic_id,
                topics::helpers::get_topic_id(),
            );
            let stream_id = self
                .streams2
                .with_stream_by_id(stream_id, streams::helpers::get_stream_id());
            self.permissioner.borrow().leave_consumer_group(
                session.get_user_id(),
                stream_id as u32,
                topic_id as u32,
            ).with_error_context(|error| format!("{COMPONENT} (error: {error}) - permission denied to leave consumer group for user {} on stream ID: {}, topic ID: {}", session.get_user_id(), stream_id, topic_id))?;
        }
        self.streams2.with_consumer_group_by_id_mut(
            stream_id,
            topic_id,
            group_id,
            topics::helpers::leave_consumer_group(self.id, session.client_id),
        );

        // TODO:
        // self.leave_consumer_group_by_client();
        Ok(())
    }

    pub fn leave_consumer_group_by_client(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        client_id: u32,
    ) -> Result<(), IggyError> {
        // TODO:
        /*
        self.client_manager.borrow_mut().leave_consumer_group(
            client_id,
            stream_id_value,
            topic_id_value,
            group_id,
        )
        */
        Ok(())
    }
}
