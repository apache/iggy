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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::streaming::polling_consumer::ConsumerGroupId;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::DELETE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let cg = shard.delete_consumer_group(session, &self.stream_id, &self.topic_id, &self.group_id).with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {} for topic with ID: {} in stream with ID: {} for session: {}",
                self.group_id, self.topic_id, self.stream_id, session
            )
        })?;
        let cg_id = cg.id();
        let partition_ids = cg.partitions();

        // Remove all consumer group members from ClientManager using helper functions to resolve identifiers
        let stream_id_usize = shard.streams.with_stream_by_id(
            &self.stream_id,
            crate::streaming::streams::helpers::get_stream_id(),
        );
        let topic_id_usize = shard.streams.with_topic_by_id(
            &self.stream_id,
            &self.topic_id,
            crate::streaming::topics::helpers::get_topic_id(),
        );

        // Get members from the deleted consumer group and make them leave
        let slab = cg.members().inner().shared_get();
        for (_, member) in slab.iter() {
            if let Err(err) = shard.client_manager.leave_consumer_group(
                member.client_id,
                stream_id_usize,
                topic_id_usize,
                cg_id,
            ) {
                tracing::warn!(
                    "{COMPONENT} (error: {err}) - failed to make client leave consumer group for client ID: {}, group ID: {}",
                    member.client_id,
                    cg_id
                );
            }
        }

        let cg_id_spez = ConsumerGroupId(cg_id);
        // Delete all consumer group offsets for this group using the specialized method
        shard.delete_consumer_group_offsets(
            cg_id_spez,
            &self.stream_id,
            &self.topic_id,
            partition_ids,
        ).await.with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete consumer group offsets for group ID: {} in stream: {}, topic: {}",
                cg_id_spez,
                self.stream_id,
                self.topic_id
            )
        })?;

        let event = ShardEvent::DeletedConsumerGroup {
            id: cg_id,
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            group_id: self.group_id.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteConsumerGroup(self),
            )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply delete consumer group for stream_id: {}, topic_id: {}, group_id: {cg_id}, session: {session}",
                    stream_id, topic_id
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteConsumerGroup(delete_consumer_group) => Ok(delete_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
