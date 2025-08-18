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
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let cg = shard.delete_consumer_group2(session, &self.stream_id, &self.topic_id, &self.group_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {} for topic with ID: {} in stream with ID: {} for session: {}",
                self.group_id, self.topic_id, self.stream_id, session
            )
        })?;
        let cg_id = cg.id();
        let event = ShardEvent::DeletedConsumerGroup2 {
            id: cg_id,
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            group_id: self.group_id.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteConsumerGroup(self),
            )
            .await
            .with_error_context(|error| {
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
