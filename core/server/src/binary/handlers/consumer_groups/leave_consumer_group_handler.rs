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
use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::shard::BroadcastResult;
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::leave_consumer_group::LeaveConsumerGroup;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for LeaveConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::LEAVE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_leave_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string(), iggy_group_id = self.group_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        shard
            .leave_consumer_group(
                session,
                &self.stream_id,
                &self.topic_id,
                &self.group_id,
            )
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to leave consumer group for stream_id: {}, topic_id: {}, group_id: {}, session: {}",
                    self.stream_id, self.topic_id, self.group_id, session
                )
            })?;

        // Update ClientManager and broadcast event to other shards
        let client_id = session.client_id;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let group_id = self.group_id.clone();

        let event = ShardEvent::LeftConsumerGroup {
            client_id,
            stream_id,
            topic_id,
            group_id,
        };
        match shard.broadcast_event_to_all_shards(event).await {
            BroadcastResult::Success(_) => {}

            BroadcastResult::PartialSuccess { errors, .. } => {
                for (shard_id, error) in errors {
                    tracing::warn!("Shard {} failed to process event: {:?}", shard_id, error);
                }
            }

            BroadcastResult::Failure(_) => {
                return Err(IggyError::ShardCommunicationError(0));
            }
        }

        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for LeaveConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LeaveConsumerGroup(leave_consumer_group) => Ok(leave_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
