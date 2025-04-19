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
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteConsumerGroup {
    fn code(&self) -> u32 {
        iggy::command::DELETE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let mut system = system.write().await;
        system
                .delete_consumer_group(
                    session,
                    &self.stream_id,
                    &self.topic_id,
                    &self.group_id,
                )
                .await.with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {} for topic with ID: {} in stream with ID: {} for session: {}",
                    self.group_id, self.topic_id, self.stream_id, session
                ))?;

        let system = system.downgrade();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let group_id = self.group_id.clone();

        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteConsumerGroup(self),
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply delete consumer group for stream_id: {}, topic_id: {}, group_id: {:?}, session: {}",
                    stream_id, topic_id, group_id, session
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
