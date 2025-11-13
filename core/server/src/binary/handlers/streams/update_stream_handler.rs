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
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::update_stream::UpdateStream;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdateStream {
    fn code(&self) -> u32 {
        iggy_common::UPDATE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_update_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        shard
        .update_stream(session, &self.stream_id, self.name.clone())
        .with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to update stream with id: {stream_id}, session: {session}")
        })?;

        let event = ShardEvent::UpdatedStream {
            stream_id: self.stream_id.clone(),
            name: self.name.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;
        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::UpdateStream(self))
            .await
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply update stream with id: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for UpdateStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdateStream(update_stream) => Ok(update_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
