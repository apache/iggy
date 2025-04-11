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
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::delete_stream::DeleteStream;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteStream {
    fn code(&self) -> u32 {
        iggy::command::DELETE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();

        let mut system = system.write().await;
        system
                .delete_stream(session, &self.stream_id)
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to delete stream with ID: {stream_id}, session: {session}")
                })?;

        let system = system.downgrade();
        system
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteStream(self))
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteStream(delete_stream) => Ok(delete_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
