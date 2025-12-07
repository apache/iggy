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

use std::rc::Rc;

use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::{IggyError, SenderKind, delete_personal_access_token::DeletePersonalAccessToken};
use tracing::{debug, instrument};

use crate::{
    binary::{
        command::{BinaryServerCommand, ServerCommand, ServerCommandHandler},
        handlers::{personal_access_tokens::COMPONENT, utils::receive_and_validate},
    },
    shard::IggyShard,
    state::command::EntryCommand,
    streaming::session::Session,
};

impl ServerCommandHandler for DeletePersonalAccessToken {
    fn code(&self) -> u32 {
        iggy_common::DELETE_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_delete_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let token_name = self.name.clone();

        shard
                .delete_personal_access_token(session, &self.name)
                .with_error(|error| {format!(
                    "{COMPONENT} (error: {error}) - failed to delete personal access token with name: {token_name}, session: {session}"
                )})?;

        // Broadcast the event to other shards
        let event = crate::shard::transmission::event::ShardEvent::DeletedPersonalAccessToken {
            user_id: session.get_user_id(),
            name: self.name.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeletePersonalAccessToken(DeletePersonalAccessToken {
                    name: self.name,
                }),
            )
            .await
            .with_error(|error| {format!(
                "{COMPONENT} (error: {error}) - failed to apply delete personal access token with name: {token_name}, session: {session}"
            )})?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeletePersonalAccessToken {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeletePersonalAccessToken(delete_personal_access_token) => {
                Ok(delete_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
