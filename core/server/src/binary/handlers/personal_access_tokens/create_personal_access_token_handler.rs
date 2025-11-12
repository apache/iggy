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
use crate::binary::mapper;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreatePersonalAccessToken {
    fn code(&self) -> u32 {
        iggy_common::CREATE_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_create_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let (personal_access_token, token) = shard
                .create_personal_access_token(session, &self.name, self.expiry)
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                        self.name
                    )
                })?;
        let bytes = mapper::map_raw_pat(&token);
        let hash = personal_access_token.token.to_string();
        let event = ShardEvent::CreatedPersonalAccessToken {
            personal_access_token: personal_access_token.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                    command: CreatePersonalAccessToken {
                        name: self.name.to_owned(),
                        expiry: self.expiry,
                    },
                    hash,
                }),
            )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                    self.name
                )
            })?;
        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreatePersonalAccessToken {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreatePersonalAccessToken(create_personal_access_token) => {
                Ok(create_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
