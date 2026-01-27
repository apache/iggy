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

use crate::binary::command::{
    BinaryServerCommand, HandlerResult, ServerCommand, ServerCommandHandler,
};
use crate::binary::handlers::personal_access_tokens::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::{Identifier, IggyError, SenderKind};
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
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let user_id = session.get_user_id();

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::CreatePersonalAccessToken {
                user_id,
                name: self.name.clone(),
                expiry: self.expiry,
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::CreatePersonalAccessToken { name, expiry, .. } =
                        payload
                {
                    let (personal_access_token, token) = shard
                        .create_personal_access_token(user_id, &name, expiry)
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to create personal access token with name: {name}, user: {user_id}"
                            )
                        })?;

                    let bytes = mapper::map_raw_pat(&token);
                    let hash = personal_access_token.token.to_string();

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
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply create personal access token with name: {}, session: {session}",
                                self.name
                            )
                        })?;

                    sender.send_ok_response(&bytes).await?;
                } else {
                    unreachable!(
                        "Expected a CreatePersonalAccessToken request inside of CreatePersonalAccessToken handler"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreatePersonalAccessTokenResponse(_pat, token) => {
                    let bytes = mapper::map_raw_pat(&token);
                    sender.send_ok_response(&bytes).await?;
                }
                ShardResponse::ErrorResponse(err) => return Err(err),
                _ => unreachable!(
                    "Expected a CreatePersonalAccessTokenResponse inside of CreatePersonalAccessToken handler"
                ),
            },
        };
        Ok(HandlerResult::Finished)
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
