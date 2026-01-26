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
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

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
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let token_name = self.name.clone();
        let user_id = session.get_user_id();

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::DeletePersonalAccessToken {
                user_id,
                name: self.name.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::DeletePersonalAccessToken { name, .. } = payload
                {
                    shard.delete_personal_access_token(user_id, &name).error(
                        |e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to delete personal access token with name: {name}, user: {user_id}"
                            )
                        },
                    )?;

                    shard
                        .state
                        .apply(
                            session.get_user_id(),
                            &EntryCommand::DeletePersonalAccessToken(DeletePersonalAccessToken {
                                name: self.name,
                            }),
                        )
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply delete personal access token with name: {token_name}, session: {session}"
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected a DeletePersonalAccessToken request inside of DeletePersonalAccessToken handler"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::DeletePersonalAccessTokenResponse => {
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => return Err(err),
                _ => unreachable!(
                    "Expected a DeletePersonalAccessTokenResponse inside of DeletePersonalAccessToken handler"
                ),
            },
        }
        Ok(HandlerResult::Finished)
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
