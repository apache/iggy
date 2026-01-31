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
use crate::binary::handlers::users::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use err_trail::ErrContext;
use iggy_common::change_password::ChangePassword;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::info;
use tracing::{debug, instrument};

impl ServerCommandHandler for ChangePassword {
    fn code(&self) -> u32 {
        iggy_common::CHANGE_PASSWORD_CODE
    }

    #[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;

        // Check if user is changing someone else's password
        let target_user = shard.get_user(&self.user_id)?;
        if target_user.id != session.get_user_id() {
            shard.metadata.perm_change_password(session.get_user_id())?;
        }

        let user_id_for_log = self.user_id.clone();
        let new_password_hash = crypto::hash_password(&self.new_password);
        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::ChangePassword {
                session_user_id: session.get_user_id(),
                user_id: self.user_id.clone(),
                current_password: self.current_password.clone(),
                new_password: self.new_password.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::ChangePassword {
                        user_id,
                        current_password,
                        new_password,
                        ..
                    } = payload
                {
                    info!("Changing password for user with ID: {}...", user_id);

                    let _user_guard = shard.fs_locks.user_lock.lock().await;
                    shard
                        .change_password(&user_id, &current_password, &new_password)
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to change password for user_id: {}, session: {session}",
                                user_id
                            )
                        })?;

                    info!("Changed password for user with ID: {}.", user_id_for_log);

                    shard
                        .state
                        .apply(
                            session.get_user_id(),
                            &EntryCommand::ChangePassword(ChangePassword {
                                user_id: user_id_for_log.clone(),
                                current_password: "".into(),
                                new_password: new_password_hash.clone(),
                            }),
                        )
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply change password for user_id: {}, session: {session}",
                                user_id_for_log
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected a ChangePassword request inside of ChangePassword handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::ChangePasswordResponse => {
                    info!("Changed password for user with ID: {}.", user_id_for_log);
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a ChangePasswordResponse inside of ChangePassword handler, impossible state"
                ),
            },
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for ChangePassword {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::ChangePassword(change_password) => Ok(change_password),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
