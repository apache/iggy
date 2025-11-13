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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::delete_user::DeleteUser;
use iggy_common::{Identifier, IggyError};
use tracing::info;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteUser {
    fn code(&self) -> u32 {
        iggy_common::DELETE_USER_CODE
    }

    #[instrument(skip_all, name = "trace_delete_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::DeleteUser {
                session_user_id: session.get_user_id(),
                user_id: self.user_id,
            },
        };
        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(shard_message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = shard_message
                    && let ShardRequestPayload::DeleteUser { user_id, .. } = payload
                {
                    info!("Deleting user with ID: {}...", user_id);
                    let _user_guard = shard.fs_locks.user_lock.lock().await;
                    let user = shard
                        .delete_user(session, &user_id)
                        .with_error(|error| {
                        format!(
                            "{COMPONENT} (error: {error}) - failed to delete user with ID: {}, session: {session}",
                            user_id
                        )
                    })?;

                    info!("Deleted user: {} with ID: {}.", user.username, user.id);
                    let event = ShardEvent::DeletedUser { user_id };
                    shard.broadcast_event_to_all_shards(event).await?;

                    shard
                        .state
                        .apply(user.id, &EntryCommand::DeleteUser(DeleteUser { user_id: user.id.try_into().unwrap() }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply delete user with ID: {user_id}, session: {session}",
                                user_id = user.id,
                                session = session
                            )
                        })?;
                    sender.send_empty_ok_response().await?;
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::DeletedUser(user) => {
                    shard
                        .state
                        .apply(user.id, &EntryCommand::DeleteUser(DeleteUser { user_id: user.id.try_into().unwrap() }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply delete user with ID: {user_id}, session: {session}",
                                user_id = user.id,
                                session = session
                            )
                        })?;
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => {
                    unreachable!(
                        "Expected a DeleteUser request inside of DeleteUser handler, impossible state"
                    );
                }
            },
        }

        Ok(())
    }
}

impl BinaryServerCommand for DeleteUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteUser(delete_user) => Ok(delete_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
