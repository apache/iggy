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
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::state::models::CreateUserWithId;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::create_user::CreateUser;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::debug;
use tracing::instrument;

impl ServerCommandHandler for CreateUser {
    fn code(&self) -> u32 {
        iggy_common::CREATE_USER_CODE
    }

    #[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
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
            payload: ShardRequestPayload::CreateUser {
                user_id: session.get_user_id(),
                username: self.username.clone(),
                password: self.password.clone(),
                status: self.status,
                permissions: self.permissions.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::CreateUser {
                        username,
                        password,
                        status,
                        permissions,
                        ..
                    } = payload
                {
                    let _user_guard = shard.fs_locks.user_lock.lock().await;
                    let user = shard
                        .create_user(session, &username, &password, status, permissions.clone())
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to create user with name: {}, session: {}",
                                username, session
                            )
                        })?;

                    let user_id = user.id;

                    let event = ShardEvent::CreatedUser {
                        user_id,
                        username: username.clone(),
                        password: password.clone(),
                        status,
                        permissions: permissions.clone(),
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    let response = mapper::map_user(&user);

                    shard
                        .state
                        .apply(
                            session.get_user_id(),
                            &EntryCommand::CreateUser(CreateUserWithId {
                                user_id,
                                command: CreateUser {
                                    username: self.username.to_owned(),
                                    password: crypto::hash_password(&self.password),
                                    status: self.status,
                                    permissions: self.permissions.clone(),
                                },
                            }),
                        )
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create user with name: {}, session: {session}",
                                self.username
                            )
                        })?;

                    sender.send_ok_response(&response).await?;
                } else {
                    unreachable!(
                        "Expected a CreateUser request inside of CreateUser handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreateUserResponse(user) => {
                    let user_id = user.id;
                    let response = mapper::map_user(&user);

                    shard
                        .state
                        .apply(
                            session.get_user_id(),
                            &EntryCommand::CreateUser(CreateUserWithId {
                                user_id,
                                command: CreateUser {
                                    username: self.username.to_owned(),
                                    password: crypto::hash_password(&self.password),
                                    status: self.status,
                                    permissions: self.permissions.clone(),
                                },
                            }),
                        )
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create user for user_id: {user_id}, session: {session}"
                            )
                        })?;

                    sender.send_ok_response(&response).await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a CreateUserResponse inside of CreateUser handler, impossible state"
                ),
            },
        }

        Ok(())
    }
}

impl BinaryServerCommand for CreateUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateUser(create_user) => Ok(create_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
