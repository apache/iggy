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

use crate::shard::IggyShard;
use std::rc::Rc;

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::state::models::CreateUserWithId;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::create_user::CreateUser;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateUser {
    fn code(&self) -> u32 {
        iggy_common::CREATE_USER_CODE
    }

    #[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let user = shard
                .create_user(
                    session,
                    &self.username,
                    &self.password,
                    self.status,
                    self.permissions.clone(),
                )
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create user with name: {}, session: {session}",
                        self.username
                    )
                })?;
        let user_id = user.id;
        let response = mapper::map_user(&user);

        // For the security of the system, we hash the password before storing it in metadata.
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
                }
            }),
        )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create user with name: {}, session: {session}",
                    self.username
                )
            })?;
        sender.send_ok_response(&response).await?;
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
