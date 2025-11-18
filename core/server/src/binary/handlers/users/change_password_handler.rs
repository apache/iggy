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
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::utils::crypto;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::change_password::ChangePassword;
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
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        info!("Changing password for user with ID: {}...", self.user_id);
        shard
                .change_password(
                    session,
                    &self.user_id,
                    &self.current_password,
                    &self.new_password,
                )
                .with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to change password for user_id: {}, session: {session}",
                        self.user_id
                    )
                })?;

        info!("Changed password for user with ID: {}.", self.user_id);

        let event = ShardEvent::ChangedPassword {
            user_id: self.user_id.clone(),
            current_password: self.current_password.clone(),
            new_password: self.new_password.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;

        // For the security of the system, we hash the password before storing it in metadata.
        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::ChangePassword(ChangePassword {
                    user_id: self.user_id.to_owned(),
                    current_password: "".into(),
                    new_password: crypto::hash_password(&self.new_password),
                }),
            )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply change password for user_id: {}, session: {session}",
                    self.user_id
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
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
