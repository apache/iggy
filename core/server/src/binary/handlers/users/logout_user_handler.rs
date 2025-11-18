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
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::logout_user::LogoutUser;
use tracing::info;
use tracing::{debug, instrument};

impl ServerCommandHandler for LogoutUser {
    fn code(&self) -> u32 {
        iggy_common::LOGOUT_USER_CODE
    }

    #[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        info!("Logging out user with ID: {}...", session.get_user_id());
        shard.logout_user(session).with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to logout user, session: {session}")
        })?;
        info!("Logged out user with ID: {}.", session.get_user_id());
        session.clear_user_id();
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for LogoutUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LogoutUser(logout_user) => Ok(logout_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
