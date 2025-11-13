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
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::{debug, instrument};

impl ServerCommandHandler for LoginWithPersonalAccessToken {
    fn code(&self) -> u32 {
        iggy_common::LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_login_with_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let user = shard
            .login_with_personal_access_token(&self.token, Some(session))
            .with_error(|error| {
                let redacted_token = if self.token.len() > 4 {
                    format!("{}****", &self.token[..4])
                } else {
                    "****".to_string()
                };
                format!(
                    "{COMPONENT} (error: {error}) - failed to login with personal access token: {redacted_token}, session: {session}",
                )
            })?;
        let identity_info = mapper::map_identity_info(user.id);
        sender.send_ok_response(&identity_info).await?;
        Ok(())
    }
}

impl BinaryServerCommand for LoginWithPersonalAccessToken {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LoginWithPersonalAccessToken(login_with_personal_access_token) => {
                Ok(login_with_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
