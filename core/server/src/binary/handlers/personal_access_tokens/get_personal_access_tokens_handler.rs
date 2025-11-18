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
use crate::binary::handlers::personal_access_tokens::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::get_personal_access_tokens::GetPersonalAccessTokens;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetPersonalAccessTokens {
    fn code(&self) -> u32 {
        iggy_common::GET_PERSONAL_ACCESS_TOKENS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let personal_access_tokens = shard
            .get_personal_access_tokens(session)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get personal access tokens with session: {session}")
            })?;
        let personal_access_tokens = mapper::map_personal_access_tokens(personal_access_tokens);
        sender.send_ok_response(&personal_access_tokens).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetPersonalAccessTokens {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetPersonalAccessTokens(get_personal_access_tokens) => {
                Ok(get_personal_access_tokens)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
