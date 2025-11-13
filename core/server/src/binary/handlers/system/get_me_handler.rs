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
use crate::binary::handlers::system::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::get_me::GetMe;
use std::rc::Rc;

impl ServerCommandHandler for GetMe {
    fn code(&self) -> u32 {
        iggy_common::GET_ME_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        let Some(client) = shard
            .get_client(session, session.client_id)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get current client for session: {session}")
            })?
        else {
            return Err(IggyError::ClientNotFound(session.client_id));
        };

        let bytes = mapper::map_client(&client);

        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetMe {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetMe(get_me) => Ok(get_me),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
