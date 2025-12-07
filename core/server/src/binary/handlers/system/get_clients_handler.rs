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

use err_trail::ErrContext;
use iggy_common::{IggyError, SenderKind, get_clients::GetClients};
use tracing::debug;

use crate::{
    binary::{
        command::{BinaryServerCommand, ServerCommand, ServerCommandHandler},
        handlers::{system::COMPONENT, utils::receive_and_validate},
        mapper,
    },
    shard::IggyShard,
    streaming::session::Session,
};

impl ServerCommandHandler for GetClients {
    fn code(&self) -> u32 {
        iggy_common::GET_CLIENTS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let clients = shard.get_clients(session).with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get clients, session: {session}")
        })?;
        let clients = mapper::map_clients(clients).await;
        sender.send_ok_response(&clients).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetClients {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetClients(get_clients) => Ok(get_clients),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
