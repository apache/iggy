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
use crate::binary::handlers::streams::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::slab::traits_ext::{EntityComponentSystem, IntoComponents};
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::get_streams::GetStreams;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetStreams {
    fn code(&self) -> u32 {
        iggy_common::GET_STREAMS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard
            .permissioner
            .borrow()
            .get_streams(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get streams for user {}",
                    session.get_user_id()
                )
            })?;

        let response = shard.streams.with_components(|stream_ref| {
            let (roots, stats) = stream_ref.into_components();
            mapper::map_streams(&roots, &stats)
        });
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetStreams {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetStreams(get_streams) => Ok(get_streams),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
