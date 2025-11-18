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
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::slab::traits_ext::EntityComponentSystem;
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::get_stream::GetStream;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetStream {
    fn code(&self) -> u32 {
        iggy_common::GET_STREAM_CODE
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
        let exists = shard.ensure_stream_exists(&self.stream_id).is_ok();
        if !exists {
            sender.send_empty_ok_response().await?;
            return Ok(());
        }
        let stream_id = shard
            .streams
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        let has_permission = shard
            .permissioner
            .borrow()
            .get_stream(session.get_user_id(), stream_id)
            .with_error(|e| {
                format!(
                    "permission denied to get stream with ID: {} for user with ID: {}, error: {e}",
                    self.stream_id,
                    session.get_user_id(),
                )
            })
            .is_ok();
        if !has_permission {
            sender.send_empty_ok_response().await?;
            return Ok(());
        }
        let response = shard
            .streams
            .with_components_by_id(stream_id, |(root, stats)| mapper::map_stream(&root, &stats));
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetStream(get_stream) => Ok(get_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
