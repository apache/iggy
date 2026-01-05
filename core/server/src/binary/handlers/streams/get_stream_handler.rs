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

use crate::binary::command::{
    BinaryServerCommand, HandlerResult, ServerCommand, ServerCommandHandler,
};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::SenderKind;
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
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;

        // Read from SharedMetadata for cross-shard consistency
        let metadata = shard.shared_metadata.load();
        let stream_id = match metadata.get_stream_id(&self.stream_id) {
            Some(id) => id,
            None => {
                sender.send_empty_ok_response().await?;
                return Ok(HandlerResult::Finished);
            }
        };

        let has_permission = shard
            .permissioner
            .get_stream(session.get_user_id(), stream_id)
            .error(|e: &IggyError| {
                format!(
                    "permission denied to get stream with ID: {} for user with ID: {}, error: {e}",
                    self.stream_id,
                    session.get_user_id(),
                )
            })
            .is_ok();
        if !has_permission {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        }

        let stream_meta = metadata
            .streams
            .get(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;

        // Get stats from SharedStatsStore
        let stream_stats = shard.shared_stats.get_stream_stats(stream_id);

        let response = mapper::map_stream_from_metadata_with_stats(
            stream_meta,
            stream_stats.as_deref(),
            |topic_id| shard.shared_stats.get_topic_stats(stream_id, topic_id),
        );
        sender.send_ok_response(&response).await?;
        Ok(HandlerResult::Finished)
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
