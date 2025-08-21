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
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard_info;
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_stream::DeleteStream;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteStream {
    fn code(&self) -> u32 {
        iggy_common::DELETE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();

        let stream2 = shard
            .delete_stream2(session, &self.stream_id)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete stream2 with ID: {stream_id}, session: {session}")
            })?;
        shard_info!(
            shard.id,
            "Deleted stream with name: {}, ID: {}",
            stream2.root().name(),
            stream2.id()
        );
        let event = ShardEvent::DeletedStream2 {
            id: stream2.id(),
            stream_id: self.stream_id.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;
        // Drop the stream to force readers/writers to be dropped.
        drop(stream2);
        // Stream files and directories have been deleted by delete_stream_from_disk

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteStream(self))
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteStream(delete_stream) => Ok(delete_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
