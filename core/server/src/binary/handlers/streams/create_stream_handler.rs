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
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard_info;
use crate::state::command::EntryCommand;
use crate::state::models::CreateStreamWithId;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::StreamStats;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::create_stream::CreateStream;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateStream {
    fn code(&self) -> u32 {
        iggy_common::CREATE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_create_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id;
        let name = self.name.clone();
        let stats = Arc::new(StreamStats::new());

        let new_stream_id = shard
            .create_stream2(session, stream_id, self.name.clone(), stats.clone())
            .await?;
        shard_info!(
            shard.id,
            "Created stream with new API, Stream ID: {}, name: '{}'.",
            new_stream_id,
            name
        );
        let event = ShardEvent::CreatedStream2 {
            id: new_stream_id,
            name: self.name.clone(),
            stats,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        //TODO: Replace the mapping from line 89 with this once the Stream layer is finished.
        let _ = shard.streams2.with_stream_by_id(
            &Identifier::numeric(new_stream_id as u32).unwrap(),
            |stream| mapper::map_stream2(stream),
        );

        let created_stream_id = shard
                .create_stream(session, stream_id, &name)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create stream with id: {stream_id:?}, session: {session}"
                    )
                })?;
        shard_info!(
            shard.id,
            "Created stream with ID: {}, name: '{}'.",
            created_stream_id,
            name
        );
        let event = ShardEvent::CreatedStream { stream_id, name };
        // Broadcast the event to all shards.
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        let stream = shard.find_stream(session, &created_stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to find created stream with id: {created_stream_id:?}, session: {session}"
                )
            })?;
        let response = mapper::map_stream(&stream);

        shard
            .state
        .apply(session.get_user_id(), &EntryCommand::CreateStream(CreateStreamWithId {
            stream_id: stream.stream_id,
            command: self
        }))            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create stream for id: {stream_id:?}, session: {session}"
                )
            })?;
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreateStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateStream(create_stream) => Ok(create_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
