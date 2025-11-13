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
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::info;
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
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::DeleteStream {
                user_id: session.get_user_id(),
                stream_id: self.stream_id,
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::DeleteStream { stream_id, .. } = payload
                {
                    // Acquire stream lock to serialize filesystem operations
                    let _stream_guard = shard.fs_locks.stream_lock.lock().await;
                    let stream = shard
                            .delete_stream(session, &stream_id)
                            .await
                            .with_error(|error| {
                                format!("{COMPONENT} (error: {error}) - failed to delete stream with ID: {stream_id}, session: {session}")
                            })?;
                    info!(
                        "Deleted stream with name: {}, ID: {}",
                        stream.root().name(),
                        stream.id()
                    );

                    let event = ShardEvent::DeletedStream {
                        id: stream.id(),
                        stream_id: stream_id.clone(),
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::DeleteStream(DeleteStream { stream_id: stream_id.clone() }))
                        .await
                        .with_error(|error| {
                            format!("{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}, session: {session}")
                        })?;
                    sender.send_empty_ok_response().await?;
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::DeleteStreamResponse(stream) => {
                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::DeleteStream(DeleteStream { stream_id: (stream.id() as u32).try_into().unwrap() }))
                        .await
                        .with_error(|error| {
                            format!("{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}, session: {session}")
                        })?;
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => {
                    unreachable!(
                        "Expected a DeleteStreamResponse inside of DeleteStream handler, impossible state"
                    );
                }
            },
        }

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
