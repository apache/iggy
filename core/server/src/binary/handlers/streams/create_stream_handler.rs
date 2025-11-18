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
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::slab::traits_ext::{EntityComponentSystem, EntityMarker};
use crate::state::command::EntryCommand;
use crate::state::models::CreateStreamWithId;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::create_stream::CreateStream;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
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
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::CreateStream {
                user_id: session.get_user_id(),
                name: self.name.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::CreateStream { name, .. } = payload
                {
                    // Acquire stream lock to serialize filesystem operations
                    let _stream_guard = shard.fs_locks.stream_lock.lock().await;

                    let stream = shard.create_stream(session, name).await?;
                    let created_stream_id = stream.id();

                    let event = ShardEvent::CreatedStream {
                        id: created_stream_id,
                        stream: stream.clone(),
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    let response = shard
                        .streams
                        .with_components_by_id(created_stream_id, |(root, stats)| {
                            mapper::map_stream(&root, &stats)
                        });

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::CreateStream(CreateStreamWithId {
                            stream_id: created_stream_id as u32,
                            command: self
                        }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create stream for id: {created_stream_id}, session: {session}"
                            )
                        })?;

                    sender.send_ok_response(&response).await?;
                } else {
                    unreachable!(
                        "Expected a CreateStream request inside of CreateStream handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreateStreamResponse(stream) => {
                    let created_stream_id = stream.id();
                    let response = mapper::map_stream(stream.root(), stream.stats());

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::CreateStream(CreateStreamWithId {
                            stream_id: created_stream_id as u32,
                            command: self
                        }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create stream for id: {created_stream_id}, session: {session}"
                            )
                        })?;

                    sender.send_ok_response(&response).await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a CreateStreamResponse inside of CreateStream handler, impossible state"
                ),
            },
        }

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
