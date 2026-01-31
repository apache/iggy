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
use crate::binary::handlers::streams::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for PurgeStream {
    fn code(&self) -> u32 {
        iggy_common::PURGE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let stream_id = shard.resolve_stream_id(&self.stream_id)?;
        shard
            .metadata
            .perm_purge_stream(session.get_user_id(), stream_id)?;

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::PurgeStream {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
            },
        };

        let stream_id_for_log = self.stream_id.clone();
        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::PurgeStream { stream_id, .. } = payload
                {
                    shard.purge_stream(&stream_id).await.error(|e: &IggyError| {
                        format!(
                            "{COMPONENT} (error: {e}) - failed to purge stream with id: {stream_id_for_log}, session: {session}"
                        )
                    })?;

                    let event = ShardEvent::PurgedStream {
                        stream_id: stream_id.clone(),
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::PurgeStream(self))
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply purge stream with id: {stream_id_for_log}, session: {session}"
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected a PurgeStream request inside of PurgeStream handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::PurgeStreamResponse => {
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a PurgeStreamResponse inside of PurgeStream handler, impossible state"
                ),
            },
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for PurgeStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PurgeStream(purge_stream) => Ok(purge_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
