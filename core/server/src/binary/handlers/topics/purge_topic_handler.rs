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
use crate::binary::handlers::topics::COMPONENT;
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
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for PurgeTopic {
    fn code(&self) -> u32 {
        iggy_common::PURGE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_purge_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let (stream_id, topic_id) = shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        shard
            .metadata
            .perm_purge_topic(session.get_user_id(), stream_id, topic_id)?;

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::PurgeTopic {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
            },
        };

        let stream_id_for_log = self.stream_id.clone();
        let topic_id_for_log = self.topic_id.clone();
        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::PurgeTopic {
                        stream_id,
                        topic_id,
                        ..
                    } = payload
                {
                    shard
                        .purge_topic(&stream_id, &topic_id)
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to purge topic with id: {topic_id_for_log}, stream_id: {stream_id_for_log}"
                            )
                        })?;

                    let event = ShardEvent::PurgedTopic {
                        stream_id: stream_id.clone(),
                        topic_id: topic_id.clone(),
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::PurgeTopic(self))
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply purge topic with id: {topic_id_for_log}, stream_id: {stream_id_for_log}"
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected a PurgeTopic request inside of PurgeTopic handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::PurgeTopicResponse => {
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a PurgeTopicResponse inside of PurgeTopic handler, impossible state"
                ),
            },
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for PurgeTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PurgeTopic(purge_topic) => Ok(purge_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
