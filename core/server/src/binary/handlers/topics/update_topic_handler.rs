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
use crate::binary::{handlers::topics::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::{streams, topics};
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdateTopic {
    fn code(&self) -> u32 {
        iggy_common::UPDATE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_update_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        mut self,
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
            payload: ShardRequestPayload::UpdateTopic {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                name: self.name.clone(),
                message_expiry: self.message_expiry,
                compression_algorithm: self.compression_algorithm,
                max_topic_size: self.max_topic_size,
                replication_factor: self.replication_factor,
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::UpdateTopic {
                        stream_id,
                        topic_id,
                        name,
                        message_expiry,
                        compression_algorithm,
                        max_topic_size,
                        replication_factor,
                        ..
                    } = payload
                {
                    shard.update_topic(
                        session,
                        &stream_id,
                        &topic_id,
                        name.clone(),
                        message_expiry,
                        compression_algorithm,
                        max_topic_size,
                        replication_factor,
                    )?;

                    let name_changed = !name.is_empty();
                    let lookup_topic_id = if name_changed {
                        Identifier::named(&name).unwrap()
                    } else {
                        topic_id.clone()
                    };

                    self.message_expiry = shard.streams.with_topic_by_id(
                        &stream_id,
                        &lookup_topic_id,
                        topics::helpers::get_message_expiry(),
                    );
                    self.max_topic_size = shard.streams.with_topic_by_id(
                        &stream_id,
                        &lookup_topic_id,
                        topics::helpers::get_max_topic_size(),
                    );

                    let stream_id_num = shard
                        .streams
                        .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());

                    let event = ShardEvent::UpdatedTopic {
                        stream_id: stream_id.clone(),
                        topic_id: topic_id.clone(),
                        name: name.clone(),
                        message_expiry: self.message_expiry,
                        compression_algorithm: self.compression_algorithm,
                        max_topic_size: self.max_topic_size,
                        replication_factor: self.replication_factor,
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::UpdateTopic(self))
                        .await
                        .with_error(|error| format!(
                            "{COMPONENT} (error: {error}) - failed to apply update topic with id: {topic_id} in stream with ID: {stream_id_num}, session: {session}"
                        ))?;
                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected an UpdateTopic request inside of UpdateTopic handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::UpdateTopicResponse => {
                    let stream_id = shard
                        .streams
                        .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::UpdateTopic(self))
                        .await
                        .with_error(|error| format!(
                            "{COMPONENT} (error: {error}) - failed to apply update topic in stream with ID: {stream_id}, session: {session}"
                        ))?;

                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected an UpdateTopicResponse inside of UpdateTopic handler, impossible state"
                ),
            },
        }

        Ok(())
    }
}

impl BinaryServerCommand for UpdateTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdateTopic(update_topic) => Ok(update_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
