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
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{Identifier, IggyError, SenderKind};
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
    ) -> Result<HandlerResult, IggyError> {
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

                    // Get stream and topic IDs from SharedMetadata
                    let stream_id_num = shard
                        .shared_metadata
                        .get_stream_id(&stream_id)
                        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                    let topic_id_num = shard
                        .shared_metadata
                        .get_topic_id(stream_id_num, &lookup_topic_id)
                        .ok_or_else(|| {
                            IggyError::TopicIdNotFound(lookup_topic_id.clone(), stream_id.clone())
                        })?;

                    // Get topic metadata from SharedMetadata
                    let snapshot = shard.shared_metadata.load();
                    let stream_meta = snapshot
                        .streams
                        .get(&stream_id_num)
                        .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                    let topic_meta = stream_meta.get_topic(topic_id_num).ok_or_else(|| {
                        IggyError::TopicIdNotFound(lookup_topic_id.clone(), stream_id.clone())
                    })?;

                    self.message_expiry = topic_meta.message_expiry;
                    self.max_topic_size = topic_meta.max_topic_size;
                    drop(snapshot);

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::UpdateTopic(self))
                        .await
                        .error(|e: &IggyError| format!(
                            "{COMPONENT} (error: {e}) - failed to apply update topic with id: {topic_id} in stream with ID: {stream_id_num}, session: {session}"
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
                    // Get stream_id from SharedMetadata (don't access local slab)
                    let stream_id = shard
                        .shared_metadata
                        .get_stream_id(&self.stream_id)
                        .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::UpdateTopic(self))
                        .await
                        .error(|e: &IggyError| format!(
                            "{COMPONENT} (error: {e}) - failed to apply update topic in stream with ID: {stream_id}, session: {session}"
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

        Ok(HandlerResult::Finished)
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
