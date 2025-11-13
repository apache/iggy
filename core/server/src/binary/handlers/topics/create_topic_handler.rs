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
use crate::binary::{handlers::topics::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::state::models::CreateTopicWithId;
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::create_topic::CreateTopic;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateTopic {
    fn code(&self) -> u32 {
        iggy_common::CREATE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_create_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
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
            payload: ShardRequestPayload::CreateTopic {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                name: self.name.clone(),
                partitions_count: self.partitions_count,
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
                    && let ShardRequestPayload::CreateTopic {
                        stream_id,
                        name,
                        message_expiry,
                        compression_algorithm,
                        max_topic_size,
                        replication_factor,
                        partitions_count,
                        ..
                    } = payload
                {
                    // Acquire topic lock to serialize filesystem operations
                    let _topic_guard = shard.fs_locks.topic_lock.lock().await;

                    let topic = shard
                        .create_topic(
                            session,
                            &stream_id,
                            name,
                            message_expiry,
                            compression_algorithm,
                            max_topic_size,
                            replication_factor,
                        )
                        .await?;
                    self.message_expiry = topic.root().message_expiry();
                    self.max_topic_size = topic.root().max_topic_size();

                    let stream_id_num = shard
                        .streams
                        .with_stream_by_id(&stream_id, streams::helpers::get_stream_id());
                    let topic_id = topic.id();

                    let event = ShardEvent::CreatedTopic {
                        stream_id: stream_id.clone(),
                        topic,
                    };

                    shard.broadcast_event_to_all_shards(event).await?;
                    let partitions = shard
                        .create_partitions(
                            session,
                            &stream_id,
                            &Identifier::numeric(topic_id as u32).unwrap(),
                            partitions_count,
                        )
                        .await?;
                    let event = ShardEvent::CreatedPartitions {
                        stream_id: stream_id.clone(),
                        topic_id: Identifier::numeric(topic_id as u32).unwrap(),
                        partitions,
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    let response = shard.streams.with_topic_by_id(
                        &stream_id,
                        &Identifier::numeric(topic_id as u32).unwrap(),
                        |(root, _, stats)| mapper::map_topic(&root, &stats),
                    );
                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::CreateTopic(CreateTopicWithId {
                            topic_id: topic_id as u32,
                            command: self
                        }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create topic for stream_id: {stream_id_num}, topic_id: {topic_id:?}"
                            )
                        })?;
                    sender.send_ok_response(&response).await?;
                } else {
                    unreachable!(
                        "Expected a CreateTopic request inside of CreateTopic handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreateTopicResponse(topic) => {
                    let topic_id = topic.id();
                    self.message_expiry = topic.root().message_expiry();
                    self.max_topic_size = topic.root().max_topic_size();

                    let stream_id = shard
                        .streams
                        .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());

                    let response = shard.streams.with_topic_by_id(
                        &self.stream_id,
                        &Identifier::numeric(topic_id as u32).unwrap(),
                        |(root, _, stats)| mapper::map_topic(&root, &stats),
                    );

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::CreateTopic(CreateTopicWithId {
                            topic_id: topic_id as u32,
                            command: self
                        }))
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to apply create topic for stream_id: {stream_id}, topic_id: {topic_id:?}"
                            )
                        })?;

                    sender.send_ok_response(&response).await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a CreateTopicResponse inside of CreateTopic handler, impossible state"
                ),
            },
        }

        Ok(())
    }
}

impl BinaryServerCommand for CreateTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateTopic(create_topic) => Ok(create_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
