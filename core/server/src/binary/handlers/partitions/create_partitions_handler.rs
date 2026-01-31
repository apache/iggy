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
use crate::binary::handlers::partitions::COMPONENT;
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
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreatePartitions {
    fn code(&self) -> u32 {
        iggy_common::CREATE_PARTITIONS_CODE
    }

    #[instrument(skip_all, name = "trace_create_partitions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let (stream_id_numeric, topic_id_numeric) =
            shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        shard.metadata.perm_create_partitions(
            session.get_user_id(),
            stream_id_numeric,
            topic_id_numeric,
        )?;

        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::CreatePartitions {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                partitions_count: self.partitions_count,
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::CreatePartitions {
                        stream_id,
                        topic_id,
                        partitions_count,
                        ..
                    } = payload
                {
                    let _partition_guard = shard.fs_locks.partition_lock.lock().await;

                    // Get numeric IDs BEFORE create (for rebalance operation)
                    let (numeric_stream_id, numeric_topic_id) =
                        shard.resolve_topic_id(&stream_id, &topic_id)?;

                    let partition_infos = shard
                        .create_partitions(&stream_id, &topic_id, partitions_count)
                        .await?;

                    let event = ShardEvent::CreatedPartitions {
                        stream_id: stream_id.clone(),
                        topic_id: topic_id.clone(),
                        partitions: partition_infos,
                    };
                    shard.broadcast_event_to_all_shards(event).await?;

                    let total_partition_count = shard
                        .metadata
                        .partitions_count(numeric_stream_id, numeric_topic_id)
                        as u32;
                    shard.writer().rebalance_consumer_groups_for_topic(
                        numeric_stream_id,
                        numeric_topic_id,
                        total_partition_count,
                    );

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::CreatePartitions(self))
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply create partitions for stream_id: {numeric_stream_id}, topic_id: {numeric_topic_id}, session: {session}"
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected a CreatePartitions request inside of CreatePartitions handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreatePartitionsResponse(_partitions) => {
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a CreatePartitionsResponse inside of CreatePartitions handler, impossible state"
                ),
            },
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for CreatePartitions {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreatePartitions(create_partitions) => Ok(create_partitions),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
