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
use crate::binary::handlers::consumer_groups::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::state::models::CreateConsumerGroupWithId;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::{Identifier, IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::CREATE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_create_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
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
            .perm_create_consumer_group(session.get_user_id(), stream_id, topic_id)?;

        let request = ShardRequest {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partition_id: 0,
            payload: ShardRequestPayload::CreateConsumerGroup {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                name: self.name.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::CreateConsumerGroup {
                        stream_id: recoil_stream_id,
                        topic_id: recoil_topic_id,
                        name,
                        ..
                    } = payload
                {
                    let cg_id =
                        shard.create_consumer_group(&recoil_stream_id, &recoil_topic_id, name)?;

                    let stream_id = self.stream_id.clone();
                    let topic_id = self.topic_id.clone();
                    shard
                        .state
                        .apply(
                            session.get_user_id(),
                            &EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
                                group_id: cg_id as u32,
                                command: self,
                            }),
                        )
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply create consumer group for stream_id: {stream_id}, topic_id: {topic_id}, group_id: {cg_id}, session: {session}"
                            )
                        })?;

                    let (numeric_stream_id, numeric_topic_id) =
                        shard.resolve_topic_id(&stream_id, &topic_id)?;

                    let cg_identifier = Identifier::numeric(cg_id as u32).unwrap();
                    let response = shard
                        .metadata
                        .get_consumer_group(numeric_stream_id, numeric_topic_id, cg_id)
                        .map(|cg| mapper::map_consumer_group_from_meta(&cg))
                        .ok_or_else(|| {
                            IggyError::ConsumerGroupIdNotFound(cg_identifier, topic_id.clone())
                        })?;
                    sender.send_ok_response(&response).await?;
                } else {
                    unreachable!(
                        "Expected a CreateConsumerGroup request inside of CreateConsumerGroup handler"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::CreateConsumerGroupResponse(cg_id) => {
                    let (numeric_stream_id, numeric_topic_id) =
                        shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;

                    let cg_identifier = Identifier::numeric(cg_id as u32).unwrap();
                    let response = shard
                        .metadata
                        .get_consumer_group(numeric_stream_id, numeric_topic_id, cg_id)
                        .map(|cg| mapper::map_consumer_group_from_meta(&cg))
                        .ok_or_else(|| {
                            IggyError::ConsumerGroupIdNotFound(cg_identifier, self.topic_id.clone())
                        })?;
                    sender.send_ok_response(&response).await?;
                }
                ShardResponse::ErrorResponse(err) => return Err(err),
                _ => unreachable!(
                    "Expected a CreateConsumerGroupResponse inside of CreateConsumerGroup handler"
                ),
            },
        };
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for CreateConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateConsumerGroup(create_consumer_group) => Ok(create_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
