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
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::join_consumer_group::JoinConsumerGroup;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for JoinConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::JOIN_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_join_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string(), iggy_group_id = self.group_id.as_string()))]
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
            .perm_join_consumer_group(session.get_user_id(), stream_id, topic_id)?;
        shard.ensure_consumer_group_exists(&self.stream_id, &self.topic_id, &self.group_id)?;

        let request = ShardRequest {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partition_id: 0,
            payload: ShardRequestPayload::JoinConsumerGroup {
                user_id: session.get_user_id(),
                client_id: session.client_id,
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                group_id: self.group_id.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::JoinConsumerGroup {
                        client_id,
                        stream_id,
                        topic_id,
                        group_id,
                        ..
                    } = payload
                {
                    shard
                        .join_consumer_group(client_id, &stream_id, &topic_id, &group_id)
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to join consumer group for stream_id: {}, topic_id: {}, group_id: {}, session: {}",
                                stream_id, topic_id, group_id, session
                            )
                        })?;
                } else {
                    unreachable!(
                        "Expected a JoinConsumerGroup request inside of JoinConsumerGroup handler"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::JoinConsumerGroupResponse => {}
                ShardResponse::ErrorResponse(err) => return Err(err),
                _ => unreachable!(
                    "Expected a JoinConsumerGroupResponse inside of JoinConsumerGroup handler"
                ),
            },
        }

        sender.send_empty_ok_response().await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for JoinConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::JoinConsumerGroup(join_consumer_group) => Ok(join_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
