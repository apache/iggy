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
use crate::metadata::ConsumerGroupMeta;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::polling_consumer::ConsumerGroupId;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::DELETE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let (numeric_stream_id, numeric_topic_id) =
            shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        shard.metadata.perm_delete_consumer_group(
            session.get_user_id(),
            numeric_stream_id,
            numeric_topic_id,
        )?;

        let request = ShardRequest {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partition_id: 0,
            payload: ShardRequestPayload::DeleteConsumerGroup {
                user_id: session.get_user_id(),
                stream_id: self.stream_id.clone(),
                topic_id: self.topic_id.clone(),
                group_id: self.group_id.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        let cg_meta: ConsumerGroupMeta = match shard
            .send_request_to_shard_or_recoil(None, message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::DeleteConsumerGroup {
                        stream_id,
                        topic_id,
                        group_id,
                        ..
                    } = payload
                {
                    shard.delete_consumer_group(&stream_id, &topic_id, &group_id).error(|e: &IggyError| {
                        format!(
                            "{COMPONENT} (error: {e}) - failed to delete consumer group with ID: {} for topic with ID: {} in stream with ID: {} for session: {}",
                            group_id, topic_id, stream_id, session
                        )
                    })?
                } else {
                    unreachable!(
                        "Expected a DeleteConsumerGroup request inside of DeleteConsumerGroup handler"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::DeleteConsumerGroupResponse => {
                    sender.send_empty_ok_response().await?;
                    return Ok(HandlerResult::Finished);
                }
                ShardResponse::ErrorResponse(err) => return Err(err),
                _ => unreachable!(
                    "Expected a DeleteConsumerGroupResponse inside of DeleteConsumerGroup handler"
                ),
            },
        };

        let cg_id = cg_meta.id;

        for (_, member) in cg_meta.members.iter() {
            if let Err(err) = shard.client_manager.leave_consumer_group(
                member.client_id,
                numeric_stream_id,
                numeric_topic_id,
                cg_id,
            ) {
                tracing::warn!(
                    "{COMPONENT} (error: {err}) - failed to make client leave consumer group for client ID: {}, group ID: {}",
                    member.client_id,
                    cg_id
                );
            }
        }

        let cg_id_spez = ConsumerGroupId(cg_id);
        shard.delete_consumer_group_offsets(
            cg_id_spez,
            &self.stream_id,
            &self.topic_id,
            &cg_meta.partitions,
        ).await.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to delete consumer group offsets for group ID: {} in stream: {}, topic: {}",
                cg_id_spez,
                self.stream_id,
                self.topic_id
            )
        })?;

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteConsumerGroup(self),
            )
            .await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to apply delete consumer group for stream_id: {}, topic_id: {}, group_id: {cg_id}, session: {session}",
                    stream_id, topic_id
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for DeleteConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteConsumerGroup(delete_consumer_group) => Ok(delete_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
