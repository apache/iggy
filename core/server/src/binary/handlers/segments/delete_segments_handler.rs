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
use crate::binary::{handlers::partitions::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_segments::DeleteSegments;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteSegments {
    fn code(&self) -> u32 {
        iggy_common::DELETE_SEGMENTS_CODE
    }

    #[instrument(skip_all, name = "trace_delete_segments", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id as usize;
        let segments_count = self.segments_count;

        // Ensure authentication and topic exists
        shard.ensure_authenticated(session)?;
        shard.ensure_topic_exists(&stream_id, &topic_id)?;
        shard.ensure_partition_exists(&stream_id, &topic_id, partition_id)?;

        // Get numeric IDs for namespace
        let numeric_stream_id = shard
            .streams
            .with_stream_by_id(&stream_id, streaming::streams::helpers::get_stream_id());

        let numeric_topic_id = shard.streams.with_topic_by_id(
            &stream_id,
            &topic_id,
            streaming::topics::helpers::get_topic_id(),
        );

        // Route request to the correct shard
        let namespace = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let payload = ShardRequestPayload::DeleteSegments { segments_count };
        let request = ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
        let message = ShardMessage::Request(request);

        match shard
            .send_request_to_shard_or_recoil(Some(&namespace), message)
            .await?
        {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(crate::shard::transmission::message::ShardRequest {
                    stream_id,
                    topic_id,
                    partition_id,
                    payload,
                }) = message
                    && let ShardRequestPayload::DeleteSegments { segments_count } = payload
                {
                    shard
                        .delete_segments_base(&stream_id, &topic_id, partition_id, segments_count)
                        .await
                        .with_error(|error| {
                            format!(
                                "{COMPONENT} (error: {error}) - failed to delete segments for topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
                            )
                        })?;
                } else {
                    return Err(IggyError::InvalidCommand);
                }
            }
            ShardSendRequestResult::Response(response) => {
                if !matches!(response, ShardResponse::DeleteSegments) {
                    return Err(IggyError::InvalidCommand);
                }
            }
        }

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteSegments(self),
            )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply 'delete segments' command for partition with ID: {partition_id} in topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
                )
            })?;

        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteSegments {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteSegments(delete_segments) => Ok(delete_segments),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
