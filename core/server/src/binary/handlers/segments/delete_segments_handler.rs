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
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;

        shard
            .delete_segments(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partition_id as usize,
                self.segments_count,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete segments for topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
                )
            })?;
        let event = ShardEvent::DeletedSegments {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partition_id: self.partition_id as usize,
            segments_count: self.segments_count,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeleteSegments(self),
            )
            .await
            .with_error_context(|error| {
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
