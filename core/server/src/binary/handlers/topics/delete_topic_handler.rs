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
use crate::shard_info;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_topic::DeleteTopic;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteTopic {
    fn code(&self) -> u32 {
        iggy_common::DELETE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        // TODO: There is a correctness bug,
        // We have to first apply the state, then proceed with deleting the topic from the disk.
        // Otherwise if we delete the topic from disk first and the server crashes
        // we end up in a state where the topic is deleted from the disk, but during state recreation it would be recreated,
        // without it's segments.
        debug!("session: {session}, command: {self}");
        let topic = shard
            .delete_topic2(session, &self.stream_id, &self.topic_id)
            .await?;
        let stream_id = shard
            .streams2
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        let topic_id = topic.root().id();
        shard_info!(
            shard.id,
            "Deleted topic with name: {}, ID: {} in stream with ID: {}",
            topic.root().name(),
            topic_id,
            stream_id
        );

        let event = ShardEvent::DeletedTopic2 {
            id: topic_id,
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteTopic(self))
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to apply delete topic with ID: {topic_id} in stream with ID: {stream_id}, session: {session}",
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteTopic(delete_topic) => Ok(delete_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
