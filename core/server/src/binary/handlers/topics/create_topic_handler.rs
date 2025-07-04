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
use crate::state::command::EntryCommand;
use crate::state::models::CreateTopicWithId;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::create_topic::CreateTopic;
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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id;
        let (shards_assignment, created_topic_id) = shard
                .create_topic(
                    session,
                    &self.stream_id,
                    self.topic_id,
                    &self.name,
                    self.partitions_count,
                    self.message_expiry,
                    self.compression_algorithm,
                    self.max_topic_size,
                    self.replication_factor,
                )
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to create topic for stream_id: {stream_id}, topic_id: {topic_id:?}"
                ))?;
        let event = ShardEvent::CreatedTopic {
            stream_id: stream_id.clone(),
            topic_id,
            name: self.name.clone(),
            partitions_count: self.partitions_count,
            message_expiry: self.message_expiry,
            compression_algorithm: self.compression_algorithm,
            max_topic_size: self.max_topic_size,
            replication_factor: self.replication_factor,
            shards_assignment,
        };
        // Broadcast the event to all shards.
        let _responses = shard.broadcast_event_to_all_shards(event.into());

        let stream = shard
            .get_stream(&self.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get stream for stream_id: {stream_id}"
                )
            })?;
        let topic = stream.get_topic(&created_topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get topic with ID: {created_topic_id} in stream with ID: {stream_id}"
                )
            })?;
        self.message_expiry = topic.message_expiry;
        self.max_topic_size = topic.max_topic_size;
        let topic_id = topic.topic_id;
        let response = mapper::map_topic(topic).await;

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::CreateTopic(CreateTopicWithId {
                topic_id,
                command: self
            }))
            .await
            .with_error_context(|error| {
                format!(
                "{COMPONENT} (error: {error}) - failed to apply create topic for stream_id: {stream_id}, topic_id: {topic_id:?}"
            )
            })?;
        sender.send_ok_response(&response).await?;
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
