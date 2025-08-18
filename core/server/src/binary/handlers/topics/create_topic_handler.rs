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
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::state::models::CreateTopicWithId;
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use error_set::ErrContext;
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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let topic = shard
            .create_topic2(
                session,
                &self.stream_id,
                self.name.clone(),
                self.message_expiry,
                self.compression_algorithm,
                self.max_topic_size,
                self.replication_factor,
            )
            .await?;
        self.message_expiry = topic.root().message_expiry();
        self.max_topic_size = topic.root().max_topic_size();

        let stream_id = shard
            .streams2
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        let topic_id = topic.id();
        // Send events for topic creation.
        let event = ShardEvent::CreatedTopic2 {
            stream_id: self.stream_id.clone(),
            topic,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;

        let partitions = shard
            .create_partitions2(
                session,
                &self.stream_id,
                &Identifier::numeric(topic_id as u32).unwrap(),
                self.partitions_count,
            )
            .await?;
        let event = ShardEvent::CreatedPartitions2 {
            stream_id: self.stream_id.clone(),
            topic_id: Identifier::numeric(topic_id as u32).unwrap(),
            partitions,
        };
        let _responses = shard.broadcast_event_to_all_shards(event).await;
        // TODO: Create shard_table records for partitions.
        let response = shard.streams2.with_topic_by_id(
            &self.stream_id,
            &Identifier::numeric(topic_id as u32).unwrap(),
            |(root, stats)| mapper::map_topic(&root, &stats),
        );
        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::CreateTopic(CreateTopicWithId {
                topic_id: topic_id as u32,
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
