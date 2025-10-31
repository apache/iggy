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
use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::state::models::CreateConsumerGroupWithId;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::{Identifier, IggyError};
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
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let cg = shard.create_consumer_group(
            session,
            &self.stream_id,
            &self.topic_id,
            self.name.clone(),
        )?;
        let cg_id = cg.id();

        let event = ShardEvent::CreatedConsumerGroup {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            cg,
        };
        shard.broadcast_event_to_all_shards(event).await?;

        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        shard
            .state
        .apply(
            session.get_user_id(),
           &EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
                group_id: cg_id as u32,
                command: self
            }),
        )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create consumer group for stream_id: {stream_id}, topic_id: {topic_id}, group_id: {cg_id}, session: {session}"
                )
            })?;
        let response = shard.streams.with_consumer_group_by_id(
            &stream_id,
            &topic_id,
            &Identifier::numeric(cg_id as u32).unwrap(),
            |(root, members)| mapper::map_consumer_group(root, members),
        );
        sender.send_ok_response(&response).await?;
        Ok(())
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
