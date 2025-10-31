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
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::purge_topic::PurgeTopic;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for PurgeTopic {
    fn code(&self) -> u32 {
        iggy_common::PURGE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_purge_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let topic_id = self.topic_id.clone();
        let stream_id = self.stream_id.clone();

        shard
            .purge_topic(session, &self.stream_id, &self.topic_id)
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to purge topic with id: {}, stream_id: {}",
                    self.topic_id, self.stream_id
                )
            })?;

        let event = crate::shard::transmission::event::ShardEvent::PurgedTopic {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::PurgeTopic(self))
            .await
            .with_error(|error| {
                format!(
                "{COMPONENT} (error: {error}) - failed to apply purge topic with id: {topic_id}, stream_id: {stream_id}",
            )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for PurgeTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PurgeTopic(purge_topic) => Ok(purge_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
