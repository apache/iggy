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
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::topics;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::update_topic::UpdateTopic;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdateTopic {
    fn code(&self) -> u32 {
        iggy_common::UPDATE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_update_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        mut self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard.update_topic2(
            session,
            &self.stream_id,
            &self.topic_id,
            self.name.clone(),
            self.message_expiry,
            self.compression_algorithm,
            self.max_topic_size,
            self.replication_factor,
        );
        self.message_expiry = shard.streams2.with_topic_by_id(
            &self.stream_id,
            &self.topic_id,
            topics::helpers::get_message_expiry(),
        );
        self.max_topic_size = shard.streams2.with_topic_by_id(
            &self.stream_id,
            &self.topic_id,
            topics::helpers::get_max_topic_size(),
        );
        let event = ShardEvent::UpdatedTopic2 {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            name: self.name.clone(),
            message_expiry: self.message_expiry,
            compression_algorithm: self.compression_algorithm,
            max_topic_size: self.max_topic_size,
            replication_factor: self.replication_factor,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;
        let topic_id = self.topic_id.clone();
        let stream_id = self.stream_id.clone();

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::UpdateTopic(self))
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to apply update topic with id: {topic_id}, stream_id: {stream_id}, session: {session}"
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for UpdateTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdateTopic(update_topic) => Ok(update_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
