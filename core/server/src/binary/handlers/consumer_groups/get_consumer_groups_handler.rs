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
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use anyhow::Result;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_consumer_groups::GetConsumerGroups;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetConsumerGroups {
    fn code(&self) -> u32 {
        iggy_common::GET_CONSUMER_GROUPS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard.ensure_topic_exists(&self.stream_id, &self.topic_id)?;

        // Get IDs from SharedMetadata
        let numeric_stream_id = shard
            .shared_metadata
            .get_stream_id(&self.stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;
        let numeric_topic_id = shard
            .shared_metadata
            .get_topic_id(numeric_stream_id, &self.topic_id)
            .ok_or_else(|| {
                IggyError::TopicIdNotFound(self.topic_id.clone(), self.stream_id.clone())
            })?;

        shard.permissioner.get_consumer_groups(
            session.get_user_id(),
            numeric_stream_id,
            numeric_topic_id,
        )?;

        // Get consumer groups from SharedMetadata
        let snapshot = shard.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;
        let topic_meta = stream_meta.get_topic(numeric_topic_id).ok_or_else(|| {
            IggyError::TopicIdNotFound(self.topic_id.clone(), self.stream_id.clone())
        })?;

        let groups: Vec<_> = topic_meta.consumer_groups.values().cloned().collect();
        drop(snapshot);

        let response = mapper::map_consumer_groups_meta(&groups);
        sender.send_ok_response(&response).await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetConsumerGroups {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerGroups(get_consumer_groups) => Ok(get_consumer_groups),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
