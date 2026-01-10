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
    AuthenticatedHandler, BinaryServerCommand, HandlerResult, ServerCommand,
};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::streaming::auth::Auth;
use crate::streaming::session::Session;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_consumer_group::GetConsumerGroup;
use std::rc::Rc;
use tracing::debug;

impl AuthenticatedHandler for GetConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::GET_CONSUMER_GROUP_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        auth: Auth,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        let exists = shard
            .ensure_consumer_group_exists(&self.stream_id, &self.topic_id, &self.group_id)
            .is_ok();
        if !exists {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        }

        let (numeric_stream_id, numeric_topic_id) =
            shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        let has_permission = shard
            .permissioner
            .get_consumer_group(auth.user_id(), numeric_stream_id, numeric_topic_id)
            .is_ok();
        if !has_permission {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        }

        let numeric_group_id = shard
            .metadata
            .get_consumer_group_id(numeric_stream_id, numeric_topic_id, &self.group_id)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(self.group_id.clone(), self.topic_id.clone())
            })?;

        let metadata = shard.metadata.load();
        let consumer_group = metadata
            .streams
            .get(numeric_stream_id)
            .and_then(|s| s.topics.get(numeric_topic_id))
            .and_then(|t| t.consumer_groups.get(numeric_group_id))
            .map(mapper::map_consumer_group_from_meta)
            .ok_or_else(|| {
                IggyError::ConsumerGroupIdNotFound(self.group_id.clone(), self.topic_id.clone())
            })?;
        sender.send_ok_response(&consumer_group).await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerGroup(get_consumer_group) => Ok(get_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
