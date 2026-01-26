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
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_topic::GetTopic;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetTopic {
    fn code(&self) -> u32 {
        iggy_common::GET_TOPIC_CODE
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

        let Some(numeric_stream_id) = shard.metadata.get_stream_id(&self.stream_id) else {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        };

        let Some(numeric_topic_id) = shard
            .metadata
            .get_topic_id(numeric_stream_id, &self.topic_id)
        else {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        };

        let has_permission = shard
            .metadata
            .perm_get_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .is_ok();
        if !has_permission {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        }

        let response = shard.get_topic_from_metadata(numeric_stream_id, numeric_topic_id);
        sender.send_ok_response(&response).await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetTopic(get_topic) => Ok(get_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
