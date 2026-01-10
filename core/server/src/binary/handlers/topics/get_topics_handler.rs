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
use crate::shard::IggyShard;
use crate::streaming::auth::Auth;
use crate::streaming::session::Session;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_topics::GetTopics;
use std::rc::Rc;
use tracing::debug;

impl AuthenticatedHandler for GetTopics {
    fn code(&self) -> u32 {
        iggy_common::GET_TOPICS_CODE
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
        shard.ensure_stream_exists(&self.stream_id)?;

        let Some(numeric_stream_id) = shard.metadata.get_stream_id(&self.stream_id) else {
            sender.send_empty_ok_response().await?;
            return Ok(HandlerResult::Finished);
        };

        shard
            .permissioner
            .get_topics(auth.user_id(), numeric_stream_id)?;

        let response = shard.get_topics_from_shared_metadata(numeric_stream_id);
        sender.send_ok_response(&response).await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetTopics {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetTopics(get_topics) => Ok(get_topics),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
