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
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use iggy_common::IggyError;
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
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        let exists = shard
            .ensure_topic_exists(&self.stream_id, &self.topic_id)
            .is_ok();
        if !exists {
            sender.send_empty_ok_response().await?;
            return Ok(());
        }

        let numeric_stream_id = shard
            .streams
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        let has_permission = shard
            .permissioner
            .borrow()
            .get_topic(
                session.get_user_id(),
                numeric_stream_id,
                self.topic_id
                    .get_u32_value()
                    .unwrap_or(0)
                    .try_into()
                    .unwrap(),
            )
            .is_ok();
        if !has_permission {
            sender.send_empty_ok_response().await?;
            return Ok(());
        }

        let response =
            shard
                .streams
                .with_topic_by_id(&self.stream_id, &self.topic_id, |(root, _, stats)| {
                    mapper::map_topic(&root, &stats)
                });
        sender.send_ok_response(&response).await?;
        Ok(())
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
