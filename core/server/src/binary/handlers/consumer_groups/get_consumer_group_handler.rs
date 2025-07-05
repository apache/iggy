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
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::get_consumer_group::GetConsumerGroup;
use std::rc::Rc;
use tracing::debug;

use super::COMPONENT;

impl ServerCommandHandler for GetConsumerGroup {
    fn code(&self) -> u32 {
        iggy_common::GET_CONSUMER_GROUP_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let stream_id = &self.stream_id;
        let stream = shard.find_stream(session, &self.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get stream for stream_id: {stream_id}"
                )
            })?;
        let Ok(consumer_group) =
            shard.get_consumer_group(session, &stream, &self.topic_id, &self.group_id)
        else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };
        let Some(consumer_group) = consumer_group else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let consumer_group = mapper::map_consumer_group(&consumer_group);
        sender.send_ok_response(&consumer_group).await?;
        Ok(())
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
