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
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

impl ServerCommandHandler for GetConsumerOffset {
    fn code(&self) -> u32 {
        iggy::command::GET_CONSUMER_OFFSET_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        let Ok(offset) = system
            .get_consumer_offset(
                session,
                &self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
            )
            .await
        else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let Some(offset) = offset else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let offset = mapper::map_consumer_offset(&offset);
        sender.send_ok_response(&offset).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetConsumerOffset {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerOffset(get_consumer_offset) => Ok(get_consumer_offset),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
