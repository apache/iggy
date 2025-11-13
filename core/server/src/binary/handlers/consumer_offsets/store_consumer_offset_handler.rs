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

use std::rc::Rc;

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use tracing::debug;

impl ServerCommandHandler for StoreConsumerOffset {
    fn code(&self) -> u32 {
        iggy_common::STORE_CONSUMER_OFFSET_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard
            .store_consumer_offset(
                session,
                self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                self.offset,
            )
            .await
            .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset for stream_id: {}, topic_id: {}, partition_id: {:?}, offset: {}, session: {}",
                self.stream_id, self.topic_id, self.partition_id, self.offset, session
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for StoreConsumerOffset {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::StoreConsumerOffset(store_consumer_offset) => Ok(store_consumer_offset),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
