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

use crate::binary::command::{BinaryServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::models::messaging::INDEX_SIZE;
use iggy::prelude::*;
use iggy::utils::sizeable::Sizeable;
use tracing::instrument;

impl ServerCommandHandler for SendMessages {
    fn code(&self) -> u32 {
        iggy::command::SEND_MESSAGES_CODE
    }

    #[instrument(skip_all, name = "trace_send_messages", fields(
        iggy_user_id = session.get_user_id(),
        iggy_client_id = session.client_id,
        iggy_stream_id = self.stream_id.as_string(),
        iggy_topic_id = self.topic_id.as_string(),
        partitioning = %self.partitioning
    ))]
    async fn handle(
        mut self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        let length = length - 4;
        let mut buffer = BytesMut::with_capacity(length as usize);
        unsafe {
            buffer.set_len(length as usize);
        }
        sender.read(&mut buffer).await?;

        let mut element_size;

        let stream_id = Identifier::from_raw_bytes(buffer.chunk())?;
        element_size = stream_id.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);
        self.stream_id = stream_id;

        let topic_id = Identifier::from_raw_bytes(buffer.chunk())?;
        element_size = topic_id.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);
        self.topic_id = topic_id;

        let partitioning = Partitioning::from_raw_bytes(buffer.chunk())?;
        element_size = partitioning.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);
        self.partitioning = partitioning;

        let messages_count = buffer.get_u32_le() as usize;

        let mut indexes_and_messages = buffer.split();

        let messages = indexes_and_messages.split_off(messages_count * INDEX_SIZE);
        let indexes = indexes_and_messages;

        let indexes = IggyIndexesMut::from_bytes(indexes);

        let batch = IggyMessagesBatchMut::from_indexes_and_messages(indexes, messages);

        batch.validate()?;

        let system = system.read().await;
        system
            .append_messages(
                session,
                &self.stream_id,
                &self.topic_id,
                &self.partitioning,
                batch,
                None,
            )
            .await?;
        drop(system);

        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for SendMessages {
    async fn from_sender(
        _sender: &mut SenderKind,
        _code: u32,
        _length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }
}
