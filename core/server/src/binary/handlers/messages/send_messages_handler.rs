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
use crate::streaming::utils::PooledBuffer;
use anyhow::Result;
use iggy_common::Identifier;
use iggy_common::Sizeable;
use iggy_common::INDEX_SIZE;
use iggy_common::{IggyError, Partitioning, SendMessages, Validatable};
use tracing::instrument;

impl ServerCommandHandler for SendMessages {
    fn code(&self) -> u32 {
        iggy_common::SEND_MESSAGES_CODE
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
        let total_payload_size = length as usize - std::mem::size_of::<u32>();
        let metadata_len_field_size = std::mem::size_of::<u32>();

        let mut metadata_length_buffer = [0u8; 4];
        sender.read(&mut metadata_length_buffer).await?;
        let metadata_size = u32::from_le_bytes(metadata_length_buffer);

        let mut metadata_buffer = PooledBuffer::with_capacity(metadata_size as usize);
        unsafe { metadata_buffer.set_len(metadata_size as usize) };
        sender.read(&mut metadata_buffer).await?;

        let mut element_size = 0;

        let stream_id = Identifier::from_raw_bytes(&metadata_buffer)?;
        element_size += stream_id.get_size_bytes().as_bytes_usize();
        self.stream_id = stream_id;

        let topic_id = Identifier::from_raw_bytes(&metadata_buffer[element_size..])?;
        element_size += topic_id.get_size_bytes().as_bytes_usize();
        self.topic_id = topic_id;

        let partitioning = Partitioning::from_raw_bytes(&metadata_buffer[element_size..])?;
        element_size += partitioning.get_size_bytes().as_bytes_usize();
        self.partitioning = partitioning;

        let messages_count = u32::from_le_bytes(
            metadata_buffer[element_size..element_size + 4]
                .try_into()
                .unwrap(),
        );
        let indexes_size = messages_count as usize * INDEX_SIZE;

        let mut indexes_buffer = PooledBuffer::with_capacity(indexes_size);
        unsafe { indexes_buffer.set_len(indexes_size) };
        sender.read(&mut indexes_buffer).await?;

        let messages_size =
            total_payload_size - metadata_size as usize - indexes_size - metadata_len_field_size;
        let mut messages_buffer = PooledBuffer::with_capacity(messages_size);
        unsafe { messages_buffer.set_len(messages_size) };
        sender.read(&mut messages_buffer).await?;

        let indexes = IggyIndexesMut::from_bytes(indexes_buffer, 0);
        let batch = IggyMessagesBatchMut::from_indexes_and_messages(
            messages_count,
            indexes,
            messages_buffer,
        );

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
