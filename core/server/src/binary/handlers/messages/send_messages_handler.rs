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
use crate::shard::IggyShard;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut};
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use anyhow::Result;
use compio::buf::{IntoInner as _, IoBuf};
use iggy_common::INDEX_SIZE;
use iggy_common::Identifier;
use iggy_common::Sizeable;
use iggy_common::{IggyError, Partitioning, SendMessages, Validatable};
use std::rc::Rc;
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
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        let total_payload_size = length as usize - std::mem::size_of::<u32>();
        let metadata_len_field_size = std::mem::size_of::<u32>();

        let metadata_length_buffer = PooledBuffer::with_capacity(4);
        let (result, metadata_len_buf) = sender.read(metadata_length_buffer.slice(0..4)).await;
        let metadata_len_buf = metadata_len_buf.into_inner();
        result?;
        let metadata_size = u32::from_le_bytes(metadata_len_buf[..].try_into().unwrap());

        let metadata_buffer = PooledBuffer::with_capacity(metadata_size as usize);
        let (result, metadata_buf) = sender
            .read(metadata_buffer.slice(0..metadata_size as usize))
            .await;
        result?;
        let metadata_buf = metadata_buf.into_inner();

        let mut element_size = 0;

        let stream_id = Identifier::from_raw_bytes(&metadata_buf)?;
        element_size += stream_id.get_size_bytes().as_bytes_usize();
        self.stream_id = stream_id;

        let topic_id = Identifier::from_raw_bytes(&metadata_buf[element_size..])?;
        element_size += topic_id.get_size_bytes().as_bytes_usize();
        self.topic_id = topic_id;

        let partitioning = Partitioning::from_raw_bytes(&metadata_buf[element_size..])?;
        element_size += partitioning.get_size_bytes().as_bytes_usize();
        self.partitioning = partitioning;

        let messages_count = u32::from_le_bytes(
            metadata_buf[element_size..element_size + 4]
                .try_into()
                .unwrap(),
        );
        let indexes_size = messages_count as usize * INDEX_SIZE;

        let indexes_buffer = PooledBuffer::with_capacity(indexes_size);
        let (result, indexes_buffer) = sender.read(indexes_buffer.slice(0..indexes_size)).await;
        result?;
        let indexes_buffer = indexes_buffer.into_inner();

        let messages_size =
            total_payload_size - metadata_size as usize - indexes_size - metadata_len_field_size;
        let messages_buffer = PooledBuffer::with_capacity(messages_size);
        let (result, messages_buffer) = sender.read(messages_buffer.slice(0..messages_size)).await;
        result?;
        let messages_buffer = messages_buffer.into_inner();

        let indexes = IggyIndexesMut::from_bytes(indexes_buffer, 0);
        let batch = IggyMessagesBatchMut::from_indexes_and_messages(
            messages_count,
            indexes,
            messages_buffer,
        );
        batch.validate()?;

        let user_id = session.get_user_id();
        shard
            .append_messages(
                user_id,
                self.stream_id,
                self.topic_id,
                &self.partitioning,
                batch,
            )
            .await?;

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
