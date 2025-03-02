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
use crate::binary::{handlers::messages::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use error_set::ErrContext;
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Partitioning, SendMessages};
use iggy::models::batch::{IggyHeader, IggyMutableBatch, IGGY_BATCH_OVERHEAD};
use iggy::utils::sizeable::Sizeable;
use tracing::{debug, instrument};

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
        self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        let mut metadata_len_bytes = [0u8; 4];
        sender.read(&mut metadata_len_bytes).await?;
        let metadata_len = u32::from_le_bytes(metadata_len_bytes);

        let mut metadata = vec![0u8; metadata_len as usize];
        sender.read(&mut metadata).await?;
        let metadata = Bytes::from(metadata);

        let stream_id = Identifier::from_bytes(metadata.clone())?;
        let mut position = stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(metadata.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partitioning = Partitioning::from_bytes(metadata.slice(position..))?;

        let mut header_bytes = [0u8; IGGY_BATCH_OVERHEAD as usize];
        sender.read(&mut header_bytes).await?;
        let header = IggyHeader::from_bytes(&header_bytes);

        let batch_length = length - metadata_len - IGGY_BATCH_OVERHEAD as u32 - 4 - 4; // TODO(hubcio): magic numbers
        let mut batch_buffer = BytesMut::with_capacity(batch_length as _);
        unsafe { batch_buffer.set_len(batch_length as _) };
        sender.read(&mut batch_buffer).await?;
        let batch = IggyMutableBatch::new(header, batch_buffer);

        debug!("session: {session}, command: {self}");

        let system = system.read().await;
        system
            .append_messages(session, &stream_id, &topic_id, &partitioning, batch, None)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to append messages, session: {session}"
                )
            })?;

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
