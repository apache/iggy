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

use super::COMPONENT;
use crate::binary::command::{BinaryServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardMessage, ShardRequest, ShardRequestPayload};
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut};
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use anyhow::Result;
use compio::buf::{IntoInner as _, IoBuf};
use iggy_common::Identifier;
use iggy_common::Sizeable;
use iggy_common::{INDEX_SIZE, IdKind};
use iggy_common::{IggyError, Partitioning, PartitioningKind, SendMessages, Validatable};
use nix::libc;
use std::rc::Rc;
use tracing::{info, instrument};

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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        let total_payload_size = length as usize - std::mem::size_of::<u32>();
        let metadata_len_field_size = std::mem::size_of::<u32>();

        let mut metadata_length_buffer = PooledBuffer::with_capacity(4);

        let metadata_length_buffer = sender
            .read(metadata_length_buffer.slice(0..4))
            .await?
            .into_inner();
        let metadata_size = u32::from_le_bytes(metadata_length_buffer[0..4].try_into().unwrap());

        let mut metadata_buffer = PooledBuffer::with_capacity(metadata_size as usize);
        let metadata_buf = sender
            .read(metadata_buffer.slice(0..metadata_size as usize))
            .await?
            .into_inner();

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

        let mut indexes_buffer = PooledBuffer::with_capacity(indexes_size + 512); // extra space for possible padding to not cause reallocations
        let indexes_buffer = sender
            .read(indexes_buffer.slice(0..indexes_size))
            .await?
            .into_inner();

        let messages_size =
            total_payload_size - metadata_size as usize - indexes_size - metadata_len_field_size;

        let mut messages_buffer = PooledBuffer::with_capacity(messages_size + 512); // extra space for possible padding to not cause reallocations
        let messages_buffer = sender
            .read(messages_buffer.slice(0..messages_size))
            .await?
            .into_inner();

        let user_id = session.get_user_id();

        let stream = shard.get_stream(&self.stream_id)?;
        let topic = stream.get_topic(&self.topic_id)?;
        let partition_id = match self.partitioning.kind {
            PartitioningKind::PartitionId => {
                u32::from_le_bytes(self.partitioning.value[..4].try_into().unwrap())
            }
            _ => 0,
        };

        let namespace = IggyNamespace::new(stream.stream_id, topic.topic_id, partition_id);
        if let Some(target_shard) = shard.find_shard(&namespace) {
            if target_shard.id() != shard.id {
                use crate::tcp::tcp_sender::TcpSender;
                use std::os::fd::AsRawFd;

                if let SenderKind::Tcp(TcpSender { stream: tcp_stream }) = sender {
                    let raw_fd = tcp_stream.as_raw_fd();
                    let new_fd = unsafe { libc::dup(raw_fd) };
                    if new_fd == -1 {
                        return Err(IggyError::CannotReadMessagePayload);
                    }

                    let mut initial_data = Vec::new();
                    initial_data.extend_from_slice(&(length).to_le_bytes());
                    initial_data.extend_from_slice(&(self.code()).to_le_bytes());
                    initial_data.extend_from_slice(&(metadata_size).to_le_bytes());
                    initial_data.extend_from_slice(&metadata_buf[..]);
                    initial_data.extend_from_slice(&indexes_buffer[..]);
                    initial_data.extend_from_slice(&messages_buffer[..]);

                    let payload = ShardRequestPayload::SocketTransfer {
                        fd: new_fd,
                        from_shard: shard.id,
                        client_id: session.client_id,
                        user_id: session.get_user_id(),
                        ip_address: session.ip_address,
                        initial_data,
                    };
                    let request =
                        ShardRequest::new(stream.stream_id, topic.topic_id, partition_id, payload);
                    let message = ShardMessage::Request(request);

                    match target_shard.send_request(message).await? {
                        ShardResponse::SendMessages => {
                            info!(
                                "Socket transferred from shard {} to shard {}",
                                shard.id,
                                target_shard.id()
                            );
                            // Return SocketTransferred to signal clean exit without sending response
                            // The socket has been transferred, so the original handler should stop
                            return Err(IggyError::SocketTransferred);
                        }
                        _ => {
                            unsafe {
                                libc::close(new_fd);
                            }
                            return Err(IggyError::ShardCommunicationError(target_shard.id()));
                        }
                    }
                }
            }
        }

        let indexes = IggyIndexesMut::from_bytes(indexes_buffer, 0);
        let batch = IggyMessagesBatchMut::from_indexes_and_messages(
            messages_count,
            indexes,
            messages_buffer,
        );
        batch.validate()?;

        shard
            .append_messages(
                user_id,
                &self.stream_id,
                &self.topic_id,
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
