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
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::session::Session;
use crate::streaming::utils::PooledBuffer;
use anyhow::Result;
use iggy_common::{IggyError, PollMessages};
use std::rc::Rc;
use tracing::{debug, trace};

#[derive(Debug)]
pub struct IggyPollMetadata {
    pub partition_id: u32,
    pub current_offset: u64,
}

impl IggyPollMetadata {
    pub fn new(partition_id: u32, current_offset: u64) -> Self {
        Self {
            partition_id,
            current_offset,
        }
    }
}

impl ServerCommandHandler for PollMessages {
    fn code(&self) -> u32 {
        iggy_common::POLL_MESSAGES_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let PollMessages {
            consumer,
            partition_id,
            strategy,
            count,
            auto_commit,
            stream_id,
            topic_id,
        } = self;
        let args = PollingArgs::new(strategy, count, auto_commit);

        let user_id = session.get_user_id();
        let client_id = session.client_id;
        let (metadata, mut batch) = shard
            .poll_messages(
                client_id,
                user_id,
                stream_id,
                topic_id,
                consumer,
                partition_id,
                args,
            )
            .await?;

        // Collect all chunks first into a Vec to extend their lifetimes.
        // This ensures the Bytes (in reality Arc<[u8]>) references from each IggyMessagesBatch stay alive
        // throughout the async vectored I/O operation, preventing "borrowed value does not live
        // long enough" errors while optimizing transmission by using larger chunks.

        // 4 bytes for partition_id + 8 bytes for current_offset + 4 bytes for messages_count + size of all batches.
        let response_length = 4 + 8 + 4 + batch.size();
        let response_length_bytes = response_length.to_le_bytes();

        let mut bufs = Vec::with_capacity(batch.containers_count() + 5);
        let mut partition_id_buf = PooledBuffer::with_capacity(4);
        let mut current_offset_buf = PooledBuffer::with_capacity(8);
        let mut count_buf = PooledBuffer::with_capacity(4);
        partition_id_buf.put_u32_le(metadata.partition_id);
        current_offset_buf.put_u64_le(metadata.current_offset);
        count_buf.put_u32_le(batch.count());

        bufs.push(partition_id_buf);
        bufs.push(current_offset_buf);
        bufs.push(count_buf);

        batch.iter_mut().for_each(|m| bufs.push(m.take_messages()));
        trace!(
            "Sending {} messages to client ({} bytes) to client",
            batch.count(),
            response_length
        );

        sender
            .send_ok_response_vectored(&response_length_bytes, bufs)
            .await?;
        Ok(())
    }
}

impl BinaryServerCommand for PollMessages {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PollMessages(poll_messages) => Ok(poll_messages),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
