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
use crate::binary::handlers::messages::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::system::messages::PollingArgs;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardMessage, ShardRequest, ShardRequestPayload};
use crate::streaming::segments::IggyMessagesBatchSet;
use crate::streaming::session::Session;
use crate::to_iovec;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::{IggyError, PollMessages};
use std::io::IoSlice;
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
        session: &Rc<Session>,
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
        let (metadata, batch) = shard
            .poll_messages(
                client_id,
                user_id,
                &stream_id,
                &topic_id,
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

        let partition_id = metadata.partition_id.to_le_bytes();
        let current_offset = metadata.current_offset.to_le_bytes();
        let count = batch.count().to_le_bytes();

        let mut iovecs = Vec::with_capacity(batch.containers_count() + 3);
        iovecs.push(to_iovec(&partition_id));
        iovecs.push(to_iovec(&current_offset));
        iovecs.push(to_iovec(&count));

        iovecs.extend(batch.iter().map(|m| to_iovec(&m)));
        trace!(
            "Sending {} messages to client ({} bytes) to client",
            batch.count(),
            response_length
        );

        sender
            .send_ok_response_vectored(&response_length_bytes, iovecs)
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
