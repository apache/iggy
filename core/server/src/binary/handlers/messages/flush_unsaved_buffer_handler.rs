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
use crate::binary::{handlers::messages::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::{FlushUnsavedBuffer, IggyError};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for FlushUnsavedBuffer {
    fn code(&self) -> u32 {
        iggy_common::FLUSH_UNSAVED_BUFFER_CODE
    }

    #[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string(), iggy_partition_id = self.partition_id, iggy_fsync = self.fsync))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let user_id = session.get_user_id();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let fsync = self.fsync;

        shard
            .flush_unsaved_buffer(
                user_id,
                self.stream_id,
                self.topic_id,
                partition_id as usize,
                fsync,
            )
            .await
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to flush unsaved buffer for stream_id: {}, topic_id: {}, partition_id: {}, session: {}",
                    stream_id, topic_id, partition_id, session
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for FlushUnsavedBuffer {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::FlushUnsavedBuffer(flush_unsaved_buffer) => Ok(flush_unsaved_buffer),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
