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

use crate::binary::{handlers::messages::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::flush_unsaved_buffer::FlushUnsavedBuffer;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_flush_unsaved_buffer", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string(), iggy_partition_id = command.partition_id, iggy_fsync = command.fsync))]
pub async fn handle(
    command: FlushUnsavedBuffer,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();
    let partition_id = command.partition_id;
    let fsync = command.fsync;
    system
        .flush_unsaved_buffer(session, stream_id, topic_id, partition_id, fsync)
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to flush unsaved buffer for stream ID: {}, topic ID: {}, partition_id: {}, session: {}",
                command.stream_id, command.topic_id, command.partition_id, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
