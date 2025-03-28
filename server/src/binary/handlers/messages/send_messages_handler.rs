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

use crate::binary::handlers::messages::COMPONENT;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::random_id;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::send_messages::SendMessages;
use tracing::debug;

pub async fn handle(
    command: SendMessages,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();
    let partitioning = command.partitioning.clone();
    let mut messages = command.messages;
    messages.iter_mut().for_each(|msg| {
        if msg.id == 0 {
            msg.id = random_id::get_uuid();
        }
    });
    // TODO(haze): Add confirmation level after testing is complete
    system
        .append_messages(session, stream_id, topic_id, partitioning, messages, None)
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to append messages for stream ID: {}, topic ID: {}, partitioning: {}, session: {}",
                command.stream_id, command.topic_id, command.partitioning, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
