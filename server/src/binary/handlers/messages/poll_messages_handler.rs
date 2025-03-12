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
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use tracing::debug;

pub async fn handle(
    command: PollMessages,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let messages = system
        .poll_messages(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            PollingArgs::new(command.strategy, command.count, command.auto_commit),
        )
        .await
        .with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - failed to poll messages for consumer: {}, stream ID: {}, topic ID: {}, partition_id: {:?}, session: {}.",
            command.consumer, command.stream_id, command.topic_id, command.partition_id, session
        ))?;
    let messages = mapper::map_polled_messages(&messages);
    sender.send_ok_response(&messages).await?;
    Ok(())
}
