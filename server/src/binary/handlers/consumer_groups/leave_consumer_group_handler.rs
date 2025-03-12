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

use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_leave_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string(), iggy_group_id = command.group_id.as_string()))]
pub async fn handle(
    command: LeaveConsumerGroup,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system.leave_consumer_group(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.group_id,
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to leave consumer group for stream ID: {}, topic ID: {}, group ID: {}, session: {}",
                command.stream_id, command.topic_id, command.group_id, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
