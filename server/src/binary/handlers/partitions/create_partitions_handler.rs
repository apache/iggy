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

use crate::binary::{handlers::partitions::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::partitions::create_partitions::CreatePartitions;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_partitions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string()))]
pub async fn handle(
    command: CreatePartitions,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    system.create_partitions(
                session,
                &command.stream_id,
                &command.topic_id,
                command.partitions_count,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create partitions for stream ID: {}, topic ID: {}, session: {}",
                    command.stream_id, command.topic_id, session
                )
            })?;

    let system = system.downgrade();
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();

    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::CreatePartitions(command),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create partitions for stream ID: {}, topic ID: {}, session: {}",
                stream_id, topic_id, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
