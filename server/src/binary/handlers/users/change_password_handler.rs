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

use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::change_password::ChangePassword;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: ChangePassword,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    system.change_password(
                session,
                &command.user_id,
                &command.current_password,
                &command.new_password,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to change password for user ID: {}, session: {session}",
                    command.user_id
                )
            })?;

    // For the security of the system, we hash the password before storing it in metadata.
    let system = system.downgrade();
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::ChangePassword(ChangePassword {
                user_id: command.user_id.to_owned(),
                current_password: "".into(),
                new_password: crypto::hash_password(&command.new_password),
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply change password for user ID: {}, session: {session}",
                command.user_id
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
