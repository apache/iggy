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

use crate::binary::mapper;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::state::models::CreatePersonalAccessTokenWithHash;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: CreatePersonalAccessToken,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let system = system.read().await;
    let token = system
            .create_personal_access_token(session, &command.name, command.expiry)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                    command.name
                )
            })?;
    let bytes = mapper::map_raw_pat(&token);
    let token_hash = PersonalAccessToken::hash_token(&token);

    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash {
                hash: token_hash,
                command: CreatePersonalAccessToken {
                    name: command.name.to_owned(),
                    expiry: command.expiry,
                }
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to create personal access token with name: {}, session: {session}",
                command.name
            )
        })?;
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
