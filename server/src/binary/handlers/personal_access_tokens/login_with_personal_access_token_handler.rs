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
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_login_with_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: LoginWithPersonalAccessToken,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let user = system
        .login_with_personal_access_token(&command.token, Some(session))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to login with personal access token: {}, session: {session}",
                command.token
            )
        })?;
    let identity_info = mapper::map_identity_info(user.id);
    sender.send_ok_response(&identity_info).await?;
    Ok(())
}
