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

use crate::binary::dispatch::HandlerResult;
use crate::binary::handlers::users::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::users::LoginUserRequest;
use iggy_binary_protocol::responses::users::IdentityResponse;
use iggy_common::login_user::LoginUser;
use iggy_common::{IggyError, SenderKind, Validatable};
use secrecy::SecretString;
use std::rc::Rc;
use tracing::{debug, info, instrument, warn};

#[instrument(skip_all, name = "trace_login_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_login_user(
    req: LoginUserRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    if shard.is_shutting_down() {
        warn!("Rejecting login request during shutdown");
        return Err(IggyError::Disconnected);
    }

    let username = req.username.as_str();
    debug!("session: {session}, command: login_user, username: {username}");

    let command = LoginUser {
        username: username.to_string(),
        password: SecretString::from(req.password.as_str()),
        version: req.version.clone(),
        context: req.context.clone(),
    };
    command.validate()?;

    info!("Logging in user: {username} ...");
    let user = shard
        .login_user(username, &req.password, Some(session))
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to login user with name: {username}, session: {session}",
            )
        })?;
    info!("Logged in user: {username} with ID: {}.", user.id);

    let response = IdentityResponse { user_id: user.id };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
