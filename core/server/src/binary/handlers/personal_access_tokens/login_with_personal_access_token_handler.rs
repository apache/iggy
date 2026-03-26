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
use crate::binary::handlers::personal_access_tokens::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::personal_access_tokens::LoginWithPersonalAccessTokenRequest;
use iggy_binary_protocol::responses::users::IdentityResponse;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::{IggyError, SenderKind, Validatable};
use secrecy::SecretString;
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_login_with_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_login_with_personal_access_token(
    req: LoginWithPersonalAccessTokenRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: login_with_personal_access_token");
    let token = req.token.as_str();

    let command = LoginWithPersonalAccessToken {
        token: SecretString::from(token),
    };
    command.validate()?;

    let user = shard
        .login_with_personal_access_token(token, Some(session))
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to login with personal access token, session: {session}",
            )
        })?;
    let response = IdentityResponse { user_id: user.id };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
