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
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::personal_access_tokens::{
    GetPersonalAccessTokensResponse, PersonalAccessTokenResponse,
};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_personal_access_tokens(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_personal_access_tokens");
    shard.ensure_authenticated(session)?;
    let personal_access_tokens = shard
        .get_personal_access_tokens(session.get_user_id())
        .error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - failed to get personal access tokens for user: {}",
                session.get_user_id()
            )
        })?;
    let tokens: Vec<PersonalAccessTokenResponse> = personal_access_tokens
        .iter()
        .map(|pat| {
            Ok(PersonalAccessTokenResponse {
                name: WireName::new(pat.name.as_ref()).map_err(|_| IggyError::InvalidCommand)?,
                expiry_at: pat.expiry_at.map_or(0, |e| e.as_micros()),
            })
        })
        .collect::<Result<_, IggyError>>()?;
    let response = GetPersonalAccessTokensResponse { tokens };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
