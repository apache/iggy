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
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::personal_access_tokens::CreatePersonalAccessTokenRequest;
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::{IggyError, IggyExpiry, SenderKind, Validatable};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_create_personal_access_token(
    req: CreatePersonalAccessTokenRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: create_personal_access_token, name: {}",
        req.name.as_str()
    );
    shard.ensure_authenticated(session)?;

    let command = CreatePersonalAccessToken {
        name: req.name.to_string(),
        expiry: IggyExpiry::from(req.expiry),
    };
    command.validate()?;

    let request =
        ShardRequest::control_plane(ShardRequestPayload::CreatePersonalAccessTokenRequest {
            user_id: session.get_user_id(),
            command,
        });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::CreatePersonalAccessTokenResponse(_, token) => {
            let response = RawPersonalAccessTokenResponse {
                token: WireName::new(token).map_err(|_| IggyError::InvalidCommand)?,
            };
            sender.send_ok_response(&response.to_bytes()).await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected CreatePersonalAccessTokenResponse"),
    }

    Ok(HandlerResult::Finished)
}
