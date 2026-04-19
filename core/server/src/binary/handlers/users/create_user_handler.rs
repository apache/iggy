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
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::responses::users::{UserDetailsResponse, UserResponse};
use iggy_common::defaults::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use iggy_common::wire_conversions::permissions_to_wire;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_create_user(
    req: CreateUserRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: create_user, username: {}",
        req.username.as_str()
    );
    shard.ensure_authenticated(session)?;
    shard.metadata.perm_create_user(session.get_user_id())?;

    let username_len = req.username.as_str().len();
    if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&username_len) {
        return Err(IggyError::InvalidUsername);
    }
    let password_len = req.password.len();
    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&password_len) {
        return Err(IggyError::InvalidPassword);
    }

    let request = ShardRequest::control_plane(ShardRequestPayload::CreateUserRequest {
        user_id: session.get_user_id(),
        command: req,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::CreateUserResponse(user) => {
            let response = UserDetailsResponse {
                user: UserResponse {
                    id: user.id,
                    created_at: user.created_at.as_micros(),
                    status: user.status.as_code(),
                    username: WireName::new(&user.username)
                        .map_err(|_| IggyError::InvalidCommand)?,
                },
                permissions: user.permissions.as_ref().map(permissions_to_wire),
            };
            sender.send_ok_response(&response.to_bytes()).await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected CreateUserResponse"),
    }

    Ok(HandlerResult::Finished)
}
