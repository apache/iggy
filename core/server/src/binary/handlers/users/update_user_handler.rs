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

use crate::binary::dispatch::{HandlerResult, wire_id_to_identifier};
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_binary_protocol::requests::users::UpdateUserRequest;
use iggy_common::update_user::UpdateUser;
use iggy_common::{IggyError, SenderKind, UserStatus, Validatable};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_update_user(
    req: UpdateUserRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: update_user, user_id: {:?}",
        req.user_id
    );
    shard.ensure_authenticated(session)?;
    shard.metadata.perm_update_user(session.get_user_id())?;

    let user_id = wire_id_to_identifier(&req.user_id)?;
    let status = req.status.map(UserStatus::from_code).transpose()?;

    let command = UpdateUser {
        user_id,
        username: req.username.map(|n| n.to_string()),
        status,
    };
    command.validate()?;

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdateUserRequest {
        user_id: session.get_user_id(),
        command,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::UpdateUserResponse(_) => {
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected UpdateUserResponse"),
    }

    Ok(HandlerResult::Finished)
}
