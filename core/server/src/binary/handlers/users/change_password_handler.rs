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
use iggy_binary_protocol::requests::users::ChangePasswordRequest;
use iggy_common::defaults::{MAX_PASSWORD_LENGTH, MIN_PASSWORD_LENGTH};
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_change_password(
    req: ChangePasswordRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: change_password, user_id: {:?}",
        req.user_id
    );
    shard.ensure_authenticated(session)?;

    let current_len = req.current_password.len();
    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&current_len) {
        return Err(IggyError::InvalidPassword);
    }
    let new_len = req.new_password.len();
    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&new_len) {
        return Err(IggyError::InvalidPassword);
    }

    let request = ShardRequest::control_plane(ShardRequestPayload::ChangePasswordRequest {
        user_id: session.get_user_id(),
        command: req,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::ChangePasswordResponse => {
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected ChangePasswordResponse"),
    }

    Ok(HandlerResult::Finished)
}
