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

use crate::binary::dispatch::{
    HandlerResult, wire_id_to_identifier, wire_permissions_to_permissions,
};
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_binary_protocol::requests::users::UpdatePermissionsRequest;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_permissions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_update_permissions(
    req: UpdatePermissionsRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: update_permissions, user_id: {:?}",
        req.user_id
    );
    shard.ensure_authenticated(session)?;
    shard
        .metadata
        .perm_update_permissions(session.get_user_id())?;

    let user_id = wire_id_to_identifier(&req.user_id)?;

    let target_user = shard.get_user(&user_id)?;
    if target_user.is_root() {
        return Err(IggyError::CannotChangePermissions(target_user.id));
    }

    let command = UpdatePermissions {
        user_id,
        permissions: req
            .permissions
            .as_ref()
            .map(wire_permissions_to_permissions),
    };

    let request = ShardRequest::control_plane(ShardRequestPayload::UpdatePermissionsRequest {
        user_id: session.get_user_id(),
        command,
    });

    match shard.send_to_control_plane(request).await? {
        ShardResponse::UpdatePermissionsResponse => {
            sender.send_empty_ok_response().await?;
        }
        ShardResponse::ErrorResponse(err) => return Err(err),
        _ => unreachable!("Expected UpdatePermissionsResponse"),
    }

    Ok(HandlerResult::Finished)
}
