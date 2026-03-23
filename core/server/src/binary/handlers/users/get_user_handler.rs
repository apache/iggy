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

use crate::binary::dispatch::{HandlerResult, domain_permissions_to_wire, wire_id_to_identifier};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::users::GetUserRequest;
use iggy_binary_protocol::responses::users::{UserDetailsResponse, UserResponse};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_user(
    req: GetUserRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: get_user, user_id: {:?}",
        req.user_id
    );
    shard.ensure_authenticated(session)?;

    let user_id = wire_id_to_identifier(&req.user_id)?;

    let Some(user) = shard.metadata.query_user(session.get_user_id(), &user_id)? else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let response = UserDetailsResponse {
        user: UserResponse {
            id: user.id,
            created_at: user.created_at.as_micros(),
            status: user.status.as_code(),
            username: WireName::new(user.username.as_ref())
                .map_err(|_| IggyError::InvalidCommand)?,
        },
        permissions: user.permissions.as_deref().map(domain_permissions_to_wire),
    };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
