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
use crate::streaming::session::Session;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::responses::users::{GetUsersResponse, UserResponse};
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_users(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_users");
    shard.ensure_authenticated(session)?;

    let users = shard.metadata.query_users(session.get_user_id())?;
    let wire_users: Vec<UserResponse> = users
        .iter()
        .map(|u| {
            Ok(UserResponse {
                id: u.id,
                created_at: u.created_at.as_micros(),
                status: u.status.as_code(),
                username: WireName::new(u.username.as_ref())
                    .map_err(|_| IggyError::InvalidCommand)?,
            })
        })
        .collect::<Result<_, IggyError>>()?;
    let response = GetUsersResponse { users: wire_users };
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
