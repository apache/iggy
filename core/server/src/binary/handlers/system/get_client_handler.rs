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

use super::get_me_handler::build_client_details_response;
use crate::binary::dispatch::HandlerResult;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::requests::system::GetClientRequest;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_client(
    req: GetClientRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!(
        "session: {session}, command: get_client, client_id: {}",
        req.client_id
    );
    shard.ensure_authenticated(session)?;
    shard.metadata.perm_get_client(session.get_user_id())?;

    if req.client_id == 0 {
        return Err(IggyError::InvalidClientId);
    }

    let Some(client) = shard.get_client(req.client_id) else {
        sender.send_empty_ok_response().await?;
        return Ok(HandlerResult::Finished);
    };

    let response = build_client_details_response(&client);
    sender.send_ok_response(&response.to_bytes()).await?;
    Ok(HandlerResult::Finished)
}
