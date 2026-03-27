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
use crate::binary::handlers::users::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, info, instrument};

#[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle_logout_user(
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: logout_user");
    shard.ensure_authenticated(session)?;
    info!("Logging out user with ID: {}...", session.get_user_id());
    shard.logout_user(session).error(|e: &IggyError| {
        format!("{COMPONENT} (error: {e}) - failed to logout user, session: {session}")
    })?;
    info!("Logged out user with ID: {}.", session.get_user_id());
    session.clear_user_id();
    sender.send_empty_ok_response().await?;
    Ok(HandlerResult::Finished)
}
