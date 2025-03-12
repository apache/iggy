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

use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::ping::Ping;
use iggy::utils::timestamp::IggyTimestamp;
use tracing::debug;

pub async fn handle(
    command: Ping,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let client_manager = system.client_manager.read().await;
    if let Some(client) = client_manager.try_get_client(session.client_id) {
        let mut client = client.write().await;
        let now = IggyTimestamp::now();
        client.last_heartbeat = now;
        debug!("Updated last heartbeat to: {now} for session: {session}");
    }

    sender.send_empty_ok_response().await?;
    Ok(())
}
