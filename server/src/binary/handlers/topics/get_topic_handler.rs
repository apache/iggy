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

use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::get_topic::GetTopic;
use tracing::debug;

pub async fn handle(
    command: GetTopic,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let Ok(topic) = system.try_find_topic(session, &command.stream_id, &command.topic_id) else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let Some(topic) = topic else {
        sender.send_empty_ok_response().await?;
        return Ok(());
    };

    let topic = mapper::map_topic(topic).await;
    sender.send_ok_response(&topic).await?;
    Ok(())
}
