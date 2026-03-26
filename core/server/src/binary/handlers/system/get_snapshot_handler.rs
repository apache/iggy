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
use bytes::Bytes;
use iggy_binary_protocol::requests::system::GetSnapshotRequest;
use iggy_common::{IggyError, SenderKind, SnapshotCompression, SystemSnapshotType};
use std::rc::Rc;
use tracing::debug;

pub async fn handle_get_snapshot(
    req: GetSnapshotRequest,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    debug!("session: {session}, command: get_snapshot");
    shard.ensure_authenticated(session)?;

    let compression = SnapshotCompression::from_code(req.compression)?;
    let snapshot_types: Vec<SystemSnapshotType> = req
        .snapshot_types
        .iter()
        .map(|&code| SystemSnapshotType::from_code(code))
        .collect::<Result<_, _>>()?;

    if snapshot_types.contains(&SystemSnapshotType::All) && snapshot_types.len() > 1 {
        return Err(IggyError::InvalidCommand);
    }

    let snapshot = shard.get_snapshot(compression, &snapshot_types).await?;
    let bytes = Bytes::copy_from_slice(&snapshot.0);
    sender.send_ok_response(&bytes).await?;
    Ok(HandlerResult::Finished)
}
