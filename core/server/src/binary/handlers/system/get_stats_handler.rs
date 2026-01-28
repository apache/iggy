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

use crate::binary::command::{
    BinaryServerCommand, HandlerResult, ServerCommand, ServerCommandHandler,
};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_stats::GetStats;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetStats {
    fn code(&self) -> u32 {
        iggy_common::GET_STATS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard.metadata.perm_get_stats(session.get_user_id())?;

        let request = ShardRequest::control_plane(ShardRequestPayload::GetStats {
            user_id: session.get_user_id(),
        });

        match shard.send_to_control_plane(request).await? {
            ShardResponse::GetStatsResponse(stats) => {
                sender.send_ok_response(&mapper::map_stats(&stats)).await?;
            }
            ShardResponse::ErrorResponse(err) => return Err(err),
            _ => unreachable!("Expected GetStatsResponse"),
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetStats {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let server_command = receive_and_validate(sender, code, length).await?;
        match server_command {
            ServerCommand::GetStats(get_stats) => Ok(get_stats),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
