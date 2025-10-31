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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::system::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::get_stats::GetStats;
use iggy_common::{Identifier, IggyError};
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
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        // Route GetStats to shard0 only
        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::GetStats {
                user_id: session.get_user_id(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(_) => {
                let stats = shard.get_stats().await.with_error(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to get stats, session: {session}"
                    )
                })?;
                let bytes = mapper::map_stats(&stats);
                sender.send_ok_response(&bytes).await?;
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::GetStatsResponse(stats) => {
                    let bytes = mapper::map_stats(&stats);
                    sender.send_ok_response(&bytes).await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected a GetStatsResponse inside of GetStats handler, impossible state"
                ),
            },
        }

        Ok(())
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
