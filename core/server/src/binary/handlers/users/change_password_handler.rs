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
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardRequest, ShardRequestPayload};
use crate::streaming::session::Session;
use iggy_common::change_password::ChangePassword;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for ChangePassword {
    fn code(&self) -> u32 {
        iggy_common::CHANGE_PASSWORD_CODE
    }

    #[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;

        let target_user = shard.get_user(&self.user_id)?;
        if target_user.id != session.get_user_id() {
            shard.metadata.perm_change_password(session.get_user_id())?;
        }

        let request = ShardRequest::control_plane(ShardRequestPayload::ChangePasswordRequest {
            user_id: session.get_user_id(),
            command: self,
        });

        match shard.send_to_control_plane(request).await? {
            ShardResponse::ChangePasswordResponse => {
                sender.send_empty_ok_response().await?;
            }
            ShardResponse::ErrorResponse(err) => return Err(err),
            _ => unreachable!("Expected ChangePasswordResponse"),
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for ChangePassword {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::ChangePassword(change_password) => Ok(change_password),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
