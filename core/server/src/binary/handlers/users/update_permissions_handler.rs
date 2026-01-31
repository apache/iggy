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

use std::rc::Rc;

use crate::binary::command::{
    BinaryServerCommand, HandlerResult, ServerCommand, ServerCommandHandler,
};
use crate::binary::handlers::users::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::shard::IggyShard;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{
    ShardMessage, ShardRequest, ShardRequestPayload, ShardSendRequestResult,
};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::{Identifier, IggyError, SenderKind};
use tracing::info;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdatePermissions {
    fn code(&self) -> u32 {
        iggy_common::UPDATE_PERMISSIONS_CODE
    }

    #[instrument(skip_all, name = "trace_update_permissions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard
            .metadata
            .perm_update_permissions(session.get_user_id())?;

        // Check if target user is root - cannot change root user permissions
        let target_user = shard.get_user(&self.user_id)?;
        if target_user.is_root() {
            return Err(IggyError::CannotChangePermissions(target_user.id));
        }

        let user_id_for_log = self.user_id.clone();
        let request = ShardRequest {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: 0,
            payload: ShardRequestPayload::UpdatePermissions {
                session_user_id: session.get_user_id(),
                user_id: self.user_id.clone(),
                permissions: self.permissions.clone(),
            },
        };

        let message = ShardMessage::Request(request);
        match shard.send_request_to_shard_or_recoil(None, message).await? {
            ShardSendRequestResult::Recoil(message) => {
                if let ShardMessage::Request(ShardRequest { payload, .. }) = message
                    && let ShardRequestPayload::UpdatePermissions {
                        user_id,
                        permissions,
                        ..
                    } = payload
                {
                    let _user_guard = shard.fs_locks.user_lock.lock().await;
                    shard
                        .update_permissions(&user_id, permissions)
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to update permissions for user_id: {}, session: {session}",
                                user_id
                            )
                        })?;

                    info!("Updated permissions for user with ID: {}.", user_id_for_log);

                    shard
                        .state
                        .apply(session.get_user_id(), &EntryCommand::UpdatePermissions(self))
                        .await
                        .error(|e: &IggyError| {
                            format!(
                                "{COMPONENT} (error: {e}) - failed to apply update permissions for user_id: {}, session: {session}",
                                user_id_for_log
                            )
                        })?;

                    sender.send_empty_ok_response().await?;
                } else {
                    unreachable!(
                        "Expected an UpdatePermissions request inside of UpdatePermissions handler, impossible state"
                    );
                }
            }
            ShardSendRequestResult::Response(response) => match response {
                ShardResponse::UpdatePermissionsResponse => {
                    info!("Updated permissions for user with ID: {}.", user_id_for_log);
                    sender.send_empty_ok_response().await?;
                }
                ShardResponse::ErrorResponse(err) => {
                    return Err(err);
                }
                _ => unreachable!(
                    "Expected an UpdatePermissionsResponse inside of UpdatePermissions handler, impossible state"
                ),
            },
        }

        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for UpdatePermissions {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdatePermissions(update_permissions) => Ok(update_permissions),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
