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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::update_permissions::UpdatePermissions;
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
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        shard
                .update_permissions(session, &self.user_id, self.permissions.clone())
                .with_error(|error| format!("{COMPONENT} (error: {error}) - failed to update permissions for user_id: {}, session: {session}",
                    self.user_id
                ))?;
        info!("Updated permissions for user with ID: {}.", self.user_id);
        let event = ShardEvent::UpdatedPermissions {
            user_id: self.user_id.clone(),
            permissions: self.permissions.clone(),
        };
        shard.broadcast_event_to_all_shards(event).await?;

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::UpdatePermissions(self),
            )
            .await?;
        sender.send_empty_ok_response().await?;
        Ok(())
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
