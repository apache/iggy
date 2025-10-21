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
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
//use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy_common::get_cluster_metadata::GetClusterMetadata;
use iggy_common::{BytesSerializable, IggyError};
use tracing::{debug, instrument};

impl ServerCommandHandler for GetClusterMetadata {
    fn code(&self) -> u32 {
        iggy_common::GET_CLUSTER_METADATA_CODE
    }

    #[instrument(skip_all, name = "trace_get_cluster_metadata", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let cluster_metadata = shard.get_cluster_metadata(session)?;

        let response = cluster_metadata.to_bytes();
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetClusterMetadata {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetClusterMetadata(get_cluster_metadata) => Ok(get_cluster_metadata),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
