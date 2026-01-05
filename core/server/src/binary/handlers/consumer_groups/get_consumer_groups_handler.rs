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
    AuthenticatedHandler, BinaryServerCommand, HandlerResult, ServerCommand,
};
use crate::binary::handlers::utils::receive_and_validate;
use crate::shard::IggyShard;
use crate::streaming::auth::Auth;
use crate::streaming::session::Session;
use iggy_common::IggyError;
use iggy_common::SenderKind;
use iggy_common::get_consumer_groups::GetConsumerGroups;
use std::rc::Rc;
use tracing::debug;

impl AuthenticatedHandler for GetConsumerGroups {
    fn code(&self) -> u32 {
        iggy_common::GET_CONSUMER_GROUPS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        auth: Auth,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        let (numeric_stream_id, numeric_topic_id) =
            shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        shard.permissioner.get_consumer_groups(
            auth.user_id(),
            numeric_stream_id,
            numeric_topic_id,
        )?;

        let metadata = shard.metadata.load();

        let consumer_groups = metadata
            .streams
            .get(numeric_stream_id)
            .and_then(|s| s.topics.get(numeric_topic_id))
            .map(|topic| {
                use bytes::{BufMut, BytesMut};
                let mut bytes = BytesMut::new();
                for (_, cg_meta) in topic.consumer_groups.iter() {
                    bytes.put_u32_le(cg_meta.id as u32);
                    bytes.put_u32_le(cg_meta.partitions.len() as u32);
                    bytes.put_u32_le(cg_meta.members.len() as u32);
                    bytes.put_u8(cg_meta.name.len() as u8);
                    bytes.put_slice(cg_meta.name.as_bytes());
                }
                bytes.freeze()
            })
            .unwrap_or_default();
        sender.send_ok_response(&consumer_groups).await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for GetConsumerGroups {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerGroups(get_consumer_groups) => Ok(get_consumer_groups),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
