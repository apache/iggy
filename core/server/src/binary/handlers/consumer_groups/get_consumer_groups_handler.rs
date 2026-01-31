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
use crate::streaming::session::Session;
use bytes::{BufMut, BytesMut};
use iggy_common::get_consumer_groups::GetConsumerGroups;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetConsumerGroups {
    fn code(&self) -> u32 {
        iggy_common::GET_CONSUMER_GROUPS_CODE
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
        let (stream_id, topic_id) = shard.resolve_topic_id(&self.stream_id, &self.topic_id)?;
        shard
            .metadata
            .perm_get_consumer_groups(session.get_user_id(), stream_id, topic_id)?;

        let consumer_groups = shard.metadata.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .map(|topic| {
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
                .unwrap_or_default()
        });
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
