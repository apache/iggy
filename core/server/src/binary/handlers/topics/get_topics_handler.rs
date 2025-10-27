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
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::shard::IggyShard;
use crate::slab::traits_ext::{EntityComponentSystem, IntoComponents};
use crate::streaming::session::Session;
use crate::streaming::streams;
use anyhow::Result;
use iggy_common::IggyError;
use iggy_common::get_topics::GetTopics;
use std::rc::Rc;
use tracing::debug;

impl ServerCommandHandler for GetTopics {
    fn code(&self) -> u32 {
        iggy_common::GET_TOPICS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard.ensure_stream_exists(&self.stream_id)?;
        let numeric_stream_id = shard
            .streams
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        shard
            .permissioner
            .borrow()
            .get_topics(session.get_user_id(), numeric_stream_id)?;

        let response = shard.streams.with_topics(&self.stream_id, |topics| {
            topics.with_components(|topics| {
                let (roots, _, stats) = topics.into_components();
                mapper::map_topics(&roots, &stats)
            })
        });
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetTopics {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetTopics(get_topics) => Ok(get_topics),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
