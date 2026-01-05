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
use crate::streaming::session::Session;
use anyhow::Result;
use iggy_common::IggyError;
use iggy_common::SenderKind;
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
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        shard.ensure_authenticated(session)?;
        shard.ensure_stream_exists(&self.stream_id)?;

        // Get stream ID from SharedMetadata
        let numeric_stream_id = shard
            .shared_metadata
            .get_stream_id(&self.stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;

        shard
            .permissioner
            .get_topics(session.get_user_id(), numeric_stream_id)?;

        // Get topics from SharedMetadata
        let snapshot = shard.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;

        // Collect topics with their stats
        let mut topics_with_stats: Vec<_> = Vec::new();
        for (topic_id, topic) in stream_meta.topics.iter() {
            if let Some(stats) = shard
                .shared_stats
                .get_topic_stats(numeric_stream_id, *topic_id)
            {
                topics_with_stats.push((topic, stats));
            }
        }

        let refs: Vec<_> = topics_with_stats
            .iter()
            .map(|(t, s)| (*t, s.as_ref()))
            .collect();
        let response = mapper::map_topics_meta(&refs);

        sender.send_ok_response(&response).await?;
        Ok(HandlerResult::Finished)
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
