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
use crate::binary::handlers::partitions::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::shard::IggyShard;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::{IggyError, SenderKind};
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeletePartitions {
    fn code(&self) -> u32 {
        iggy_common::DELETE_PARTITIONS_CODE
    }

    #[instrument(skip_all, name = "trace_delete_partitions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();

        // Acquire partition lock to serialize filesystem operations
        let _partition_guard = shard.fs_locks.partition_lock.lock().await;

        let _deleted_partition_ids = shard
            .delete_partitions(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partitions_count,
            )
            .await?;

        // Get remaining partition IDs from SharedMetadata
        let numeric_stream_id = shard
            .shared_metadata
            .get_stream_id(&self.stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;
        let numeric_topic_id = shard
            .shared_metadata
            .get_topic_id(numeric_stream_id, &self.topic_id)
            .ok_or_else(|| {
                IggyError::TopicIdNotFound(self.topic_id.clone(), self.stream_id.clone())
            })?;

        let snapshot = shard.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(self.stream_id.clone()))?;
        let topic_meta = stream_meta.get_topic(numeric_topic_id).ok_or_else(|| {
            IggyError::TopicIdNotFound(self.topic_id.clone(), self.stream_id.clone())
        })?;
        let remaining_partition_ids: Vec<usize> = topic_meta.partitions.keys().copied().collect();
        drop(snapshot);

        // Rebalance consumer groups with remaining partition IDs via SharedMetadata
        shard.shared_metadata.rebalance_consumer_groups(
            &self.stream_id,
            &self.topic_id,
            &remaining_partition_ids,
        )?;

        shard
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::DeletePartitions(self),
            )
            .await
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to apply delete partitions for stream_id: {stream_id}, topic_id: {topic_id}, session: {session}"
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(HandlerResult::Finished)
    }
}

impl BinaryServerCommand for DeletePartitions {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeletePartitions(delete_partitions) => Ok(delete_partitions),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
