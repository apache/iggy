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
use crate::binary::{handlers::partitions::COMPONENT, sender::SenderKind};

use crate::shard::IggyShard;
use crate::shard::transmission::event::ShardEvent;
use crate::slab::traits_ext::EntityMarker;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::{streams, topics};
use anyhow::Result;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::create_partitions::CreatePartitions;
use std::rc::Rc;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreatePartitions {
    fn code(&self) -> u32 {
        iggy_common::CREATE_PARTITIONS_CODE
    }

    #[instrument(skip_all, name = "trace_create_partitions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        // Acquire partition lock to serialize filesystem operations
        let _partition_guard = shard.fs_locks.partition_lock.lock().await;

        let partitions = shard
            .create_partitions(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partitions_count,
            )
            .await?;
        let partition_ids = partitions.iter().map(|p| p.id()).collect::<Vec<_>>();
        let event = ShardEvent::CreatedPartitions {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partitions,
        };
        shard.broadcast_event_to_all_shards(event).await?;

        shard.streams.with_topic_by_id_mut(
            &self.stream_id,
            &self.topic_id,
            topics::helpers::rebalance_consumer_groups(&partition_ids),
        );

        let stream_id = shard
            .streams
            .with_stream_by_id(&self.stream_id, streams::helpers::get_stream_id());
        let topic_id = shard.streams.with_topic_by_id(
            &self.stream_id,
            &self.topic_id,
            topics::helpers::get_topic_id(),
        );
        shard
        .state
        .apply(
            session.get_user_id(),
            &EntryCommand::CreatePartitions(self),
        )
        .await
        .with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create partitions for stream_id: {stream_id}, topic_id: {topic_id}, session: {session}"
            )
        })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreatePartitions {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreatePartitions(create_partitions) => Ok(create_partitions),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
