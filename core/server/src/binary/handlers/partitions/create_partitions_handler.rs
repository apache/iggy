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
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::locking::IggyRwLockFn;
use std::num;
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
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        shard
            .create_partitions2(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partitions_count,
            )
            .await?;
        let event = ShardEvent::CreatedPartitions2 {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partitions_count: self.partitions_count,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        let partition_ids = shard
            .create_partitions(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partitions_count,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create partitions for stream_id: {}, topic_id: {}, session: {}",
                    self.stream_id, self.topic_id, session
                )
            })?;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let event = ShardEvent::CreatedPartitions {
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
            partitions_count: partition_ids.len() as u32,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        let stream = shard.get_stream(&stream_id).unwrap();
        let topic = stream.get_topic(&topic_id).unwrap();
        let numeric_stream_id = stream.stream_id;
        let numeric_topic_id = topic.topic_id;

        let records = shard
            .create_shard_table_records(&partition_ids, numeric_stream_id, numeric_topic_id)
            .collect::<Vec<_>>();
        // Open partition and segments for that particular shard.
        for (ns, shard_info) in records.iter() {
            let partition = topic.get_partition(ns.partition_id).unwrap();
            let mut partition = partition.write().await;
            partition.persist().await.unwrap();
            if shard_info.id() == shard.id {
                let partition_id = ns.partition_id;
                partition.open().await.with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to open partition with ID: {partition_id} in topic with ID: {topic_id} for stream with ID: {stream_id}"
                    )
                })?;
            }
        }
        shard.insert_shard_table_records(records);
        // Broadcast the event to all shards.
        let event = ShardEvent::CreatedShardTableRecords {
            stream_id: numeric_stream_id,
            topic_id: numeric_topic_id,
            partition_ids,
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        shard
        .state
        .apply(
            session.get_user_id(),
            &EntryCommand::CreatePartitions(self),
        )
        .await
        .with_error_context(|error| {
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
