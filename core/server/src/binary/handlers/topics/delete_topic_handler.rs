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
use crate::binary::{handlers::topics::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::event::ShardEvent;
use crate::shard_info;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::locking::IggyRwLockFn;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteTopic {
    fn code(&self) -> u32 {
        iggy_common::DELETE_TOPIC_CODE
    }

    #[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let topic2 = shard
            .delete_topic2(session, &self.stream_id, &self.topic_id)
            .await?;
        let event = ShardEvent::DeletedTopic2 {
            id: topic2.id(),
            stream_id: self.stream_id.clone(),
            topic_id: self.topic_id.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        let topic = shard
                .delete_topic(session, &self.stream_id, &self.topic_id)
                .await
                .with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to delete topic with ID: {} in stream with ID: {}, session: {session}",
                    &self.topic_id, &self.stream_id
                ))?;
        shard_info!(
            shard.id,
            "Deleted topic with name: {}, ID: {} in stream with ID: {}",
            topic.name,
            topic.topic_id,
            topic.stream_id
        );
        let numeric_topic_id = topic.topic_id;
        let numeric_stream_id = topic.stream_id;
        let partitions = topic.get_partitions();
        let mut namespaces = Vec::new();
        for partition in partitions {
            let partition_id = partition.read().await.partition_id;
            let namespace = IggyNamespace::new(numeric_stream_id, topic.topic_id, partition_id);
            namespaces.push(namespace);
        }
        let records = shard.remove_shard_table_records(&namespaces);
        for (ns, shard_info) in records {
            if shard_info.id() == shard.id {
                let partition = topic.get_partition(ns.partition_id).unwrap();
                let mut partition = partition.write().await;
                let partition_id = partition.partition_id;
                partition.delete().await.with_error_context(|error| {
                        format!("{COMPONENT} (error: {error}) - failed to delete partition in stream: {self}, partition: {partition_id}")
                    })?;
            }
        }
        let event = ShardEvent::DeletedShardTableRecords { namespaces };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;
        topic.delete().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to delete topic in stream: {self}")
        })?;

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteTopic(self))
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to apply delete topic with ID: {numeric_topic_id} in stream with ID: {numeric_stream_id}, session: {session}",
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteTopic(delete_topic) => Ok(delete_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
