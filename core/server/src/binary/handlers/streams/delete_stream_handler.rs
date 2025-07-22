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
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::event::ShardEvent;
use crate::shard_info;
use crate::state::command::EntryCommand;
use crate::streaming::partitions::partition;
use crate::streaming::session::Session;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::locking::IggyRwLockFn;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, instrument};

impl ServerCommandHandler for DeleteStream {
    fn code(&self) -> u32 {
        iggy_common::DELETE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_delete_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Rc<Session>,
        shard: &Rc<IggyShard>,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();

        let stream = shard
                .delete_stream(session, &self.stream_id)
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to delete stream with ID: {stream_id}, session: {session}")
                })?;
        shard_info!(
            shard.id,
            "Deleted stream with name: {}, ID: {}",
            stream.name,
            stream.stream_id
        );
        // First we have to go over all of the partitions inside of topics
        // and make sure that they are deleted.
        for topic in stream.get_topics() {
            let partitions = topic.get_partitions();
            let mut namespaces = Vec::new();
            for partition in partitions {
                let partition_id = partition.read().await.partition_id;
                let namespace = IggyNamespace::new(stream.stream_id, topic.topic_id, partition_id);
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
        }
        if stream.delete().await.is_err() {
            return Err(IggyError::CannotDeleteStream(stream.stream_id));
        }
        let event = ShardEvent::DeletedStream {
            stream_id: self.stream_id.clone(),
        };
        let _responses = shard.broadcast_event_to_all_shards(event.into()).await;

        shard
            .state
            .apply(session.get_user_id(), &EntryCommand::DeleteStream(self))
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply delete stream with ID: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteStream(delete_stream) => Ok(delete_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
