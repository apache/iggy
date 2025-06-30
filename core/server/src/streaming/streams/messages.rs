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

use error_set::ErrContext;
use iggy_common::{Consumer, Identifier, IggyError, Partitioning};
use tracing::trace;

use super::COMPONENT;
use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    shard::{
        namespace::IggyNamespace,
        transmission::message::{ShardMessage, ShardRequest, ShardRequestPayload},
    },
    streaming::{
        partitions::partition,
        polling_consumer::PollingConsumer,
        segments::{IggyMessagesBatchMut, IggyMessagesBatchSet},
        streams::stream::Stream,
        topics::topic::Topic,
    },
};
use std::sync::atomic::Ordering;

impl Stream {
    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    // TODO: Create a types module, where we will put types of those closures.
    pub async fn append_messages(
        &self,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        append_messages: impl AsyncFnOnce(&Topic, u32) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        // It's quite empty there, but in the future if we would like to introduce consumer group subscription
        // based on topic name wildcards, we would have to lift those to stream level,
        // thus it would be perfect place to perform those calculations here.
        let topic = self.get_topic(topic_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get topic with ID: {} for stream ID: {}",
                topic_id, self.stream_id
            )
        })?;

        let partition_id = topic.calculate_partition_id(partitioning)?;
        append_messages(topic, partition_id).await
    }

    pub async fn poll_messages(
        &self,
        topic_id: &Identifier,
        client_id: u32,
        consumer: Consumer,
        maybe_partition_id: Option<u32>,
        auto_commit: bool,
        poll_messages: impl AsyncFnOnce(
            &Topic,
            PollingConsumer,
            u32,
        )
            -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError>,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        // Same as in `append_messages` method.
        let topic = self.get_topic(topic_id).with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to get topic with ID: {} for stream ID: {}",
                topic_id, self.stream_id
            )
        })?;
        if !topic.has_partitions() {
            return Err(IggyError::NoPartitions(topic.topic_id, self.stream_id));
        }

        let Some((consumer, partition_id)) = topic
                .resolve_consumer_with_partition_id(&consumer, client_id, maybe_partition_id, true)
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to resolve consumer with partition id, consumer: {consumer}, client ID: {}, partition ID: {:?}", client_id, maybe_partition_id))? else {
                todo!("Send early response");
                };

        let (metadata, batch) = poll_messages(topic, consumer, partition_id).await?;

        if auto_commit && !batch.is_empty() {
            let offset = batch
                .last_offset()
                .expect("Batch set should have at least one batch");
            trace!(
                "Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}",
                offset, consumer, self.stream_id, topic_id, partition_id
            );
            topic
                .store_consumer_offset_internal(consumer, offset, partition_id)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset internal, polling consumer: {consumer}, offset: {offset}, partition ID: {partition_id}"))?;
        }
        Ok((metadata, batch))
    }
}
