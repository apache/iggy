// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bench_report::numeric_parameter::BenchmarkNumericParameter;
use iggy::prelude::*;

use crate::actors::{ApiLabel, BatchMetrics, BenchmarkInit};

#[derive(Debug, Clone)]
pub struct BenchmarkConsumerConfig {
    pub consumer_id: u32,
    pub consumer_group_id: Option<u32>,
    pub stream_id: String,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub warmup_time: IggyDuration,
    pub polling_kind: PollingKind,
    pub poll_options: DeferredPollOptions,
    pub origin_timestamp_latency_calculation: bool,
    pub pretty: bool,
}

pub trait ConsumerClient: Send + Sync {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError>;

    /// Rewinds the stored offsets of this client's partitions to the start, so the measured
    /// phase does not read on from where the warmup left off.
    async fn reset_offsets(&mut self) -> Result<(), IggyError>;
}
pub trait BenchmarkConsumerClient: ConsumerClient + BenchmarkInit + ApiLabel + Send + Sync {}

/// Deletes the offset `consumer` stored in every partition of the topic.
///
/// The bench cannot read the partition count from its arguments: the consumer kinds that take
/// this path report 0 partitions and never create the streams they poll, so the topic details
/// supply the count. An offset that is not stored, or a partition that another member of the
/// group owns, is a state a clean reset passes through rather than a failure.
pub async fn clear_consumer_offsets(
    client: &IggyClient,
    consumer: &Consumer,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<(), IggyError> {
    let details = client
        .get_topic(stream_id, topic_id)
        .await?
        .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
    let partitions: Vec<u32> = details
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect();

    for partition_id in &partitions {
        delete_offset(client, consumer, stream_id, topic_id, *partition_id).await?;
    }

    // A poll or a queued offset store from the phase before the reset can still reach the
    // server after the deletes above, and auto-commit rides the poll. Reading back once
    // turns an offset that came back into one that is deleted again.
    for partition_id in &partitions {
        let stored = client
            .get_consumer_offset(consumer, stream_id, topic_id, Some(*partition_id))
            .await?;
        if stored.is_some() {
            delete_offset(client, consumer, stream_id, topic_id, *partition_id).await?;
        }
    }

    Ok(())
}

/// Deletes the offset stored for one partition, and confirms the outcome when the delete fails.
///
/// A group member owns only some of the topic's partitions, so it is refused on the rest. That
/// refusal names the partition, and means the offset is not this member's to clear either way.
/// Any other failure could still be the ordinary "nothing stored for this partition", which the
/// transports spell differently: a typed error over TCP, a bare 404 over HTTP. Asking what is
/// stored separates the two without losing a real error behind a transport-shaped guess.
async fn delete_offset(
    client: &IggyClient,
    consumer: &Consumer,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: u32,
) -> Result<(), IggyError> {
    match client
        .delete_consumer_offset(consumer, stream_id, topic_id, Some(partition_id))
        .await
    {
        Ok(()) | Err(IggyError::ConsumerGroupPartitionNotOwned(..)) => Ok(()),
        Err(error) => {
            let stored = client
                .get_consumer_offset(consumer, stream_id, topic_id, Some(partition_id))
                .await?;
            if stored.is_none() { Ok(()) } else { Err(error) }
        }
    }
}
