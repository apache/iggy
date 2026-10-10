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
    pub origin_timestamp_latency_calculation: bool,
    pub pretty: bool,
}

pub trait ConsumerClient: Send + Sync {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError>;

    /// Moves the read position back to the start of every partition, so the measured phase
    /// does not read on from where the warmup left off. Group members also delete the offsets
    /// that the warmup committed.
    async fn reset_offsets(&mut self) -> Result<(), IggyError>;
}
pub trait BenchmarkConsumerClient: ConsumerClient + BenchmarkInit + ApiLabel + Send + Sync {}

/// Deletes this member's group offsets on every partition of the topic. The server refuses a
/// partition of another member or an empty partition, and logs a WARN for each. It accepts an
/// owned partition that is pending handoff, which a group sync does not list.
pub async fn clear_group_offsets(
    client: &IggyClient,
    group: &Consumer,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<(), IggyError> {
    let topic = client
        .get_topic(stream_id, topic_id)
        .await?
        .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
    for partition in &topic.partitions {
        clear_group_offset(client, group, stream_id, topic_id, partition.id).await?;
    }
    Ok(())
}

/// Stores offset 0 before the delete. A cluster refuses to delete an offset that is not
/// replicated yet. While the first commit of the group on a partition replicates, a delete alone
/// fails, and that commit then brings the warmup offset back. The store commits after that
/// commit, so the delete finds a replicated offset.
async fn clear_group_offset(
    client: &IggyClient,
    group: &Consumer,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: u32,
) -> Result<(), IggyError> {
    match client
        .store_consumer_offset(group, stream_id, topic_id, Some(partition_id), 0)
        .await
    {
        Ok(()) => {}
        // A partition of another member, or an empty partition, which holds no offset.
        Err(IggyError::ConsumerGroupPartitionNotOwned(..) | IggyError::InvalidOffset(_)) => {
            return Ok(());
        }
        Err(error) => return Err(error),
    }
    client
        .delete_consumer_offset(group, stream_id, topic_id, Some(partition_id))
        .await
}
