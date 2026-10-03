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

    /// Rewinds the stored offsets of this client's partitions to the start, so the measured
    /// phase does not read on from where the warmup left off.
    async fn reset_offsets(&mut self) -> Result<(), IggyError>;
}
pub trait BenchmarkConsumerClient: ConsumerClient + BenchmarkInit + ApiLabel + Send + Sync {}

/// Deletes this group member's offsets. Offset-strategy consumers rewind their local cursors.
pub async fn clear_consumer_offsets(
    client: &IggyClient,
    consumer: &Consumer,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<(), IggyError> {
    let partitions = client
        .get_consumer_group_assignment(stream_id, topic_id, &consumer.id)
        .await?
        .unwrap_or_default();

    for partition_id in &partitions {
        delete_offset(client, consumer, stream_id, topic_id, *partition_id).await?;
    }

    // A high-level consumer can be dropped with an in-flight poll that auto-commits.
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

/// A rebalance can transfer ownership after the assignment was read. Missing offsets are
/// already reset, even when a replica still serves a stale offset read.
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
        Ok(())
        | Err(
            IggyError::ConsumerGroupPartitionNotOwned(..) | IggyError::ConsumerOffsetNotFound(_),
        ) => Ok(()),
        Err(error) => Err(error),
    }
}
