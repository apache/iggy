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

//! Bench flags that change a result. Durations are integer microseconds (`_us` suffix),
//! sizes are integer bytes.

use super::benchmark_kind::BenchmarkKind;
use serde::{Deserialize, Serialize};

const LEGACY_MAX_WAIT_US: u64 = 1_000_000;
const LEGACY_MIN_COUNT: u32 = 1;
const LEGACY_READ_AMPLIFICATION: f32 = 1.05;

/// Every bench flag that changes a result, with the bench defaults applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkOptions {
    pub topic: BenchmarkTopicOptions,
    /// `None` for the producer-only kinds, which never poll.
    pub consumer: Option<BenchmarkConsumerOptions>,
    pub client: BenchmarkClientOptions,
    pub tcp: BenchmarkTcpOptions,
}

/// Topic options. They only take effect in a run that creates the topics: not with `reuse_streams`
/// on existing streams, and never for the consumer-only kinds, which read topics that an earlier
/// run created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BenchmarkTopicOptions {
    pub durability: BenchmarkDurability,
    /// `None`: the server default of the version under test.
    pub messages_required_to_save: Option<u32>,
    pub max_topic_size: BenchmarkMaxTopicSize,
    pub message_expiry: BenchmarkMessageExpiry,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkConsumerOptions {
    /// A topic option, so it shares the conditions of [`BenchmarkTopicOptions`].
    pub consumer_offset_durability: BenchmarkDurability,
    pub max_wait_us: u64,
    pub min_count: u32,
    /// `--max-bytes` as passed. `None`: the bench default, 1 MiB or one full batch reply,
    /// whichever is larger. The effective default depends on the message size, the batch
    /// size and the protocol header sizes, so it would never match the `legacy` value of
    /// older reports and would split their history.
    pub max_bytes: Option<u32>,
    /// Multiplier of the consumer rate limit. `None` for the kinds without separate
    /// producers and consumers.
    pub read_amplification: Option<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BenchmarkClientOptions {
    pub warmup_time_us: u64,
    pub high_level_api: bool,
    pub reuse_streams: bool,
}

/// TCP client flags. Both are `false` for the other transports, which have no such flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BenchmarkTcpOptions {
    pub nodelay: bool,
    pub tls: bool,
}

/// Completion policy of messages or consumer offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkDurability {
    Replicated,
    Persisted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMaxTopicSize {
    /// The server default of the version under test.
    ServerDefault,
    Unlimited,
    Bytes(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMessageExpiry {
    /// The server default of the version under test.
    ServerDefault,
    NeverExpire,
    AfterUs(u64),
}

impl BenchmarkOptions {
    /// The options for a report written before the bench recorded them: the bench defaults from the
    /// day the options arrived. Frozen on purpose, so a later change to a bench default doesn't
    /// move old runs to another workload.
    pub const fn legacy(kind: BenchmarkKind) -> Self {
        let read_amplification = match kind {
            BenchmarkKind::PinnedProducerAndConsumer
            | BenchmarkKind::BalancedProducerAndConsumerGroup => Some(LEGACY_READ_AMPLIFICATION),
            _ => None,
        };
        let consumer = match kind {
            BenchmarkKind::PinnedProducer | BenchmarkKind::BalancedProducer => None,
            _ => Some(BenchmarkConsumerOptions {
                consumer_offset_durability: BenchmarkDurability::Replicated,
                max_wait_us: LEGACY_MAX_WAIT_US,
                min_count: LEGACY_MIN_COUNT,
                max_bytes: None,
                read_amplification,
            }),
        };
        Self {
            topic: BenchmarkTopicOptions {
                durability: BenchmarkDurability::Replicated,
                messages_required_to_save: None,
                max_topic_size: BenchmarkMaxTopicSize::Unlimited,
                message_expiry: BenchmarkMessageExpiry::NeverExpire,
            },
            consumer,
            client: BenchmarkClientOptions {
                warmup_time_us: 0,
                high_level_api: false,
                reuse_streams: false,
            },
            tcp: BenchmarkTcpOptions {
                nodelay: false,
                tls: false,
            },
        }
    }
}
