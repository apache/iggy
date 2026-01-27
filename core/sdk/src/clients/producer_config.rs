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
use crate::clients::MIB;
use crate::clients::producer_error_callback::{ErrorCallback, LogErrorCallback};
use crate::clients::producer_sharding::{OrderedSharding, Sharding};
use bon::Builder;
use iggy_common::{IggyByteSize, IggyDuration};
use std::sync::Arc;

/// Determines how the `send_messages` API should behave when problem is encountered
#[derive(Debug, Clone)]
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

// Configuration for the *background* (asynchronous) producer
/// # Examples
///
/// ```
/// use iggy::prelude::*;
/// use iggy_common::{IggyDuration, IggyByteSize};
///
/// // Use default config
/// let config = BackgroundConfig::builder()
///     .build();
///
/// // Set custom batch size and disable length limit
/// let config = BackgroundConfig::builder()
///     .batch_size(256 * 1024)         // 256 KiB
///     .batch_length(0)                // unlimited
///     .build();
///
/// // Configure low-latency flush
/// let config = BackgroundConfig::builder()
///     .linger_time(IggyDuration::from(200)) // 200ms
///     .build();
///
/// // Disable all limits (not recommended for production)
/// let config = BackgroundConfig::builder()
///     .batch_size(0)
///     .batch_length(0)
///     .max_buffer_size(IggyByteSize::from(0))
///     .max_in_flight(0)
///     .build();
/// ```
#[derive(Debug, Builder)]
pub struct BackgroundConfig {
    /// Number of shard-workers that run in parallel.
    ///
    /// With the default `OrderedSharding` strategy, messages to the same
    /// stream/topic are always routed to the same shard, preserving ordering.
    /// Increasing shards improves throughput only when sending to multiple streams/topics.
    ///
    /// With `BalancedSharding`, messages are distributed round-robin across all shards
    /// for maximum single-destination throughput, but ordering is **not** preserved.
    #[builder(default = 1)]
    pub num_shards: usize,
    /// How long a shard may wait before flushing an *incomplete* batch.
    ///
    /// Combines with `batch_size` / `batch_length`: whichever limit fires
    /// first triggers the flush.
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
    /// User-supplied asynchronous callback that will be executed whenever
    /// the producer encounters an error it cannot automatically recover from
    /// (e.g. network failure).
    #[builder(default = Arc::new(Box::new(LogErrorCallback)))]
    pub error_callback: Arc<Box<dyn ErrorCallback + Send + Sync>>,
    /// Strategy that maps a message to a shard.
    ///
    /// Default is `OrderedSharding` which routes all messages for the same
    /// stream/topic to the same shard, preserving message ordering.
    ///
    /// Use `BalancedSharding` for maximum throughput when ordering doesn't matter.
    #[builder(default = Box::new(OrderedSharding))]
    pub sharding: Box<dyn Sharding + Send + Sync>,
    /// Maximum **total size in bytes** of a batch.
    /// `0` ⇒ unlimited (size-based batching disabled).
    #[builder(default = MIB)]
    pub batch_size: usize,
    /// Maximum **number of messages** per batch.
    /// `0` ⇒ unlimited (length-based batching disabled).
    #[builder(default = 1000)]
    pub batch_length: usize,
    /// Action to apply when back-pressure limits are reached
    #[builder(default = BackpressureMode::Block)]
    pub failure_mode: BackpressureMode,
    /// Upper bound for the **bytes held in memory** across *all* shards.
    /// `IggyByteSize::from(0)` ⇒ unlimited.
    #[builder(default = IggyByteSize::from(32 * MIB as u64))]
    pub max_buffer_size: IggyByteSize,
    /// Maximum number of **in-flight requests** (batches being sent).
    ///
    /// **WARNING**: Using more than 1 may cause message reordering if retries occur.
    /// With max_in_flight > 1, a failed batch could be retried after later batches succeed.
    ///
    /// The default is `1` to preserve message ordering.
    /// `0` ⇒ unlimited (no ordering guarantee).
    #[builder(default = 1)]
    pub max_in_flight: usize,
}

/// Configuration for the *synchronous* (blocking) producer.
/// # Examples
///
/// ```rust
/// use iggy::prelude::*;
/// use iggy_common::IggyDuration;
///
/// // Send messages one-by-one (max latency, min memory per request)
/// let cfg = DirectConfig::builder()
///     .batch_length(1)
///     .linger_time(IggyDuration::from(0))
///     .build();
///
/// // Send in chunks of up to 500 messages,
/// // with a delay of at least 200 ms between consecutive sends.
/// let cfg = DirectConfig::builder()
///     .batch_length(500)
///     .linger_time(IggyDuration::from(200))
///     .build();
/// ```
#[derive(Clone, Builder)]
pub struct DirectConfig {
    /// Maximum number of messages to pack into **one** synchronous request.
    /// `0` ⇒ MAX_BATCH_LENTH().
    #[builder(default = 1000)]
    pub batch_length: u32,
    /// How long to wait for more messages before flushing the current set.
    #[builder(default = IggyDuration::from(0))]
    pub linger_time: IggyDuration,
}
