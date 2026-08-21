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

use crate::clients::MIB;
use crate::clients::producer_error_callback::{ErrorCallback, LogErrorCallback};
use crate::clients::producer_sharding::{OrderedSharding, Sharding};
use bon::Builder;
use iggy_common::{IggyByteSize, IggyDuration};
use std::sync::Arc;

/// What a background send does when the dispatcher's memory budget,
/// [`BackgroundConfig::max_buffer_size`], is exhausted.
///
/// Set it through [`BackgroundConfig::failure_mode`]. None of the three
/// modes retries, and a single batch larger than the whole budget fails with
/// [`IggyError::BackgroundSendBufferOverflow`] under all of them.
///
/// [`IggyError::BackgroundSendBufferOverflow`]: iggy_common::IggyError::BackgroundSendBufferOverflow
#[derive(Debug, Clone)]
pub enum BackpressureMode {
    /// Waits for as long as it takes for the budget to free up. The default.
    ///
    /// Paces the caller to the rate the workers write at without ever dropping a batch, at the cost
    /// of a send that waits indefinitely while the server refuses writes.
    Block,
    /// Waits for the given duration, then fails the send with
    /// [`IggyError::BackgroundSendTimeout`](iggy_common::IggyError::BackgroundSendTimeout).
    BlockWithTimeout(IggyDuration),
    /// Gives up at once with
    /// [`IggyError::BackgroundSendBufferOverflow`](iggy_common::IggyError::BackgroundSendBufferOverflow),
    /// leaving the batch unqueued.
    FailImmediately,
}

/// Configuration for a producer that sends messages in background.
///
/// A background producer passes every send to a [`ProducerDispatcher`], which returns once the batch
/// is queued and writes it later on one of its worker [`Shard`]s. This type configures that
/// machinery. It defines on how many workers the load should be distributed, on which `Shard` a batch lands on,
/// when a worker stops buffering and writes, how many bytes may be queued before a send has to wait, and where a write
/// that failed is reported.
///
/// # Defaults
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`num_shards`](Self::num_shards) | 1 | how many workers write in concurrently |
/// | [`sharding`](Self::sharding) | [`OrderedSharding`] | which worker a batch is queued on |
/// | [`batch_size`](Self::batch_size) | 1 MiB | flush once a worker holds this many bytes |
/// | [`batch_length`](Self::batch_length) | 1000 | flush once a worker holds this many queued sends |
/// | [`linger_time`](Self::linger_time) | 1 ms | flush this long after the previous flush regardless |
/// | [`max_buffer_size`](Self::max_buffer_size) | 32 MiB | bytes the whole producer may hold |
/// | [`failure_mode`](Self::failure_mode) | [`BackpressureMode::Block`] | what a send does when that budget is full |
/// | [`max_in_flight`](Self::max_in_flight) | 1 | requests being written at once |
/// | [`error_callback`](Self::error_callback) | [`LogErrorCallback`] | what happens to a failed write |
///
/// The defaults preserve message ordering. A single request in flight, and a strategy that binds a
/// stream/topic pair to one specific worker. Raising [`num_shards`](Self::num_shards) together with
/// [`BalancedSharding`], or raising [`max_in_flight`](Self::max_in_flight), trades ordering for
/// throughput.
///
/// # Zero values
///
/// `0` disables the [`batch_size`](Self::batch_size), [`batch_length`](Self::batch_length) flush
/// thresholds. Choosing `0` for [`max_buffer_size`](Self::max_buffer_size) and [`max_in_flight`](Self::max_in_flight)
/// sets them both to `Semaphore::MAX_PERMITS`, while [`num_shards`](Self::num_shards) reads `0` as one worker.
///
/// # Examples
///
/// ```
/// use iggy::clients::producer_config::BackpressureMode;
/// use iggy::prelude::*;
/// use std::time::Duration;
///
/// // Ordered and bounded, as described above.
/// let ordered = BackgroundConfig::builder().build();
///
/// // Throughput: four workers share one topic, flushing at 4 MiB, with 256 MiB of queue.
/// // Up to 8 requests can be in-flight at the same time across `Shard`s.
/// let fast = BackgroundConfig::builder()
///     .num_shards(4)
///     .sharding(Box::new(BalancedSharding::default()))
///     .batch_size(4 * 1024 * 1024)
///     .max_buffer_size(IggyByteSize::from(256 * 1024 * 1024))
///     .max_in_flight(8)
///     .build();
///
/// // Latency: flush every 5 ms or every 50 queued sends, and never make a send wait for the
/// // queue. Batches that do not fit are dropped with `BackgroundSendBufferOverflow`.
/// let responsive = BackgroundConfig::builder()
///     .linger_time(IggyDuration::new(Duration::from_millis(5)))
///     .batch_length(50)
///     .failure_mode(BackpressureMode::FailImmediately)
///     .build();
/// ```
///
/// [`background()`]: crate::clients::producer_builder::IggyProducerBuilder::background
/// [`BalancedSharding`]: crate::clients::producer_sharding::BalancedSharding
/// [`ProducerDispatcher`]: crate::clients::producer_dispatcher::ProducerDispatcher
/// [`Shard`]: crate::clients::producer_sharding::Shard
#[derive(Debug, Builder)]
pub struct BackgroundConfig {
    /// Number of worker [`Shard`]s the dispatcher runs, each with a queue of its own.
    ///
    /// Every batch is routed to exactly one of them by [`sharding`](Self::sharding), and each worker
    /// buffers and writes independently.
    ///
    /// `0` is read as one worker.
    ///
    /// [`Shard`]: crate::clients::producer_sharding::Shard
    #[builder(default = 1)]
    pub num_shards: usize,
    /// Upper bound on how long a worker holds an incomplete batch before writing it.
    ///
    /// The deadline runs from the previous flush.
    /// A worker flushes as soon as any of `linger_time`, [`batch_length`](Self::batch_length) or
    /// [`batch_size`](Self::batch_size) is reached. Lowering it reduces the time the write is delayed
    /// at the price of smaller writes.
    ///
    /// Note that [`IggyDuration::from`] reads a plain number as **microseconds**, so the default of
    /// `1000` is 1 ms.
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
    /// Where a background write that failed ends up.
    ///
    /// The dispatcher runs one task owning this callback and invokes it once per failed request with
    /// an [`ErrorCtx`]. The cause, the destination, the messages that were not written, and the
    /// confirmations of the chunks that were.
    ///
    /// The default [`LogErrorCallback`] logs the failure and drops the messages with the context.
    /// Implement [`ErrorCallback`] with your own logic to keep them.
    ///
    /// [`ErrorCtx`]: crate::clients::producer_error_callback::ErrorCtx
    /// [`send_retries()`]: crate::clients::producer_builder::IggyProducerBuilder::send_retries
    #[builder(default = Arc::new(Box::new(LogErrorCallback)))]
    pub error_callback: Arc<Box<dyn ErrorCallback + Send + Sync>>,
    /// Picks the worker a batch is queued on, out of [`num_shards`](Self::num_shards).
    ///
    /// The default [`OrderedSharding`] hashes stream and topic, so every batch for one topic queues
    /// on one worker and keeps the order it was dispatched in. [`BalancedSharding`] hands them out
    /// round-robin, which lets a single topic occupy all workers but gives up that order. Implement
    /// [`Sharding`] to build your own logic for picking a `Shard`.
    ///
    /// [`BalancedSharding`]: crate::clients::producer_sharding::BalancedSharding
    #[builder(default = Box::new(OrderedSharding))]
    pub sharding: Box<dyn Sharding + Send + Sync>,
    /// Flush threshold in bytes buffered on one worker.
    ///
    /// The ceiling in number of bytes for the batch. Send messages accumulate into batches
    /// and get flushed once the sum reaches this value. `0` disables the threshold
    /// and leaves [`batch_length`](Self::batch_length) and [`linger_time`](Self::linger_time) to
    /// trigger the flush.
    ///
    /// This is a per-worker flush trigger.
    /// Note, it is unrelated to [`max_buffer_size`](Self::max_buffer_size), which caps the producer as a whole.
    #[builder(default = MIB)]
    pub batch_size: usize,
    /// Flush threshold in number of queued batches on one worker.
    ///
    /// Counts the batches a worker holds, not the individual messages inside them. A worker
    /// flushes after this many calls to [`send()`] have been routed to it. `0` disables the
    /// threshold.
    ///
    /// [`send()`]: crate::clients::producer::IggyProducer::send
    #[builder(default = 1000)]
    pub batch_length: usize,
    /// What a send does once [`max_buffer_size`](Self::max_buffer_size) is exhausted.
    #[builder(default = BackpressureMode::Block)]
    pub failure_mode: BackpressureMode,
    /// Upper bound for the **bytes buffered or in flight** across *all* shards.
    /// Bytes remain charged until the corresponding write completes.
    /// `IggyByteSize::from(0)` ⇒ unlimited.
    #[builder(default = IggyByteSize::from(32 * MIB as u64))]
    pub max_buffer_size: IggyByteSize,
    /// Upper bound on the requests being written concurrently, shared by *all* workers.
    ///
    /// A worker takes one of these permits before each request and holds it until the write returns,
    /// so this bounds write concurrency across the producer rather than per worker, and it is not
    /// what bounds queued bytes.
    ///
    /// Above `1`, a request that is retried can land after a later one that succeeded straight away,
    /// so only the default of `1` preserves the order of two batches on the same worker.
    #[builder(default = 1)]
    pub max_in_flight: usize,
}

/// Configuration for a direct producer.
///
/// A direct producer writes from the calling task. [`send()`] splits the batch into requests of at
/// most [`batch_length`](Self::batch_length) messages, awaits them one after another and returns
/// their confirmations. Nothing is buffered between calls. Compared to background mode so there
/// is no queue to bound, no worker to route to and nothing to flush on shutdown.
///
/// A send that fails part way through returns [`IggyError::ProducerSendFailed`], where `committed`
/// holds the confirmations of the requests that went through and `failed` the tail that did not, so
/// resending `failed` completes the send.
///
/// # Examples
///
/// ```rust
/// use iggy::prelude::*;
/// use std::time::Duration;
///
/// // One request per message, sent as fast as the caller offers them.
/// let low_latency = DirectConfig::builder()
///     .batch_length(1)
///     .linger_time(IggyDuration::from(0))
///     .build();
///
/// // Up to 500 messages per request, and at least 200 ms between two sends.
/// let paced = DirectConfig::builder()
///     .batch_length(500)
///     .linger_time(IggyDuration::new(Duration::from_millis(200)))
///     .build();
/// ```
///
/// [`background()`]: crate::clients::producer_builder::IggyProducerBuilder::background
/// [`direct()`]: crate::clients::producer_builder::IggyProducerBuilder::direct
/// [`send()`]: crate::clients::producer::IggyProducer::send
/// [`IggyError::ProducerSendFailed`]: iggy_common::IggyError::ProducerSendFailed
#[derive(Clone, Builder)]
pub struct DirectConfig {
    /// Maximum number of messages packed into one request.
    ///
    /// A send carrying more than this is split into consecutive requests of this size, each awaited
    /// before the next one starts, so a batch of 2500 becomes three requests at the default. A
    /// failure therefore leaves the requests before it written.
    ///
    /// `0` means the client's ceiling of 1,000,000 messages per request.
    #[builder(default = 1000)]
    pub batch_length: u32,
    /// Smallest gap between two requests of this producer.
    ///
    /// A send waits out whatever is left of this interval since the last request before issuing its
    /// first one, which paces a producer sending in a tight loop. It does not space out the requests
    /// within one send. The default of zero does not wait at all.
    ///
    /// Note that [`IggyDuration::from`] reads a plain number as **microseconds**.
    #[builder(default = IggyDuration::from(0))]
    pub linger_time: IggyDuration,
}
