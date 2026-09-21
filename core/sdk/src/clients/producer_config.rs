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

/// What a background send does when the dispatcher's byte budget,
/// [`BackgroundConfig::max_buffer_size`], is exhausted.
///
/// Set it through [`BackgroundConfig::failure_mode`]. These modes govern waiting for buffer capacity,
/// not retries of a server write. A single batch larger than the whole budget fails with
/// [`IggyError::BackgroundSendBufferOverflow`] under all of them.
///
/// [`IggyError::BackgroundSendBufferOverflow`]: iggy_common::IggyError::BackgroundSendBufferOverflow
#[derive(Debug, Clone)]
pub enum BackpressureMode {
    /// Waits until enough byte-budget capacity is released (default).
    ///
    /// This wait has no timeout. It can last indefinitely if queued or in-flight writes do not
    /// complete and release their permits.
    Block,
    /// Waits for the given duration, then fails the send with
    /// [`IggyError::BackgroundSendTimeout`](iggy_common::IggyError::BackgroundSendTimeout).
    BlockWithTimeout(IggyDuration),
    /// Gives up at once with
    /// [`IggyError::BackgroundSendBufferOverflow`](iggy_common::IggyError::BackgroundSendBufferOverflow),
    /// leaving the batch unqueued.
    FailImmediately,
}

/// Configuration for a producer that sends messages in the background.
///
/// A background producer passes every non-empty send to a [`ProducerDispatcher`]. The dispatcher
/// returns once the batch is queued, and one of its worker [`Shard`]s writes the batch later. This type controls
/// how many workers exist, which worker receives a batch, when workers flush, how many bytes may be
/// queued or in flight, and where write failures are reported.
///
/// # Defaults
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`num_shards`](Self::num_shards) | 1 | how many worker queues exist |
/// | [`sharding`](Self::sharding) | [`OrderedSharding`] | which worker a batch is queued on |
/// | [`batch_size`](Self::batch_size) | 1 MiB | flush once a worker holds this many bytes |
/// | [`batch_length`](Self::batch_length) | 1000 | flush once a worker holds this many queued sends |
/// | [`linger_time`](Self::linger_time) | 1 ms | how long a worker holds a non-empty buffer before flushing it |
/// | [`max_buffer_size`](Self::max_buffer_size) | 32 MiB | bytes the whole producer may hold |
/// | [`failure_mode`](Self::failure_mode) | [`BackpressureMode::Block`] | what a send does when that budget is full |
/// | [`max_in_flight`](Self::max_in_flight) | 1 | requests being written at once |
/// | [`error_callback`](Self::error_callback) | [`LogErrorCallback`] | what happens to a failed write |
///
/// The default [`OrderedSharding`] strategy preserves dispatch order for each stream/topic pair by
/// routing that pair to one worker. That worker awaits each request, including its retries, before
/// starting the next. [`BalancedSharding`] can route consecutive batches for one destination to
/// different workers and therefore gives up that ordering. [`max_in_flight`](Self::max_in_flight)
/// only controls how many workers may write concurrently. It does not make a single worker process
/// more than one request at a time.
///
/// # Zero values
///
/// `0` disables the [`batch_size`](Self::batch_size) and
/// [`batch_length`](Self::batch_length) flush thresholds. A zero
/// [`linger_time`](Self::linger_time) flushes as soon as the worker picks up a send. A zero
/// [`max_buffer_size`](Self::max_buffer_size) is treated as unbounded. A zero
/// [`max_in_flight`](Self::max_in_flight) uses `Semaphore::MAX_PERMITS`, and a zero
/// [`num_shards`](Self::num_shards) is read as one worker.
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
/// // Throughput: four workers share one topic, flushing at 4 MiB, with a 256 MiB byte budget.
/// // Up to 4 requests can be in flight at the same time, one per worker.
/// let fast = BackgroundConfig::builder()
///     .num_shards(4)
///     .sharding(Box::new(BalancedSharding::default()))
///     .batch_size(4 * 1024 * 1024)
///     .max_buffer_size(IggyByteSize::from(256 * 1024 * 1024))
///     .max_in_flight(4)
///     .build();
///
/// // Latency: flush within 5 ms or after 50 queued sends. Do not wait for byte-budget capacity.
/// // The bounded per-worker channel can still make dispatch wait when its 256 slots are occupied.
/// let responsive = BackgroundConfig::builder()
///     .linger_time(IggyDuration::new(Duration::from_millis(5)))
///     .batch_length(50)
///     .failure_mode(BackpressureMode::FailImmediately)
///     .build();
/// ```
///
/// [`BalancedSharding`]: crate::clients::producer_sharding::BalancedSharding
/// [`ProducerDispatcher`]: crate::clients::producer_dispatcher::ProducerDispatcher
/// [`Shard`]: crate::clients::producer_sharding::Shard
#[derive(Debug, Builder)]
pub struct BackgroundConfig {
    /// Number of worker [`Shard`]s the dispatcher runs, each with a queue of its own.
    ///
    /// Every batch is routed to exactly one worker by [`sharding`](Self::sharding), and each worker
    /// buffers and writes independently.
    ///
    /// `0` is read as one worker.
    ///
    /// [`Shard`]: crate::clients::producer_sharding::Shard
    #[builder(default = 1)]
    pub num_shards: usize,
    /// Upper bound on how long a worker holds a non-empty buffer before flushing it.
    ///
    /// The window starts when a send enters an empty buffer, so an idle worker does not wake up.
    /// A worker flushes as soon as any of `linger_time`, [`batch_length`](Self::batch_length) or
    /// [`batch_size`](Self::batch_size) is reached. Lowering it reduces the time the write is delayed
    /// at the price of smaller writes. `0` flushes as soon as the worker picks up a send.
    ///
    /// Note that [`IggyDuration::from`] reads a plain number as **microseconds**, so the default of
    /// `1000` is 1 ms.
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
    /// Where a background write that failed ends up.
    ///
    /// The dispatcher runs one task that owns this callback. A worker invokes it with an [`ErrorCtx`]
    /// when its backend returns [`IggyError::ProducerSendFailed`]. Other error variants from a custom
    /// backend are logged by the worker without invoking this callback. The context contains the
    /// cause, destination, unconfirmed tail, and confirmations returned for earlier chunks.
    ///
    /// The default [`LogErrorCallback`] logs the failure and drops the messages with the context.
    /// Implement [`ErrorCallback`] with your own logic to keep them.
    ///
    /// [`ErrorCtx`]: crate::clients::producer_error_callback::ErrorCtx
    /// [`IggyError::ProducerSendFailed`]: iggy_common::IggyError::ProducerSendFailed
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
    /// Sends accumulate until their reported sizes reach or exceed this value. The threshold is
    /// checked after a send is added, so it is a flush trigger rather than a hard size ceiling.
    /// `0` disables the threshold
    /// and leaves [`batch_length`](Self::batch_length) and [`linger_time`](Self::linger_time) to
    /// trigger the flush.
    ///
    /// This is a per-worker flush trigger. It is separate from
    /// [`max_buffer_size`](Self::max_buffer_size), which caps the producer as a whole.
    #[builder(default = MIB)]
    pub batch_size: usize,
    /// Flush threshold in number of queued batches on one worker.
    ///
    /// Counts the queued sends a worker holds, not the individual messages inside them. A worker
    /// flushes after this many dispatches have been routed to it. `0` disables the threshold.
    ///
    #[builder(default = 1000)]
    pub batch_length: usize,
    /// What a send does once [`max_buffer_size`](Self::max_buffer_size) is exhausted.
    #[builder(default = BackpressureMode::Block)]
    pub failure_mode: BackpressureMode,
    /// Upper bound for the **bytes buffered or in flight** across *all* shards.
    /// Bytes remain charged until the corresponding write completes.
    /// `IggyByteSize::from(0)` means unlimited. A nonzero value greater than
    /// `Semaphore::MAX_PERMITS` makes [`ProducerDispatcher::new`] panic.
    ///
    /// [`ProducerDispatcher::new`]: crate::clients::producer_dispatcher::ProducerDispatcher::new
    #[builder(default = IggyByteSize::from(32 * MIB as u64))]
    pub max_buffer_size: IggyByteSize,
    /// Upper bound on the requests being written concurrently, shared by *all* workers.
    ///
    /// A worker takes one permit before each request and holds it until the write returns. This
    /// bounds write concurrency across the producer rather than per worker, and it does not bound
    /// queued bytes.
    ///
    /// Each worker still sends sequentially regardless of this value. Raising it only lets different
    /// workers write concurrently. Per-destination order is therefore governed by
    /// [`sharding`](Self::sharding): [`OrderedSharding`] keeps one destination on one sequential
    /// worker, while strategies that spread a destination across workers may reorder it.
    /// A nonzero value greater than `Semaphore::MAX_PERMITS` makes
    /// [`ProducerDispatcher::new`] panic.
    ///
    /// [`ProducerDispatcher::new`]: crate::clients::producer_dispatcher::ProducerDispatcher::new
    #[builder(default = 1)]
    pub max_in_flight: usize,
}

/// Configuration for a direct producer.
///
/// A direct producer writes from the calling task. [`send()`] splits a batch into requests of at
/// most [`batch_length`](Self::batch_length) messages, awaits them one after another and returns
/// their confirmations. Nothing is buffered between calls. Unlike background mode, there is no
/// queue to bound, no worker to route to, and nothing to flush on shutdown.
///
/// A send that fails part way through returns [`IggyError::ProducerSendFailed`], where `committed`
/// holds the confirmations returned for earlier requests and `failed` holds the unconfirmed tail.
/// See [`send()`] for what resending that tail means.
///
/// # Examples
///
/// ```rust
/// use iggy::prelude::*;
/// use std::time::Duration;
///
/// // One request per message, with no configured delay between sequential calls.
/// let low_latency = DirectConfig::builder()
///     .batch_length(1)
///     .linger_time(IggyDuration::from(0))
///     .build();
///
/// // Up to 500 messages per request, with a 200 ms minimum gap between sequential sends.
/// let paced = DirectConfig::builder()
///     .batch_length(500)
///     .linger_time(IggyDuration::new(Duration::from_millis(200)))
///     .build();
/// ```
///
/// [`send()`]: crate::clients::producer::IggyProducer::send
/// [`IggyError::ProducerSendFailed`]: iggy_common::IggyError::ProducerSendFailed
#[derive(Clone, Builder)]
pub struct DirectConfig {
    /// Maximum number of messages in one request.
    ///
    /// A send carrying more than this is split into consecutive requests of this size, each awaited
    /// before the next one starts. A batch of 2500 therefore becomes three requests at the default.
    ///
    /// `0` limits to 1,000,000 messages per request.
    #[builder(default = 1000)]
    pub batch_length: u32,
    /// Requested minimum gap between sequential direct sends.
    ///
    /// A send waits out whatever is left of this interval since the previous request completed
    /// successfully.
    /// Concurrent callers can wait against the same timestamp and then proceed together, so this is
    /// not a global rate limiter.
    /// When one call is split by [`batch_length`](Self::batch_length), the linger interval is applied
    /// before the call rather than between its chunks. The default of zero does not wait.
    #[builder(default = IggyDuration::from(0))]
    pub linger_time: IggyDuration,
}
