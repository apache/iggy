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

use crate::clients::producer::ProducerCoreBackend;
use crate::clients::producer_config::{BackgroundConfig, BackpressureMode};
use crate::clients::producer_error_callback::{ErrorCallback, ErrorCtx};
use crate::clients::producer_sharding::{Shard, ShardMessage, ShardMessageWithPermit};
use futures::FutureExt;
use iggy_common::{Identifier, IggyByteSize, IggyError, IggyMessage, Partitioning, Sizeable};
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinHandle;

/// The background machinery of an [`IggyProducer`](crate::clients::producer::IggyProducer), built from a
/// [`BackgroundConfig`] when the producer is configured with
/// [`IggyProducerBuilder::background`](crate::clients::producer_builder::IggyProducerBuilder::background).
///
/// The dispatcher owns the background workers responsible for writing messages (the [`Shard`]s)
/// and coordinates two limits, both configured on [`BackgroundConfig`]:
/// [`max_buffer_size`](BackgroundConfig::max_buffer_size), the upper bound on the message bytes it
/// holds at once, and [`max_in_flight`](BackgroundConfig::max_in_flight), the upper bound on the
/// requests being written at once across all of its workers.
///
/// A background send dispatches messages to a [`Shard`] through a channel. The batch is queued and
/// the write happens later on one of the shard workers owned by this type. Write failures therefore
/// surface on a shard after the queueing caller has returned. The shard forwards each failure to the
/// dispatcher's error task, which invokes [`BackgroundConfig::error_callback`]. The default
/// [`LogErrorCallback`] logs the failure, and applications can provide another [`ErrorCallback`]
/// implementation.
///
/// Which worker a batch lands on, and what that means for ordering, is described in
/// [`IggyProducer`](crate::clients::producer::IggyProducer).
///
/// # Examples
///
/// Configuring background sending through the producer builder:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let producer = client
///     .producer("my-stream", "my-topic")?
///     .background(BackgroundConfig::builder().num_shards(4).build())
///     .build();
/// producer.init().await?;
///
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
/// producer.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// You can also implement a backend and wrap the dispatcher around it. This example prints instead
/// of sending anything to a server.
///
/// ```rust,no_run
/// use iggy::clients::producer::ProducerCoreBackend;
/// use iggy::clients::producer_dispatcher::ProducerDispatcher;
/// use iggy::prelude::*;
/// use std::str::FromStr;
/// use std::sync::Arc;
///
/// #[derive(Debug)]
/// struct CountingBackend;
///
/// impl ProducerCoreBackend for CountingBackend {
///     async fn send_internal(
///         &self,
///         stream: &Identifier,
///         topic: &Identifier,
///         messages: Vec<IggyMessage>,
///         _partitioning: Option<Arc<Partitioning>>,
///     ) -> Result<SendMessagesResponse, IggyError> {
///         println!("{} messages to {stream}/{topic}", messages.len());
///         Ok(SendMessagesResponse { confirmations: Vec::new() })
///     }
/// }
///
/// # async fn example() -> Result<(), IggyError> {
/// let dispatcher = ProducerDispatcher::new(
///     Arc::new(CountingBackend),
///     BackgroundConfig::builder().num_shards(2).build(),
/// );
///
/// let stream = Arc::new(Identifier::named("my-stream")?);
/// let topic = Arc::new(Identifier::named("my-topic")?);
///
/// // Returns once the batch is queued, not once it is written.
/// dispatcher
///     .dispatch(vec![IggyMessage::from_str("hello")?], stream, topic, None)
///     .await?;
///
/// // Writes what is still queued. Dropping the dispatcher instead may lose buffered messages.
/// dispatcher.shutdown().await;
/// # Ok(())
/// # }
/// ```
///
/// # Write constraints
///
/// ## Memory budget
///
/// [`dispatch()`](Self::dispatch) charges a batch against a budget of
/// [`BackgroundConfig::max_buffer_size`] bytes before queueing it. The charge is released when
/// [`ProducerCoreBackend::send_internal`] returns, so the budget covers queued sends and requests
/// whose result is still pending. What gets charged is the size [`ShardMessage`] reports, which
/// counts stream and topic identifiers alongside the messages rather than payloads alone.
///
/// [`BackgroundConfig::failure_mode`] decides how the byte budget backpressures the caller. The
/// bounded channel feeding each shard is a separate source of backpressure and can also make a
/// dispatch wait. The second configured limit, [`BackgroundConfig::max_in_flight`], is shared by the
/// workers rather than the callers. A worker takes one of its permits for the batch it is about to
/// write, so it bounds concurrent writes across all workers, not queued bytes.
///
/// ## In-flight limit
///
/// [`BackgroundConfig::max_in_flight`] limits how many workers may write concurrently. The permit
/// pool is shared by every worker and defaults to one. A shard itself remains sequential for every
/// value: it awaits a request and all of its retries before starting its next request. Raising the
/// limit therefore affects concurrency between shards. It can expose reordering only when the
/// configured sharding strategy sends one ordered destination to different shards.
///
/// # Shutdown
///
/// [`shutdown()`](Self::shutdown) broadcasts a stop signal, drains every shard channel, flushes each
/// remaining buffer, and waits for the shard and error tasks. Dropping the dispatcher provides no
/// such completion guarantee and can lose batches that a shard has buffered but not written.
///
/// [`ErrorCallback`]: crate::clients::producer_error_callback::ErrorCallback
/// [`LogErrorCallback`]: crate::clients::producer_error_callback::LogErrorCallback
pub struct ProducerDispatcher {
    shards: Vec<Shard>,
    config: Arc<BackgroundConfig>,
    closed: AtomicBool,
    bytes_permit: Arc<Semaphore>,
    stop_tx: broadcast::Sender<()>,
    join_handle: JoinHandle<()>,
}

impl ProducerDispatcher {
    /// Spawns the [`Shard`] workers that write messages to the server.
    ///
    /// The dispatcher owns the shards over which writes are distributed.
    /// [`BackgroundConfig::sharding`] decides which of the
    /// [`BackgroundConfig::num_shards`] workers receives each batch.
    ///
    /// The dispatcher also starts an error task. When a shard receives
    /// [`IggyError::ProducerSendFailed`] from its backend, it sends the context to this task, which
    /// invokes the [`ErrorCallback`] configured through [`BackgroundConfig::error_callback`]. A
    /// custom backend error that is not `ProducerSendFailed` is logged by the shard and does not
    /// invoke the callback.
    ///
    /// Two semaphores enforce [`BackgroundConfig::max_buffer_size`] and
    /// [`BackgroundConfig::max_in_flight`]. Both are shared by every shard and therefore apply to
    /// the entire producer rather than per worker. They
    /// count different things at different points of a batch's life: `max_buffer_size` is charged in
    /// bytes by [`dispatch()`](Self::dispatch) before the batch is queued and released once it has
    /// completed, so it bounds queued and in-flight bytes together, while `max_in_flight` is taken
    /// as one permit by a worker that is about to write and released when that request returns, so
    /// it bounds concurrent requests and says nothing about their size. A batch therefore has to pass
    /// the byte budget to enter a queue, and to take a request slot to leave it.
    ///
    /// # Panics
    ///
    /// Panics when a nonzero [`BackgroundConfig::max_buffer_size`] or
    /// [`BackgroundConfig::max_in_flight`] exceeds `Semaphore::MAX_PERMITS`.
    ///
    /// # Examples
    ///
    /// Four workers allowed to write in parallel, fed round-robin, with a 64 MiB budget for queued
    /// and in-flight bytes:
    ///
    /// ```rust,no_run
    /// # use iggy::clients::producer::ProducerCoreBackend;
    /// use iggy::clients::producer_dispatcher::ProducerDispatcher;
    /// use iggy::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # #[derive(Debug)]
    /// # struct Backend;
    /// # impl ProducerCoreBackend for Backend {
    /// #     async fn send_internal(
    /// #         &self,
    /// #         _stream: &Identifier,
    /// #         _topic: &Identifier,
    /// #         _messages: Vec<IggyMessage>,
    /// #         _partitioning: Option<Arc<Partitioning>>,
    /// #     ) -> Result<SendMessagesResponse, IggyError> {
    /// #         Ok(SendMessagesResponse { confirmations: Vec::new() })
    /// #     }
    /// # }
    /// # fn example(backend: Arc<Backend>) {
    /// let dispatcher = ProducerDispatcher::new(
    ///     backend,
    ///     BackgroundConfig::builder()
    ///         .num_shards(4)
    ///         .max_in_flight(4)
    ///         // Ordering is given up for throughput: a batch can land on any of the four workers.
    ///         .sharding(Box::new(BalancedSharding::default()))
    ///         .max_buffer_size(IggyByteSize::from(64 * 1024 * 1024))
    ///         .build(),
    /// );
    /// # }
    /// ```
    ///
    /// `num_shards(0)` is read as one worker. A zero byte budget is treated as unbounded and a zero
    /// in-flight limit uses the semaphore maximum. This dispatcher therefore never refuses a batch
    /// for lack of byte-budget capacity, although its bounded shard channel can still make dispatch
    /// wait:
    ///
    /// ```rust,no_run
    /// # use iggy::clients::producer::ProducerCoreBackend;
    /// use iggy::clients::producer_dispatcher::ProducerDispatcher;
    /// use iggy::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # #[derive(Debug)]
    /// # struct Backend;
    /// # impl ProducerCoreBackend for Backend {
    /// #     async fn send_internal(
    /// #         &self,
    /// #         _stream: &Identifier,
    /// #         _topic: &Identifier,
    /// #         _messages: Vec<IggyMessage>,
    /// #         _partitioning: Option<Arc<Partitioning>>,
    /// #     ) -> Result<SendMessagesResponse, IggyError> {
    /// #         Ok(SendMessagesResponse { confirmations: Vec::new() })
    /// #     }
    /// # }
    /// # fn example(backend: Arc<Backend>) {
    /// let dispatcher = ProducerDispatcher::new(
    ///     backend,
    ///     BackgroundConfig::builder()
    ///         .num_shards(0)
    ///         .max_buffer_size(IggyByteSize::from(0))
    ///         .max_in_flight(0)
    ///         .build(),
    /// );
    /// # }
    /// ```
    ///
    /// [`ErrorCallback`]: crate::clients::producer_error_callback::ErrorCallback
    pub fn new(core: Arc<impl ProducerCoreBackend>, config: BackgroundConfig) -> Self {
        let num_shards = if config.num_shards == 0 {
            1
        } else {
            config.num_shards
        };
        let mut shards = Vec::with_capacity(num_shards);
        let config = Arc::new(config);

        let (err_tx, err_rx) = flume::unbounded::<ErrorCtx>();
        let err_callback = config.error_callback.clone();
        let (stop_tx, _) = broadcast::channel::<()>(1);

        let handle = tokio::spawn(async move {
            while let Ok(ctx) = err_rx.recv_async().await {
                if let Err(panic) = call_error_callback(&**err_callback, ctx).await {
                    tracing::error!("error_callback panicked: {:?}", panic);
                }
            }
            tracing::debug!("error-callback worker finished");
        });

        let max_buffer_size = config.max_buffer_size.as_bytes_u64();
        assert!(
            max_buffer_size == 0 || max_buffer_size <= Semaphore::MAX_PERMITS as u64,
            "max_buffer_size cannot exceed {} bytes on this platform",
            Semaphore::MAX_PERMITS
        );
        let bytes_permit = Arc::new(Semaphore::new(max_buffer_size as usize));

        let slots_permit = Arc::new(Semaphore::new(if config.max_in_flight == 0 {
            Semaphore::MAX_PERMITS
        } else {
            config.max_in_flight
        }));

        for _ in 0..num_shards {
            let stop_rx = stop_tx.subscribe();
            shards.push(Shard::new(
                core.clone(),
                config.clone(),
                slots_permit.clone(),
                err_tx.clone(),
                stop_rx,
            ));
        }

        Self {
            shards,
            config,
            closed: AtomicBool::new(false),
            bytes_permit,
            stop_tx,
            join_handle: handle,
        }
    }

    /// Queues a batch on one of the worker [`Shard`]s and returns without waiting for it to be written.
    ///
    /// The batch is charged against the [`BackgroundConfig::max_buffer_size`] semaphore before it is
    /// queued. Its permit travels with it, so those bytes stay charged until
    /// [`ProducerCoreBackend::send_internal`] returns. A batch larger than the entire budget can
    /// never be charged and fails with
    /// [`IggyError::BackgroundSendBufferOverflow`].
    ///
    /// When the budget is exhausted, [`BackgroundConfig::failure_mode`] decides what happens to the
    /// caller. [`BackpressureMode::FailImmediately`] gives up with
    /// [`IggyError::BackgroundSendBufferOverflow`], [`BackpressureMode::Block`] waits until enough
    /// capacity is released, and [`BackpressureMode::BlockWithTimeout`] waits for its duration before
    /// failing with [`IggyError::BackgroundSendTimeout`]. These modes do not control retries of the
    /// server write.
    ///
    /// [`BackgroundConfig::sharding`] then picks the worker, and the batch is handed to that worker's
    /// queue. The queue holds 256 entries, so a full queue can make the caller wait independently of
    /// the byte-budget failure mode.
    ///
    /// # Errors
    ///
    /// [`IggyError::ProducerClosed`] once [`shutdown()`](Self::shutdown) has begun, and
    /// [`IggyError::BackgroundSendError`] if the picked worker is already gone, which leaves the
    /// batch unqueued and unsent in both cases. The budget can additionally fail the call with
    /// [`IggyError::BackgroundSendBufferOverflow`] or [`IggyError::BackgroundSendTimeout`] as
    /// described above. `BackgroundSendBufferOverflow` is also returned when the batch's reported
    /// size does not fit the semaphore API's `u32` permit count, including when the configured byte
    /// budget is unbounded.
    ///
    /// # Panics
    ///
    /// Panics if the configured [`Sharding`](crate::clients::producer_sharding::Sharding)
    /// implementation returns an index outside the dispatcher's shard list.
    ///
    /// # Examples
    ///
    /// Dispatching with a strategy for partitioning.
    ///
    /// ```rust,no_run
    /// # use iggy::clients::producer::ProducerCoreBackend;
    /// use iggy::clients::producer_dispatcher::ProducerDispatcher;
    /// use iggy::prelude::*;
    /// use std::str::FromStr;
    /// use std::sync::Arc;
    ///
    /// # #[derive(Debug)]
    /// # struct Backend;
    /// # impl ProducerCoreBackend for Backend {
    /// #     async fn send_internal(
    /// #         &self,
    /// #         _stream: &Identifier,
    /// #         _topic: &Identifier,
    /// #         _messages: Vec<IggyMessage>,
    /// #         _partitioning: Option<Arc<Partitioning>>,
    /// #     ) -> Result<SendMessagesResponse, IggyError> {
    /// #         Ok(SendMessagesResponse { confirmations: Vec::new() })
    /// #     }
    /// # }
    /// # async fn example(dispatcher: ProducerDispatcher) -> Result<(), IggyError> {
    /// let stream = Arc::new(Identifier::named("orders")?);
    /// let topic = Arc::new(Identifier::named("created")?);
    /// let partitioning = Arc::new(Partitioning::messages_key_str("order-42")?);
    ///
    /// dispatcher
    ///     .dispatch(
    ///         vec![IggyMessage::from_str("order created")?],
    ///         stream.clone(),
    ///         topic.clone(),
    ///         Some(partitioning),
    ///     )
    ///     .await?;
    ///
    /// // Dispatch only guarantees that the first batch was queued. A worker may already have
    /// // started or completed its write.
    /// dispatcher
    ///     .dispatch(vec![IggyMessage::from_str("order updated")?], stream, topic, None)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Fail immediately if the `max_buffer_size` is exceeded.
    ///
    /// ```rust,no_run
    /// # use iggy::clients::producer::ProducerCoreBackend;
    /// use iggy::clients::producer_config::BackpressureMode;
    /// use iggy::clients::producer_dispatcher::ProducerDispatcher;
    /// use iggy::prelude::*;
    /// use std::str::FromStr;
    /// use std::sync::Arc;
    ///
    /// # #[derive(Debug)]
    /// # struct Backend;
    /// # impl ProducerCoreBackend for Backend {
    /// #     async fn send_internal(
    /// #         &self,
    /// #         _stream: &Identifier,
    /// #         _topic: &Identifier,
    /// #         _messages: Vec<IggyMessage>,
    /// #         _partitioning: Option<Arc<Partitioning>>,
    /// #     ) -> Result<SendMessagesResponse, IggyError> {
    /// #         Ok(SendMessagesResponse { confirmations: Vec::new() })
    /// #     }
    /// # }
    /// # async fn example(backend: Arc<Backend>) -> Result<(), IggyError> {
    /// let dispatcher = ProducerDispatcher::new(
    ///     backend,
    ///     BackgroundConfig::builder()
    ///         .max_buffer_size(IggyByteSize::from(1024 * 1024))
    ///         .failure_mode(BackpressureMode::FailImmediately)
    ///         .build(),
    /// );
    ///
    /// let messages = vec![IggyMessage::from_str("hello")?];
    /// let stream = Arc::new(Identifier::named("orders")?);
    /// let topic = Arc::new(Identifier::named("created")?);
    ///
    /// match dispatcher.dispatch(messages, stream, topic, None).await {
    ///     Ok(()) => println!("queued"),
    ///     // The workers are behind, or this single batch is larger than the whole budget.
    ///     Err(IggyError::BackgroundSendBufferOverflow) => println!("dropped, budget is full"),
    ///     Err(IggyError::ProducerClosed) => println!("dropped, dispatcher is shutting down"),
    ///     Err(error) => return Err(error),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn dispatch(
        &self,
        messages: Vec<IggyMessage>,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(IggyError::ProducerClosed);
        }

        let shard_message = ShardMessage {
            messages,
            stream,
            topic,
            partitioning,
        };
        let batch_bytes = shard_message.get_size_bytes();

        if self.config.max_buffer_size != 0 && batch_bytes > self.config.max_buffer_size {
            return Err(IggyError::BackgroundSendBufferOverflow);
        }

        let permit_count = Self::permit_count(batch_bytes)?;
        let bytes_permit = if self.config.max_buffer_size == 0 {
            None
        } else {
            let permit = match self
                .bytes_permit
                .clone()
                .try_acquire_many_owned(permit_count)
            {
                Ok(permit) => permit,
                Err(_) => match &self.config.failure_mode {
                    BackpressureMode::FailImmediately => {
                        return Err(IggyError::BackgroundSendBufferOverflow);
                    }
                    BackpressureMode::Block => self
                        .bytes_permit
                        .clone()
                        .acquire_many_owned(permit_count)
                        .await
                        .map_err(|_| IggyError::BackgroundSendError)?,
                    BackpressureMode::BlockWithTimeout(timeout_duration) => {
                        match tokio::time::timeout(
                            timeout_duration.get_duration(),
                            self.bytes_permit.clone().acquire_many_owned(permit_count),
                        )
                        .await
                        {
                            Ok(Ok(permit)) => permit,
                            Ok(Err(_)) => return Err(IggyError::BackgroundSendError),
                            Err(_) => return Err(IggyError::BackgroundSendTimeout),
                        }
                    }
                },
            };
            Some(permit)
        };

        let shard_ix = self.config.sharding.pick_shard(
            self.shards.len(),
            &shard_message.messages,
            &shard_message.stream,
            &shard_message.topic,
        );

        debug_assert!(shard_ix < self.shards.len());

        let shard = &self.shards[shard_ix];

        shard
            .send(ShardMessageWithPermit::new(shard_message, bytes_permit))
            .await
    }

    fn permit_count(batch_size: IggyByteSize) -> Result<u32, IggyError> {
        u32::try_from(batch_size.as_bytes_u64())
            .map_err(|_| IggyError::BackgroundSendBufferOverflow)
    }

    /// Flushes each shard's buffer and stops its worker. Dropping the
    /// dispatcher instead of calling this silently discards any buffered,
    /// not-yet-sent messages.
    pub async fn shutdown(mut self) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }

        let _ = self.stop_tx.send(());

        for shard in self.shards.drain(..) {
            if let Err(e) = shard.handle.await {
                tracing::error!("shard panicked: {e:?}");
            }
        }

        // After shards are closed await the error callback task,
        // that might drain queued errors from the final flush.
        if let Err(e) = self.join_handle.await {
            tracing::error!("error-worker panicked: {e:?}");
        }
    }
}

/// Catches a panic in `call()` itself as well as in the future it returns, so one misbehaving
/// callback cannot end the error task and silently drop every later failure.
async fn call_error_callback(
    callback: &(dyn ErrorCallback + Send + Sync),
    ctx: ErrorCtx,
) -> Result<(), Box<dyn Any + Send>> {
    let future = std::panic::catch_unwind(AssertUnwindSafe(|| callback.call(ctx)))?;
    AssertUnwindSafe(future).catch_unwind().await
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::sleep;

    use crate::clients::producer::{MockProducerCoreBackend, no_confirmations};
    use crate::clients::producer_sharding::Sharding;

    use super::*;

    fn dummy_identifier() -> Arc<Identifier> {
        Arc::new(Identifier::numeric(1).unwrap())
    }

    fn dummy_message(size: usize) -> IggyMessage {
        IggyMessage::builder()
            .payload(Bytes::from(vec![0u8; size]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_dispatch_successful() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let msg = dummy_message(5);
        let config = BackgroundConfig::builder()
            .max_buffer_size(100.into())
            .max_in_flight(10)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        sleep(Duration::from_millis(100)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dispatch_succeeds_with_unlimited_buffer_and_in_flight_requests() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let config = BackgroundConfig::builder()
            .max_buffer_size(0.into())
            .max_in_flight(0)
            .batch_length(1)
            .build();
        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        assert_eq!(dispatcher.bytes_permit.available_permits(), 0);
        dispatcher
            .dispatch(
                vec![dummy_message(5)],
                dummy_identifier(),
                dummy_identifier(),
                None,
            )
            .await
            .unwrap();
        dispatcher.shutdown().await;
    }

    #[cfg(target_pointer_width = "64")]
    #[tokio::test]
    async fn test_dispatcher_supports_buffer_budget_above_u32_max() {
        let mock = MockProducerCoreBackend::new();
        let budget_size = u32::MAX as u64 + 1;
        let config = BackgroundConfig::builder()
            .max_buffer_size(budget_size.into())
            .build();
        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        assert_eq!(
            dispatcher.bytes_permit.available_permits(),
            budget_size as usize
        );
        dispatcher.shutdown().await;
    }

    #[test]
    fn test_permit_count_rejects_batch_above_u32_max() {
        let result = ProducerDispatcher::permit_count(IggyByteSize::from(u32::MAX as u64 + 1));

        assert!(matches!(
            result,
            Err(IggyError::BackgroundSendBufferOverflow)
        ));
    }

    #[tokio::test]
    async fn test_dispatch_fails_on_buffer_overflow_immediate() {
        let mock = MockProducerCoreBackend::new();

        let msg = dummy_message(200);
        let config = BackgroundConfig::builder()
            .max_buffer_size(100.into())
            .failure_mode(BackpressureMode::FailImmediately)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        assert!(matches!(
            result,
            Err(IggyError::BackgroundSendBufferOverflow)
        ));
    }

    #[tokio::test]
    async fn test_dispatch_times_out_on_block_with_timeout() {
        let mock = MockProducerCoreBackend::new();

        let msg = dummy_message(200);
        let config = BackgroundConfig::builder()
            .max_buffer_size(msg.get_size_bytes() + 100.into())
            .max_in_flight(1)
            .failure_mode(BackpressureMode::BlockWithTimeout(
                Duration::from_millis(50).into(),
            ))
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let _keep = dispatcher
            .bytes_permit
            .clone()
            .acquire_many_owned(msg.get_size_bytes().as_bytes_u32() + 100)
            .await;

        let result = dispatcher
            .dispatch(vec![msg], dummy_identifier(), dummy_identifier(), None)
            .await;

        assert!(matches!(result, Err(IggyError::BackgroundSendTimeout)));
    }

    #[tokio::test]
    async fn test_dispatch_waits_and_succeeds_on_block_mode() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal()
            .times(1)
            .returning(|_, _, _, _| Box::pin(async { Ok(no_confirmations()) }));

        let msg = ShardMessage {
            stream: dummy_identifier(),
            topic: dummy_identifier(),
            messages: vec![dummy_message(5)],
            partitioning: None,
        };

        let config = BackgroundConfig::builder()
            .max_buffer_size(msg.get_size_bytes())
            .max_in_flight(1)
            .failure_mode(BackpressureMode::Block)
            .num_shards(1)
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let _block = dispatcher
            .bytes_permit
            .clone()
            .acquire_many_owned(msg.get_size_bytes().as_bytes_u32())
            .await
            .unwrap();

        let msg_clone = ShardMessage {
            stream: msg.stream.clone(),
            topic: msg.topic.clone(),
            messages: msg.messages,
            partitioning: msg.partitioning.clone(),
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(_block);
        });

        let result = dispatcher
            .dispatch(
                msg_clone.messages,
                msg_clone.topic,
                msg_clone.stream,
                msg_clone.partitioning,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(result.is_ok());
    }

    #[derive(Clone, Debug)]
    struct TestSharding {
        called: Arc<AtomicUsize>,
    }

    impl Sharding for TestSharding {
        fn pick_shard(
            &self,
            num_shards: usize,
            _messages: &[IggyMessage],
            _stream: &Identifier,
            _topic: &Identifier,
        ) -> usize {
            self.called.fetch_add(1, Ordering::SeqCst);
            num_shards - 1
        }
    }

    #[derive(Clone, Debug)]
    struct TestErrorCallback {
        called: Arc<AtomicUsize>,
        last_batch_len: Arc<AtomicUsize>,
    }

    impl ErrorCallback for TestErrorCallback {
        fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
            self.called.fetch_add(1, Ordering::SeqCst);
            self.last_batch_len
                .store(ctx.messages.len(), Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn test_custom_sharding_and_error_callback() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal().returning(|_, _, _, _| {
            Box::pin(async {
                Err(IggyError::ProducerSendFailed {
                    cause: Box::new(IggyError::Error),
                    failed: Arc::new(vec![dummy_message(10)]),
                    committed: Arc::new(Vec::new()),
                    stream_name: "1".to_string(),
                    topic_name: "1".to_string(),
                })
            })
        });

        let sharding_called = Arc::new(AtomicUsize::new(0));
        let error_called = Arc::new(AtomicUsize::new(0));
        let last_batch_len = Arc::new(AtomicUsize::new(0));

        let config = BackgroundConfig::builder()
            .num_shards(1)
            .error_callback(Arc::new(Box::new(TestErrorCallback {
                called: error_called.clone(),
                last_batch_len: last_batch_len.clone(),
            })))
            .sharding(Box::new(TestSharding {
                called: sharding_called.clone(),
            }))
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        let result = dispatcher
            .dispatch(
                vec![dummy_message(10)],
                dummy_identifier(),
                dummy_identifier(),
                None,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(result.is_ok());
        assert_eq!(sharding_called.load(Ordering::SeqCst), 1);
        assert_eq!(error_called.load(Ordering::SeqCst), 1);
        assert_eq!(last_batch_len.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_shutdown_reports_errors_from_final_flush() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal().returning(|_, _, _, _| {
            Box::pin(async {
                Err(IggyError::ProducerSendFailed {
                    cause: Box::new(IggyError::Error),
                    failed: Arc::new(vec![dummy_message(10)]),
                    committed: Arc::new(Vec::new()),
                    stream_name: "1".to_string(),
                    topic_name: "1".to_string(),
                })
            })
        });

        let error_called = Arc::new(AtomicUsize::new(0));
        let last_batch_len = Arc::new(AtomicUsize::new(0));

        let config = BackgroundConfig::builder()
            .num_shards(1)
            .linger_time(Duration::from_secs(60).into())
            .error_callback(Arc::new(Box::new(TestErrorCallback {
                called: error_called.clone(),
                last_batch_len: last_batch_len.clone(),
            })))
            .build();

        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        dispatcher
            .dispatch(
                vec![dummy_message(10)],
                dummy_identifier(),
                dummy_identifier(),
                None,
            )
            .await
            .unwrap();

        dispatcher.shutdown().await;

        assert_eq!(error_called.load(Ordering::SeqCst), 1);
        assert_eq!(last_batch_len.load(Ordering::SeqCst), 1);
    }

    /// Panics inside `call()` itself, before any future exists, on the first invocation only.
    #[derive(Debug)]
    struct PanicOnceErrorCallback {
        called: Arc<AtomicUsize>,
    }

    impl ErrorCallback for PanicOnceErrorCallback {
        fn call(&self, _ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
            if self.called.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("first failure panics before returning a future");
            }
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn test_error_task_survives_panic_in_error_callback_call() {
        let mut mock = MockProducerCoreBackend::new();
        mock.expect_send_internal().returning(|_, _, _, _| {
            Box::pin(async {
                Err(IggyError::ProducerSendFailed {
                    cause: Box::new(IggyError::Error),
                    failed: Arc::new(vec![dummy_message(10)]),
                    committed: Arc::new(Vec::new()),
                    stream_name: "1".to_string(),
                    topic_name: "1".to_string(),
                })
            })
        });

        let called = Arc::new(AtomicUsize::new(0));
        let config = BackgroundConfig::builder()
            .num_shards(1)
            .error_callback(Arc::new(Box::new(PanicOnceErrorCallback {
                called: called.clone(),
            })))
            .build();
        let dispatcher = ProducerDispatcher::new(Arc::new(mock), config);

        // Distinct topics keep the two sends from merging into one request, whichever branch of
        // the worker ends up flushing them.
        for topic_id in 1..=2 {
            dispatcher
                .dispatch(
                    vec![dummy_message(10)],
                    dummy_identifier(),
                    Arc::new(Identifier::numeric(topic_id).unwrap()),
                    None,
                )
                .await
                .unwrap();
        }
        dispatcher.shutdown().await;

        assert_eq!(
            called.load(Ordering::SeqCst),
            2,
            "the failure after the panicking one must still reach the callback"
        );
    }
}
