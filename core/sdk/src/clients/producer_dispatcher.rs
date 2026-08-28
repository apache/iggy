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
use crate::clients::producer_error_callback::ErrorCtx;
use crate::clients::producer_sharding::{Shard, ShardMessage, ShardMessageWithPermit};
use futures::FutureExt;
use iggy_common::{Identifier, IggyByteSize, IggyError, IggyMessage, Partitioning, Sizeable};
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
/// A background send is a dispatch of messages to a [`Shard`]. Dispatching means, that the messages are send down the
/// sending end of a channel, where the receiver is listened on a [`Shard`] worker.
/// So a batch is queued and the write happens later, on one of the [`Shard`] workers this type owns.
/// Consequently, errors from writes surface on `Shard`s which channel those errors back to the dispatcher's callback
/// (comp. [`Self::new()`]). These errors are logged with the default [`LogErrorCallback`], but other
/// implementations of the [`ErrorCallback`] trait can be set through
/// [`BackgroundConfig::error_callback`].
///
/// Which worker a batch lands on, and what that means for ordering, is described on
/// [`IggyProducer`](crate::clients::producer::IggyProducer).
///
/// # Examples
///
/// Configuring background send through the producer builder:
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
/// You can implement your own backend/ logic on how to send and wrap the dispatcher around that.
/// Here, just print instead of sending anything to a server for illustration.
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
/// // Writes what is still queued. Dropping the dispatcher instead discards it.
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
/// [`BackgroundConfig::max_buffer_size`] bytes before queueing it, and the charge is released once a
/// worker has written the batch, so the budget covers what is waiting in the queues as well as what
/// is on the wire. What gets charged is the size [`ShardMessage`] reports, which counts the stream
/// and topic identifiers alongside the messages rather than the payloads alone.
///
/// This budget is the only backpressure a background send can observe, and
/// [`BackgroundConfig::failure_mode`] decides what exhausting it does to the caller. The second
/// limit, [`BackgroundConfig::max_in_flight`], is shared by the workers rather than the callers: a
/// worker takes one of its permits for the batch it is about to write, so it bounds concurrent
/// writes across all workers, not queued bytes.
///
/// ## In-flight Limit
///
/// You can configure the `ProducerDispatcher` with a maximum number of requests that are allowed to be
/// written concurrently through [`BackgroundConfig::max_in_flight`]. That budget is shared by every
/// worker rather than granted per worker, and its default is one. Note, that you risk losing the ordering of batches
/// if you increase that number. For adjacent messages split in two batches sending of the first batch might fail and trigger a retry.
/// However, that batch might be raced by the second batch should it succeed immediately.
///
/// # Shutdown
///
/// [`shutdown()`](Self::shutdown) is what makes the queued batches observable. It stops the workers,
/// waits for their last flush, and only then returns. Dropping the dispatcher instead ends the
/// workers wherever they are and discards whatever they still hold.
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
    /// Spawns the [`Shard`]s, i.e. the workers that can write messages to the server.
    ///
    /// The `ProducerDispatcher` spawns and owns the `Shards` on which the write load
    /// is distributed. Which of the [`BackgroundConfig::num_shards`] is picked to write is decided
    /// by [`BackgroundConfig::sharding`].
    ///
    /// Additionally, a dispatcher starts a task that listens for error callbacks. So should a `Shard` fail
    /// to write some messages (including retries), that failure is not received on the same task,
    /// but hits the [`ErrorCallback`] set as [`BackgroundConfig::error_callback`].
    ///
    /// To cap the amount of data a dispatcher handles two semaphores are initialized from
    /// [`BackgroundConfig::max_buffer_size`] and [`BackgroundConfig::max_in_flight`]. Both are shared
    /// by every `Shard` and therefore hold for the entire producer, rather than per worker. They
    /// count different things at different points of a batch's life: `max_buffer_size` is charged in
    /// bytes by [`dispatch()`](Self::dispatch) before the batch is queued and released once it has
    /// been written, so it bounds queued and in-flight bytes together, while `max_in_flight` is taken
    /// as one permit by the worker that is about to write and released when that request returns, so
    /// it bounds concurrent requests and says nothing about their size. A batch therefore has to pass
    /// the byte budget to enter a queue, and to take a request slot to leave it.
    ///
    /// # Examples
    ///
    /// Four workers writing in parallel, fed round-robin, with a budget of 64 MiB in queued bytes:
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
    ///         // Ordering is given up for throughput: a batch can land on any of the four workers.
    ///         .sharding(Box::new(BalancedSharding::default()))
    ///         .max_buffer_size(IggyByteSize::from(64 * 1024 * 1024))
    ///         .build(),
    /// );
    /// # }
    /// ```
    ///
    /// `num_shards(0)` is read as one worker, and both limits read `0` as unbounded, so this
    /// dispatcher writes from a single worker and never refuses a batch for lack of budget:
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
                if let Err(panic) = std::panic::AssertUnwindSafe(err_callback.call(ctx))
                    .catch_unwind()
                    .await
                {
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
    /// The batch is charged against the [`BackgroundConfig::max_buffer_size`] budget before it is
    /// queued (a semaphore), and the permit travels with it so those bytes stay charged until a worker has written
    /// them. A batch larger than the entire budget can never be charged and fails with
    /// [`IggyError::BackgroundSendBufferOverflow`].
    ///
    /// When the budget is exhausted, [`BackgroundConfig::failure_mode`] decides what happens to the
    /// caller. [`BackpressureMode::FailImmediately`] gives up with
    /// [`IggyError::BackgroundSendBufferOverflow`], [`BackpressureMode::Block`] waits for as long as
    /// it takes to acquire the message bytes from the budget, and [`BackpressureMode::BlockWithTimeout`] waits
    /// for its duration before failing with [`IggyError::BackgroundSendTimeout`]. None of the three retries.
    ///
    /// [`BackgroundConfig::sharding`] then picks the worker based on the configured strategy, and the batch is handed to its queue.
    /// That queue is bounded (=256), exceeding that limit can force the caller to wait.
    ///
    /// # Errors
    ///
    /// [`IggyError::ProducerClosed`] once [`shutdown()`](Self::shutdown) has begun, and
    /// [`IggyError::BackgroundSendError`] if the picked worker is already gone, which leaves the
    /// batch unqueued and unsent in both cases. The budget can additionally fail the call with
    /// [`IggyError::BackgroundSendBufferOverflow`] or [`IggyError::BackgroundSendTimeout`] as
    /// described above.
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
    /// // Nothing is written yet. Both batches are queued, and a worker writes them later.
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

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::time::sleep;

    use crate::clients::producer::{MockProducerCoreBackend, no_confirmations};
    use crate::clients::producer_error_callback::ErrorCallback;
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
}
