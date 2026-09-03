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

use iggy_common::{
    Identifier, IggyError, IggyMessage, Partitioning, SendMessagesConfirmationResponse,
};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use tracing::error;

/// Everything known about a background write that did not return a usable confirmation.
///
/// A [`background()`] producer returns from [`send()`] once the batch is queued. The write happens
/// later on one of the dispatcher's worker [`Shard`]s, when there is no caller waiting for its
/// result. The worker therefore sends an `ErrorCtx` to the dedicated error task, which invokes the
/// [`ErrorCallback`] configured in [`BackgroundConfig::error_callback`].
///
/// # What to do with it
///
/// [`messages`](Self::messages) is the unconfirmed tail of the send. No further automatic retry will
/// be attempted. Depending on [`cause`](Self::cause), the retry budget may be exhausted, the failure
/// may be deliberately non-retriable, or encryption or partitioning may have failed before a request
/// was sent. The tail is not proof that nothing committed. A request can commit before its response
/// is lost, and an HTTP confirmation decoding failure specifically occurs after a successful status.
/// Resending `messages` is therefore an at-least-once operation and can create duplicates.
/// Encryption mutates messages before the write, so this tail contains encrypted messages when the
/// producer uses an encryptor. Passing them back through the same producer would encrypt them again.
///
/// [`stream`](Self::stream) and [`topic`](Self::topic) identify the destination.
/// [`partitioning`](Self::partitioning) contains only the per-send override passed to the dispatcher.
/// `None` means the producer's configured or default partitioning was used, not that the request had
/// no partitioning. A callback can retain the context, persist it in a dead-letter store, alert an
/// operator, or retry it when duplicate delivery is acceptable. The default [`LogErrorCallback`]
/// only logs the failure and then drops the context.
///
/// [`background()`]: crate::clients::producer_builder::IggyProducerBuilder::background
/// [`send()`]: crate::clients::producer::IggyProducer::send
/// [`BackgroundConfig::error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
/// [`Shard`]: crate::clients::producer_sharding::Shard
#[derive(Debug)]
pub struct ErrorCtx {
    /// Error that ended the send. No further automatic retry will be attempted.
    pub cause: Box<IggyError>,
    /// Stream identifier used by the failed request.
    pub stream: Arc<Identifier>,
    /// Stream name configured when the producer was built.
    ///
    /// For a failure from
    /// [`IggyProducer::send_to`](crate::clients::producer::IggyProducer::send_to), this may not name
    /// [`Self::stream`].
    pub stream_name: String,
    /// Topic identifier used by the failed request.
    pub topic: Arc<Identifier>,
    /// Topic name configured when the producer was built.
    ///
    /// For a failure from
    /// [`IggyProducer::send_to`](crate::clients::producer::IggyProducer::send_to), this may not name
    /// [`Self::topic`].
    pub topic_name: String,
    /// Per-send partitioning override, or `None` when the producer configuration was used.
    pub partitioning: Option<Arc<Partitioning>>,
    /// Unconfirmed tail of the send, see [`ErrorCtx`] for what resending it means.
    pub messages: Arc<Vec<IggyMessage>>,
    /// Confirmations returned for chunks before the failure.
    pub committed: Arc<Vec<SendMessagesConfirmationResponse>>,
}

/// Handles a background write failure after the queueing caller has returned.
///
/// A [`background()`](crate::clients::producer_builder::IggyProducerBuilder::background) producer
/// acknowledges a send once it is queued, so a later write failure cannot be returned by
/// [`IggyProducer::send`]. The dispatcher owns one implementation of this trait, set with
/// [`BackgroundConfig::error_callback`], and invokes it with an [`ErrorCtx`] whenever a worker's
/// backend returns [`IggyError::ProducerSendFailed`]. Other error variants from a custom backend are
/// logged by the worker without invoking this callback.
///
/// # Implementing it
///
/// - [`call()`](Self::call) returns a boxed future that the error task awaits, so the callback may do
///   asynchronous I/O.
/// - It runs on its own task rather than on a shard worker, so awaiting it does not stall batching.
///   Calls are serialized. One failure is handled at a time, and the unbounded error channel can
///   grow while a callback is slow.
/// - A panic inside it, whether in [`call()`](Self::call) itself or in the returned future, is
///   caught and logged, and the next failure is still delivered.
/// - `Send + Sync + Debug + 'static` is required because the dispatcher's task owns the callback for
///   the producer's lifetime and [`BackgroundConfig`] implements [`Debug`].
///
/// # Example
///
/// Forward each failed batch to a separate task instead of dropping it. The callback only enqueues
/// the context, so a slow store does not hold up later callbacks:
///
/// ```no_run
/// use iggy::clients::producer_error_callback::{ErrorCallback, ErrorCtx};
/// use iggy::prelude::*;
/// use std::pin::Pin;
/// use std::sync::Arc;
/// use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
/// use tracing::warn;
///
/// #[derive(Debug)]
/// struct FailedMessages {
///     failures: UnboundedSender<ErrorCtx>,
/// }
///
/// impl ErrorCallback for FailedMessages {
///     fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
///         let failures = self.failures.clone();
///         Box::pin(async move {
///             let num_messages = ctx.messages.len();
///             if failures.send(ctx).is_err() {
///                 warn!(num_messages, "Failed messages task is gone, dropping messages");
///             }
///         })
///     }
/// }
///
/// // Replace this warning with durable storage or another application-specific policy.
/// async fn drain(mut failures: UnboundedReceiver<ErrorCtx>) {
///     while let Some(ctx) = failures.recv().await {
///         warn!(
///             cause = %ctx.cause,
///             stream_name = ctx.stream_name,
///             topic_name = ctx.topic_name,
///             num_messages = ctx.messages.len(),
///             "Received failed batch",
///         );
///     }
/// }
///
/// # async fn example() {
/// let (failures, receiver) = tokio::sync::mpsc::unbounded_channel();
/// tokio::spawn(drain(receiver));
///
/// let config = BackgroundConfig::builder()
///     .error_callback(Arc::new(Box::new(FailedMessages { failures })))
///     .build();
/// # }
/// ```
///
/// [`BackgroundConfig`]: crate::clients::producer_config::BackgroundConfig
/// [`BackgroundConfig::error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
/// [`IggyProducer::send`]: crate::clients::producer::IggyProducer::send
pub trait ErrorCallback: Send + Sync + Debug + 'static {
    /// Handles one failed request described by `ctx`.
    ///
    /// The dispatcher's error task calls this once per failed request and awaits the returned future
    /// before taking the next failure from the queue.
    fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Default [`ErrorCallback`] implementation that logs the error using `tracing::error!`.
///
/// Logs include stream, topic, optional partitioning, number of messages, how many earlier chunks
/// returned confirmations, and the cause.
///
/// The messages themselves are dropped with the context, so a background producer that keeps this
/// callback has no way to recover them. Implement [`ErrorCallback`] to hold on to them.
#[derive(Debug, Default)]
pub struct LogErrorCallback;

impl ErrorCallback for LogErrorCallback {
    fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let partitioning = ctx
                .partitioning
                .as_ref()
                .map(|p| format!("{p:?}"))
                .unwrap_or_else(|| "None".to_string());

            error!(
                cause = %ctx.cause,
                stream = %ctx.stream,
                stream_name = ctx.stream_name,
                topic = %ctx.topic,
                topic_name = ctx.topic_name,
                partitioning = %partitioning,
                num_messages = ctx.messages.len(),
                committed_confirmations = ctx.committed.len(),
                "Failed to send messages in background task",
            );
        })
    }
}
