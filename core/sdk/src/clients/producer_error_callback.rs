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

/// Everything that is known about a background send that failed.
///
/// A [`background()`] producer returns from [`send()`] once the batch is queued and the write itself
/// happens later, on one of the dispatcher's worker [`Shard`]s. So for the caller there
/// is no `Result` to handle acknowledgments or failures. Instead, the worker puts the failure into an
/// `ErrorCtx`, sends it to a dedicated error callback worker, where it lands on the
/// [`BackgroundConfig::error_callback`], i.e. something that implements the [`ErrorCallback`] trait.
///
/// # What to do with it
///
/// [`messages`](Self::messages) is a batch that failed to commit to the server and therefore may be sent again.
/// [`stream`](Self::stream), [`topic`](Self::topic) and [`partitioning`](Self::partitioning) are the
/// destination it was meant for. Note, that configured retries at the dispatcher are already spent at this point.
/// A callback can resend it, park it in a dead letter store, write it to disk, or raise an
/// The default implementation [`LogErrorCallback`] logs the error messages. A background producer that keeps it
/// loses every message a background write fails on. However, you can also
/// implement the [`ErrorCallback`] trait yourself and come up with your own logic that defines
/// what should be done with failures.
///
/// [`background()`]: crate::clients::producer_builder::IggyProducerBuilder::background
/// [`direct()`]: crate::clients::producer_builder::IggyProducerBuilder::direct
/// [`encryptor()`]: crate::clients::producer_builder::IggyProducerBuilder::encryptor
/// [`send_retries()`]: crate::clients::producer_builder::IggyProducerBuilder::send_retries
/// [`send()`]: crate::clients::producer::IggyProducer::send
/// [`send_to()`]: crate::clients::producer::IggyProducer::send_to
/// [`shutdown()`]: crate::clients::producer::IggyProducer::shutdown
/// [`BackgroundConfig::error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
/// [`ProducerDispatcher`]: crate::clients::producer_dispatcher::ProducerDispatcher
/// [`Shard`]: crate::clients::producer_sharding::Shard
#[derive(Debug)]
pub struct ErrorCtx {
    pub cause: Box<IggyError>,
    pub stream: Arc<Identifier>,
    pub stream_name: String,
    pub topic: Arc<Identifier>,
    pub topic_name: String,
    pub partitioning: Option<Arc<Partitioning>>,
    pub messages: Arc<Vec<IggyMessage>>,
    /// Confirmations of the chunks that committed before the failure; `messages`
    /// is the tail that did not.
    pub committed: Arc<Vec<SendMessagesConfirmationResponse>>,
}

/// Handles a background send that failed, in place of the caller that is no longer there.
///
/// A [`background()`](crate::clients::producer_builder::IggyProducerBuilder::background) producer
/// acknowledges a send once it is queued, so a write that fails afterwards has nobody to return an
/// error to. The dispatcher owns one implementation of this trait, set with
/// [`BackgroundConfig::error_callback`], and invokes it with an [`ErrorCtx`] for every request that
/// fails.
///
/// # Implementing it
///
/// - [`call()`](Self::call) returns a boxed future the dispatcher awaits, so the callback may do I/O.
/// - It runs on a task of its own, never on a worker, so awaiting it does not stall batching. Calls
///   are serialized though: one failure is handled at a time, and a slow callback lets the queue of
///   pending failures grow, since that channel is unbounded.
/// - A panic inside it is caught and logged, and the next failure is still delivered.
/// - `Send + Sync + Debug + 'static` because the dispatcher's task owns it for as long as the
///   producer lives, and [`BackgroundConfig`] is [`Debug`].
/// - Do not call back into the producer that owns the callback. The callback is built first and
///   handed to the config, so the producer does not exist yet; forward the context to something that
///   does.
///
/// # Example
///
/// Hand every failed batch to a task that outlives the send instead of dropping it. The callback
/// itself only forwards the context, so a slow store never holds up the failures queued behind it:
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
/// // Implements however lost messages should be handled. Here, we warn. Could also write to file, notify, ...
/// async fn drain(mut failures: UnboundedReceiver<ErrorCtx>) {
///     while let Some(ctx) = failures.recv().await {
///         warn!(
///             cause = %ctx.cause,
///             stream_name = ctx.stream_name,
///             topic_name = ctx.topic_name,
///             num_messages = ctx.messages.len(),
///             "Storing failed batch",
///         );
///     }
/// }
///
/// let (failures, receiver) = tokio::sync::mpsc::unbounded_channel();
/// tokio::spawn(drain(receiver));
///
/// let config = BackgroundConfig::builder()
///     .error_callback(Arc::new(Box::new(FailedMessages { failures })))
///     .build();
/// ```
///
/// [`BackgroundConfig`]: crate::clients::producer_config::BackgroundConfig
/// [`BackgroundConfig::error_callback`]: crate::clients::producer_config::BackgroundConfig::error_callback
pub trait ErrorCallback: Send + Sync + Debug + 'static {
    /// Handles one failed request, described by `ctx`.
    ///
    /// Called once per failed request by the dispatcher's error task, which awaits the returned
    /// future before taking the next failure off the queue.
    fn call(&self, ctx: ErrorCtx) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Default implementation of [`ErrorCallback`] that logs the error using `tracing::error!`.
///
/// Logs include stream, topic, optional partitioning, number of messages, how
/// many chunks committed before the failure, and the cause.
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
