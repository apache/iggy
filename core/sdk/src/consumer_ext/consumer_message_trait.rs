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

use crate::consumer_ext::MessageConsumer;
use crate::prelude::ReadError;
use async_trait::async_trait;
use tokio::sync::oneshot;

/// Reads an [`IggyConsumer`] in a loop and hands every message to a [`MessageConsumer`].
///
/// [`IggyConsumer`] implements this trait, and the trait is in the prelude, so
/// [`consume_messages()`](Self::consume_messages) is available on any consumer you build. It
/// replaces the `while let` loop you would otherwise write around
/// [`StreamExt::next`](futures_util::StreamExt::next).
///
/// Under [`CommitPolicy::Auto`] the loop counts each message as its [`CommitAfter`] says. With
/// [`CommitAfter::Processing`] a message counts once your handler returned `Ok`, and a handler
/// that fails ends the loop instead of being skipped.
///
/// # Examples
///
/// Read a topic until Ctrl-C, then store the counted offsets and leave the group:
///
/// ```rust,no_run
/// use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
/// use iggy::prelude::*;
/// use tokio::sync::oneshot;
///
/// struct PrintMessage;
///
/// impl MessageConsumer for PrintMessage {
///     async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
///         println!("Offset: {}", message.message.header.offset);
///         Ok(())
///     }
/// }
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let mut consumer = client
///     .consumer_group("order-workers", "my-stream", "my-topic")?
///     .poll_options(PollOptions {
///         commit: CommitPolicy::Auto(AutoCommit::default()),
///         ..PollOptions::wait(IggyDuration::ONE_SECOND)
///     })
///     .polling_strategy(PollingStrategy::next())
///     .build();
/// consumer.init().await?;
///
/// let (sender, receiver) = oneshot::channel();
/// tokio::spawn(async move {
///     tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
///     let _ = sender.send(());
/// });
///
/// consumer.consume_messages(&PrintMessage, receiver).await?;
///
/// consumer.shutdown().await?;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// [`CommitAfter`]: crate::prelude::CommitAfter
/// [`CommitAfter::Processing`]: crate::prelude::CommitAfter::Processing
/// [`CommitPolicy::Auto`]: crate::prelude::CommitPolicy::Auto
/// [`IggyConsumer`]: crate::prelude::IggyConsumer
#[async_trait]
pub trait IggyConsumerMessageExt<'a> {
    /// Reads messages until a shutdown signal arrives, and hands each one to `message_consumer`.
    ///
    /// The loop stops when `shutdown_rx` resolves, which happens both when the sender sends and
    /// when the sender is dropped. It also stops when the consumer has no more messages, as
    /// after `shutdown()` on the consumer. Either way it returns `Ok(())` and leaves the consumer
    /// usable, so storing the counted offsets and leaving the group is still your call.
    ///
    /// Under `CommitPolicy::Auto`, a message counts before the consumer runs with
    /// `CommitAfter::Delivery`, and once it returns with success with `CommitAfter::Processing`.
    /// With `Processing`, a consumer that fails ends the loop and leaves its message uncounted.
    /// Under every other policy, a message whose handler returned an error is logged and
    /// skipped, and the loop reads on.
    ///
    /// # Errors
    ///
    /// Returns a [`ReadError`] for the error that ended the loop. It names its partition, if it
    /// has one, so the caller can recover that partition and consume again. Every other read
    /// error is logged and the loop reads on. The implementation on `IggyConsumer` lists the
    /// errors that end the loop.
    async fn consume_messages<P>(
        &mut self,
        message_consumer: &'a P,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ReadError>
    where
        P: MessageConsumer + Sync;
}
