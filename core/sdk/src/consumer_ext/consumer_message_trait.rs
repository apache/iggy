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
use crate::prelude::IggyError;
use async_trait::async_trait;
use tokio::sync::oneshot;

/// Reads an [`IggyConsumer`] in a loop and hands every message to a [`MessageConsumer`].
///
/// [`IggyConsumer`] implements this trait, and the trait is in the prelude, so
/// [`consume_messages()`](Self::consume_messages) is available on any consumer you build. It
/// replaces the `while let` loop you would otherwise write around
/// [`StreamExt::next`](futures_util::StreamExt::next).
///
/// # Examples
///
/// Read a topic until Ctrl-C, then commit and leave the group:
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
///     .auto_commit(AutoCommit::After(AutoCommitAfter::ConsumingEachMessage))
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
/// [`AutoCommitAfter`]: crate::prelude::AutoCommitAfter
/// [`IggyConsumer`]: crate::prelude::IggyConsumer
#[async_trait]
pub trait IggyConsumerMessageExt<'a> {
    /// Reads messages until a shutdown signal arrives, and hands each one to `message_consumer`.
    ///
    /// # Errors
    ///
    /// Return any [`IggyError`] that describes what went wrong while consuming.
    async fn consume_messages<P>(
        &mut self,
        message_consumer: &'a P,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), IggyError>
    where
        P: MessageConsumer + Sync;
}
