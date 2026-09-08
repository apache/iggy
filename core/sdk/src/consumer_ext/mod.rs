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

//! Runs the read loop of an [`IggyConsumer`] for you.
//!
//! An [`IggyConsumer`] is a [`Stream`](futures::Stream), so reading from it normally means writing
//! a `while let` loop, matching on the result of every message, and watching for a stop signal.
//! This module replaces that loop with two pieces:
//!
//! - [`MessageConsumer`] is what you write. It handles one message.
//! - [`IggyConsumerMessageExt::consume_messages`] is the loop. It reads messages, hands each one to
//!   your [`MessageConsumer`], and returns when a shutdown signal arrives.
//!
//! [`IggyConsumerMessageExt`] is in the prelude. [`MessageConsumer`] is not, so import it from
//! this module.
//!
//! # Examples
//!
//! Handle every message with a printer, and stop the loop on Ctrl-C:
//!
//! ```rust,no_run
//! use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
//! use iggy::prelude::*;
//! use tokio::sync::oneshot;
//!
//! struct PrintMessage;
//!
//! impl MessageConsumer for PrintMessage {
//!     async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
//!         let payload = String::from_utf8_lossy(&message.message.payload);
//!         println!("Offset {}: {payload}", message.message.header.offset);
//!         Ok(())
//!     }
//! }
//!
//! # async fn example() -> Result<(), IggyError> {
//! let config = IggyConsumerConfig::from_stream_topic(
//!     "my-stream",
//!     "my-topic",
//!     100,
//!     IggyDuration::new_from_secs(1),
//! )?;
//! let (client, mut consumer) =
//!     IggyStreamConsumer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
//!         .await?;
//!
//! let (sender, receiver) = oneshot::channel();
//! tokio::spawn(async move {
//!     tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
//!     let _ = sender.send(());
//! });
//!
//! consumer.consume_messages(&PrintMessage, receiver).await?;
//!
//! consumer.shutdown().await?;
//! client.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`IggyConsumer`]: crate::prelude::IggyConsumer

mod consumer_message_ext;
mod consumer_message_trait;

use crate::{clients::consumer::ReceivedMessage, prelude::IggyError};
pub use consumer_message_trait::IggyConsumerMessageExt;

/// Handles one message that [`IggyConsumerMessageExt::consume_messages`] read.
///
/// Implement this trait on your own type and pass a reference to it. The loop calls
/// [`consume()`](Self::consume) once per message, in the order the messages arrive, and waits for
/// each call before it reads the next message. A slow handler therefore slows the reading down.
///
/// [`consume()`](Self::consume) takes `&self`, so state that changes needs interior mutability,
/// such as an [`AtomicU64`](std::sync::atomic::AtomicU64) or a
/// [`Mutex`](tokio::sync::Mutex). [`MessageConsumer`] is also implemented for `&T`, so a reference
/// to your type is itself a [`MessageConsumer`].
///
/// [`MessageConsumer`] is not in the prelude. Import it from
/// [`consumer_ext`](crate::consumer_ext).
///
/// # Examples
///
/// Count the messages that were handled, and reject an empty payload:
///
/// ```rust
/// use iggy::consumer_ext::MessageConsumer;
/// use iggy::prelude::*;
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// struct CountMessages {
///     handled: AtomicU64,
/// }
///
/// impl MessageConsumer for CountMessages {
///     async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
///         if message.message.payload.is_empty() {
///             return Err(IggyError::EmptyMessagePayload);
///         }
///         self.handled.fetch_add(1, Ordering::Relaxed);
///         Ok(())
///     }
/// }
/// ```
#[allow(dead_code)] // Clippy can't see that the trait is used
#[trait_variant::make(MessageConsumer: Send)]
pub trait LocalMessageConsumer {
    /// Handles one message.
    ///
    /// # Errors
    ///
    /// Return any [`IggyError`] that describes why this message could not be handled.
    /// [`IggyConsumerMessageExt::consume_messages`] logs that error and reads on, so a failed
    /// message stops nothing. The error never suppresses a commit either, because every
    /// [`AutoCommitAfter`](crate::prelude::AutoCommitAfter) variant applies its own trigger
    /// whatever the handler returned. [`ConsumingEachMessage`] therefore commits the offset of a
    /// failed message, while [`ConsumingEveryNthMessage`] and [`ConsumingAllMessages`] commit it
    /// only when the offset meets their condition. Keep a message that must not be lost yourself,
    /// or commit by hand with [`AutoCommit::Disabled`](crate::prelude::AutoCommit::Disabled).
    ///
    /// [`ConsumingAllMessages`]: crate::prelude::AutoCommitAfter::ConsumingAllMessages
    /// [`ConsumingEachMessage`]: crate::prelude::AutoCommitAfter::ConsumingEachMessage
    /// [`ConsumingEveryNthMessage`]: crate::prelude::AutoCommitAfter::ConsumingEveryNthMessage
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError>;
}

// Default implementation for `&T`
// https://users.rust-lang.org/t/hashmap-get-dereferenced/33558
impl<T: MessageConsumer + Send + Sync> MessageConsumer for &T {
    /// Passes the message to the [`MessageConsumer`] behind this reference.
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
        (**self).consume(message).await
    }
}
