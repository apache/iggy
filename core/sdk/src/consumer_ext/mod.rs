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

mod consumer_message_ext;
mod consumer_message_trait;

use crate::{clients::consumer::ReceivedMessage, prelude::IggyError};
pub use consumer_message_trait::IggyConsumerMessageExt;

/// Trait for message consumer
#[allow(dead_code)] // Clippy can't see that the trait is used
#[trait_variant::make(MessageConsumer: Send)]
pub trait LocalMessageConsumer {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `message` - The received message to consume
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError>;
}

// Default implementation for `&T`
// https://users.rust-lang.org/t/hashmap-get-dereferenced/33558
impl<T: MessageConsumer + Send + Sync> MessageConsumer for &T {
    /// Consume a message from the message bus.
    ///
    /// # Arguments
    ///
    /// * `message` - The received message to consume
    ///
    /// # Errors
    ///
    /// * `IggyError` - If the message consumer fails to consume the message
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
        (**self).consume(message).await
    }
}
