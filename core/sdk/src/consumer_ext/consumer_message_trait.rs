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

#[async_trait]
pub trait IggyConsumerMessageExt<'a> {
    /// This function starts an event loop that consumes messages from the stream and
    /// applies the provided consumer. The loop will exit when the shutdown receiver is triggered.
    ///
    /// The event of a `ClientCommitPolicy` counts a message after the consumer returns, also
    /// after a failure. Its interval can store a message whose consumer still runs, because the
    /// delivery of a message counts when the stream yields it.
    ///
    /// An error that ends the loop names its partition, if it has one, so the caller can
    /// recover that partition and consume again.
    ///
    /// # Arguments
    ///
    /// * `message_consumer`: The consumer to send messages to.
    /// * `shutdown_rx`: The receiver to listen to for shutdown.
    ///
    async fn consume_messages<P>(
        &mut self,
        message_consumer: &'a P,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), ReadError>
    where
        P: MessageConsumer + Sync;
}
