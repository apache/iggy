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

use super::{
    frame::{ShardFrame, ShardResponse},
    message::ShardMessage,
};
use iggy_common::IggyError;
use tracing::error;

pub type StopSender = async_channel::Sender<()>;
pub type StopReceiver = async_channel::Receiver<()>;

/// Inter-shard communication channel
pub struct ShardConnector<T> {
    pub id: u16,
    pub sender: flume::Sender<T>,
    pub receiver: Receiver<T>,
    pub stop_receiver: StopReceiver,
    pub stop_sender: StopSender,
}

impl<T> Clone for ShardConnector<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            stop_receiver: self.stop_receiver.clone(),
            stop_sender: self.stop_sender.clone(),
        }
    }
}

impl<T> ShardConnector<T> {
    /// Creates a new shard connector with unbounded capacity.
    pub fn new(id: u16) -> Self {
        let (sender, receiver) = flume::unbounded();
        let (stop_sender, stop_receiver) = async_channel::bounded(1);
        Self {
            id,
            sender,
            receiver: Receiver::new(receiver),
            stop_receiver,
            stop_sender,
        }
    }

    /// Sends a message to this shard.
    ///
    /// For unbounded channels, this operation is infallible and never blocks.
    pub fn send(&self, data: T) {
        let _ = self.sender.send(data);
    }
}

impl ShardConnector<ShardFrame> {
    /// Sends a request and waits for a response.
    /// This implements the request-response pattern for inter-shard communication.
    pub async fn send_request(&self, message: ShardMessage) -> Result<ShardResponse, IggyError> {
        let (sender, receiver) = async_channel::bounded(1);
        // Note: sender needs to be passed to ShardFrame to keep the channel open
        self.send(ShardFrame::new(message, Some(sender)));

        receiver.recv().await.map_err(|err| {
            error!("Failed to receive response from shard {}: {err}", self.id);
            IggyError::ShardCommunicationError
        })
    }
}

/// Wrapper around flume's Receiver that provides Clone capability.
///
/// This wraps the flume receiver to allow cloning while still providing
/// access to the underlying receiver for direct use.
pub struct Receiver<T> {
    pub inner: flume::Receiver<T>,
}

impl<T> Receiver<T> {
    fn new(receiver: flume::Receiver<T>) -> Self {
        Self { inner: receiver }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
