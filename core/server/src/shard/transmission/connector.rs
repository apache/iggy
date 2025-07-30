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
use futures::{Stream, task::AtomicWaker};
use sharded_queue::ShardedQueue;
use std::{
    sync::{Arc, atomic::AtomicUsize},
    task::Poll,
};

pub type StopSender = async_channel::Sender<()>;
pub type StopReceiver = async_channel::Receiver<()>;

pub struct ShardConnector<T> {
    pub id: u16,
    pub sender: Sender<T>,
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

// TODO(numinex) - replace async_channel with some other form of one shot async channel.
// !!!!!IMPORTANT!!!! the one shot channel Sender/Receiver has to be Cloneable!!!!!
impl<T> ShardConnector<T> {
    pub fn new(id: u16, max_concurrent_thread_count: usize) -> Self {
        let channel = Arc::new(ShardedChannel::new(max_concurrent_thread_count));
        let (sender, receiver) = channel.unbounded();
        let (stop_sender, stop_receiver) = async_channel::bounded(1);
        Self {
            id,
            receiver,
            sender,
            stop_receiver,
            stop_sender,
        }
    }

    pub fn send(&self, data: T) {
        self.sender.send(data);
    }
}

pub struct Receiver<T> {
    channel: Arc<ShardedChannel<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

pub struct Sender<T> {
    channel: Arc<ShardedChannel<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, data: T) {
        self.channel
            .task_queue
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.channel.queue.push_back(data);
        self.channel.waker.wake();
    }
}

pub struct ShardedChannel<T> {
    queue: ShardedQueue<T>,
    task_queue: AtomicUsize,
    waker: AtomicWaker,
}

impl<T> ShardedChannel<T> {
    pub fn new(max_concurrent_thread_count: usize) -> Self {
        let waker = AtomicWaker::new();

        Self {
            queue: ShardedQueue::new(max_concurrent_thread_count),
            task_queue: AtomicUsize::new(0),
            waker,
        }
    }
}

pub trait ShardedChannelsSplit<T> {
    fn unbounded(&self) -> (Sender<T>, Receiver<T>);

    fn sender(&self) -> Sender<T>;
}

impl<T> ShardedChannelsSplit<T> for Arc<ShardedChannel<T>> {
    fn unbounded(&self) -> (Sender<T>, Receiver<T>) {
        let tx = self.sender();
        let rx = Receiver {
            channel: Arc::clone(self),
        };

        (tx, rx)
    }

    fn sender(&self) -> Sender<T> {
        Sender {
            channel: Arc::clone(self),
        }
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let old = self
            .channel
            .task_queue
            .load(std::sync::atomic::Ordering::Relaxed);
        if old == 0 {
            self.channel.waker.register(cx.waker());
            return Poll::Pending;
        }

        assert!(old > 0);
        self.channel
            .task_queue
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        let item = self.channel.queue.pop_front_or_spin_wait_item();
        Poll::Ready(Some(item))
    }
}
