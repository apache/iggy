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

use crate::shard::IggyShard;
use ahash::AHashMap;
use async_channel::{Receiver, Sender};
use server_common::sharding::IggyNamespace;
use std::{cell::RefCell, time::Instant};
use std::time::Duration;

const MAX_WAITERS_PER_NAMESPACE: usize = 1024;

#[derive(Debug)]
struct PollWaiter {
    id: u64,
    wake_sender: Sender<()>,
    deadline: Option<Instant>,
}

#[derive(Debug, Default)]
pub(crate) struct PollWaiterRegistry {
    next_id: u64,
    waiters: AHashMap<IggyNamespace, Vec<PollWaiter>>,
}

impl PollWaiterRegistry {
    fn register(
        &mut self,
        namespace: IggyNamespace,
        timeout: Duration,
    ) -> Option<(u64, Receiver<()>)> {
        self.prune_namespace(&namespace);
        let waiters = self.waiters.entry(namespace).or_default();
        if waiters.len() >= MAX_WAITERS_PER_NAMESPACE {
            return None;
        }

        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;
        let (wake_sender, wake_receiver) = async_channel::bounded(1);
        waiters.push(PollWaiter {
            id,
            wake_sender,
            deadline: Instant::now().checked_add(timeout),
        });

        Some((id, wake_receiver))
    }

    fn remove(&mut self, namespace: &IggyNamespace, id: u64) {
        let Some(waiters) = self.waiters.get_mut(namespace) else {
            return;
        };
        waiters.retain(|waiter| waiter.id != id);
        if waiters.is_empty() {
            self.waiters.remove(namespace);
        }
    }

    fn wake_namespace(&mut self, namespace: &IggyNamespace) {
        let Some(waiters) = self.waiters.remove(namespace) else {
            return;
        };
        for waiter in waiters {
            let _ = waiter.wake_sender.try_send(());
        }
    }

    fn wake_topic(&mut self, stream_id: usize, topic_id: usize) {
        let namespaces = self
            .waiters
            .keys()
            .copied()
            .filter(|namespace| {
                namespace.stream_id() == stream_id && namespace.topic_id() == topic_id
            })
            .collect::<Vec<_>>();
        for namespace in namespaces {
            self.wake_namespace(&namespace);
        }
    }

    fn wake_stream(&mut self, stream_id: usize) {
        let namespaces = self
            .waiters
            .keys()
            .copied()
            .filter(|namespace| namespace.stream_id() == stream_id)
            .collect::<Vec<_>>();
        for namespace in namespaces {
            self.wake_namespace(&namespace);
        }
    }

    fn prune_namespace(&mut self, namespace: &IggyNamespace) {
        let now = Instant::now();
        let Some(waiters) = self.waiters.get_mut(namespace) else {
            return;
        };
        waiters.retain(|waiter| {
            !waiter.wake_sender.is_closed()
                && waiter.deadline.is_none_or(|deadline| deadline > now)
        });
        if waiters.is_empty() {
            self.waiters.remove(namespace);
        }
    }
}

pub(crate) struct PollWaiterRegistration<'a> {
    namespace: IggyNamespace,
    id: u64,
    receiver: Receiver<()>,
    registry: &'a RefCell<PollWaiterRegistry>,
}

impl PollWaiterRegistration<'_> {
    pub(crate) async fn wait(&self) -> bool {
        self.receiver.recv().await.is_ok()
    }
}

impl Drop for PollWaiterRegistration<'_> {
    fn drop(&mut self) {
        self.registry.borrow_mut().remove(&self.namespace, self.id);
    }
}

impl IggyShard {
    pub(crate) fn register_poll_waiter(
        &self,
        namespace: IggyNamespace,
        timeout: Duration,
    ) -> Option<PollWaiterRegistration<'_>> {
        let (id, receiver) = self.poll_waiters.borrow_mut().register(namespace, timeout)?;
        Some(PollWaiterRegistration {
            namespace,
            id,
            receiver,
            registry: &self.poll_waiters,
        })
    }

    pub(crate) fn wake_poll_waiters(&self, namespace: &IggyNamespace) {
        self.poll_waiters.borrow_mut().wake_namespace(namespace);
    }

    pub(crate) fn wake_topic_poll_waiters(&self, stream_id: usize, topic_id: usize) {
        self.poll_waiters.borrow_mut().wake_topic(stream_id, topic_id);
    }

    pub(crate) fn wake_stream_poll_waiters(&self, stream_id: usize) {
        self.poll_waiters.borrow_mut().wake_stream(stream_id);
    }
}
