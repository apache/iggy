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

use crate::{
    AckMessage, Consensus, Plane, PlaneIdentity, Project, ReplicateMessage, RequestMessage,
};
use iggy_common::variadic;

#[derive(Debug)]
pub struct MuxPlane<T> {
    inner: T,
}

impl<T> MuxPlane<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<C, T> Plane<C> for MuxPlane<T>
where
    C: Consensus,
    T: Plane<C>,
{
    async fn on_request(&self, message: RequestMessage<C>)
    where
        RequestMessage<C>: Project<ReplicateMessage<C>, C, Consensus = C> + Clone,
    {
        self.inner.on_request(message).await;
    }

    async fn on_replicate(&self, message: ReplicateMessage<C>)
    where
        ReplicateMessage<C>: Project<AckMessage<C>, C, Consensus = C> + Clone,
    {
        self.inner.on_replicate(message).await;
    }

    async fn on_ack(&self, message: AckMessage<C>) {
        self.inner.on_ack(message).await;
    }
}

impl<C> Plane<C> for ()
where
    C: Consensus,
{
    async fn on_request(&self, _message: RequestMessage<C>)
    where
        RequestMessage<C>: Project<ReplicateMessage<C>, C, Consensus = C> + Clone,
    {
    }

    async fn on_replicate(&self, _message: ReplicateMessage<C>)
    where
        ReplicateMessage<C>: Project<AckMessage<C>, C, Consensus = C> + Clone,
    {
    }

    async fn on_ack(&self, _message: AckMessage<C>) {}
}

impl<C, Head, Tail> Plane<C> for variadic!(Head, ...Tail)
where
    C: Consensus,
    Head: Plane<C> + PlaneIdentity<C>,
    Tail: Plane<C>,
{
    async fn on_request(&self, message: RequestMessage<C>)
    where
        RequestMessage<C>: Project<ReplicateMessage<C>, C, Consensus = C> + Clone,
    {
        if self.0.is_applicable(&message) {
            self.0.on_request(message).await;
        } else {
            self.1.on_request(message).await;
        }
    }

    async fn on_replicate(&self, message: ReplicateMessage<C>)
    where
        ReplicateMessage<C>: Project<AckMessage<C>, C, Consensus = C> + Clone,
    {
        if self.0.is_applicable(&message) {
            self.0.on_replicate(message).await;
        } else {
            self.1.on_replicate(message).await;
        }
    }

    async fn on_ack(&self, message: AckMessage<C>) {
        if self.0.is_applicable(&message) {
            self.0.on_ack(message).await;
        } else {
            self.1.on_ack(message).await;
        }
    }
}
