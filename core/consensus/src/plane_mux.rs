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
    #[must_use]
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    #[must_use]
    pub const fn inner(&self) -> &T {
        &self.inner
    }

    pub const fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

// Consensus runs on single-threaded compio shards; futures are intentionally !Send.
#[allow(clippy::future_not_send)]
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

// Consensus runs on single-threaded compio shards; futures are intentionally !Send.
#[allow(clippy::future_not_send)]
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

pub trait MetadataHandle {
    type Metadata;
    fn metadata(&self) -> &Self::Metadata;
    fn metadata_mut(&mut self) -> &mut Self::Metadata;
}

pub trait PartitionsHandle {
    type Partitions;
    fn partitions(&self) -> &Self::Partitions;
    fn partitions_mut(&mut self) -> &mut Self::Partitions;
}

impl<M, Tail> MetadataHandle for (M, Tail) {
    type Metadata = M;
    fn metadata(&self) -> &Self::Metadata {
        &self.0
    }
    fn metadata_mut(&mut self) -> &mut Self::Metadata {
        &mut self.0
    }
}

impl<M, P> PartitionsHandle for (M, (P, ())) {
    type Partitions = P;
    fn partitions(&self) -> &Self::Partitions {
        &self.1.0
    }
    fn partitions_mut(&mut self) -> &mut Self::Partitions {
        &mut self.1.0
    }
}

impl<T: MetadataHandle> MetadataHandle for MuxPlane<T> {
    type Metadata = T::Metadata;
    fn metadata(&self) -> &Self::Metadata {
        self.inner.metadata()
    }
    fn metadata_mut(&mut self) -> &mut Self::Metadata {
        self.inner.metadata_mut()
    }
}

impl<T: PartitionsHandle> PartitionsHandle for MuxPlane<T> {
    type Partitions = T::Partitions;
    fn partitions(&self) -> &Self::Partitions {
        self.inner.partitions()
    }
    fn partitions_mut(&mut self) -> &mut Self::Partitions {
        self.inner.partitions_mut()
    }
}

// Consensus runs on single-threaded compio shards; futures are intentionally !Send.
#[allow(clippy::future_not_send)]
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
