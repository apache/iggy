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

use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use iggy_common::{IggyByteSize, IggyError};
use std::fmt::Debug;

// TODO: Will have to revisit this Journal abstraction....
// I don't like that it has to leak impl detail via the `Inner` struct in order to be functional.

#[derive(Default, Debug)]
pub struct Inner {
    pub base_offset: u64,
    pub current_offset: u64,
    pub first_timestamp: u64,
    pub end_timestamp: u64,
    pub messages_count: u32,
    pub size: IggyByteSize,
}

#[derive(Default, Debug)]
pub struct MemoryMessageJournal {
    batches: IggyMessagesBatchSet,
    inner: Inner,
}

impl Clone for MemoryMessageJournal {
    fn clone(&self) -> Self {
        Self {
            batches: Default::default(),
            inner: Default::default(),
        }
    }
}

impl Journal for MemoryMessageJournal {
    type Container = IggyMessagesBatchSet;
    type Entry = IggyMessagesBatchMut;
    type Inner = Inner;
    type AppendResult = Result<(u32, u32), IggyError>;

    fn append(&mut self, entry: Self::Entry) -> Self::AppendResult {
        let batch_messages_count = entry.count();
        tracing::trace!(
            "Coalescing batch with base_offset: {}, current_offset: {}, self.messages_count: {}, batch.count: {}",
            self.inner.base_offset,
            self.inner.current_offset,
            self.inner.messages_count,
            batch_messages_count
        );

        let batch_size = entry.size();
        let first_timestamp = entry.first_timestamp().unwrap();
        let last_timestamp = entry.last_timestamp().unwrap();
        self.batches.add_batch(entry);

        if self.inner.first_timestamp == 0 {
            self.inner.first_timestamp = first_timestamp;
        }
        self.inner.end_timestamp = last_timestamp;
        self.inner.messages_count += batch_messages_count;
        self.inner.current_offset = self.inner.base_offset + self.inner.messages_count as u64 - 1;
        self.inner.size = IggyByteSize::from(self.inner.size.as_bytes_u64() + batch_size as u64);

        Ok((self.inner.messages_count, self.inner.size.as_bytes_u32()))
    }

    async fn flush(&self) -> Result<(), IggyError> {
        Ok(())
    }

    fn init(&mut self, inner: Self::Inner) {
        self.inner = inner
    }

    fn get<U>(&self, filter: impl FnOnce(&Self::Container) -> U) -> U {
        filter(&self.batches)
    }

    fn commit(&mut self) -> Self::Container {
        self.inner.base_offset = self.inner.current_offset + 1;
        self.inner.first_timestamp = 0;
        self.inner.end_timestamp = 0;
        self.inner.size = IggyByteSize::default();
        self.inner.messages_count = 0;
        std::mem::take(&mut self.batches)
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub trait Journal {
    type Container;
    type Entry;
    type Inner;
    type AppendResult;

    fn init(&mut self, inner: Self::Inner);

    fn append(&mut self, entry: Self::Entry) -> Self::AppendResult;

    fn get<U>(&self, filter: impl FnOnce(&Self::Container) -> U) -> U;

    fn commit(&mut self) -> Self::Container;

    fn is_empty(&self) -> bool;

    fn inner(&self) -> &Self::Inner;

    // `flush` is only useful in case of an journal that has disk backed WAL.
    // This could be merged together with `append`, but not doing this for two reasons.
    // 1. In case of the `Journal` being used as part of structure that utilizes interior mutability, async with borrow_mut is not possible.
    // 2. Having it as separate function allows for more optimal usage patterns, e.g. batching multiple appends before flushing.
    fn flush(&self) -> impl Future<Output = Result<(), IggyError>>;
}
