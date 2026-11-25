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

//! A memory store for the tests of the tier service and of the jobs that run
//! on it, which can fail, stall or slow down its calls.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ops::Range;
use std::time::Duration;

use futures::StreamExt;
use iggy_object_store::{
    DeleteOutcome, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutOptions,
};

#[derive(Default)]
pub struct TestStore {
    pub inner: MemoryStore,
    /// Every call, puts included.
    pub calls: Cell<usize>,
    pub puts: Cell<usize>,
    /// Errors that the next calls return, in order, before a call reaches the
    /// memory store.
    pub failures: RefCell<VecDeque<ObjectStoreError>>,
    /// Calls that never answer.
    pub hangs: Cell<bool>,
    /// The wait before a put takes each piece of its body, and before it
    /// takes the end of the body.
    pub piece_delay: Cell<Duration>,
    /// A put that has taken this many pieces takes no more.
    pub stall_after_pieces: Cell<Option<usize>>,
    /// A put that has taken its whole body never answers.
    pub hang_after_body: Cell<bool>,
    /// A put takes its whole body, keeps none of it and succeeds, so a test
    /// can send a large body without holding it.
    pub discard_bodies: Cell<bool>,
}

impl TestStore {
    /// The start of every call: count it, then hang or fail as the test set.
    async fn enter(&self) -> Result<(), ObjectStoreError> {
        self.calls.set(self.calls.get() + 1);
        if self.hangs.get() {
            std::future::pending::<()>().await;
        }
        self.failures.borrow_mut().pop_front().map_or(Ok(()), Err)
    }
}

impl ObjectStore for TestStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        self.puts.set(self.puts.get() + 1);
        self.enter().await?;
        let mut pieces = body.into_pieces();
        let mut bytes = Vec::new();
        let mut taken = 0;
        loop {
            if self.stall_after_pieces.get() == Some(taken) {
                std::future::pending::<()>().await;
            }
            compio::time::sleep(self.piece_delay.get()).await;
            let Some(piece) = pieces.next().await else {
                break;
            };
            let piece = piece.map_err(ObjectStoreError::Body)?;
            if !self.discard_bodies.get() {
                bytes.extend_from_slice(&piece);
            }
            taken += 1;
        }
        if self.hang_after_body.get() {
            std::future::pending::<()>().await;
        }
        if self.discard_bodies.get() {
            return Ok(());
        }
        self.inner
            .put(key, PutBody::from_bytes(bytes), options)
            .await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.enter().await?;
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.enter().await?;
        self.inner.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        self.enter().await?;
        self.inner.delete(keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.enter().await?;
        self.inner.list(request).await
    }
}
