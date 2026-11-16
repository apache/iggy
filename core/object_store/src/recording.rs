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

//! A store that records the calls that reach it, for tests that show which
//! calls get to a provider.

use std::cell::RefCell;
use std::ops::Range;
use std::rc::Rc;

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore, PutBody, PutMode,
    PutOptions,
};

/// One call that reached a [`RecordingStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreCall {
    Put { key: String, mode: PutMode },
    GetRange { key: String, range: Range<u64> },
    Head { key: String },
    Delete { keys: Vec<String> },
    List { prefix: String },
}

impl StoreCall {
    /// Whether the call can change the store: a put, conditional or not, or
    /// a delete.
    #[must_use]
    pub const fn mutates(&self) -> bool {
        matches!(self, Self::Put { .. } | Self::Delete { .. })
    }
}

/// The calls that a [`RecordingStore`] got, in order. A test keeps this
/// handle after the store moves into the code under test.
#[derive(Debug, Clone, Default)]
pub struct StoreCalls {
    calls: Rc<RefCell<Vec<StoreCall>>>,
}

impl StoreCalls {
    #[must_use]
    pub fn all(&self) -> Vec<StoreCall> {
        self.calls.borrow().clone()
    }

    /// The calls that can change the store, see [`StoreCall::mutates`].
    #[must_use]
    pub fn mutations(&self) -> Vec<StoreCall> {
        self.calls
            .borrow()
            .iter()
            .filter(|call| call.mutates())
            .cloned()
            .collect()
    }

    fn push(&self, call: StoreCall) {
        self.calls.borrow_mut().push(call);
    }
}

/// An [`ObjectStore`] that records each call and then passes it to the
/// store it wraps. A call is recorded before the inner store answers, so a
/// call that never ends still shows.
#[derive(Debug, Default)]
pub struct RecordingStore<S> {
    inner: S,
    calls: StoreCalls,
}

impl<S> RecordingStore<S> {
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            calls: StoreCalls::default(),
        }
    }

    /// The calls that this store got, and gets from now on.
    #[must_use]
    pub fn calls(&self) -> StoreCalls {
        self.calls.clone()
    }
}

impl<S: ObjectStore> ObjectStore for RecordingStore<S> {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        self.calls.push(StoreCall::Put {
            key: key.to_owned(),
            mode: options.mode.clone(),
        });
        self.inner.put(key, body, options).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.calls.push(StoreCall::GetRange {
            key: key.to_owned(),
            range: range.clone(),
        });
        self.inner.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.calls.push(StoreCall::Head {
            key: key.to_owned(),
        });
        self.inner.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        self.calls.push(StoreCall::Delete {
            keys: keys.to_vec(),
        });
        self.inner.delete(keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.calls.push(StoreCall::List {
            prefix: request.prefix.to_owned(),
        });
        self.inner.list(request).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::memory::MemoryStore;

    #[test]
    fn given_calls_when_recording_should_keep_each_in_order_and_pass_it_on() {
        let store = RecordingStore::new(MemoryStore::new());
        let calls = store.calls();
        let key = "iggy/meta/identity".to_owned();

        block_on(store.put(
            &key,
            PutBody::from_bytes(Bytes::from_static(b"claim")),
            &PutOptions::default(),
        ))
        .unwrap();
        block_on(store.get_range(&key, 0..5)).unwrap();
        block_on(store.head(&key)).unwrap();
        block_on(store.list(&ListRequest::new("iggy/"))).unwrap();
        let deleted = block_on(store.delete(std::slice::from_ref(&key))).unwrap();

        assert!(deleted.failed.is_empty());

        assert_eq!(
            calls.all(),
            vec![
                StoreCall::Put {
                    key: key.clone(),
                    mode: PutMode::Overwrite,
                },
                StoreCall::GetRange {
                    key: key.clone(),
                    range: 0..5,
                },
                StoreCall::Head { key: key.clone() },
                StoreCall::List {
                    prefix: "iggy/".to_owned(),
                },
                StoreCall::Delete {
                    keys: vec![key.clone()],
                },
            ]
        );
        assert_eq!(
            calls.mutations(),
            vec![
                StoreCall::Put {
                    key: key.clone(),
                    mode: PutMode::Overwrite,
                },
                StoreCall::Delete { keys: vec![key] },
            ]
        );
    }
}
