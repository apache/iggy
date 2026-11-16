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

//! A store that only reads, for the bucket of another cluster.
//!
//! A cluster reads the objects of its peers and never changes them, so no
//! put, conditional create or delete may reach a peer's store, whatever code
//! makes the call. [`ReadOnlyStore`] keeps its provider behind a trait that
//! has only the reads, so no call that it forwards can write.

use std::ops::Range;

use futures::FutureExt;
use futures::future::LocalBoxFuture;

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore, PutBody, PutOptions,
};

/// An [`ObjectStore`] that passes reads to its provider and refuses every
/// put and delete with [`ObjectStoreError::ReadOnly`], before any I/O.
///
/// The refusal is permanent, so a retry policy never repeats it. A
/// conditional create is a put, and it is refused the same way.
pub struct ReadOnlyStore {
    reader: Box<dyn ObjectReader>,
}

impl ReadOnlyStore {
    /// A store that reads through `provider`. Nothing keeps a handle that can
    /// write through it.
    #[must_use]
    pub fn new(provider: impl ObjectStore + 'static) -> Self {
        Self {
            reader: Box::new(Reads(provider)),
        }
    }
}

impl ObjectStore for ReadOnlyStore {
    async fn put(
        &self,
        key: &str,
        _body: PutBody,
        _options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::ReadOnly {
            operation: "PUT",
            key: key.to_owned(),
        })
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.reader.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.reader.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        Err(ObjectStoreError::ReadOnly {
            operation: "DELETE",
            key: keys.first().cloned().unwrap_or_default(),
        })
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.reader.list(request).await
    }
}

/// The reads of a provider: all that a [`ReadOnlyStore`] keeps of it.
trait ObjectReader {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>>;

    fn head<'a>(&'a self, key: &'a str)
    -> LocalBoxFuture<'a, Result<ObjectMeta, ObjectStoreError>>;

    fn list<'a>(
        &'a self,
        request: &'a ListRequest<'a>,
    ) -> LocalBoxFuture<'a, Result<ListPage, ObjectStoreError>>;
}

/// A provider seen through its reads only.
struct Reads<S>(S);

impl<S: ObjectStore> ObjectReader for Reads<S> {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        self.0.get_range(key, range).boxed_local()
    }

    fn head<'a>(
        &'a self,
        key: &'a str,
    ) -> LocalBoxFuture<'a, Result<ObjectMeta, ObjectStoreError>> {
        self.0.head(key).boxed_local()
    }

    fn list<'a>(
        &'a self,
        request: &'a ListRequest<'a>,
    ) -> LocalBoxFuture<'a, Result<ListPage, ObjectStoreError>> {
        self.0.list(request).boxed_local()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::identity::{IdentityRead, StoreIdentity};
    use crate::layout::KeyLayout;
    use crate::memory::MemoryStore;
    use crate::recording::{RecordingStore, StoreCall};
    use crate::store::PutMode;

    const PEER: StoreIdentity = StoreIdentity { cluster_uid: 7 };

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    /// A peer's store that already holds the peer's claim, behind a recorder.
    fn claimed_peer() -> RecordingStore<MemoryStore> {
        let store = MemoryStore::new();
        assert!(block_on(PEER.claim(&store, &layout())).unwrap());
        RecordingStore::new(store)
    }

    fn refused(result: &Result<impl std::fmt::Debug, ObjectStoreError>, operation: &str) {
        assert!(
            matches!(
                result,
                Err(ObjectStoreError::ReadOnly { operation: refused, .. }) if *refused == operation
            ),
            "{result:?}"
        );
    }

    #[test]
    fn given_writes_when_calling_should_refuse_each_before_the_provider() {
        let provider = claimed_peer();
        let calls = provider.calls();
        let store = ReadOnlyStore::new(provider);
        let key = layout().identity_key();
        let overwrite = PutOptions::default();
        let if_absent = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };

        for options in [&overwrite, &if_absent] {
            refused(
                &block_on(store.put(&key, PutBody::from_bytes(Bytes::new()), options)),
                "PUT",
            );
        }
        refused(
            &block_on(StoreIdentity { cluster_uid: 8 }.claim(&store, &layout())),
            "PUT",
        );
        refused(
            &block_on(store.delete(std::slice::from_ref(&key))),
            "DELETE",
        );
        refused(&block_on(store.delete(&[])), "DELETE");

        assert_eq!(calls.all(), Vec::new(), "no write reached the provider");
        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(PEER),
            "the peer's claim is unchanged"
        );
    }

    #[test]
    fn given_reads_when_calling_should_pass_each_to_the_provider() {
        let provider = claimed_peer();
        let calls = provider.calls();
        let store = ReadOnlyStore::new(provider);
        let key = layout().identity_key();

        assert_eq!(
            block_on(StoreIdentity::read(&store, &layout())).unwrap(),
            IdentityRead::Found(PEER)
        );
        assert!(block_on(store.head(&key)).is_ok());
        let page = block_on(store.list(&ListRequest::new("iggy/"))).unwrap();

        assert_eq!(
            page.objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec![key.as_str()]
        );
        assert!(
            matches!(
                calls.all().as_slice(),
                [
                    StoreCall::GetRange { key: read, .. },
                    StoreCall::Head { key: headed },
                    StoreCall::List { prefix },
                ] if *read == key && *headed == key && prefix == "iggy/"
            ),
            "{:?}",
            calls.all()
        );
        assert_eq!(calls.mutations(), Vec::new());
    }
}
