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

//! The upload of a fresh object: the messages of the batches a shard took
//! in one upload window, stored before any of them is committed.

use bytes::Bytes;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_object_store::{
    FreshObjectId, KeyLayout, ObjectStore, ObjectStoreError, PutMode, PutOptions, TierExecutor,
    TierJobCancelled, TierJobClass, TierJobRequest, TierScopeToken,
};

use crate::segment_upload::memory_body;

/// The result of an upload: the store's answer, or that the job's scope was
/// cancelled first.
pub type FreshUploadOutcome = Result<Result<(), ObjectStoreError>, TierJobCancelled>;

/// Stores the fresh objects of a shard, behind a pointer, so the shard does
/// not carry the executor's type.
pub trait FreshObjectUploader {
    /// Store `bytes` as the fresh object `object_id` under `scope`.
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome>;
}

/// Uploads through a tier executor, in its class for fresh uploads.
///
/// An id names one object for good, so the put creates the object and never
/// replaces one. A put that the store already took, which a retry after a
/// lost answer finds, counts as stored. So the caller can upload the same
/// bytes under the same id again after an upload that failed.
///
/// The object the retry finds counts as this one only when it has the same
/// length and the same CRC32C as the bytes sent. Two writers under one id
/// need two nodes of one replica id that started in the same microsecond,
/// and a reader takes what the store holds for what the id names. The store
/// reports the CRC32C it computed in its head. With a store that keeps none,
/// the upload reads the object back and compares the bytes, a read that
/// only a lost answer costs.
pub struct ExecutorFreshUploader<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorFreshUploader<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the uploads go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> FreshObjectUploader for ExecutorFreshUploader<E> {
    fn upload(
        &self,
        object_id: FreshObjectId,
        bytes: Bytes,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshUploadOutcome> {
        let key = self.layout.fresh_key(object_id.get());
        let request =
            TierJobRequest::new(TierJobClass::FreshUpload, scope).with_bytes(bytes.len() as u64);
        self.executor
            .submit(request, move |store| async move {
                let crc32c = crc32c::crc32c(&bytes);
                let options = PutOptions {
                    mode: PutMode::IfAbsent,
                    crc32c: Some(crc32c),
                    ..PutOptions::default()
                };
                match store.put(&key, memory_body(&bytes), &options).await {
                    Err(ObjectStoreError::PreconditionFailed { .. }) => {
                        if holds_sent_bytes(&*store, &key, &bytes, crc32c).await? {
                            Ok(())
                        } else {
                            Err(ObjectStoreError::PreconditionFailed { key })
                        }
                    }
                    put => put,
                }
            })
            .boxed_local()
    }
}

/// Whether the object under `key` is `sent`, whose CRC32C is `crc32c`.
async fn holds_sent_bytes<S: ObjectStore>(
    store: &S,
    key: &str,
    sent: &Bytes,
    crc32c: u32,
) -> Result<bool, ObjectStoreError> {
    let meta = store.head(key).await?;
    let size = sent.len() as u64;
    if meta.size != size {
        return Ok(false);
    }
    if let Some(stored) = meta.crc32c {
        return Ok(stored == crc32c);
    }
    Ok(store.get_range(key, 0..size).await?.bytes == *sent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_object_store::{
        DeleteOutcome, FreshObject, InlineTierExecutor, ListPage, ListRequest, MemoryStore,
        ObjectMeta, ObjectRange, PutBody, RecordingStore, StoreCall, TierJobHandle, TierScope,
    };
    use std::cell::RefCell;
    use std::future::Future;
    use std::ops::Range;
    use std::rc::Rc;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn uploader() -> ExecutorFreshUploader<InlineTierExecutor<MemoryStore>> {
        ExecutorFreshUploader::new(InlineTierExecutor::new(MemoryStore::new()), layout())
    }

    fn object(blob: &[u8]) -> Bytes {
        let mut object = FreshObject::new();
        object.push(blob);
        object.into_bytes()
    }

    #[compio::test]
    async fn given_a_fresh_object_when_uploading_should_store_it_under_the_key_of_its_id() {
        let uploader = uploader();
        let id = FreshObjectId::new(1, 2, 3, 4).unwrap();
        let bytes = object(b"messages");

        let outcome = uploader
            .upload(id, bytes.clone(), TierScope::new().token())
            .await;

        assert!(matches!(outcome, Ok(Ok(()))), "{outcome:?}");
        let store = uploader.executor().store();
        let key = layout().fresh_key(id.get());
        assert_eq!(store.keys(), std::slice::from_ref(&key));
        let stored = store.get_range(&key, 0..bytes.len() as u64).await.unwrap();
        assert_eq!(stored.bytes, bytes);
    }

    /// An executor that runs each job at once and keeps what it was asked
    /// for.
    struct Recording {
        store: Rc<MemoryStore>,
        requests: RefCell<Vec<(TierJobClass, u64)>>,
    }

    impl TierExecutor for Recording {
        type Store = MemoryStore;

        fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
        where
            T: 'static,
            F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
            Fut: Future<Output = T> + 'static,
        {
            self.requests
                .borrow_mut()
                .push((request.class(), request.bytes()));
            let job = job(Rc::clone(&self.store));
            TierJobHandle::new(async move { Ok(job.await) })
        }
    }

    /// Producers wait for the upload, so it must not queue behind the
    /// uploads of segments.
    #[compio::test]
    async fn given_a_fresh_object_when_uploading_should_ask_for_the_class_of_fresh_uploads() {
        let uploader = ExecutorFreshUploader::new(
            Recording {
                store: Rc::new(MemoryStore::new()),
                requests: RefCell::default(),
            },
            layout(),
        );
        let bytes = object(b"messages");
        let size = bytes.len() as u64;

        let outcome = uploader
            .upload(
                FreshObjectId::new(1, 2, 3, 4).unwrap(),
                bytes,
                TierScope::new().token(),
            )
            .await;

        assert!(matches!(outcome, Ok(Ok(()))), "{outcome:?}");
        assert_eq!(
            uploader.executor().requests.take(),
            [(TierJobClass::FreshUpload, size)]
        );
    }

    #[compio::test]
    async fn given_an_upload_the_store_already_took_when_uploading_again_should_count_as_stored() {
        let uploader = uploader();
        let id = FreshObjectId::new(1, 2, 3, 4).unwrap();
        let bytes = object(b"messages");
        let scope = TierScope::new();
        uploader
            .upload(id, bytes.clone(), scope.token())
            .await
            .unwrap()
            .unwrap();

        let again = uploader.upload(id, bytes, scope.token()).await;

        assert!(matches!(again, Ok(Ok(()))), "{again:?}");
    }

    #[compio::test]
    async fn given_another_object_under_the_id_when_uploading_should_refuse_and_keep_it() {
        let uploader = uploader();
        let id = FreshObjectId::new(1, 2, 3, 4).unwrap();
        let scope = TierScope::new();
        let first = object(b"messages");
        uploader
            .upload(id, first.clone(), scope.token())
            .await
            .unwrap()
            .unwrap();

        let other = uploader
            .upload(id, object(b"other messages"), scope.token())
            .await;

        assert!(matches!(
            other,
            Ok(Err(ObjectStoreError::PreconditionFailed { .. }))
        ));
        let store = uploader.executor().store();
        let key = layout().fresh_key(id.get());
        let stored = store.get_range(&key, 0..first.len() as u64).await.unwrap();
        assert_eq!(stored.bytes, first);
    }

    /// The reads of objects that reached `calls`.
    fn reads(calls: &[StoreCall]) -> Vec<StoreCall> {
        calls
            .iter()
            .filter(|call| matches!(call, StoreCall::GetRange { .. }))
            .cloned()
            .collect()
    }

    #[compio::test]
    async fn given_another_object_of_the_same_length_under_the_id_when_uploading_should_refuse_and_keep_it()
     {
        let store = RecordingStore::new(MemoryStore::new());
        let calls = store.calls();
        let uploader = ExecutorFreshUploader::new(InlineTierExecutor::new(store), layout());
        let id = FreshObjectId::new(1, 2, 3, 4).unwrap();
        let scope = TierScope::new();
        let first = object(b"messages");
        uploader
            .upload(id, first.clone(), scope.token())
            .await
            .unwrap()
            .unwrap();
        let other = object(b"massages");
        assert_eq!(other.len(), first.len());

        let again = uploader.upload(id, other, scope.token()).await;

        assert!(matches!(
            again,
            Ok(Err(ObjectStoreError::PreconditionFailed { .. }))
        ));
        let key = layout().fresh_key(id.get());
        let stored = uploader
            .executor()
            .store()
            .get_range(&key, 0..first.len() as u64)
            .await
            .unwrap();
        assert_eq!(stored.bytes, first);
        assert_eq!(reads(&calls.all()).len(), 1, "only the test reads");
    }

    /// A store that keeps no CRC32C of what it holds, as Azure Blob does.
    struct KeepsNoChecksum(RecordingStore<MemoryStore>);

    impl ObjectStore for KeepsNoChecksum {
        async fn put(
            &self,
            key: &str,
            body: PutBody,
            options: &PutOptions,
        ) -> Result<(), ObjectStoreError> {
            self.0.put(key, body, options).await
        }

        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> Result<ObjectRange, ObjectStoreError> {
            self.0.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
            let meta = self.0.head(key).await?;
            Ok(ObjectMeta {
                crc32c: None,
                ..meta
            })
        }

        async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
            self.0.delete(keys).await
        }

        async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
            self.0.list(request).await
        }
    }

    #[compio::test]
    async fn given_a_store_that_keeps_no_checksum_when_an_upload_finds_an_object_should_read_it_back_to_compare()
     {
        let store = RecordingStore::new(MemoryStore::new());
        let calls = store.calls();
        let uploader =
            ExecutorFreshUploader::new(InlineTierExecutor::new(KeepsNoChecksum(store)), layout());
        let id = FreshObjectId::new(1, 2, 3, 4).unwrap();
        let scope = TierScope::new();
        let first = object(b"messages");
        uploader
            .upload(id, first.clone(), scope.token())
            .await
            .unwrap()
            .unwrap();

        let again = uploader.upload(id, first.clone(), scope.token()).await;
        let other = uploader
            .upload(id, object(b"massages"), scope.token())
            .await;
        let shorter = uploader.upload(id, object(b"message"), scope.token()).await;

        assert!(matches!(again, Ok(Ok(()))), "{again:?}");
        for refused in [other, shorter] {
            assert!(matches!(
                refused,
                Ok(Err(ObjectStoreError::PreconditionFailed { .. }))
            ));
        }
        let read_back = StoreCall::GetRange {
            key: layout().fresh_key(id.get()),
            range: 0..first.len() as u64,
        };
        assert_eq!(reads(&calls.all()), [read_back.clone(), read_back]);
    }
}
