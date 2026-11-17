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
    FreshObjectId, KeyLayout, ObjectStore, ObjectStoreError, PutBody, PutMode, PutOptions,
    TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScopeToken,
};

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
                let options = PutOptions {
                    mode: PutMode::IfAbsent,
                    crc32c: Some(crc32c::crc32c(&bytes)),
                    ..PutOptions::default()
                };
                let size = bytes.len() as u64;
                match store.put(&key, PutBody::from_bytes(bytes), &options).await {
                    // The store holds an object under the id. Only this
                    // upload writes it, so it is this one when it is as long.
                    Err(ObjectStoreError::PreconditionFailed { .. }) => {
                        match store.head(&key).await {
                            Ok(meta) if meta.size == size => Ok(()),
                            Ok(_) => Err(ObjectStoreError::PreconditionFailed { key }),
                            Err(error) => Err(error),
                        }
                    }
                    put => put,
                }
            })
            .boxed_local()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_object_store::{
        FreshObject, InlineTierExecutor, MemoryStore, TierJobHandle, TierScope,
    };
    use std::cell::RefCell;
    use std::future::Future;
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
}
