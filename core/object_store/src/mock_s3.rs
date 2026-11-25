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

use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, SystemTime};

use crate::error::ObjectStoreError;
use crate::memory::{Objects, checked_crc32c, read_body};
use crate::store::{
    DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore, PutBody,
    PutOptions, validate_delete, validate_key, validate_metadata, validate_prefix, validate_range,
};

/// An [`ObjectStore`] in the memory of the process that answers each call
/// after the latency of a remote store, for benchmarks and tests only.
///
/// Clones share their objects, so every shard of a node reads what any
/// other shard put. Nothing is written anywhere else, and the objects go
/// when the last clone does.
///
/// A put takes its whole body, waits out its latency and commits, so a put
/// dropped while it waits stores nothing. A read, head or listing looks the
/// store up first and answers after its latency. Each call waits on the
/// runtime's timer on its own, so calls in flight overlap as they do on a
/// remote store. A call with no latency answers without the timer.
#[derive(Debug, Clone, Default)]
pub struct MockS3Store {
    objects: Arc<RwLock<Objects>>,
    latency: MockS3Latency,
}

/// What [`MockS3Store`] adds to the time of each call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MockS3Latency {
    /// Each put, from the end of its body to its commit.
    pub put: Duration,
    /// Each ranged read.
    pub get: Duration,
    /// Each head, delete and listing.
    pub other: Duration,
    /// How fast the body of a put or the bytes of a read move, on top of the
    /// latency. Zero is no limit.
    pub bytes_per_second: u64,
}

impl MockS3Store {
    /// An empty store whose calls take `latency`.
    #[must_use]
    pub fn new(latency: MockS3Latency) -> Self {
        Self {
            objects: Arc::default(),
            latency,
        }
    }

    #[must_use]
    pub const fn latency(&self) -> MockS3Latency {
        self.latency
    }

    /// Every key, in byte order.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.read(Objects::keys)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.read(Objects::len)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // The guards never cross an await, so a shard never parks another one
    // behind a lock.
    fn read<T>(&self, call: impl FnOnce(&Objects) -> T) -> T {
        call(&self.objects.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn write<T>(&self, call: impl FnOnce(&mut Objects) -> T) -> T {
        call(&mut self.objects.write().unwrap_or_else(PoisonError::into_inner))
    }
}

impl MockS3Latency {
    /// The time of a call with a fixed latency of `fixed` that moves `bytes`.
    #[must_use]
    pub fn of(&self, fixed: Duration, bytes: u64) -> Duration {
        if self.bytes_per_second == 0 || bytes == 0 {
            return fixed;
        }
        let nanos = u128::from(bytes) * 1_000_000_000 / u128::from(self.bytes_per_second);
        fixed.saturating_add(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ))
    }
}

impl ObjectStore for MockS3Store {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let bytes = read_body(body).await?;
        let crc32c = checked_crc32c(key, &bytes, options);
        wait(self.latency.of(self.latency.put, bytes.len() as u64)).await;
        let crc32c = crc32c?;
        self.write(|objects| objects.insert(key, bytes, crc32c, options, SystemTime::now()))
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        validate_key(key)?;
        validate_range(&range)?;
        let read = self.read(|objects| objects.get_range(key, &range));
        let bytes = read.as_ref().map_or(0, |read| read.bytes.len() as u64);
        wait(self.latency.of(self.latency.get, bytes)).await;
        read
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let meta = self.read(|objects| objects.head(key));
        wait(self.latency.other).await;
        meta
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        validate_delete(keys)?;
        wait(self.latency.other).await;
        Ok(self.write(|objects| objects.delete(keys)))
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        let page = self.read(|objects| objects.list(request));
        wait(self.latency.other).await;
        Ok(page)
    }
}

async fn wait(duration: Duration) {
    if !duration.is_zero() {
        compio::time::sleep(duration).await;
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::future::{self, Either};

    use super::*;
    use crate::contract::shard_runtime;

    const PUT: Duration = Duration::from_millis(60);
    const GET: Duration = Duration::from_millis(40);
    const OTHER: Duration = Duration::from_millis(20);

    fn slow() -> MockS3Store {
        MockS3Store::new(MockS3Latency {
            put: PUT,
            get: GET,
            other: OTHER,
            bytes_per_second: 0,
        })
    }

    fn body(bytes: &'static [u8]) -> PutBody {
        PutBody::from_bytes(Bytes::from_static(bytes))
    }

    async fn timed<T>(call: impl Future<Output = T>) -> (T, Duration) {
        let started = Instant::now();
        let output = call.await;
        (output, started.elapsed())
    }

    #[test]
    fn given_latencies_when_calling_should_wait_out_the_latency_of_each_call() {
        let store = slow();
        shard_runtime().block_on(async {
            let options = PutOptions::default();
            let (put, put_took) = timed(store.put("a/b", body(b"bytes"), &options)).await;
            let (read, get_took) = timed(store.get_range("a/b", 0..u64::MAX)).await;
            let (head, head_took) = timed(store.head("a/b")).await;
            let (page, list_took) = timed(store.list(&ListRequest::new("a/"))).await;
            let (missing, missing_took) = timed(store.get_range("a/missing", 0..1)).await;
            let (deleted, delete_took) = timed(store.delete(&["a/b".to_owned()])).await;

            put.unwrap();
            assert_eq!(read.unwrap().bytes, "bytes");
            assert_eq!(head.unwrap().size, 5);
            assert_eq!(page.unwrap().objects.len(), 1);
            assert!(matches!(missing, Err(ObjectStoreError::NotFound { .. })));
            assert!(deleted.unwrap().is_complete());
            assert!(put_took >= PUT, "put took {put_took:?}");
            assert!(get_took >= GET, "get took {get_took:?}");
            assert!(missing_took >= GET, "a missing key took {missing_took:?}");
            for took in [head_took, list_took, delete_took] {
                assert!(took >= OTHER, "took {took:?}");
                assert!(took < PUT, "took {took:?}, the latency of a put");
            }
            assert!(store.is_empty());
        });
    }

    #[test]
    fn given_concurrent_puts_when_waiting_should_overlap() {
        let store = slow();
        let options = PutOptions::default();
        let keys: Vec<String> = (0..8).map(|index| format!("a/{index}")).collect();

        let (results, took) = shard_runtime().block_on(timed(future::join_all(
            keys.iter()
                .map(|key| store.put(key, body(b"bytes"), &options)),
        )));

        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(store.keys(), keys);
        assert!(took >= PUT, "took {took:?}");
        assert!(
            took < PUT * 4,
            "took {took:?}, as if the puts ran one by one"
        );
    }

    #[test]
    fn given_a_put_dropped_while_it_waits_when_reading_should_find_nothing() {
        let store = slow();
        let options = PutOptions::default();

        shard_runtime().block_on(async {
            let put = Box::pin(store.put("a/b", body(b"bytes"), &options));
            let early = Box::pin(compio::time::sleep(PUT / 4));
            assert!(matches!(future::select(put, early).await, Either::Right(_)));
        });

        assert!(store.is_empty());
    }

    #[test]
    fn given_a_rate_when_moving_bytes_should_add_their_time_to_the_latency() {
        let latency = MockS3Latency {
            bytes_per_second: 1024 * 1024,
            ..MockS3Latency::default()
        };

        assert_eq!(latency.of(GET, 0), GET);
        assert_eq!(
            latency.of(GET, 512 * 1024),
            GET + Duration::from_millis(500)
        );
        assert_eq!(
            MockS3Latency::default().of(GET, u64::MAX),
            GET,
            "zero is no limit"
        );
        assert_eq!(
            MockS3Latency {
                bytes_per_second: 1,
                ..latency
            }
            .of(Duration::ZERO, u64::MAX),
            Duration::from_nanos(u64::MAX),
            "saturates instead of overflowing"
        );
    }

    #[test]
    fn given_a_body_of_one_piece_when_putting_should_keep_its_bytes_without_a_copy() {
        let store = MockS3Store::default();
        let bytes = Bytes::from(vec![7u8; 4096]);

        let read = shard_runtime().block_on(async {
            store
                .put(
                    "a/b",
                    PutBody::from_bytes(bytes.clone()),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
            store.get_range("a/b", 0..u64::MAX).await.unwrap()
        });

        assert_eq!(read.bytes.as_ptr(), bytes.as_ptr());
    }

    #[test]
    fn given_clones_on_other_threads_when_calling_should_share_one_store() {
        let store = MockS3Store::default();
        let writers: Vec<_> = (0..4)
            .map(|shard| {
                let store = store.clone();
                thread::spawn(move || {
                    shard_runtime().block_on(store.put(
                        &format!("a/{shard}"),
                        body(b"bytes"),
                        &PutOptions::default(),
                    ))
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap().unwrap();
        }

        let reader = store.clone();
        let listed = thread::spawn(move || {
            shard_runtime().block_on(async {
                let page = reader.list(&ListRequest::new("a/")).await.unwrap();
                assert!(
                    reader
                        .delete(&["a/0".to_owned()])
                        .await
                        .unwrap()
                        .is_complete()
                );
                page.objects.len()
            })
        })
        .join()
        .unwrap();

        assert_eq!(listed, 4);
        assert_eq!(store.keys(), ["a/1", "a/2", "a/3"]);
    }
}
