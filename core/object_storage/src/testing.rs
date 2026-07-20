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

//! Shared test suite for every [`ObjectStore`] backend

use bytes::Bytes;

use crate::{ConditionalWriteSupport, ObjectStorageError, ObjectStore, S3_MIN_PART_SIZE};

/// Deterministic byte pattern of `len`
pub fn pattern(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// put then get the whole object back and verify it's byte-identical
pub async fn assert_put_get_roundtrip<S: ObjectStore + ?Sized>(store: &S) {
    for len in [11usize, 1024, 1024 * 1024] {
        let key = format!("roundtrip/{len}");
        let data = pattern(len);
        store.put(&key, data.clone()).await.unwrap();
        let got = store.get_range(&key, 0..len as u64).await.unwrap();
        assert_eq!(got, data, "roundtrip mismatch at len {len}");
    }
}

/// second put at the same key replaces the first
pub async fn assert_overwrite<S: ObjectStore + ?Sized>(store: &S) {
    let key = "overwrite/key";
    store.put(key, pattern(100)).await.unwrap();
    store.put(key, pattern(200)).await.unwrap();
    assert_eq!(store.head(key).await.unwrap().size, 200);
}

/// `get_range` returns exactly the requested sub-range
pub async fn assert_get_subrange<S: ObjectStore + ?Sized>(store: &S) {
    let key = "subrange/key";
    let data = pattern(1000);
    store.put(key, data.clone()).await.unwrap();
    let got = store.get_range(key, 250..750).await.unwrap();
    assert_eq!(got, data.slice(250..750));
}

/// An out-of-range read errors instead of silently clamping
pub async fn assert_get_range_out_of_bounds<S: ObjectStore + ?Sized>(store: &S) {
    let key = "oob/key";
    store.put(key, pattern(100)).await.unwrap();
    let err = store.get_range(key, 50..500).await.unwrap_err();
    assert!(
        matches!(err, ObjectStorageError::RangeOutOfBounds { .. }),
        "expected RangeOutOfBounds, got {err:?}"
    );
}

/// `get_range` / `head` on a missing key report NotFound
pub async fn assert_missing_key_not_found<S: ObjectStore + ?Sized>(store: &S) {
    let err = store.get_range("missing/key", 0..1).await.unwrap_err();
    assert!(
        matches!(err, ObjectStorageError::NotFound(_)),
        "get_range: {err:?}"
    );
    let err = store.head("missing/key").await.unwrap_err();
    assert!(
        matches!(err, ObjectStorageError::NotFound(_)),
        "head: {err:?}"
    );
}

/// `delete` must be idempotent and remove the object
pub async fn assert_delete_idempotent<S: ObjectStore + ?Sized>(store: &S) {
    let key = "delete/key";
    store.put(key, pattern(10)).await.unwrap();
    store.delete(key).await.unwrap();
    store.delete(key).await.unwrap(); // second delete is a no-op success
    let err = store.head(key).await.unwrap_err();
    assert!(matches!(err, ObjectStorageError::NotFound(_)), "{err:?}");
}

/// `list_prefix` returns matching keys in lexicographic order and paginates
pub async fn assert_list_prefix_ordered_and_paginated<S: ObjectStore + ?Sized>(store: &S) {
    const COUNT: usize = 1001;
    for i in 0..COUNT {
        store
            .put(&format!("listed/{i:05}"), pattern(1))
            .await
            .unwrap();
    }
    store.put("other/key", pattern(1)).await.unwrap();

    let listed = store.list_prefix("listed/").await.unwrap();
    assert_eq!(listed.len(), COUNT, "pagination lost keys");
    assert!(
        listed.iter().all(|meta| meta.key.starts_with("listed/")),
        "prefix filter leaked non-matching keys"
    );
    let keys: Vec<&str> = listed.iter().map(|meta| meta.key.as_str()).collect();
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(keys, sorted, "list_prefix not lexicographically ordered");
}

/// multipart upload round-trips as their concatenation
pub async fn assert_multipart_roundtrip<S: ObjectStore + ?Sized>(store: &S) {
    let key = "multipart/key";
    let part = pattern(S3_MIN_PART_SIZE as usize);
    let mut upload = store.put_multipart(key, S3_MIN_PART_SIZE).await.unwrap();
    upload.upload_part(part.clone()).await.unwrap();
    upload.upload_part(part.clone()).await.unwrap();
    upload.complete().await.unwrap();

    let total = S3_MIN_PART_SIZE * 2;
    assert_eq!(store.head(key).await.unwrap().size, total);
    let got = store.get_range(key, 0..total).await.unwrap();
    assert_eq!(&got[..part.len()], &part[..]);
    assert_eq!(&got[part.len()..], &part[..]);
}

/// aborted multipart upload leaves nothing behind
pub async fn assert_multipart_abort<S: ObjectStore + ?Sized>(store: &S) {
    let key = "multipart/aborted";
    let mut upload = store.put_multipart(key, S3_MIN_PART_SIZE).await.unwrap();
    upload
        .upload_part(pattern(S3_MIN_PART_SIZE as usize))
        .await
        .unwrap();
    upload.abort().await.unwrap();
    let err = store.head(key).await.unwrap_err();
    assert!(matches!(err, ObjectStorageError::NotFound(_)), "{err:?}");
}

/// part size below the S3 minimum is rejected early
pub async fn assert_part_size_floor<S: ObjectStore + ?Sized>(store: &S) {
    let result = store.put_multipart("floor/key", S3_MIN_PART_SIZE - 1).await;
    assert!(
        matches!(result, Err(ObjectStorageError::PartSizeTooSmall { .. })),
        "expected PartSizeTooSmall for a sub-minimum part size"
    );
}

/// conditional create branched on the backend's advertised capability:
/// * `Atomic` backends reject the second create
/// * `BestEffort` backends overwrite
pub async fn assert_put_if_absent_capability_aware<S: ObjectStore + ?Sized>(store: &S) {
    let key = "conditional/key";
    let first = pattern(10);
    let second = pattern(20);
    store.put_if_absent(key, first.clone()).await.unwrap();

    match store.conditional_write_support() {
        ConditionalWriteSupport::Atomic => {
            let err = store.put_if_absent(key, second).await.unwrap_err();
            assert!(
                matches!(err, ObjectStorageError::AlreadyExists(_)),
                "{err:?}"
            );
            assert_eq!(store.head(key).await.unwrap().size, first.len() as u64);
        }
        ConditionalWriteSupport::BestEffort => {
            // Racy contract: the second create is allowed to win, and mustn't error
            store.put_if_absent(key, second.clone()).await.unwrap();
            assert_eq!(store.head(key).await.unwrap().size, second.len() as u64);
        }
    }
}

/// Runs the shared [`ObjectStore`] contract tests against a backend
///
/// - `mod`: names this test module, so the suite can run more than once (e.g. one per backend)
/// - `attr`: a single-threaded async test attribute (`compio::test` or plain `tokio::test`; not `multi_thread`, since the futures are `!Send`)
/// - `fixture`: an async block that builds a fresh backend for each test.
///
/// ```ignore
/// object_store_contract_tests!(mod in_memory_atomic, attr = compio::test, fixture = async {
///     object_storage::InMemoryStorage::new()
/// });
/// ```
#[macro_export]
macro_rules! object_store_contract_tests {
    (mod $suite:ident, attr = $attr:meta, fixture = $fixture:expr $(,)?) => {
        mod $suite {
            #[allow(unused_imports)]
            use super::*;

            $crate::__ost_case!($attr, $fixture, put_get_roundtrip, assert_put_get_roundtrip);
            $crate::__ost_case!($attr, $fixture, overwrite, assert_overwrite);
            $crate::__ost_case!($attr, $fixture, get_subrange, assert_get_subrange);
            $crate::__ost_case!(
                $attr,
                $fixture,
                get_range_out_of_bounds,
                assert_get_range_out_of_bounds
            );
            $crate::__ost_case!(
                $attr,
                $fixture,
                missing_key_not_found,
                assert_missing_key_not_found
            );
            $crate::__ost_case!($attr, $fixture, delete_idempotent, assert_delete_idempotent);
            $crate::__ost_case!(
                $attr,
                $fixture,
                list_prefix_ordered_and_paginated,
                assert_list_prefix_ordered_and_paginated
            );
            $crate::__ost_case!(
                $attr,
                $fixture,
                multipart_roundtrip,
                assert_multipart_roundtrip
            );
            $crate::__ost_case!($attr, $fixture, multipart_abort, assert_multipart_abort);
            $crate::__ost_case!($attr, $fixture, part_size_floor, assert_part_size_floor);
            $crate::__ost_case!(
                $attr,
                $fixture,
                put_if_absent_capability_aware,
                assert_put_if_absent_capability_aware
            );
        }
    };
}

/// Expands one contract test: a `#[$attr]` async fn that builds a fresh store from `$fixture` and
/// runs one `$crate::testing::$assert` assertion. Internal to [`object_store_contract_tests!`];
/// not a public API
#[doc(hidden)]
#[macro_export]
macro_rules! __ost_case {
    ($attr:meta, $fixture:expr, $test:ident, $assert:ident) => {
        #[$attr]
        async fn $test() {
            let store = $fixture.await;
            $crate::testing::$assert(&store).await;
        }
    };
}
