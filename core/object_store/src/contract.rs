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

//! The contract that every [`ObjectStore`] provider keeps.
//!
//! Each case is an async function that takes a store and a `root` key, and
//! panics at the first rule that the store breaks. A case touches only keys
//! under `root`, so cases can share a store, but each run needs a fresh
//! `root`. [`crate::contract_cases`] names every case, so the tests of a
//! provider run each one as its own test:
//!
//! ```ignore
//! macro_rules! memory_store {
//!     ($($case:ident),* $(,)?) => {$(
//!         #[test]
//!         fn $case() {
//!             let store = iggy_object_store::MemoryStore::new();
//!             futures::executor::block_on(
//!                 iggy_object_store::contract::$case(&store, stringify!($case)),
//!             );
//!         }
//!     )*};
//! }
//!
//! iggy_object_store::contract_cases!(memory_store);
//! ```

// A case panics at the first broken rule, as a test does.
#![allow(clippy::missing_panics_doc)]

use std::io;

use bytes::Bytes;
use futures::stream;

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListRequest, ListedObject, MAX_KEY_LEN, MAX_KEY_SEGMENT_LEN,
    MAX_KEYS_PER_REQUEST, MAX_METADATA_LEN, ObjectStore, PutBody, PutMode, PutOptions,
    UserMetadata,
};

/// Calls `$callback!` with the name of every case in [`crate::contract`], so
/// a provider's tests pick up new cases on their own. See the module
/// documentation for an example.
#[macro_export]
macro_rules! contract_cases {
    ($callback:ident) => {
        $callback! {
            given_object_when_heading_should_report_size_and_metadata,
            given_existing_key_when_putting_if_absent_should_refuse_and_keep_object,
            given_existing_key_when_overwriting_should_replace_object_and_metadata,
            given_streamed_body_when_putting_should_store_pieces_in_order,
            given_short_body_when_putting_should_fail_and_store_nothing,
            given_long_body_when_putting_should_fail_and_store_nothing,
            given_extra_piece_after_declared_length_when_putting_should_fail_and_store_nothing,
            given_failing_body_when_putting_should_keep_previous_object,
            given_error_after_declared_length_when_putting_should_keep_previous_object,
            given_wrong_checksum_when_putting_should_fail_and_store_nothing,
            given_object_when_reading_ranges_should_honor_edges,
            given_empty_object_when_reading_should_refuse_every_range,
            given_missing_key_when_reading_should_return_not_found,
            given_keys_when_listing_should_return_byte_order,
            given_many_keys_when_paging_should_visit_each_once_in_order,
            given_no_match_when_listing_should_return_one_empty_page,
            given_batch_when_deleting_should_remove_only_its_keys,
            given_invalid_key_in_batch_when_deleting_should_report_it_and_delete_the_rest,
            given_too_many_keys_when_deleting_should_refuse_before_any_io,
            given_invalid_key_when_calling_should_refuse_before_any_io,
            given_non_portable_metadata_when_putting_should_refuse,
        }
    };
}

pub async fn given_object_when_heading_should_report_size_and_metadata(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/1/2/3/4/00000000000000000000.log");
    let bytes = Bytes::from_static(b"segment bytes");
    let metadata = metadata(&[("end-offset", "99"), ("start-offset", "0")]);
    let options = PutOptions {
        metadata: metadata.clone(),
        ..PutOptions::default()
    };

    put(store, &key, bytes.clone(), &options).await;
    let head = store.head(&key).await.expect("head");

    assert_eq!(head.size, 13);
    assert_eq!(head.metadata, metadata);
    assert_ne!(head.etag, "");
    assert!(
        head.crc32c
            .is_none_or(|stored| stored == crc32c::crc32c(&bytes)),
        "the stored CRC32C must match the bytes: {head:?}"
    );
}

pub async fn given_existing_key_when_putting_if_absent_should_refuse_and_keep_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/conditional");
    let if_absent = |writer: &str| PutOptions {
        mode: PutMode::IfAbsent,
        metadata: metadata(&[("writer", writer)]),
        ..PutOptions::default()
    };
    put(
        store,
        &key,
        Bytes::from_static(b"first"),
        &if_absent("first"),
    )
    .await;

    let refused = store
        .put(
            &key,
            PutBody::from_bytes(Bytes::from_static(b"second")),
            &if_absent("second"),
        )
        .await;

    assert!(
        matches!(&refused, Err(ObjectStoreError::PreconditionFailed { key: refused }) if *refused == key),
        "{refused:?}"
    );
    assert_eq!(read_all(store, &key).await, Bytes::from_static(b"first"));
    let head = store.head(&key).await.expect("head");
    assert_eq!(head.metadata, metadata(&[("writer", "first")]));
}

pub async fn given_existing_key_when_overwriting_should_replace_object_and_metadata(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/replaced");
    let with_metadata = |entries: &[(&str, &str)]| PutOptions {
        metadata: metadata(entries),
        ..PutOptions::default()
    };
    put(
        store,
        &key,
        Bytes::from_static(b"first"),
        &with_metadata(&[("writer", "first")]),
    )
    .await;
    let first = store.head(&key).await.expect("head");

    put(
        store,
        &key,
        Bytes::from_static(b"second, longer"),
        &with_metadata(&[("other", "x")]),
    )
    .await;
    let second = store.head(&key).await.expect("head");

    assert_eq!(
        read_all(store, &key).await,
        Bytes::from_static(b"second, longer")
    );
    assert_eq!(second.size, 14);
    assert_eq!(second.metadata, metadata(&[("other", "x")]));
    assert_ne!(first.etag, second.etag);
}

pub async fn given_streamed_body_when_putting_should_store_pieces_in_order(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/streamed.log");
    let pieces = [
        pattern(1000, 0),
        pattern(1, 1),
        Bytes::new(),
        pattern(4096, 2),
    ];
    let whole: Bytes = pieces.concat().into();
    let options = PutOptions {
        crc32c: Some(crc32c::crc32c(&whole)),
        ..PutOptions::default()
    };
    let body = PutBody::from_stream(whole.len() as u64, stream::iter(pieces.into_iter().map(Ok)));

    store.put(&key, body, &options).await.expect("put");

    assert_eq!(
        store.head(&key).await.expect("head").size,
        whole.len() as u64
    );
    assert_eq!(read_all(store, &key).await, whole);
}

pub async fn given_short_body_when_putting_should_fail_and_store_nothing(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/short");
    let body = PutBody::from_stream(10, stream::iter([Ok(Bytes::from_static(b"12345"))]));

    let result = store.put(&key, body, &PutOptions::default()).await;

    assert!(
        matches!(result, Err(ObjectStoreError::Body(_))),
        "{result:?}"
    );
    assert_absent(store, &key).await;
}

pub async fn given_long_body_when_putting_should_fail_and_store_nothing(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/long");
    let body = PutBody::from_stream(
        4,
        stream::iter([
            Ok(Bytes::from_static(b"123")),
            Ok(Bytes::from_static(b"45")),
        ]),
    );

    let result = store.put(&key, body, &PutOptions::default()).await;

    assert!(
        matches!(result, Err(ObjectStoreError::Body(_))),
        "{result:?}"
    );
    assert_absent(store, &key).await;
}

/// The extra piece comes only after the declared length is complete, so a
/// provider that stops reading there must still refuse the body.
pub async fn given_extra_piece_after_declared_length_when_putting_should_fail_and_store_nothing(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/overrun");
    let body = PutBody::from_stream(
        4,
        stream::iter([
            Ok(Bytes::from_static(b"1234")),
            Ok(Bytes::from_static(b"5")),
        ]),
    );

    let result = store.put(&key, body, &PutOptions::default()).await;

    assert!(
        matches!(result, Err(ObjectStoreError::Body(_))),
        "{result:?}"
    );
    assert_absent(store, &key).await;
}

pub async fn given_failing_body_when_putting_should_keep_previous_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/kept");
    put(
        store,
        &key,
        Bytes::from_static(b"previous"),
        &PutOptions::default(),
    )
    .await;
    let failing = PutBody::from_stream(
        8,
        stream::iter([
            Ok(Bytes::from_static(b"next")),
            Err(io::Error::other("disk read failed")),
        ]),
    );

    let result = store.put(&key, failing, &PutOptions::default()).await;

    assert!(
        matches!(&result, Err(ObjectStoreError::Body(error)) if error.to_string() == "disk read failed"),
        "the body's own error must come back unchanged: {result:?}"
    );
    assert_eq!(read_all(store, &key).await, Bytes::from_static(b"previous"));
}

/// The body fails only after its last byte, as an upload does when the hash
/// of the whole segment does not match.
pub async fn given_error_after_declared_length_when_putting_should_keep_previous_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/verified");
    put(
        store,
        &key,
        Bytes::from_static(b"previous"),
        &PutOptions::default(),
    )
    .await;
    let failing = PutBody::from_stream(
        4,
        stream::iter([
            Ok(Bytes::from_static(b"next")),
            Err(io::Error::other("hash mismatch")),
        ]),
    );

    let result = store.put(&key, failing, &PutOptions::default()).await;

    assert!(
        matches!(&result, Err(ObjectStoreError::Body(error)) if error.to_string() == "hash mismatch"),
        "the body's own error must come back unchanged: {result:?}"
    );
    assert_eq!(read_all(store, &key).await, Bytes::from_static(b"previous"));
}

pub async fn given_wrong_checksum_when_putting_should_fail_and_store_nothing(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/checksummed");
    let data = pattern(4096, 3);
    let with_checksum = |crc32c| PutOptions {
        crc32c: Some(crc32c),
        ..PutOptions::default()
    };

    let refused = store
        .put(
            &key,
            PutBody::from_bytes(data.clone()),
            &with_checksum(crc32c::crc32c(&data) ^ 1),
        )
        .await;

    assert!(
        matches!(&refused, Err(ObjectStoreError::ChecksumMismatch { key: refused }) if *refused == key),
        "{refused:?}"
    );
    assert_absent(store, &key).await;
    put(
        store,
        &key,
        data.clone(),
        &with_checksum(crc32c::crc32c(&data)),
    )
    .await;
    assert_eq!(read_all(store, &key).await, data);
}

pub async fn given_object_when_reading_ranges_should_honor_edges(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/ranges");
    let data = pattern(256, 4);
    put(store, &key, data.clone(), &PutOptions::default()).await;
    let key_ref = key.as_str();
    let read = move |start: u64, end: u64| store.get_range(key_ref, start..end);

    assert_eq!(read(0, 256).await.expect("whole"), data);
    assert_eq!(read(10, 20).await.expect("middle"), data.slice(10..20));
    assert_eq!(read(0, 1).await.expect("first byte"), data.slice(..1));
    assert_eq!(read(255, 256).await.expect("last byte"), data.slice(255..));
    assert_eq!(
        read(250, 300).await.expect("past the end"),
        data.slice(250..)
    );
    assert_eq!(read(0, u64::MAX).await.expect("unbounded"), data);
    for (start, end) in [(256, 257), (300, 400), (u64::MAX - 1, u64::MAX)] {
        let result = read(start, end).await;
        assert!(
            matches!(&result, Err(ObjectStoreError::InvalidRange { key: refused, .. }) if *refused == key),
            "{start}..{end}: {result:?}"
        );
    }
    for (start, end) in [(5, 5), (6, 5)] {
        let result = read(start, end).await;
        assert!(
            matches!(result, Err(ObjectStoreError::InvalidRequest(_))),
            "{start}..{end}: {result:?}"
        );
    }
}

pub async fn given_empty_object_when_reading_should_refuse_every_range(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/empty");
    put(store, &key, Bytes::new(), &PutOptions::default()).await;

    assert_eq!(store.head(&key).await.expect("head").size, 0);
    for (start, end) in [(0, 1), (0, u64::MAX)] {
        let result = store.get_range(&key, start..end).await;
        assert!(
            matches!(result, Err(ObjectStoreError::InvalidRange { .. })),
            "{start}..{end}: {result:?}"
        );
    }
}

pub async fn given_missing_key_when_reading_should_return_not_found(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/missing");

    let head = store.head(&key).await;
    let read = store.get_range(&key, 0..10).await;
    let deleted = store.delete(std::slice::from_ref(&key)).await;

    assert!(
        matches!(&head, Err(ObjectStoreError::NotFound { key: missing }) if *missing == key),
        "{head:?}"
    );
    assert!(
        matches!(&read, Err(ObjectStoreError::NotFound { key: missing }) if *missing == key),
        "{read:?}"
    );
    assert!(deleted.expect("delete").is_complete());
}

/// Byte order puts `-` and `.` before `/`.
///
/// It also puts digits before uppercase and uppercase before lowercase, so a
/// store that walks directories or folds case breaks it. No two names differ
/// only in case, so a filesystem store on a case-insensitive volume can still
/// hold them all.
pub async fn given_keys_when_listing_should_return_byte_order(
    store: &impl ObjectStore,
    root: &str,
) {
    let names = [
        "b", "a/1", "a-1", "a.1", "C", "a/10", "a/2", "D", "a0", "a/x", "a/B", "a/0",
    ];
    for (index, name) in names.iter().enumerate() {
        put(
            store,
            &format!("{root}/order/{name}"),
            pattern(index + 1, 5),
            &PutOptions::default(),
        )
        .await;
    }
    put(
        store,
        &format!("{root}/order-other/x"),
        pattern(1, 6),
        &PutOptions::default(),
    )
    .await;

    let listed = list_all(store, &format!("{root}/order/"), MAX_KEYS_PER_REQUEST).await;
    let partial = list_all(store, &format!("{root}/order/a/1"), MAX_KEYS_PER_REQUEST).await;

    let mut expected: Vec<String> = names
        .iter()
        .map(|name| format!("{root}/order/{name}"))
        .collect();
    expected.sort_unstable();
    assert_eq!(keys(&listed), expected);
    assert_eq!(
        keys(&partial),
        [format!("{root}/order/a/1"), format!("{root}/order/a/10")]
    );
    for object in &listed {
        let head = store.head(&object.key).await.expect("head");
        assert_eq!(object.size, head.size, "{}", object.key);
        assert_eq!(object.etag, head.etag, "{}", object.key);
    }
}

pub async fn given_many_keys_when_paging_should_visit_each_once_in_order(
    store: &impl ObjectStore,
    root: &str,
) {
    let mut expected: Vec<String> = (0..7)
        .map(|index| format!("{root}/pages/{index:02}"))
        .collect();
    for key in expected.iter().rev() {
        put(store, key, pattern(1, 7), &PutOptions::default()).await;
    }
    expected.sort_unstable();

    for max_keys in [0, 1, 2, 3, 7, 8, MAX_KEYS_PER_REQUEST + 1] {
        let listed = list_all(store, &format!("{root}/pages/"), max_keys).await;
        assert_eq!(keys(&listed), expected, "max_keys {max_keys}");
    }
}

pub async fn given_no_match_when_listing_should_return_one_empty_page(
    store: &impl ObjectStore,
    root: &str,
) {
    put(
        store,
        &format!("{root}/present/x"),
        pattern(1, 8),
        &PutOptions::default(),
    )
    .await;

    let prefix = format!("{root}/absent/");
    let page = store.list(&ListRequest::new(&prefix)).await.expect("list");

    assert!(page.objects.is_empty(), "{page:?}");
    assert_eq!(page.next, None);
}

/// A batch that names some objects, misses others and holds a key with no
/// object.
pub async fn given_batch_when_deleting_should_remove_only_its_keys(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = |name: &str| format!("{root}/delete/{name}");
    for name in ["a", "b", "c", "d"] {
        put(store, &key(name), pattern(2, 9), &PutOptions::default()).await;
    }
    let batch = [key("a"), key("c"), key("missing")];

    let first = store.delete(&batch).await.expect("delete");
    let again = store.delete(&batch).await.expect("delete again");
    let none = store.delete(&[]).await.expect("delete nothing");

    assert!(first.is_complete(), "{first:?}");
    assert!(again.is_complete(), "{again:?}");
    assert!(none.is_complete(), "{none:?}");
    assert_absent(store, &key("a")).await;
    assert_absent(store, &key("c")).await;
    let listed = list_all(store, &format!("{root}/delete/"), MAX_KEYS_PER_REQUEST).await;
    assert_eq!(keys(&listed), [key("b"), key("d")]);
}

/// A listing can return a key that `validate_key` refuses, such as a folder
/// marker that another client wrote, and a delete of the listed keys still
/// removes the valid ones.
pub async fn given_invalid_key_in_batch_when_deleting_should_report_it_and_delete_the_rest(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = |name: &str| format!("{root}/mixed/{name}");
    for name in ["a", "b"] {
        put(store, &key(name), pattern(2, 12), &PutOptions::default()).await;
    }
    let marker = key("");
    let batch = [key("a"), marker.clone(), key("missing")];

    let outcome = store.delete(&batch).await.expect("delete");

    assert!(refused_only(&outcome, &marker), "{outcome:?}");
    assert!(!outcome.is_complete());
    let listed = list_all(store, &format!("{root}/mixed/"), MAX_KEYS_PER_REQUEST).await;
    assert_eq!(keys(&listed), [key("b")]);
}

pub async fn given_too_many_keys_when_deleting_should_refuse_before_any_io(
    store: &impl ObjectStore,
    root: &str,
) {
    let kept = format!("{root}/many/0000");
    put(store, &kept, pattern(1, 10), &PutOptions::default()).await;
    let batch: Vec<String> = (0..=MAX_KEYS_PER_REQUEST)
        .map(|index| format!("{root}/many/{index:04}"))
        .collect();

    let result = store.delete(&batch).await;

    assert!(
        matches!(result, Err(ObjectStoreError::InvalidRequest(_))),
        "{result:?}"
    );
    store
        .head(&kept)
        .await
        .expect("the object outside the refused batch");
}

pub async fn given_invalid_key_when_calling_should_refuse_before_any_io(
    store: &impl ObjectStore,
    root: &str,
) {
    let kept = format!("{root}/valid");
    put(store, &kept, pattern(1, 11), &PutOptions::default()).await;
    let too_long = format!("{root}/{}x", "x/".repeat(MAX_KEY_LEN / 2));
    let long_segment = format!("{root}/{}", "x".repeat(MAX_KEY_SEGMENT_LEN + 1));
    let invalid = [
        String::new(),
        format!("/{root}/leading"),
        format!("{root}/trailing/"),
        format!("{root}//double"),
        format!("{root}/../escape"),
        format!("{root}/.hidden"),
        format!("{root}/back\\slash"),
        format!("{root}/new\nline"),
        too_long,
        long_segment,
    ];

    for key in &invalid {
        let stored = store
            .put(
                key,
                PutBody::from_bytes(Bytes::from_static(b"x")),
                &PutOptions::default(),
            )
            .await;
        let head = store.head(key).await;
        let read = store.get_range(key, 0..1).await;
        let deleted = store.delete(std::slice::from_ref(key)).await;
        assert!(
            matches!(stored, Err(ObjectStoreError::InvalidKey { .. })),
            "put {key:?}: {stored:?}"
        );
        assert!(
            matches!(head, Err(ObjectStoreError::InvalidKey { .. })),
            "head {key:?}: {head:?}"
        );
        assert!(
            matches!(read, Err(ObjectStoreError::InvalidKey { .. })),
            "get {key:?}: {read:?}"
        );
        assert!(
            matches!(&deleted, Ok(outcome) if refused_only(outcome, key)),
            "delete {key:?}: {deleted:?}"
        );
    }
    for prefix in [format!("{root}//"), "/".to_owned(), format!("{root}/.")] {
        let result = store.list(&ListRequest::new(&prefix)).await;
        assert!(
            matches!(result, Err(ObjectStoreError::InvalidKey { .. })),
            "list {prefix:?}: {result:?}"
        );
    }
    let listed = list_all(store, &format!("{root}/"), MAX_KEYS_PER_REQUEST).await;
    assert_eq!(keys(&listed), [kept]);
}

pub async fn given_non_portable_metadata_when_putting_should_refuse(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/metadata");
    let oversized = "x".repeat(MAX_METADATA_LEN);
    let refused = [
        ("Upper", "x"),
        ("under_score", "x"),
        ("1digit", "x"),
        ("", "x"),
        ("name", " leading"),
        ("name", "trailing "),
        ("name", "caf\u{e9}"),
        ("name", "tab\t"),
        ("big", oversized.as_str()),
    ];

    for (name, value) in refused {
        let options = PutOptions {
            metadata: metadata(&[(name, value)]),
            ..PutOptions::default()
        };
        let result = store
            .put(
                &key,
                PutBody::from_bytes(Bytes::from_static(b"x")),
                &options,
            )
            .await;
        assert!(
            matches!(result, Err(ObjectStoreError::InvalidRequest(_))),
            "{name:?} = {value:?}: {result:?}"
        );
    }
    assert_absent(store, &key).await;
    let portable = metadata(&[("a", ""), ("a-1", "~ printable !")]);
    let options = PutOptions {
        metadata: portable.clone(),
        ..PutOptions::default()
    };
    put(store, &key, Bytes::from_static(b"x"), &options).await;
    assert_eq!(store.head(&key).await.expect("head").metadata, portable);
}

async fn put(store: &impl ObjectStore, key: &str, bytes: Bytes, options: &PutOptions) {
    store
        .put(key, PutBody::from_bytes(bytes), options)
        .await
        .unwrap_or_else(|error| panic!("put {key:?}: {error}"));
}

async fn read_all(store: &impl ObjectStore, key: &str) -> Bytes {
    store
        .get_range(key, 0..u64::MAX)
        .await
        .unwrap_or_else(|error| panic!("read {key:?}: {error}"))
}

async fn assert_absent(store: &impl ObjectStore, key: &str) {
    let head = store.head(key).await;
    assert!(
        matches!(head, Err(ObjectStoreError::NotFound { .. })),
        "{key:?} must hold no object: {head:?}"
    );
}

/// Whether `key` is the one failure of `outcome`, refused as an invalid key.
fn refused_only(outcome: &DeleteOutcome, key: &str) -> bool {
    matches!(
        outcome.failed.as_slice(),
        [DeleteFailure {
            key: refused,
            error: ObjectStoreError::InvalidKey { .. },
        }] if refused == key
    )
}

/// Every object under `prefix`, page by page, checking each page's length.
async fn list_all(store: &impl ObjectStore, prefix: &str, max_keys: usize) -> Vec<ListedObject> {
    let request = ListRequest::new(prefix).with_max_keys(max_keys);
    let mut listed = Vec::new();
    let mut continuation: Option<String> = None;
    // A store that never ends its listing fails here instead of hanging.
    for _ in 0..=2 * MAX_KEYS_PER_REQUEST {
        let page = match continuation.as_deref() {
            Some(after) => store.list(&request.after(after)).await,
            None => store.list(&request).await,
        }
        .unwrap_or_else(|error| panic!("list {prefix:?}: {error}"));
        assert!(
            page.objects.len() <= request.page_len(),
            "{} objects in a page of at most {}",
            page.objects.len(),
            request.page_len()
        );
        listed.extend(page.objects);
        match page.next {
            Some(next) => continuation = Some(next),
            None => return listed,
        }
    }
    panic!("listing {prefix:?} did not end");
}

fn keys(objects: &[ListedObject]) -> Vec<String> {
    objects.iter().map(|object| object.key.clone()).collect()
}

fn metadata(entries: &[(&str, &str)]) -> UserMetadata {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// `len` bytes that differ from a shifted copy, so a misplaced range shows.
fn pattern(len: usize, seed: u8) -> Bytes {
    (0..=250u8)
        .cycle()
        .skip(usize::from(seed))
        .take(len)
        .collect()
}
