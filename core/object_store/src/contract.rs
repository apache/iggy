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
//! under `root`, so cases can share a store. Each run needs a fresh `root`,
//! because a real bucket keeps the objects of earlier runs.
//!
//! Two macros name the cases, so the tests of a provider pick up new cases on
//! their own. [`crate::contract_cases`] names the cases that every provider
//! runs, and [`crate::if_absent_contract_cases`] names the cases of
//! [`PutMode::IfAbsent`]. A provider whose store cannot decide `IfAbsent`
//! atomically refuses it with [`ObjectStoreError::Unsupported`], and runs
//! [`given_store_without_atomic_create_when_putting_if_absent_should_refuse_as_unsupported`]
//! in place of the `IfAbsent` cases.
//!
//! ```no_run
//! use std::time::{SystemTime, UNIX_EPOCH};
//!
//! /// A root that no earlier run used.
//! fn run_root(case: &str) -> String {
//!     let started = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
//!     let run = format!("{}-{}", started.as_nanos(), std::process::id());
//!     format!("contract/run-{run}/{case}")
//! }
//!
//! macro_rules! memory_store {
//!     ($($case:ident),* $(,)?) => {$(
//!         #[test]
//!         fn $case() {
//!             let store = iggy_object_store::MemoryStore::new();
//!             let root = run_root(stringify!($case));
//!             futures::executor::block_on(
//!                 iggy_object_store::contract::$case(&store, &root),
//!             );
//!         }
//!     )*};
//! }
//!
//! iggy_object_store::contract_cases!(memory_store);
//! iggy_object_store::if_absent_contract_cases!(memory_store);
//! # fn main() {}
//! ```

// A case panics at the first broken rule, as a test does.
#![allow(clippy::missing_panics_doc)]

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use compio::driver::ProactorBuilder;
use compio::runtime::{Runtime, RuntimeBuilder};
use futures::channel::oneshot;
use futures::future::{self, Either};
use futures::stream::{self, StreamExt};

use crate::error::ObjectStoreError;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListRequest, ListedObject, MAX_KEY_LEN, MAX_KEY_SEGMENT_LEN,
    MAX_KEY_SEGMENTS, MAX_KEYS_PER_REQUEST, MAX_METADATA_LEN, ObjectMeta, ObjectStore, PutBody,
    PutMode, PutOptions, UserMetadata, validate_key,
};

/// How far the clock of a store can be from the local clock. A signed S3
/// request fails past 15 minutes of skew, so a store that took a put is
/// closer than that.
const CLOCK_SKEW: Duration = Duration::from_mins(15);

/// The large body is 128 pieces of 64 KiB, 8 MiB in all.
const LARGE_PIECE_LEN: usize = 64 * 1024;
const LARGE_PIECES: usize = 128;
/// The most bytes of the large body that a store holds at once: an eighth.
const LARGE_BODY_HELD_LEN: usize = 1024 * 1024;

/// Longer than what a provider reads with the metadata of an object.
const LARGE_OBJECT_LEN: usize = 50_000;

/// A runtime like a shard's, to run the cases of a provider that needs one.
/// On Linux it has no blocking pool, so a call that needs one panics.
#[must_use]
pub fn shard_runtime() -> Runtime {
    let mut proactor = ProactorBuilder::new();
    // macOS runs fs calls on the blocking pool, so it keeps one.
    if cfg!(target_os = "linux") {
        proactor.thread_pool_limit(0);
    }
    RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
        .expect("runtime")
}

/// Calls `$callback!` with the name of every case in [`crate::contract`]
/// that every provider runs, so a provider's tests pick up new cases on their
/// own. See the module documentation for an example.
#[macro_export]
macro_rules! contract_cases {
    ($callback:ident) => {
        $callback! {
            given_object_when_heading_should_report_size_and_metadata,
            given_object_when_reading_times_should_report_commit_time,
            given_existing_key_when_overwriting_should_replace_object_and_metadata,
            given_streamed_body_when_putting_should_store_pieces_in_order,
            given_large_body_when_putting_should_hold_few_pieces_at_once,
            given_short_body_when_putting_should_fail_and_store_nothing,
            given_long_body_when_putting_should_fail_and_store_nothing,
            given_extra_piece_after_declared_length_when_putting_should_fail_and_store_nothing,
            given_failing_body_when_putting_should_keep_previous_object,
            given_error_after_declared_length_when_putting_should_keep_previous_object,
            given_wrong_checksum_when_putting_should_fail_and_store_nothing,
            given_refused_put_over_object_when_reading_should_keep_bytes_and_etag,
            given_put_dropped_mid_body_when_reading_should_find_nothing,
            given_put_dropped_mid_body_over_object_when_reading_should_keep_object,
            given_object_when_reading_ranges_should_honor_edges,
            given_large_object_when_reading_ranges_should_return_their_bytes,
            given_empty_object_when_reading_should_refuse_every_range,
            given_missing_key_when_reading_should_return_not_found,
            given_keys_when_listing_should_return_byte_order,
            given_many_keys_when_paging_should_visit_each_once_in_order,
            given_keys_changing_between_pages_when_paging_should_resume_after_last_key,
            given_no_match_when_listing_should_return_one_empty_page,
            given_batch_when_deleting_should_remove_only_its_keys,
            given_invalid_key_in_batch_when_deleting_should_report_it_and_delete_the_rest,
            given_too_many_keys_when_deleting_should_refuse_before_any_io,
            given_invalid_key_when_calling_should_refuse_before_any_io,
            given_unusual_valid_keys_when_calling_should_take_them_unchanged,
            given_non_portable_metadata_when_putting_should_refuse,
        }
    };
}

/// Calls `$callback!` with the name of every case of
/// [`crate::PutMode::IfAbsent`] in [`crate::contract`]. A provider runs them
/// when its store decides a conditional create atomically, at commit.
#[macro_export]
macro_rules! if_absent_contract_cases {
    ($callback:ident) => {
        $callback! {
            given_existing_key_when_putting_if_absent_should_refuse_and_keep_object,
            given_racing_puts_when_putting_if_absent_should_let_one_win,
            given_failed_body_when_putting_if_absent_should_leave_key_free,
            given_put_dropped_mid_body_when_putting_if_absent_again_should_store_object,
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

/// Housekeeping compares `last_modified` with its grace period, so it is the
/// time that the store committed the object.
pub async fn given_object_when_reading_times_should_report_commit_time(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/dated");
    let before = SystemTime::now();
    put(store, &key, pattern(8, 13), &PutOptions::default()).await;
    let after = SystemTime::now();

    let head = store.head(&key).await.expect("head");
    let listed = list_all(store, &key, MAX_KEYS_PER_REQUEST).await;

    assert_ne!(head.last_modified, UNIX_EPOCH);
    let earliest = before.checked_sub(CLOCK_SKEW).unwrap_or(UNIX_EPOCH);
    assert!(
        (earliest..=after + CLOCK_SKEW).contains(&head.last_modified),
        "the commit time {:?} must be near the time of the put, {before:?}",
        head.last_modified
    );
    let [object] = listed.as_slice() else {
        panic!("{key:?} must list one object: {listed:?}");
    };
    assert_eq!(
        whole_seconds(object.last_modified),
        whole_seconds(head.last_modified),
        "list and head must agree to the second"
    );
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

/// A store sends each piece as it comes, so a large body never sits in
/// memory whole.
pub async fn given_large_body_when_putting_should_hold_few_pieces_at_once(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/large.log");
    let fills = (0..=250u8).cycle().take(LARGE_PIECES);
    let mut whole = Vec::with_capacity(LARGE_PIECES * LARGE_PIECE_LEN);
    for fill in fills.clone() {
        whole.resize(whole.len() + LARGE_PIECE_LEN, fill);
    }
    let options = PutOptions {
        crc32c: Some(crc32c::crc32c(&whole)),
        ..PutOptions::default()
    };
    let held = Arc::new(HeldBytes::default());
    let counted = Arc::clone(&held);
    let pieces = stream::iter(fills)
        .map(move |fill| Ok(counted.piece(Bytes::from(vec![fill; LARGE_PIECE_LEN]))));

    store
        .put(
            &key,
            PutBody::from_stream(whole.len() as u64, pieces),
            &options,
        )
        .await
        .expect("put");

    let peak = held.peak.load(Ordering::Relaxed);
    assert!(
        peak <= LARGE_BODY_HELD_LEN,
        "the store held {peak} bytes of the body at once, more than {LARGE_BODY_HELD_LEN}"
    );
    assert!(
        read_all(store, &key).await == whole,
        "the stored bytes must be the body's"
    );
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
    let previous = Bytes::from_static(b"previous");
    put(store, &key, previous.clone(), &PutOptions::default()).await;
    let before = store.head(&key).await.expect("head");
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
    assert_unchanged(store, &key, &before, &previous).await;
}

/// The body fails only after its last byte, as an upload does when the hash
/// of the whole segment does not match.
pub async fn given_error_after_declared_length_when_putting_should_keep_previous_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/verified");
    let previous = Bytes::from_static(b"previous");
    put(store, &key, previous.clone(), &PutOptions::default()).await;
    let before = store.head(&key).await.expect("head");
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
    assert_unchanged(store, &key, &before, &previous).await;
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

/// A put that the store refuses leaves the earlier object as it was. An empty
/// body that fails never replaces the object with an empty one.
pub async fn given_refused_put_over_object_when_reading_should_keep_bytes_and_etag(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/kept-whole");
    let previous = pattern(64, 14);
    put(store, &key, previous.clone(), &PutOptions::default()).await;
    let before = store.head(&key).await.expect("head");
    let data = pattern(4096, 15);
    let wrong_checksum = PutOptions {
        crc32c: Some(crc32c::crc32c(&data) ^ 1),
        ..PutOptions::default()
    };

    let refused = store
        .put(&key, PutBody::from_bytes(data), &wrong_checksum)
        .await;

    assert!(
        matches!(&refused, Err(ObjectStoreError::ChecksumMismatch { key: refused }) if *refused == key),
        "{refused:?}"
    );
    assert_unchanged(store, &key, &before, &previous).await;

    let failing_empty =
        PutBody::from_stream(0, stream::iter([Err(io::Error::other("disk read failed"))]));
    let failed = store.put(&key, failing_empty, &PutOptions::default()).await;

    assert!(
        matches!(&failed, Err(ObjectStoreError::Body(error)) if error.to_string() == "disk read failed"),
        "{failed:?}"
    );
    assert_unchanged(store, &key, &before, &previous).await;
}

/// A put that is dropped while it waits for the rest of its body stores
/// nothing, and leaves nothing that a listing shows.
pub async fn given_put_dropped_mid_body_when_reading_should_find_nothing(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/dropped");

    drop_mid_body(store, &key, &PutOptions::default()).await;

    assert_absent(store, &key).await;
    let listed = list_all(store, &format!("{root}/"), MAX_KEYS_PER_REQUEST).await;
    assert!(
        listed.is_empty(),
        "a dropped put must leave nothing to list: {listed:?}"
    );
}

pub async fn given_put_dropped_mid_body_over_object_when_reading_should_keep_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/dropped-over");
    let previous = pattern(64, 16);
    put(store, &key, previous.clone(), &PutOptions::default()).await;
    let before = store.head(&key).await.expect("head");

    drop_mid_body(store, &key, &PutOptions::default()).await;

    assert_unchanged(store, &key, &before, &previous).await;
}

pub async fn given_object_when_reading_ranges_should_honor_edges(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/ranges");
    let data = pattern(256, 4);
    put(store, &key, data.clone(), &PutOptions::default()).await;
    let head = store.head(&key).await.expect("head");
    let key_ref = key.as_str();
    let read = move |start: u64, end: u64| store.get_range(key_ref, start..end);

    let whole = read(0, 256).await.expect("whole");
    let middle = read(10, 20).await.expect("middle");

    assert_eq!(whole.bytes, data);
    assert_eq!(middle.bytes, data.slice(10..20));
    for range in [&whole, &middle] {
        assert_eq!(range.size, 256, "the size of the whole object");
        assert_eq!(range.etag, head.etag);
    }
    assert_eq!(read(0, 1).await.expect("first byte").bytes, data.slice(..1));
    assert_eq!(
        read(255, 256).await.expect("last byte").bytes,
        data.slice(255..)
    );
    assert_eq!(
        read(250, 300).await.expect("past the end").bytes,
        data.slice(250..)
    );
    assert_eq!(read(0, u64::MAX).await.expect("unbounded").bytes, data);
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

/// A provider can read the start of an object with its metadata and the rest
/// apart, so these ranges cross 4 KiB and 8 KiB and lie past both.
pub async fn given_large_object_when_reading_ranges_should_return_their_bytes(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/large");
    let data = pattern(LARGE_OBJECT_LEN, 6);
    put(store, &key, data.clone(), &PutOptions::default()).await;

    for range in [
        0..10,
        4090..4100,
        8190..8200,
        20_000..30_000,
        LARGE_OBJECT_LEN - 10..LARGE_OBJECT_LEN,
    ] {
        let read = store
            .get_range(&key, range.start as u64..range.end as u64)
            .await
            .unwrap_or_else(|error| panic!("read {range:?}: {error}"));

        assert_eq!(read.bytes, data.slice(range.clone()), "{range:?}");
        assert_eq!(read.size, LARGE_OBJECT_LEN as u64);
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

/// The retire loop deletes each page before it asks for the next.
///
/// A page resumes after the last key of the page before it, so neither those
/// deletes nor a put behind the cursor make the listing skip a key.
pub async fn given_keys_changing_between_pages_when_paging_should_resume_after_last_key(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = |name: &str| format!("{root}/changing/{name}");
    let expected: Vec<String> = (0..8).map(|index| key(&format!("b{index}"))).collect();
    for name in &expected {
        put(store, name, pattern(1, 17), &PutOptions::default()).await;
    }
    let prefix = format!("{root}/changing/");
    let request = ListRequest::new(&prefix).with_max_keys(3);
    let mut listed = Vec::new();
    let mut continuation: Option<String> = None;

    for page_index in 0..=expected.len() {
        let page = match continuation.as_deref() {
            Some(after) => store.list(&request.after(after)).await,
            None => store.list(&request).await,
        }
        .unwrap_or_else(|error| panic!("list {prefix:?}: {error}"));
        let page_keys = keys(&page.objects);
        let outcome = store.delete(&page_keys).await.expect("delete");
        assert!(outcome.is_complete(), "{outcome:?}");
        // An "a" key sorts before every "b" key, so it lands behind the cursor.
        put(
            store,
            &key(&format!("a{page_index}")),
            pattern(1, 18),
            &PutOptions::default(),
        )
        .await;
        listed.extend(page_keys);
        let Some(next) = page.next else {
            assert_eq!(listed, expected);
            return;
        };
        continuation = Some(next);
    }
    panic!("listing {prefix:?} did not end");
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
    let too_many_segments = format!("{root}/{}x", "x/".repeat(MAX_KEY_SEGMENTS));
    let invalid = [
        String::new(),
        format!("/{root}/leading"),
        format!("{root}/trailing/"),
        format!("{root}//double"),
        format!("{root}/../escape"),
        format!("{root}/.hidden"),
        format!("{root}/trailing."),
        format!("{root}/dot./inside"),
        format!("{root}/back\\slash"),
        format!("{root}/new\nline"),
        format!("{root}/non\u{fdd0}character"),
        format!("{root}/end\u{ffff}"),
        too_long,
        long_segment,
        too_many_segments,
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
    for prefix in [
        format!("{root}//"),
        "/".to_owned(),
        format!("{root}/."),
        format!("{root}/dot./"),
    ] {
        let result = store.list(&ListRequest::new(&prefix)).await;
        assert!(
            matches!(result, Err(ObjectStoreError::InvalidKey { .. })),
            "list {prefix:?}: {result:?}"
        );
    }
    let listed = list_all(store, &format!("{root}/"), MAX_KEYS_PER_REQUEST).await;
    assert_eq!(keys(&listed), [kept]);
}

/// Keys that a request signer has to encode. Each one is valid, so every call
/// takes it, and a listing returns it unchanged.
pub async fn given_unusual_valid_keys_when_calling_should_take_them_unchanged(
    store: &impl ObjectStore,
    root: &str,
) {
    let names = [
        "space in name",
        "plus+sign",
        "percent%2Fslash",
        "query?a=b&c=d",
        "hash#fragment",
        "punctuation!$'()*,;:@",
        "brackets[]{}<>^`|~\"",
        "caf\u{e9}",
        "\u{65e5}\u{672c}\u{8a9e}",
        "emoji\u{1f600}",
        "nested dir/with space",
    ];
    let key_of = |name: &str| format!("{root}/unusual/{name}");
    for (index, name) in names.iter().enumerate() {
        let key = key_of(name);
        assert!(validate_key(&key).is_ok(), "{key:?} must be a valid key");
        let bytes = pattern(index + 1, 19);

        put(store, &key, bytes.clone(), &PutOptions::default()).await;

        let head = store.head(&key).await.expect("head");
        assert_eq!(head.size, bytes.len() as u64, "{key:?}");
        assert_eq!(read_all(store, &key).await, bytes, "{key:?}");
    }
    let mut expected: Vec<String> = names.iter().map(|name| key_of(name)).collect();
    expected.sort_unstable();
    let prefix = format!("{root}/unusual/");

    let listed = list_all(store, &prefix, MAX_KEYS_PER_REQUEST).await;
    let outcome = store.delete(&expected).await.expect("delete");

    assert_eq!(keys(&listed), expected);
    assert!(outcome.is_complete(), "{outcome:?}");
    let left = list_all(store, &prefix, MAX_KEYS_PER_REQUEST).await;
    assert!(left.is_empty(), "{left:?}");
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

/// The store decides `IfAbsent` at commit, so of two puts whose bodies are
/// both in flight, exactly one succeeds. A store that checks for the key
/// before it writes lets both succeed.
pub async fn given_racing_puts_when_putting_if_absent_should_let_one_win(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/raced");
    let if_absent = if_absent();
    let (first_body, mut first_gate) = gated_body(b"first ", b"writer");
    let (second_body, mut second_gate) = gated_body(b"second ", b"writer");
    let mut first = Box::pin(store.put(&key, first_body, &if_absent));
    let mut second = Box::pin(store.put(&key, second_body, &if_absent));

    first_gate.reach(&mut first).await;
    second_gate.reach(&mut second).await;
    first_gate.release();
    second_gate.release();
    let results = future::join(first, second).await;

    let winner: &[u8] = match &results {
        (Ok(()), Err(ObjectStoreError::PreconditionFailed { .. })) => b"first writer",
        (Err(ObjectStoreError::PreconditionFailed { .. }), Ok(())) => b"second writer",
        _ => panic!("one put must win and the other must fail as PreconditionFailed: {results:?}"),
    };
    assert_eq!(read_all(store, &key).await, winner);
}

/// A failed `IfAbsent` put leaves nothing under its key, so the retry of an
/// upload succeeds.
pub async fn given_failed_body_when_putting_if_absent_should_leave_key_free(
    store: &impl ObjectStore,
    root: &str,
) {
    let if_absent = if_absent();
    for (name, body) in failing_bodies() {
        let key = format!("{root}/free/{name}");

        let result = store.put(&key, body, &if_absent).await;

        assert!(
            matches!(result, Err(ObjectStoreError::Body(_))),
            "{name}: {result:?}"
        );
        assert_absent(store, &key).await;
        put(store, &key, Bytes::from_static(b"retried"), &if_absent).await;
        assert_eq!(read_all(store, &key).await, "retried", "{name}");
    }
}

/// A dropped `IfAbsent` put leaves nothing that refuses its retry.
pub async fn given_put_dropped_mid_body_when_putting_if_absent_again_should_store_object(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/dropped-retry");
    let if_absent = if_absent();

    drop_mid_body(store, &key, &if_absent).await;

    assert_absent(store, &key).await;
    put(store, &key, Bytes::from_static(b"retried"), &if_absent).await;
    assert_eq!(read_all(store, &key).await, "retried");
}

/// For a provider whose store cannot decide [`PutMode::IfAbsent`]
/// atomically. It runs this case in place of
/// [`crate::if_absent_contract_cases`].
pub async fn given_store_without_atomic_create_when_putting_if_absent_should_refuse_as_unsupported(
    store: &impl ObjectStore,
    root: &str,
) {
    let key = format!("{root}/unsupported");

    let result = store
        .put(
            &key,
            PutBody::from_bytes(Bytes::from_static(b"x")),
            &if_absent(),
        )
        .await;

    assert!(
        matches!(result, Err(ObjectStoreError::Unsupported(_))),
        "{result:?}"
    );
    assert_absent(store, &key).await;
}

/// Counts the bytes of a body that are alive at once.
#[derive(Debug, Default)]
struct HeldBytes {
    now: AtomicUsize,
    peak: AtomicUsize,
}

impl HeldBytes {
    /// `bytes` as a piece that counts itself here while it is alive.
    fn piece(self: &Arc<Self>, bytes: Bytes) -> Bytes {
        let now = self.now.fetch_add(bytes.len(), Ordering::Relaxed) + bytes.len();
        self.peak.fetch_max(now, Ordering::Relaxed);
        Bytes::from_owner(HeldPiece {
            bytes,
            held: Arc::clone(self),
        })
    }
}

struct HeldPiece {
    bytes: Bytes,
    held: Arc<HeldBytes>,
}

impl AsRef<[u8]> for HeldPiece {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for HeldPiece {
    fn drop(&mut self) {
        self.held.now.fetch_sub(self.bytes.len(), Ordering::Relaxed);
    }
}

/// The far side of a [`gated_body`]. It sees the store wait for the rest of
/// the body, and lets the rest go.
struct BodyGate {
    reached: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl BodyGate {
    /// Drives `put` until the store took the first part of the body and waits
    /// for the rest.
    async fn reach<F>(&mut self, put: &mut F)
    where
        F: Future + Unpin,
        F::Output: fmt::Debug,
    {
        match future::select(put, &mut self.reached).await {
            Either::Left((result, _)) => panic!("the put ended before its body: {result:?}"),
            Either::Right((reached, _)) => {
                reached.expect("the store dropped the body before it asked for the rest");
            }
        }
    }

    fn release(self) {
        // The body is gone when its put was dropped, and then nothing waits.
        let _ = self.release.send(());
    }
}

/// A body that yields `first`, then holds `rest` back until its gate lets it
/// go.
fn gated_body(first: &'static [u8], rest: &'static [u8]) -> (PutBody, BodyGate) {
    let (reached_sender, reached) = oneshot::channel();
    let (release, release_receiver) = oneshot::channel::<()>();
    let tail = stream::once(async move {
        // The gate is gone when the case stopped waiting for this point.
        let _ = reached_sender.send(());
        release_receiver
            .await
            .map_err(|_| io::Error::other("the body was abandoned"))?;
        Ok::<_, io::Error>(Bytes::from_static(rest))
    });
    let body = PutBody::from_stream(
        (first.len() + rest.len()) as u64,
        stream::iter([Ok(Bytes::from_static(first))]).chain(tail),
    );
    (body, BodyGate { reached, release })
}

/// Starts a put and drops it once the store took the first part of the body
/// and waits for the rest.
async fn drop_mid_body(store: &impl ObjectStore, key: &str, options: &PutOptions) {
    let (body, mut gate) = gated_body(b"first part", b", the rest");
    let mut put = Box::pin(store.put(key, body, options));
    gate.reach(&mut put).await;
    drop(put);
}

/// Bodies that fail, each named for how it fails.
fn failing_bodies() -> [(&'static str, PutBody); 5] {
    let body =
        |len, pieces: Vec<io::Result<Bytes>>| PutBody::from_stream(len, stream::iter(pieces));
    let piece = |bytes: &'static [u8]| Ok(Bytes::from_static(bytes));
    let error = |message: &str| Err(io::Error::other(message.to_owned()));
    [
        ("short", body(10, vec![piece(b"12345")])),
        ("long", body(4, vec![piece(b"123"), piece(b"45")])),
        ("extra-piece", body(4, vec![piece(b"1234"), piece(b"5")])),
        (
            "failing",
            body(8, vec![piece(b"next"), error("disk read failed")]),
        ),
        (
            "error-after-last-piece",
            body(4, vec![piece(b"next"), error("hash mismatch")]),
        ),
    ]
}

fn if_absent() -> PutOptions {
    PutOptions {
        mode: PutMode::IfAbsent,
        ..PutOptions::default()
    }
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
        .bytes
}

async fn assert_absent(store: &impl ObjectStore, key: &str) {
    let head = store.head(key).await;
    assert!(
        matches!(head, Err(ObjectStoreError::NotFound { .. })),
        "{key:?} must hold no object: {head:?}"
    );
}

/// Asserts that `key` still holds the object that `before` describes.
async fn assert_unchanged(store: &impl ObjectStore, key: &str, before: &ObjectMeta, bytes: &[u8]) {
    let head = store
        .head(key)
        .await
        .unwrap_or_else(|error| panic!("head {key:?}: {error}"));
    assert_eq!(head.etag, before.etag, "{key:?} must keep its object");
    assert_eq!(head.size, before.size, "{key:?}");
    assert_eq!(read_all(store, key).await, bytes, "{key:?}");
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

fn whole_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// `len` bytes that differ from a shifted copy, so a misplaced range shows.
fn pattern(len: usize, seed: u8) -> Bytes {
    (0..=250u8)
        .cycle()
        .skip(usize::from(seed))
        .take(len)
        .collect()
}
