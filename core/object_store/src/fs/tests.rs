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

use std::fmt::Debug;
use std::future;
use std::rc::Rc;
use std::time::Duration;

use futures::future::{Either, select};
use futures::stream;
use tempfile::TempDir;

use super::*;
use crate::ErrorClass;
use crate::contract::shard_runtime;
use crate::store::UserMetadata;

thread_local! {
    /// What `note` saw on this thread, see [`steps`].
    pub(super) static NOTED_STEPS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn store() -> (TempDir, FsStore) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = FsStore::new(directory.path());
    (directory, store)
}

async fn put(store: &FsStore, key: &str, bytes: &'static [u8]) {
    store
        .put(key, PutBody::from_bytes(bytes), &PutOptions::default())
        .await
        .unwrap_or_else(|error| panic!("put {key:?}: {error}"));
}

/// Every key under `prefix`, `max_keys` at a time.
async fn list_keys(store: &FsStore, prefix: &str, max_keys: usize) -> Vec<String> {
    list_from(store, prefix, max_keys, None).await
}

/// Every key under `prefix` from the page that resumes after `continuation`,
/// `max_keys` at a time.
async fn list_from(
    store: &FsStore,
    prefix: &str,
    max_keys: usize,
    mut continuation: Option<String>,
) -> Vec<String> {
    let request = ListRequest::new(prefix).with_max_keys(max_keys);
    let mut keys = Vec::new();
    loop {
        let page = match continuation.as_deref() {
            Some(after) => store.list(&request.after(after)).await,
            None => store.list(&request).await,
        }
        .expect("list");
        keys.extend(page.objects.into_iter().map(|object| object.key));
        match page.next {
            Some(next) => continuation = Some(next),
            None => return keys,
        }
    }
}

/// The steps that `note` saw on this thread since the last call, with the
/// paths under `root` cut to keys.
fn steps(root: &Path) -> Vec<String> {
    let root = root.display().to_string();
    NOTED_STEPS
        .take()
        .into_iter()
        .map(|step| step.replace(&format!("{root}/"), "").replace(&root, "."))
        .collect()
}

fn keys(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|key| (*key).to_owned()).collect()
}

/// The names in `directory`, dot-names too, in byte order.
fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("read directory")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .into_string()
                .expect("UTF-8")
        })
        .collect();
    names.sort_unstable();
    names
}

/// Waits for a cleanup task to empty `directory`.
async fn wait_until_empty(directory: &Path) {
    for _ in 0..200 {
        if names(directory).is_empty() {
            return;
        }
        compio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A body that yields its first piece, then tells `reached` and waits
/// forever.
fn stalled_body(reached: oneshot::Sender<()>) -> PutBody {
    let rest = stream::once(async move {
        let _ = reached.send(());
        future::pending::<io::Result<Bytes>>().await
    });
    PutBody::from_stream(
        10,
        stream::iter([Ok(Bytes::from_static(b"first"))]).chain(rest),
    )
}

fn is_bucket_not_found<T: Debug>(result: &Result<T, ObjectStoreError>) -> bool {
    matches!(result, Err(ObjectStoreError::BucketNotFound(_)))
}

fn is_malformed<T: Debug>(result: &Result<T, ObjectStoreError>) -> bool {
    matches!(result, Err(ObjectStoreError::Malformed(_)))
}

#[test]
fn given_object_when_reading_its_file_should_find_header_then_body_on_block_boundary() {
    let (directory, store) = store();

    shard_runtime().block_on(put(&store, "a/b/object", b"body bytes"));

    let file = std::fs::read(directory.path().join("a/b/object")).expect("object file");
    assert_eq!(&file[..8], b"IGGY-OBJ");
    assert_eq!(file.len(), 4096 + 10);
    assert_eq!(&file[4096..], b"body bytes");
}

#[test]
fn given_body_longer_than_the_sync_interval_when_putting_should_store_every_piece() {
    let (_directory, store) = store();
    let piece_len = usize::try_from(SYNC_INTERVAL / 3 + 1).unwrap();
    let pieces: Vec<Bytes> = (0..7_u8)
        .map(|fill| Bytes::from(vec![fill; piece_len]))
        .collect();
    let whole = pieces.concat();
    let body = PutBody::from_stream(whole.len() as u64, stream::iter(pieces.into_iter().map(Ok)));

    shard_runtime().block_on(async {
        store
            .put("a/large", body, &PutOptions::default())
            .await
            .unwrap();

        let meta = store.head("a/large").await.unwrap();
        assert_eq!(meta.size, whole.len() as u64);
        assert_eq!(meta.crc32c, Some(crc32c::crc32c(&whole)));
        let read = store.get_range("a/large", 0..meta.size).await.unwrap();
        assert_eq!(read.bytes, whole);
    });
}

#[test]
fn given_failed_puts_when_reading_directory_should_find_no_scratch_files() {
    let (directory, store) = store();

    shard_runtime().block_on(async {
        put(&store, "a/kept", b"first").await;
        let wrong_checksum = PutOptions {
            crc32c: Some(crc32c::crc32c(b"second") ^ 1),
            ..PutOptions::default()
        };
        let failing = PutBody::from_stream(
            8,
            stream::iter([
                Ok(Bytes::from_static(b"next")),
                Err(io::Error::other("read failed")),
            ]),
        );
        let if_absent = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };

        let refused = store
            .put("a/kept", PutBody::from_bytes("second"), &wrong_checksum)
            .await;
        let failed = store.put("a/kept", failing, &PutOptions::default()).await;
        let taken = store
            .put("a/kept", PutBody::from_bytes("third"), &if_absent)
            .await;

        assert!(matches!(
            refused,
            Err(ObjectStoreError::ChecksumMismatch { .. })
        ));
        assert!(matches!(failed, Err(ObjectStoreError::Body(_))));
        assert!(matches!(
            taken,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
    });

    assert_eq!(names(&directory.path().join("a")), ["kept"]);
}

#[test]
fn given_put_dropped_mid_body_when_calling_again_should_remove_scratch_file() {
    let (directory, store) = store();
    let parent = directory.path().join("a");

    shard_runtime().block_on(async {
        let (reached_sender, reached) = oneshot::channel();
        let options = PutOptions::default();
        let mut put = Box::pin(store.put("a/dropped", stalled_body(reached_sender), &options));
        if let Either::Left((result, _)) = select(&mut put, reached).await {
            panic!("the put ended before its body: {result:?}");
        }
        let scratch = names(&parent);
        assert!(
            matches!(scratch.as_slice(), [name] if name.starts_with(".dropped.")),
            "the put must write into a scratch file: {scratch:?}"
        );

        drop(put);
        let head = store.head("a/dropped").await;
        wait_until_empty(&parent).await;

        assert!(
            matches!(head, Err(ObjectStoreError::NotFound { .. })),
            "{head:?}"
        );
    });

    assert!(names(&parent).is_empty(), "{:?}", names(&parent));
}

/// A runtime that shuts down drops the puts in its tasks, and a put's drop
/// must not spawn a task while the executor drains them.
#[test]
fn given_runtime_dropped_with_put_in_flight_when_calling_again_should_remove_scratch_file() {
    let (directory, store) = store();
    let store = Rc::new(store);
    let parent = directory.path().join("a");
    let runtime = shard_runtime();

    runtime.block_on(async {
        let (reached_sender, reached) = oneshot::channel();
        let in_flight = Rc::clone(&store);
        compio::runtime::spawn(async move {
            let body = stalled_body(reached_sender);
            let _ = in_flight
                .put("a/stalled", body, &PutOptions::default())
                .await;
        })
        .detach();
        reached.await.expect("the put reaches its body");
    });
    drop(runtime);
    let left = names(&parent);
    assert!(
        matches!(left.as_slice(), [name] if name.starts_with(".stalled.")),
        "the dropped put leaves its scratch file to the next call: {left:?}"
    );

    shard_runtime().block_on(async {
        let _ = store.head("a/stalled").await;
        wait_until_empty(&parent).await;
    });

    assert!(names(&parent).is_empty(), "{:?}", names(&parent));
}

#[test]
fn given_missing_root_when_calling_should_report_bucket_not_found() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("missing");
    let store = FsStore::new(&root);

    shard_runtime().block_on(async {
        let nested = store
            .put("a/b", PutBody::from_bytes("x"), &PutOptions::default())
            .await;
        let flat = store
            .put("b", PutBody::from_bytes("x"), &PutOptions::default())
            .await;
        let head = store.head("a/b").await;
        let read = store.get_range("a/b", 0..1).await;
        let deleted = store.delete(&["a/b".to_owned()]).await;
        let listed = store.list(&ListRequest::new("a/")).await;

        assert!(is_bucket_not_found(&nested), "{nested:?}");
        assert!(is_bucket_not_found(&flat), "{flat:?}");
        assert!(is_bucket_not_found(&head), "{head:?}");
        assert!(is_bucket_not_found(&read), "{read:?}");
        assert!(is_bucket_not_found(&deleted), "{deleted:?}");
        assert!(is_bucket_not_found(&listed), "{listed:?}");
    });

    assert!(!root.exists(), "the store must never create its root");
}

#[test]
fn given_file_without_header_when_calling_should_refuse_it_and_list_it() {
    let (directory, store) = store();
    std::fs::create_dir(directory.path().join("a")).expect("directory");
    std::fs::write(directory.path().join("a/foreign"), b"no header").expect("file");

    shard_runtime().block_on(async {
        put(&store, "a/object", b"x").await;

        let head = store.head("a/foreign").await;
        let read = store.get_range("a/foreign", 0..1).await;
        let listed = list_keys(&store, "a/", 1).await;
        let deleted = store.delete(&["a/foreign".to_owned()]).await;

        assert!(
            matches!(head, Err(ObjectStoreError::Malformed(_))),
            "{head:?}"
        );
        assert!(
            matches!(read, Err(ObjectStoreError::Malformed(_))),
            "{read:?}"
        );
        assert_eq!(listed, ["a/foreign", "a/object"]);
        assert!(deleted.expect("delete").is_complete());
    });

    assert_eq!(names(&directory.path().join("a")), ["object"]);
}

#[test]
fn given_damaged_object_file_when_reading_should_report_malformed_and_list_it() {
    let (directory, store) = store();
    let path = directory.path().join("a/object");
    let options = PutOptions {
        metadata: UserMetadata::from([("name".to_owned(), "value".to_owned())]),
        ..PutOptions::default()
    };
    shard_runtime().block_on(async {
        store
            .put("a/object", PutBody::from_bytes("body"), &options)
            .await
            .expect("put");
    });
    let mut bytes = std::fs::read(&path).expect("object file");

    // Byte 55 lies in the metadata, which the header checksum covers.
    bytes[55] ^= 1;
    std::fs::write(&path, &bytes).expect("damage the header");
    let (damaged, damaged_listing) = shard_runtime().block_on(async {
        let head = store.head("a/object").await;
        (head, store.list(&ListRequest::new("a/")).await)
    });
    bytes[55] ^= 1;
    bytes.pop();
    std::fs::write(&path, &bytes).expect("cut the body");
    let (cut, cut_listing) = shard_runtime().block_on(async {
        let read = store.get_range("a/object", 0..1).await;
        (read, store.list(&ListRequest::new("a/")).await)
    });

    assert!(is_malformed(&damaged), "{damaged:?}");
    assert!(is_malformed(&cut), "{cut:?}");
    for listing in [damaged_listing, cut_listing] {
        let page = listing.expect("list");
        assert!(
            matches!(page.objects.as_slice(), [object] if object.key == "a/object" && object.etag.is_empty()),
            "{page:?}"
        );
    }
}

/// A listing shows damaged and foreign files, so the delete of a prefix
/// finds them, and it reads only the files that it lists.
#[test]
fn given_damaged_object_files_when_listing_should_show_them_for_deletes() {
    let (directory, store) = store();
    let damage = |key: &str, at: usize| {
        let path = directory.path().join(key);
        let mut bytes = std::fs::read(&path).expect("object file");
        bytes[at] ^= 1;
        std::fs::write(&path, &bytes).expect("damage the header");
    };
    shard_runtime().block_on(async {
        for key in ["a/1", "a/2", "a/3"] {
            put(&store, key, b"x").await;
        }
    });
    // The magic, then the body length that the header checksum covers.
    damage("a/2", 0);
    damage("a/3", 16);

    shard_runtime().block_on(async {
        let first = store.list(&ListRequest::new("a/").with_max_keys(1)).await;
        let listing = store.list(&ListRequest::new("a/")).await.expect("list");
        let heads = [store.head("a/2").await, store.head("a/3").await];
        let deleted = store.delete(&keys(&["a/2", "a/3"])).await;
        let left = list_keys(&store, "a/", 1000).await;

        assert_eq!(first.expect("first page").next.as_deref(), Some("a/1"));
        let listed: Vec<_> = listing
            .objects
            .iter()
            .map(|object| (object.key.as_str(), object.size, object.etag.is_empty()))
            .collect();
        assert_eq!(
            listed,
            [("a/1", 1, false), ("a/2", 4097, true), ("a/3", 4097, true)]
        );
        for head in heads {
            assert!(is_malformed(&head), "{head:?}");
        }
        assert!(deleted.expect("delete").is_complete());
        assert_eq!(left, ["a/1"]);
    });
}

/// Reads do not check the body, so a damaged body reads back as it is. A
/// head still gives the CRC32C of the put, for a caller that reads it whole.
#[test]
fn given_damaged_body_when_reading_should_return_it_as_it_is() {
    let (directory, store) = store();
    let path = directory.path().join("a/object");
    shard_runtime().block_on(put(&store, "a/object", b"body"));
    let mut bytes = std::fs::read(&path).expect("object file");
    *bytes.last_mut().expect("a body byte") ^= 1;
    std::fs::write(&path, &bytes).expect("damage the body");

    let (head, read) = shard_runtime().block_on(async {
        let head = store.head("a/object").await.expect("head");
        (head, store.get_range("a/object", 0..4).await.expect("read"))
    });

    assert_eq!(&read.bytes[..], b"bodx");
    assert_eq!(head.crc32c, Some(crc32c::crc32c(b"body")));
}

/// A put syncs the directories of its key once per store, then its scratch
/// file. It publishes the scratch file, then syncs its directory.
#[test]
fn given_puts_when_publishing_should_sync_the_scratch_file_then_publish_it_then_sync_its_directory()
{
    let (directory, store) = store();
    let root = directory.path();
    let if_absent = PutOptions {
        mode: PutMode::IfAbsent,
        ..PutOptions::default()
    };

    let (first, second, refused, taken) = shard_runtime().block_on(async {
        put(&store, "a/b/first", b"1").await;
        let first = steps(root);
        store
            .put("a/b/second", PutBody::from_bytes("2"), &if_absent)
            .await
            .expect("put");
        let second = steps(root);
        let refused = store
            .put("a/b/second", PutBody::from_bytes("3"), &if_absent)
            .await;
        (first, second, refused, steps(root))
    });

    assert_eq!(
        first,
        [
            "sync dir .",
            "sync dir a",
            "sync scratch of a/b/first",
            "rename to a/b/first",
            "sync dir a/b"
        ]
    );
    assert_eq!(
        second,
        [
            "sync scratch of a/b/second",
            "link to a/b/second",
            "sync dir a/b"
        ]
    );
    assert!(
        matches!(refused, Err(ObjectStoreError::PreconditionFailed { .. })),
        "{refused:?}"
    );
    // The put that linked the object can have lost its answer before its
    // sync, so a refused put syncs the directory first.
    assert_eq!(taken, ["sync scratch of a/b/second", "sync dir a/b"]);
}

/// A delete syncs the directory after its removes, also for a key already
/// gone: the delete that removed it can have lost its answer before its sync.
#[test]
fn given_deletes_when_removing_should_sync_the_directory_after_the_removes() {
    let (directory, store) = store();
    let root = directory.path();

    let (present, absent) = shard_runtime().block_on(async {
        put(&store, "a/b/object", b"x").await;
        steps(root);
        let deleted = store.delete(&keys(&["a/b/object", "a/b/gone"])).await;
        assert!(deleted.expect("delete").is_complete());
        let present = steps(root);
        let deleted = store.delete(&keys(&["a/b/gone"])).await;
        assert!(deleted.expect("delete").is_complete());
        (present, steps(root))
    });

    assert_eq!(present, ["remove a/b/object", "sync dir a/b"]);
    assert_eq!(absent, ["sync dir a/b"]);
}

#[test]
fn given_each_io_error_kind_when_mapping_should_give_its_retry_class() {
    let cases = [
        (io::ErrorKind::PermissionDenied, ErrorClass::Permanent),
        (io::ErrorKind::ReadOnlyFilesystem, ErrorClass::Permanent),
        (io::ErrorKind::FileTooLarge, ErrorClass::Permanent),
        (io::ErrorKind::InvalidFilename, ErrorClass::Permanent),
        (io::ErrorKind::StorageFull, ErrorClass::Transient),
        (io::ErrorKind::QuotaExceeded, ErrorClass::Transient),
        (io::ErrorKind::Interrupted, ErrorClass::Transient),
        (io::ErrorKind::Other, ErrorClass::Transient),
    ];

    for (kind, class) in cases {
        let error = io_error("a/b", "writing", &io::Error::from(kind));
        assert_eq!(error.class(), class, "{kind:?}: {error}");
    }
}

#[test]
fn given_key_clashing_with_another_when_calling_should_refuse_or_find_nothing() {
    let (directory, store) = store();

    shard_runtime().block_on(async {
        put(&store, "a", b"object").await;
        put(&store, "c/d", b"object").await;

        let under_object = store
            .put("a/b", PutBody::from_bytes("x"), &PutOptions::default())
            .await;
        let deeper_under_object = store
            .put("a/b/c", PutBody::from_bytes("x"), &PutOptions::default())
            .await;
        let over_directory = store
            .put("c", PutBody::from_bytes("x"), &PutOptions::default())
            .await;
        let if_absent = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };
        let if_absent_over_directory = store.put("c", PutBody::from_bytes("x"), &if_absent).await;
        let head = store.head("c").await;
        let deleted = store
            .delete(&["c".to_owned(), "a/b".to_owned(), "e/f".to_owned()])
            .await;

        assert!(
            matches!(under_object, Err(ObjectStoreError::InvalidRequest(_))),
            "{under_object:?}"
        );
        assert!(
            matches!(
                deeper_under_object,
                Err(ObjectStoreError::InvalidRequest(_))
            ),
            "{deeper_under_object:?}"
        );
        assert!(
            matches!(over_directory, Err(ObjectStoreError::InvalidRequest(_))),
            "{over_directory:?}"
        );
        assert!(
            matches!(
                if_absent_over_directory,
                Err(ObjectStoreError::InvalidRequest(_))
            ),
            "{if_absent_over_directory:?}"
        );
        assert!(
            matches!(head, Err(ObjectStoreError::NotFound { .. })),
            "{head:?}"
        );
        assert!(deleted.expect("delete").is_complete());
        assert_eq!(list_keys(&store, "", 1000).await, ["a", "c/d"]);
    });

    assert_eq!(names(directory.path()), ["a", "c"]);
    assert_eq!(names(&directory.path().join("c")), ["d"]);
}

/// A continuation inside one directory must skip the directories before it
/// and still enter the ones after it.
#[test]
fn given_nested_keys_when_paging_should_visit_each_once_in_byte_order() {
    let (_directory, store) = store();
    let mut keys = [
        "p/a/1",
        "p/a/2/x",
        "p/a-b",
        "p/a.c",
        "p/a0",
        "p/b/c/d/e",
        "p/b0",
        "p/c",
        "q/x",
    ];

    shard_runtime().block_on(async {
        for key in keys.iter().rev() {
            put(&store, key, b"x").await;
        }
        keys.sort_unstable();
        let under_p = &keys[..keys.len() - 1];

        for max_keys in [1, 2, 3, 1000] {
            assert_eq!(
                list_keys(&store, "p/", max_keys).await,
                under_p,
                "max_keys {max_keys}"
            );
        }
        assert_eq!(
            list_keys(&store, "p/a", 1).await,
            ["p/a-b", "p/a.c", "p/a/1", "p/a/2/x", "p/a0"]
        );
        assert_eq!(list_keys(&store, "", 2).await, keys);
    });
}

/// The cache can hide only a put that lands while the listing runs. The
/// test sets the time of the directory back to hide one.
#[test]
fn given_directory_read_by_last_page_when_resuming_should_reuse_it_until_it_changes() {
    let (directory, store) = store();
    let parent = directory.path().join("a");
    let set_modified = |time: SystemTime| {
        std::fs::File::open(&parent)
            .and_then(|handle| handle.set_modified(time))
            .expect("directory time");
    };

    shard_runtime().block_on(async {
        for key in ["a/1", "a/3", "a/5"] {
            put(&store, key, b"x").await;
        }
        let request = ListRequest::new("a/").with_max_keys(1);

        let first = store.list(&request).await.expect("list").next;
        let read_at = std::fs::metadata(&parent)
            .and_then(|metadata| metadata.modified())
            .expect("directory time");
        put(&store, "a/4", b"x").await;
        set_modified(read_at);
        let reused = list_from(&store, "a/", 1, first).await;

        let first = store.list(&request).await.expect("list").next;
        put(&store, "a/6", b"x").await;
        set_modified(read_at + Duration::from_secs(1));
        let read_again = list_from(&store, "a/", 1, first).await;

        assert_eq!(reused, ["a/3", "a/5"]);
        assert_eq!(read_again, ["a/3", "a/4", "a/5", "a/6"]);
    });
}

#[test]
fn given_stale_scratch_file_when_listing_should_remove_only_it() {
    let (directory, store) = store();
    let parent = directory.path().join("a");
    shard_runtime().block_on(put(&store, "a/object", b"x"));
    let etag = "0123456789abcdef0123456789abcdef";
    let two_hours_ago = SystemTime::now() - Duration::from_hours(2);
    for (name, modified) in [
        (format!(".stale.{etag}"), two_hours_ago),
        (format!(".fresh.{etag}"), SystemTime::now()),
        (".hidden".to_owned(), two_hours_ago),
    ] {
        let file = std::fs::File::create(parent.join(name)).expect("file");
        file.set_modified(modified).expect("modification time");
    }

    let listed = shard_runtime().block_on(list_keys(&store, "a/", 1000));

    assert_eq!(listed, ["a/object"]);
    assert_eq!(
        names(&parent),
        [
            format!(".fresh.{etag}"),
            ".hidden".to_owned(),
            "object".to_owned()
        ]
    );
}

#[cfg(unix)]
#[test]
fn given_dot_names_and_links_when_listing_should_skip_them() {
    let (directory, store) = store();
    let parent = directory.path().join("a");

    shard_runtime().block_on(put(&store, "a/object", b"x"));
    std::fs::write(parent.join(".hidden"), b"x").expect("dot file");
    std::fs::create_dir(parent.join(".scratch")).expect("dot directory");
    std::fs::write(parent.join(".scratch/inside"), b"x").expect("file in dot directory");
    std::os::unix::fs::symlink(parent.join("object"), parent.join("link")).expect("link");

    assert_eq!(
        shard_runtime().block_on(list_keys(&store, "a/", 1000)),
        ["a/object"]
    );
}
