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

//! The sweep of the `fs` store root for the objects of partition
//! incarnations that committed metadata no longer holds, on one node.
//!
//! The tests store the objects of such incarnations by hand. Revision 0 is
//! below every revision committed metadata reaches, and no partition is ever
//! created at it, so an incarnation at revision 0 is never live.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{
    FreshObjectId, FsStore, GeoLog, KeyLayout, LongTermKey, ObjectPrefix, ObjectStore,
    PositionsKey, PutBody, PutOptions, SegmentKey, SegmentObject, StoreIdentity, TopicKey,
};
use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;
use super::tiered_storage_fs::{
    POLL_INTERVAL, STREAM, TOPIC, assert_reads_every_message, create_tiered_topic, fs_store_envs,
    metric, send_every_message, wait_for_upload,
};

/// Short, so the sweeps a delete needs fit a test. The sweep runs every
/// grace when that is shorter than an hour.
const GRACE: &str = "2s";
/// How long the sweeps may take to delete an orphan, or to show anything.
const SWEEP_DEADLINE: Duration = Duration::from_secs(60);
const SWEEPS: &str = "orphan_sweeps_total";
const ORPHANS: &str = "orphan_incarnations";
const DELETED: &str = "orphan_incarnations_deleted_total";
const IDENTITY_SKIPS: &str = "orphan_identity_skips_total";
/// Every shard exports the gauge of its node, so the sum counts the shards.
const IDENTITY_VERIFIED: &str = "object_store_identity_verified";
/// A cluster other than the one under test.
const OTHER_CLUSTER_UID: u128 = 7;

/// The incarnation at revision `created_revision` of a stream id that the
/// tests never create.
const fn absent(stream_id: u64, created_revision: u64) -> ObjectPrefix {
    ObjectPrefix {
        stream_id,
        topic_id: 0,
        partition_id: 0,
        created_revision,
    }
}

const ORPHAN: ObjectPrefix = absent(900, 0);

#[tokio::test]
async fn given_an_orphan_next_to_other_keys_when_two_sweeps_pass_its_grace_should_delete_it_alone()
{
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = single_node(store.path(), GRACE);
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;
    send_every_message(&client, &stream, &topic).await;
    wait_for_upload(store.path()).await;
    let live = keys_under(store.path(), "iggy");
    let layout = layout();
    let orphan = incarnation_keys(ORPHAN);
    let above_the_revision = incarnation_keys(absent(901, u64::MAX));
    let long_term = absent(902, 0);
    let mut geo = incarnation_keys(long_term);
    geo.push(layout.long_term_key(&LongTermKey {
        prefix: long_term,
        start_offset: 0,
        end_offset: 9,
        object: SegmentObject::Log,
    }));
    let tombstoned = absent(903, 0);
    geo.extend(incarnation_keys(tombstoned));
    geo.push(layout.topic_key(&TopicKey {
        stream_id: tombstoned.stream_id,
        topic_id: tombstoned.topic_id,
        created_revision: tombstoned.created_revision,
    }));
    let meta = vec![
        layout.fresh_key(FreshObjectId::new(9, 0, 0, 1).expect("a fresh id").get()),
        layout.positions_key(&PositionsKey {
            log: GeoLog {
                origin: "dc-b".to_owned(),
                stream_id: 900,
                topic_id: 0,
                created_revision: 0,
            },
            replica: 9,
        }),
        "iggy/meta/notes".to_owned(),
        "iggy/notes".to_owned(),
    ];
    let kept: Vec<String> = [above_the_revision, geo, meta].concat();
    put_objects(store.path(), [orphan.clone(), kept.clone()].concat()).await;
    let sweeps_before = metric(&http, SWEEPS).await;

    wait_until_gone(store.path(), &orphan).await;

    assert!(metric(&http, DELETED).await >= 1);
    // The sweeps that follow find nothing more to delete.
    let sweeps = metric(&http, SWEEPS).await;
    assert!(sweeps >= sweeps_before + 2, "{sweeps_before} then {sweeps}");
    wait_for_metric(&http, SWEEPS, |count| count >= sweeps + 2).await;
    assert_eq!(metric(&http, ORPHANS).await, 0);
    for key in kept.iter().chain(&live) {
        assert!(store.path().join(key).exists(), "the sweep deleted {key}");
    }
    assert!(store.path().join(layout.identity_key()).exists());
    assert_reads_every_message(&client, &stream, &topic).await;
}

#[tokio::test]
async fn given_a_store_identity_of_another_cluster_when_the_sweeps_run_should_delete_nothing() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    // Long enough to replace the identity between the sweep that finds the
    // orphan and the one that would delete it.
    let mut harness = single_node(store.path(), "5s");
    harness.start().await.expect("the server boots");
    let http = HttpClient::login_root(&harness).await;
    wait_for_metric(&http, IDENTITY_VERIFIED, |verified| verified >= 1).await;
    let orphan = incarnation_keys(ORPHAN);
    put_objects(store.path(), orphan.clone()).await;
    wait_for_metric(&http, ORPHANS, |found| found == 1).await;

    replace_identity(store.path(), OTHER_CLUSTER_UID).await;
    wait_for_metric(&http, IDENTITY_SKIPS, |skips| skips >= 1).await;
    sleep(Duration::from_secs(12)).await;

    for key in &orphan {
        assert!(store.path().join(key).exists(), "the sweep deleted {key}");
    }
    assert_eq!(metric(&http, DELETED).await, 0);
    assert_eq!(metric(&http, ORPHANS).await, 0);
}

#[tokio::test]
async fn given_orphan_grace_disabled_when_the_server_runs_should_sweep_nothing() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = single_node(store.path(), "disabled");
    harness.start().await.expect("the server boots");
    let http = HttpClient::login_root(&harness).await;
    wait_for_metric(&http, IDENTITY_VERIFIED, |verified| verified >= 1).await;
    let orphan = incarnation_keys(ORPHAN);
    put_objects(store.path(), orphan.clone()).await;

    // A zero grace would sweep on every reconciler pass, once a second.
    sleep(Duration::from_secs(8)).await;

    for key in &orphan {
        assert!(store.path().join(key).exists(), "the sweep deleted {key}");
    }
    assert_eq!(metric(&http, SWEEPS).await, 0);
    assert_eq!(metric(&http, ORPHANS).await, 0);
}

fn single_node(store: &Path, grace: &str) -> TestHarness {
    let mut envs: HashMap<String, String> = fs_store_envs(store);
    envs.insert(
        "IGGY_TIERED_STORAGE_ORPHAN_GRACE".to_owned(),
        grace.to_owned(),
    );
    TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .build()
        .expect("the harness")
}

fn layout() -> KeyLayout {
    KeyLayout::new("iggy").expect("the default prefix")
}

/// Both objects of two segments and the manifest of `prefix`.
fn incarnation_keys(prefix: ObjectPrefix) -> Vec<String> {
    let layout = layout();
    let mut keys: Vec<String> = [0, 10]
        .into_iter()
        .flat_map(|start_offset| {
            SegmentObject::ALL.map(|object| {
                layout.segment_key(&SegmentKey {
                    prefix,
                    start_offset,
                    object,
                })
            })
        })
        .collect();
    keys.push(layout.manifest_key(&prefix));
    keys
}

/// The keys of every object file under `directory` of the store at `root`.
fn keys_under(root: &Path, directory: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut pending = vec![root.join(directory)];
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if !entry.file_name().to_string_lossy().starts_with('.') {
                let key = path
                    .strip_prefix(root)
                    .expect("a file in the store")
                    .to_string_lossy()
                    .into_owned();
                keys.push(key);
            }
        }
    }
    keys
}

/// Put an object under each of `keys` of the store at `root`.
async fn put_objects(root: &Path, keys: Vec<String>) {
    change_store(root, StoreChange::Put(keys)).await;
}

/// Write the identity of `cluster_uid` over the store identity object, as
/// the recovery of another cluster does.
async fn replace_identity(root: &Path, cluster_uid: u128) {
    change_store(root, StoreChange::ReplaceIdentity(cluster_uid)).await;
}

enum StoreChange {
    Put(Vec<String>),
    ReplaceIdentity(u128),
}

/// Make `change` through the provider of the store at `root`, on a compio
/// runtime in a thread of its own, as the provider does not run on the
/// harness's.
async fn change_store(root: &Path, change: StoreChange) {
    let root: PathBuf = root.to_owned();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = FsStore::new(&root);
                match change {
                    StoreChange::Put(keys) => {
                        for key in &keys {
                            store
                                .put(key, PutBody::from_bytes(vec![1]), &PutOptions::default())
                                .await
                                .expect("the put of a test object");
                        }
                    }
                    StoreChange::ReplaceIdentity(cluster_uid) => {
                        StoreIdentity { cluster_uid }
                            .replace(&store, &layout())
                            .await
                            .expect("the replacement of the identity object");
                    }
                }
            });
        let _ = sender.send(());
    });
    receiver.await.expect("the change of the store finishes");
}

/// Wait until no key of `keys` is in the store at `root`.
async fn wait_until_gone(root: &Path, keys: &[String]) {
    let deadline = Instant::now() + SWEEP_DEADLINE;
    loop {
        let left: Vec<&String> = keys.iter().filter(|key| root.join(key).exists()).collect();
        if left.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {SWEEP_DEADLINE:?} the sweeps left {left:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Wait until the sum of `series` over the shards passes `ready`.
async fn wait_for_metric(http: &HttpClient, series: &str, ready: impl Fn(u64) -> bool) {
    let deadline = Instant::now() + SWEEP_DEADLINE;
    loop {
        let value = metric(http, series).await;
        if ready(value) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {SWEEP_DEADLINE:?} {series} stayed at {value}"
        );
        sleep(POLL_INTERVAL).await;
    }
}
