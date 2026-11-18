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

//! A geo topic on three nodes that share one `fs` object store: its
//! partition primary dies in the middle of an upload or of a rewrite into
//! long-term segments, or every node loses its disk.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use tempfile::TempDir;
use tokio::time::{Instant, sleep};

use crate::server::scenarios::geo_mode_faults_scenario::{
    Answer, Bucket, Ledger, MESSAGE_BYTES, POLL_INTERVAL, SETTLE_DEADLINE, STORE_PREFIX,
    SettledBucket, Topics, assert_settled, geo_manifest, produce_around, read_indexes,
};

const NODES: usize = 3;
/// Sends acknowledged before the primary dies, and after it did.
const SENDS_BEFORE: usize = 30;
const SENDS_AFTER: usize = 30;
/// Large enough that the upload of a long-term segment runs for a while.
const REWRITE_MESSAGE_BYTES: usize = 64 * 1024;
/// Sends rewritten into long-term segments before the disks are lost, and
/// sends committed after that.
const PUBLISHED_SENDS: u64 = 20;
const UNPUBLISHED_SENDS: u64 = 20;
const TRIGGER_DEADLINE: Duration = Duration::from_secs(60);
/// Uploads take milliseconds, so their files are looked for that often.
const TRIGGER_POLL: Duration = Duration::from_millis(1);
const FOREIGN_ROOT_MARKER: &str = "the store identity object names another cluster";
/// How long a view change that a restart of the metadata leader starts may
/// take to settle.
const LEADER_RESTART_PAUSE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn given_a_geo_topic_on_three_nodes_when_its_primary_dies_during_an_upload_should_read_each_acknowledged_send_once_in_order()
 {
    let store = store_on_disk();
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    await_store_claim(&mut harness, store.path()).await;
    let setup = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let topics = Topics::create(&setup, "geo-upload-primary-loss").await;
    let primary = topics.geo_primary(&setup).await;
    let client = client_of_another_node(&harness, primary).await;
    let uploads = store
        .path()
        .join(STORE_PREFIX)
        .join("meta/fresh")
        .join(format!("{primary:02x}"));

    let ledger = produce_around(
        &client,
        &topics,
        MESSAGE_BYTES,
        SENDS_BEFORE,
        SENDS_AFTER,
        async {
            // A put writes its object under a dot-name first, so one is
            // there while the primary uploads.
            await_file(&uploads, |name| name.starts_with('.')).await;
            harness
                .kill_node(usize::from(primary))
                .expect("kill the primary");
        },
    )
    .await;

    assert_ne!(topics.geo_primary(&client).await, primary);
    assert_settled(
        &client,
        &topics,
        &ledger,
        &Bucket::Fs(store.path().to_owned()),
    )
    .await;
}

#[tokio::test]
async fn given_a_geo_topic_on_three_nodes_when_its_primary_dies_during_a_rewrite_should_read_each_acknowledged_send_once_in_order()
 {
    let store = store_on_disk();
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    await_store_claim(&mut harness, store.path()).await;
    let setup = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let topics = Topics::create(&setup, "geo-rewrite-primary-loss").await;
    let primary = topics.geo_primary(&setup).await;
    let client = client_of_another_node(&harness, primary).await;
    let (stream_id, _) = topics.geo_prefix_ids(&setup).await;
    let segments = store.path().join(STORE_PREFIX).join(stream_id.to_string());

    let ledger = produce_around(
        &client,
        &topics,
        REWRITE_MESSAGE_BYTES,
        SENDS_BEFORE,
        SENDS_AFTER,
        async {
            // Only the jobs that write long-term segments put objects under
            // the stream's own prefix, and a put writes under a dot-name
            // first. The primary then dies while it stores a segment that
            // its log does not list yet.
            await_file(&segments, |name| name.starts_with('.')).await;
            harness
                .kill_node(usize::from(primary))
                .expect("kill the primary");
        },
    )
    .await;

    assert_ne!(topics.geo_primary(&client).await, primary);
    assert_settled(
        &client,
        &topics,
        &ledger,
        &Bucket::Fs(store.path().to_owned()),
    )
    .await;
}

/// A cluster that lost every disk is a new cluster: its metadata, with the
/// geo topic and where each send sits, is gone, and it gets a new
/// `cluster_uid`. Its nodes refuse the root that names the earlier one, so
/// they change no object there and accept no geo topic. The bucket keeps
/// what the earlier cluster published, the long-term segments its
/// manifest lists. The sends committed after the last rewrite sit in fresh
/// objects that nothing in the bucket names, so a reader of the bucket
/// cannot find them.
#[tokio::test]
async fn given_a_geo_topic_on_three_nodes_when_every_node_loses_its_disk_should_refuse_the_old_root_and_leave_its_objects_intact()
 {
    let store = store_on_disk();
    let bucket = Bucket::Fs(store.path().to_owned());
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    await_store_claim(&mut harness, store.path()).await;
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    let topics = Topics::create(&client, "geo-disk-loss").await;
    let mut ledger = Ledger::default();
    for _ in 0..PUBLISHED_SENDS {
        assert_eq!(
            ledger
                .send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }
    let (stream_id, topic_id) = topics.geo_prefix_ids(&client).await;
    bucket
        .await_rewritten(stream_id, topic_id, PUBLISHED_SENDS - 1)
        .await;
    // One send right after a rewrite, so the next rewrite, a pacing
    // interval away, cannot take it before the nodes die.
    assert_eq!(
        ledger
            .send_batch(
                &client,
                &topics.stream,
                &topics.geo,
                UNPUBLISHED_SENDS,
                MESSAGE_BYTES
            )
            .await,
        Answer::Acked
    );
    let read = read_indexes(&client, &topics.stream, &topics.geo)
        .await
        .expect("read the geo topic");
    ledger.assert_read(&read);
    harness.kill_cluster().expect("kill every node");
    let last = PUBLISHED_SENDS + UNPUBLISHED_SENDS - 1;
    let objects = bucket.objects().await;
    let manifest =
        geo_manifest(&objects, stream_id, topic_id).expect("the bucket holds the manifest");
    let floor = manifest.floor.expect("the manifest lists segments");
    assert!(
        floor < last,
        "every send was rewritten before the nodes died, floor {floor}"
    );
    SettledBucket {
        manifest,
        objects: objects.clone(),
    }
    .assert_long_term_log();

    for node in 0..NODES {
        harness
            .restart_node_from_clean_slate(node)
            .expect("restart the node without its disk");
    }
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !(0..NODES).all(|node| harness.node(node).stdout_contains(FOREIGN_ROOT_MARKER)) {
        assert!(
            Instant::now() < deadline,
            "not every node found the root claimed by the earlier cluster"
        );
        sleep(POLL_INTERVAL).await;
    }

    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    assert!(
        client
            .get_stream(&topics.stream)
            .await
            .expect("get stream")
            .is_none(),
        "the stream outlived every disk"
    );
    client
        .create_stream("after-disk-loss", &StreamCreateOptions::default())
        .await
        .expect("create a stream");
    let refused = client
        .create_topic(
            &Identifier::named("after-disk-loss").expect("stream identifier"),
            "geo",
            &TopicCreateOptions {
                partitions_count: Some(1),
                topic_type: Some(TopicType::Geo),
                ..TopicCreateOptions::default()
            },
        )
        .await;
    assert_eq!(refused.map(|_| ()), Err(IggyError::FeatureUnavailable));
    assert!(
        bucket.objects().await == objects,
        "a node changed objects under the root of the earlier cluster"
    );
}

fn cluster_with_fs_store(store: &Path) -> TestHarness {
    let extra_envs = HashMap::from([
        // A geo topic takes messages once the metadata primary stored its
        // object, and a node without an origin stores none.
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_TIERED_STORAGE_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    TestHarness::builder()
        .cluster_nodes(NODES)
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Waits until replica 0 claimed the store. It claims only while it leads
/// metadata, and on a busy host the lead can move away while the cluster
/// boots. The server README's way out is to restart the leader until
/// replica 0 leads.
async fn await_store_claim(harness: &mut TestHarness, store: &Path) {
    let identity = store.join(STORE_PREFIX).join("meta").join("identity");
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !identity.exists() {
        assert!(
            Instant::now() < deadline,
            "replica 0 never claimed the store"
        );
        match metadata_leader(harness).await {
            Some(leader) if leader != 0 => {
                harness
                    .restart_node(leader)
                    .expect("restart the metadata leader");
                sleep(LEADER_RESTART_PAUSE).await;
            }
            _ => sleep(POLL_INTERVAL).await,
        }
    }
}

/// The node that leads metadata, as the first node that answers says.
async fn metadata_leader(harness: &TestHarness) -> Option<usize> {
    for node in 0..NODES {
        let Ok(client) = harness.root_client_for_node(node).await else {
            continue;
        };
        let Ok(metadata) = client.get_cluster_metadata().await else {
            continue;
        };
        let port = metadata
            .nodes
            .iter()
            .find(|candidate| candidate.role == ClusterNodeRole::Leader)?
            .endpoints
            .tcp;
        return (0..NODES).find(|index| {
            harness
                .node(*index)
                .tcp_addr()
                .is_some_and(|address| address.port() == port)
        });
    }
    None
}

/// A store on a disk that syncs, unlike a temporary directory in memory, so
/// that an upload lasts long enough to be caught.
fn store_on_disk() -> TempDir {
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a directory for the object store")
}

/// A root client whose first connection goes to a node other than replica
/// `primary`, so that it outlives the primary.
async fn client_of_another_node(harness: &TestHarness, primary: u8) -> IggyClient {
    let node = (0..NODES)
        .find(|node| *node != usize::from(primary))
        .expect("another node");
    harness
        .root_client_for_node(node)
        .await
        .expect("connect a root client")
}

/// Waits until a file whose name `matches` is under `root`, at any depth.
async fn await_file(root: &Path, matches: impl Fn(&str) -> bool) {
    let deadline = Instant::now() + TRIGGER_DEADLINE;
    while find_file(root, &matches).is_none() {
        assert!(
            Instant::now() < deadline,
            "no file under {} matched within {TRIGGER_DEADLINE:?}",
            root.display()
        );
        sleep(TRIGGER_POLL).await;
    }
}

fn find_file(root: &Path, matches: &impl Fn(&str) -> bool) -> Option<PathBuf> {
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push(path);
            } else if entry.file_name().to_str().is_some_and(matches) {
                return Some(path);
            }
        }
    }
    None
}
