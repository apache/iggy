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

//! A server with three geo peers on this host: two `fs` peers whose roots
//! other clusters claimed, and an `s3` peer whose store refuses
//! connections. No container runs.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use iggy::prelude::*;
use iggy_object_store::{FsStore, KeyLayout, StoreIdentity};
use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

/// How long the first identity checks of a new server take at most.
const CHECK_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const DC_B: u128 = 0x0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b;
const DC_C: u128 = 0x0c0c_0c0c_0c0c_0c0c_0c0c_0c0c_0c0c_0c0c;
const STREAM: &str = "geo-local";
const TOPIC: &str = "orders";
const MESSAGES: u32 = 10;

/// Every entry under a directory, with its length and the time it changed.
type Snapshot = BTreeMap<PathBuf, (u64, SystemTime)>;

#[tokio::test]
async fn given_two_claimed_peers_and_one_that_refuses_connections_when_the_server_boots_should_verify_both_and_serve()
 {
    let dc_b = tempfile::tempdir().expect("a directory for dc-b");
    let dc_c = tempfile::tempdir().expect("a directory for dc-c");
    claim(dc_b.path(), DC_B).await;
    claim(dc_c.path(), DC_C).await;
    let before = [snapshot(dc_b.path()), snapshot(dc_c.path())];
    let mut harness = server_with_peers(dc_b.path(), dc_c.path(), &refusing_endpoint());

    harness
        .start()
        .await
        .expect("a peer whose store does not answer never stops the boot");

    send_and_poll(&harness).await;
    let http = HttpClient::login_root(&harness).await;
    wait_for_gauges(
        &http,
        &[
            ("dc-b", true, false),
            ("dc-c", true, false),
            ("dc-d", false, true),
        ],
    )
    .await;
    send_and_poll(&harness).await;
    assert_eq!(
        [snapshot(dc_b.path()), snapshot(dc_c.path())],
        before,
        "the server changed nothing in the store of a peer"
    );
}

/// A harness whose one server has the origin `dc-a`, the `fs` peers `dc-b`
/// and `dc-c` at the two directories, and the `s3` peer `dc-d` at
/// `dc_d_endpoint`.
fn server_with_peers(dc_b: &Path, dc_c: &Path, dc_d_endpoint: &str) -> TestHarness {
    let path = |directory: &Path| directory.to_string_lossy().into_owned();
    let envs = [
        ("IGGY_GEO_ORIGIN", "dc-a".to_owned()),
        ("IGGY_GEO_PEERS_0_ORIGIN", "dc-b".to_owned()),
        ("IGGY_GEO_PEERS_0_BACKEND", "fs".to_owned()),
        ("IGGY_GEO_PEERS_0_PATH", path(dc_b)),
        ("IGGY_GEO_PEERS_1_ORIGIN", "dc-c".to_owned()),
        ("IGGY_GEO_PEERS_1_BACKEND", "fs".to_owned()),
        ("IGGY_GEO_PEERS_1_PATH", path(dc_c)),
        ("IGGY_GEO_PEERS_2_ORIGIN", "dc-d".to_owned()),
        ("IGGY_GEO_PEERS_2_BACKEND", "s3".to_owned()),
        ("IGGY_GEO_PEERS_2_BUCKET", "iggy-dc-d".to_owned()),
        ("IGGY_GEO_PEERS_2_REGION", "us-east-1".to_owned()),
        ("IGGY_GEO_PEERS_2_ENDPOINT", dc_d_endpoint.to_owned()),
        (
            "IGGY_GEO_PEERS_2_ACCESS_KEY_ID",
            "AKIDPEEREXAMPLE".to_owned(),
        ),
        (
            "IGGY_GEO_PEERS_2_SECRET_ACCESS_KEY",
            "peerSecretKeyExample".to_owned(),
        ),
    ];
    let extra_envs: HashMap<String, String> = envs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// The URL of a port on this host that nothing listens on.
fn refusing_endpoint() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    let address = listener.local_addr().expect("the port");
    drop(listener);
    format!("http://{address}")
}

/// Claims the root `iggy` of the `fs` store at `path` for `cluster_uid`, as
/// the replica 0 of that cluster does. The provider runs on compio, not on
/// the harness's runtime.
async fn claim(path: &Path, cluster_uid: u128) {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                let claimed = StoreIdentity { cluster_uid }
                    .claim(&FsStore::new(&path), &layout)
                    .await
                    .expect("the claim");
                assert!(claimed, "the root was free");
            });
    })
    .await
    .expect("the claim task");
}

/// Sends messages to a new topic of this cluster and polls them back.
async fn send_and_poll(harness: &TestHarness) {
    let client = harness.tcp_root_client().await.expect("a root client");
    let stream = Identifier::named(STREAM).expect("the stream identifier");
    let topic = Identifier::named(TOPIC).expect("the topic identifier");
    if client
        .get_stream(&stream)
        .await
        .expect("get stream")
        .is_none()
    {
        client
            .create_stream(STREAM, &StreamCreateOptions::default())
            .await
            .expect("create stream");
        client
            .create_topic(
                &stream,
                TOPIC,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .expect("create topic");
    }
    let mut messages: Vec<IggyMessage> = (0..MESSAGES)
        .map(|index| {
            IggyMessage::builder()
                .payload(format!("order-{index}").into_bytes().into())
                .build()
                .expect("a message")
        })
        .collect();
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: MESSAGES,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll messages");
    assert_eq!(polled.messages.len(), MESSAGES as usize);
}

/// Waits until `/metrics` shows each `(origin, verified, unreachable)`.
pub(super) async fn wait_for_gauges(http: &HttpClient, expected: &[(&str, bool, bool)]) {
    let deadline = Instant::now() + CHECK_DEADLINE;
    loop {
        let exposition = scrape(http).await;
        let found: Vec<_> = expected
            .iter()
            .map(|(origin, _, _)| {
                (
                    *origin,
                    gauge(&exposition, "geo_peer_identity_verified", origin),
                    gauge(&exposition, "geo_peer_unreachable", origin),
                )
            })
            .collect();
        let wanted: Vec<_> = expected
            .iter()
            .map(|(origin, verified, unreachable)| {
                (
                    *origin,
                    Some(u64::from(*verified)),
                    Some(u64::from(*unreachable)),
                )
            })
            .collect();
        if found == wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the geo peer gauges never settled: {found:?}, expected {wanted:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(
        response.status().is_success(),
        "/metrics scrape failed: {}",
        response.status()
    );
    response.text().await.expect("the metrics text")
}

/// The value of the `name` series of the peer `origin`. Every shard exports
/// the same node gauges, so any shard's series holds it.
pub(crate) fn gauge(exposition: &str, name: &str, origin: &str) -> Option<u64> {
    let series_prefix = format!("{name}{{");
    let origin_label = format!("origin=\"{origin}\"");
    exposition
        .lines()
        .filter(|line| line.starts_with(&series_prefix))
        .find(|line| line.contains(&origin_label))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse().ok())
}

/// Every file and directory under `root`, with its length and the time it
/// last changed.
fn snapshot(root: &Path) -> Snapshot {
    let mut entries = Snapshot::new();
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory).expect("a readable directory") {
            let entry = entry.expect("a directory entry");
            let metadata = entry.metadata().expect("the entry's metadata");
            if metadata.is_dir() {
                directories.push(entry.path());
            }
            let relative = entry
                .path()
                .strip_prefix(root)
                .expect("an entry under the root")
                .to_owned();
            entries.insert(
                relative,
                (
                    metadata.len(),
                    metadata.modified().expect("the modification time"),
                ),
            );
        }
    }
    entries
}
