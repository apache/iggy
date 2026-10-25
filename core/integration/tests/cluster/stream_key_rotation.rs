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

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use iggy_common::ServerKeyContext;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::sleep;

const OLD_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const NEW_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const OLD_KEK_FINGERPRINT: &str = "39e4f0f95c3693c7";
const TOPIC_NAME: &str = "rotated-topic";
const STREAMS: [(&str, Option<u8>); 2] =
    [("rotated-own-key", Some(4)), ("rotated-generated", None)];
const MID_ROLLOUT_STREAM: &str = "created-mid-rollout";
const DEFERRED_STREAM_MARKER: &str =
    "is not served on this node until metadata catch-up commits a wrap its keys open";
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
// Several rewrapper ticks, long enough for a primary to strip a wrap if it would.
const HOLD_WINDOW: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const OLD_ONLY: KeyConfig = KeyConfig {
    key: OLD_KEY,
    previous_keys: &[],
    retired_kek_fingerprints: &[],
};
const PHASE_ONE: KeyConfig = KeyConfig {
    key: NEW_KEY,
    previous_keys: &[OLD_KEY],
    retired_kek_fingerprints: &[],
};
const PHASE_TWO: KeyConfig = KeyConfig {
    key: NEW_KEY,
    previous_keys: &[],
    retired_kek_fingerprints: &[OLD_KEK_FINGERPRINT],
};
const NEW_ONLY: KeyConfig = KeyConfig {
    key: NEW_KEY,
    previous_keys: &[],
    retired_kek_fingerprints: &[],
};

/// One node's `[encryption]` keys, applied through the environment.
struct KeyConfig {
    key: &'static str,
    previous_keys: &'static [&'static str],
    retired_kek_fingerprints: &'static [&'static str],
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_phase_one_rollout_when_rotating_should_add_new_wraps_and_keep_serving(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    let segments_before = segment_files(harness, 0);

    roll_out(harness, &PHASE_ONE).await;
    wait_for_kek_fingerprints(harness, &STREAMS.map(|(name, _)| name), &[OLD_KEY, NEW_KEY]).await;

    assert_eq!(
        segment_files(harness, 0),
        segments_before,
        "a rewrap must not touch message data"
    );
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_phase_two_rollout_when_rotating_should_drop_old_wraps(harness: &mut TestHarness) {
    let streams = STREAMS.map(|(name, _)| name);
    seed(harness).await;
    roll_out(harness, &PHASE_ONE).await;
    wait_for_kek_fingerprints(harness, &streams, &[OLD_KEY, NEW_KEY]).await;

    roll_out(harness, &PHASE_TWO).await;
    wait_for_kek_fingerprints(harness, &streams, &[NEW_KEY]).await;

    roll_out(harness, &NEW_ONLY).await;
    let client = serving_client(harness).await;
    for name in streams {
        let polled = poll_all(&client, name)
            .await
            .expect("the new key reads old messages");
        assert_eq!(polled[0].payload.as_ref(), payload(name).as_bytes());
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_stream_created_on_rotated_primary_when_old_nodes_serve_and_restart_should_open_it(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    let rotated = (leader(harness).await + 1) % harness.cluster_size();
    configure(harness, rotated, &PHASE_ONE);
    harness
        .restart_node(rotated)
        .expect("restart a backup on phase 1");
    make_leader(harness, rotated).await;

    // The primary wraps a binary create, and only one of three nodes holds the
    // new key, so the send commits only once an old node opens the stream.
    create_with_message(&serving_client(harness).await, MID_ROLLOUT_STREAM, None).await;
    wait_for_kek_fingerprints(harness, &[MID_ROLLOUT_STREAM], &[OLD_KEY, NEW_KEY]).await;

    let old = (rotated + 1) % harness.cluster_size();
    harness.restart_node(old).expect("restart an old node");
    let polled = poll_all(&node_client(harness, old).await, MID_ROLLOUT_STREAM)
        .await
        .expect("serves the stream created mid-rollout");
    assert_eq!(
        polled[0].payload.as_ref(),
        payload(MID_ROLLOUT_STREAM).as_bytes()
    );
    assert!(
        !harness.node(old).stdout_contains(DEFERRED_STREAM_MARKER),
        "a node on the old key must open every stream at boot"
    );
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_old_config_primary_when_sets_hold_new_wraps_should_keep_them(
    harness: &mut TestHarness,
) {
    let streams = STREAMS.map(|(name, _)| name);
    seed(harness).await;
    roll_out(harness, &PHASE_ONE).await;
    wait_for_kek_fingerprints(harness, &streams, &[OLD_KEY, NEW_KEY]).await;

    roll_out(harness, &OLD_ONLY).await;

    hold_kek_fingerprints(harness, &streams, &[OLD_KEY, NEW_KEY]).await;
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_new_key_only_before_phase_one_when_booting_in_a_cluster_should_serve_no_old_stream(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    harness.stop_node(2).expect("stop node 2");
    configure(harness, 2, &NEW_ONLY);
    harness
        .restart_node(2)
        .expect("a cluster node defers the streams it cannot open");

    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    while harness.node(2).stdout_occurrences(DEFERRED_STREAM_MARKER) < STREAMS.len() {
        assert!(
            Instant::now() < deadline,
            "node 2 must report every stream it cannot open"
        );
        sleep(POLL_INTERVAL).await;
    }
    let diagnostics = harness.node(2).stdout_plain();
    for key in [OLD_KEY, NEW_KEY] {
        let fingerprint = kek_fingerprint(key);
        assert!(
            diagnostics.contains(&fingerprint),
            "the diagnostics must name {fingerprint}"
        );
    }
    for (name, _) in STREAMS {
        let polled = poll_all(&serving_client(harness).await, name)
            .await
            .expect("the nodes on the old key keep serving");
        assert_eq!(polled[0].payload.as_ref(), payload(name).as_bytes());
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_rewrap_when_persisting_should_store_identical_metadata_on_every_replica(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    roll_out(harness, &PHASE_ONE).await;
    wait_for_kek_fingerprints(harness, &STREAMS.map(|(name, _)| name), &[OLD_KEY, NEW_KEY]).await;

    let data_paths: Vec<_> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    disk::wait_for_log_convergence(&data_paths).await;
    harness.stop().await.unwrap();
    disk::assert_replica_data_identical(&data_paths, true);
}

async fn seed(harness: &TestHarness) {
    let client = harness.root_client_for_node(0).await.unwrap();
    assert_eq!(kek_fingerprint(OLD_KEY), OLD_KEK_FINGERPRINT);
    for (name, seed) in STREAMS {
        create_with_message(&client, name, seed).await;
    }
    let data_paths: Vec<_> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    disk::wait_for_log_convergence(&data_paths).await;
}

async fn create_with_message(client: &IggyClient, name: &str, seed: Option<u8>) {
    client
        .create_stream(
            name,
            &StreamCreateOptions {
                encryption_key: seed
                    .map(|seed| StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap()),
                ..StreamCreateOptions::default()
            },
        )
        .await
        .unwrap();
    client
        .create_topic(
            &Identifier::named(name).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                messages_required_to_save: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut messages = vec![
        IggyMessage::builder()
            .payload(payload(name).into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &Identifier::named(name).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
}

fn configure(harness: &mut TestHarness, node: usize, config: &KeyConfig) {
    let list = |values: &[&str]| {
        let quoted: Vec<String> = values.iter().map(|value| format!("\"{value}\"")).collect();
        format!("[{}]", quoted.join(","))
    };
    let server = harness.node_mut(node);
    server.add_env("IGGY_ENCRYPTION_KEY", config.key);
    server.add_env("IGGY_ENCRYPTION_PREVIOUS_KEYS", list(config.previous_keys));
    server.add_env(
        "IGGY_ENCRYPTION_RETIRED_KEK_FINGERPRINTS",
        list(config.retired_kek_fingerprints),
    );
}

/// Restart every node in turn under `config`, checking after each restart
/// that the encrypted streams still serve.
async fn roll_out(harness: &mut TestHarness, config: &KeyConfig) {
    for node in 0..harness.cluster_size() {
        configure(harness, node, config);
        harness.restart_node(node).expect("rolling restart");
        let client = serving_client(harness).await;
        for (name, _) in STREAMS {
            let polled = poll_all(&client, name)
                .await
                .expect("serves during the rollout");
            assert_eq!(polled[0].payload.as_ref(), payload(name).as_bytes());
        }
    }
}

async fn wait_for_kek_fingerprints(harness: &TestHarness, streams: &[&str], keys: &[&str]) {
    let expected = expected_fingerprints(keys);
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    while reported_fingerprints(harness, streams).await
        != vec![Some(expected.clone()); streams.len()]
    {
        assert!(
            Instant::now() < deadline,
            "streams never reported KEK fingerprints {expected:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn hold_kek_fingerprints(harness: &TestHarness, streams: &[&str], keys: &[&str]) {
    let expected = expected_fingerprints(keys);
    let deadline = Instant::now() + HOLD_WINDOW;
    let mut reads = 0;
    while Instant::now() < deadline {
        for fingerprints in reported_fingerprints(harness, streams)
            .await
            .into_iter()
            .flatten()
        {
            assert_eq!(
                fingerprints, expected,
                "a primary dropped a wrap no retired fingerprint names"
            );
            reads += 1;
        }
        sleep(POLL_INTERVAL).await;
    }
    assert!(reads > 0, "no stream was readable during the hold window");
}

async fn reported_fingerprints(
    harness: &TestHarness,
    streams: &[&str],
) -> Vec<Option<Vec<String>>> {
    let client = serving_client(harness).await;
    let mut reported = Vec::with_capacity(streams.len());
    for name in streams {
        reported.push(
            client
                .get_stream(&Identifier::named(name).unwrap())
                .await
                .ok()
                .flatten()
                .map(|details| details.encryption_kek_fingerprints()),
        );
    }
    reported
}

fn expected_fingerprints(keys: &[&str]) -> Vec<String> {
    let mut expected: Vec<String> = keys.iter().map(|key| kek_fingerprint(key)).collect();
    expected.sort();
    expected
}

async fn serving_client(harness: &TestHarness) -> IggyClient {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        for node in 0..harness.cluster_size() {
            if let Some(client) = try_node_client(harness, node).await {
                return client;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no node served the encrypted streams"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The index of the metadata leader, once a serving node names one.
async fn leader(harness: &TestHarness) -> usize {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let roster = serving_client(harness).await.get_cluster_metadata().await;
        let leader_port = roster.ok().and_then(|roster| {
            roster
                .nodes
                .iter()
                .find(|node| node.role == ClusterNodeRole::Leader)
                .map(|node| node.endpoints.tcp)
        });
        if let Some(index) = (0..harness.cluster_size()).find(|index| {
            leader_port.is_some_and(|port| {
                harness
                    .node(*index)
                    .tcp_addr()
                    .is_some_and(|address| address.port() == port)
            })
        }) {
            return index;
        }
        assert!(Instant::now() < deadline, "no metadata leader was elected");
        sleep(POLL_INTERVAL).await;
    }
}

/// Restart whichever node leads until `target` does. A view change hands
/// leadership to the next replica in order, so this takes at most one
/// restart per other node.
async fn make_leader(harness: &mut TestHarness, target: usize) {
    for _ in 0..harness.cluster_size() {
        let current = leader(harness).await;
        if current == target {
            return;
        }
        harness
            .restart_node(current)
            .expect("restart the leader to move leadership");
    }
    assert_eq!(leader(harness).await, target, "node {target} never led");
}

async fn node_client(harness: &TestHarness, node: usize) -> IggyClient {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(client) = try_node_client(harness, node).await {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} never served the encrypted streams"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn try_node_client(harness: &TestHarness, node: usize) -> Option<IggyClient> {
    let client = harness
        .node(node)
        .tcp_client()
        .ok()?
        .with_root_login()
        .connect()
        .await
        .ok()?;
    poll_all(&client, STREAMS[0].0).await.ok()?;
    Some(client)
}

async fn poll_all(client: &IggyClient, stream: &str) -> Result<Vec<IggyMessage>, IggyError> {
    let polled = client
        .poll_messages(
            &Identifier::named(stream).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await?;
    if polled.messages.is_empty() {
        return Err(IggyError::ResourceNotFound(stream.to_owned()));
    }
    Ok(polled.messages)
}

fn segment_files(harness: &TestHarness, node: usize) -> BTreeMap<String, Vec<u8>> {
    disk::collect_comparable_files(&harness.node(node).data_path(), false)
}

fn kek_fingerprint(key: &str) -> String {
    ServerKeyContext::from_config(key, &[], true)
        .unwrap()
        .kek_fingerprint()
        .unwrap()
}

fn payload(stream: &str) -> String {
    format!("before-rotation-{stream}")
}
