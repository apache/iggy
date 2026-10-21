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
const TOPIC_NAME: &str = "rotated-topic";
const STREAMS: [(&str, Option<u8>); 2] =
    [("rotated-own-key", Some(4)), ("rotated-generated", None)];
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_phase_one_rollout_when_rotating_then_should_add_new_wraps_and_keep_serving(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    let segments_before = segment_files(harness, 0);

    roll_out(harness, &format!("[\"{OLD_KEY}\"]"), true).await;
    wait_for_kek_fingerprints(harness, &[OLD_KEY, NEW_KEY]).await;

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
async fn given_phase_two_rollout_when_rotating_then_should_drop_old_wraps(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    roll_out(harness, &format!("[\"{OLD_KEY}\"]"), true).await;
    wait_for_kek_fingerprints(harness, &[OLD_KEY, NEW_KEY]).await;

    roll_out(harness, "[]", true).await;
    wait_for_kek_fingerprints(harness, &[NEW_KEY]).await;

    for node in 0..harness.cluster_size() {
        harness
            .restart_node(node)
            .expect("restart with the new key alone");
    }
    let client = serving_client(harness).await;
    for (name, _) in STREAMS {
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
async fn given_new_key_only_before_phase_one_when_booting_then_should_refuse(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    harness.stop_node(2).expect("stop node 2");
    harness.node_mut(2).add_env("IGGY_ENCRYPTION_KEY", NEW_KEY);
    let error = harness
        .restart_node(2)
        .expect_err("a node holding only the new key must refuse boot");
    if std::env::var("IGGY_TEST_VERBOSE").is_err() {
        let diagnostics = error.to_string();
        for key in [OLD_KEY, NEW_KEY] {
            let fingerprint = kek_fingerprint(key);
            assert!(
                diagnostics.contains(&fingerprint),
                "the refusal must name {fingerprint}, got: {diagnostics}"
            );
        }
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_rewrap_when_persisting_then_should_store_identical_metadata_on_every_replica(
    harness: &mut TestHarness,
) {
    seed(harness).await;
    roll_out(harness, &format!("[\"{OLD_KEY}\"]"), true).await;
    wait_for_kek_fingerprints(harness, &[OLD_KEY, NEW_KEY]).await;

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
    for (name, seed) in STREAMS {
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
    let data_paths: Vec<_> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    disk::wait_for_log_convergence(&data_paths).await;
}

/// Restart every node in turn under `key = NEW_KEY` and the given previous
/// keys, checking after each restart that the encrypted streams still serve.
async fn roll_out(harness: &mut TestHarness, previous_keys: &str, verify_each: bool) {
    for node in 0..harness.cluster_size() {
        harness
            .node_mut(node)
            .add_env("IGGY_ENCRYPTION_KEY", NEW_KEY);
        harness
            .node_mut(node)
            .add_env("IGGY_ENCRYPTION_PREVIOUS_KEYS", previous_keys);
        harness.restart_node(node).expect("rolling restart");
        if verify_each {
            let client = serving_client(harness).await;
            for (name, _) in STREAMS {
                let polled = poll_all(&client, name)
                    .await
                    .expect("serves during the rollout");
                assert_eq!(polled[0].payload.as_ref(), payload(name).as_bytes());
            }
        }
    }
}

async fn wait_for_kek_fingerprints(harness: &TestHarness, keys: &[&str]) {
    let mut expected: Vec<String> = keys.iter().map(|key| kek_fingerprint(key)).collect();
    expected.sort();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let client = serving_client(harness).await;
        let mut converged = true;
        for (name, _) in STREAMS {
            let fingerprints = client
                .get_stream(&Identifier::named(name).unwrap())
                .await
                .ok()
                .flatten()
                .map(|details| details.encryption_kek_fingerprints());
            if fingerprints.as_ref() != Some(&expected) {
                converged = false;
            }
        }
        if converged {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "streams never reported KEK fingerprints {expected:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn serving_client(harness: &TestHarness) -> IggyClient {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        for node in 0..harness.cluster_size() {
            if let Ok(builder) = harness.node(node).tcp_client()
                && let Ok(client) = builder.with_root_login().connect().await
                && poll_all(&client, STREAMS[0].0).await.is_ok()
            {
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
