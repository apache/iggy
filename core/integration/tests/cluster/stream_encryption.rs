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

use std::time::{Duration, Instant};

use iggy::prelude::*;
use iggy_common::ServerKeyContext;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::sleep;

const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const OTHER_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";
const TOPIC_NAME: &str = "encrypted-cluster-topic";
const STREAMS: [(&str, Option<u8>); 2] =
    [("cluster-own-key", Some(9)), ("cluster-generated", None)];
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const UNBUILT_MARKER: &str = "cannot be built, its key does not open on this node";

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_encrypted_streams_when_primary_fails_then_should_read_after_failover(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.unwrap();
    seed_streams(&client, "before-failover").await;
    drop(client);

    let leader = disk::leader_node_index(harness).await;
    harness.stop_node(leader).expect("stop the primary");
    let survivors: Vec<usize> = (0..harness.cluster_size())
        .filter(|node| *node != leader)
        .collect();

    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(client) = connect_any(harness, &survivors).await
            && all_streams_poll(&client, "before-failover").await
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the surviving nodes never served the encrypted streams after the primary stopped"
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_empty_replacement_node_when_state_transferring_then_should_unwrap_stream_keys_before_serving(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.unwrap();
    seed_streams(&client, "before-rejoin").await;
    let _seed_client = client;

    harness
        .restart_node_from_clean_slate(2)
        .expect("clean-slate restart of node 2");
    // With node 1 down, quorum needs the rejoined node, so a send commits only
    // once node 2 has opened each stream's key and built its partitions.
    harness.stop_node(1).expect("stop node 1");
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Some(client) = connect_any(harness, &[2, 0]).await
            && send_to_all_streams(&client, "after-rejoin").await
            && all_streams_poll(&client, "after-rejoin").await
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rejoined node never served the encrypted streams"
        );
        sleep(POLL_INTERVAL).await;
    }
    assert!(!harness.node(2).stdout_contains(UNBUILT_MARKER));

    // A replacement holding another key never serves those streams.
    harness.restart_node(1).expect("bring node 1 back");
    harness
        .node_mut(2)
        .add_env("IGGY_ENCRYPTION_KEY", OTHER_CONFIG_KEY);
    harness
        .restart_node_from_clean_slate(2)
        .expect("clean-slate restart of node 2 under another key");
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    while !harness.node(2).stdout_contains(UNBUILT_MARKER) {
        assert!(
            Instant::now() < deadline,
            "a node under another key must refuse to build the encrypted partitions"
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(encryption = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=")
)]
async fn given_cluster_when_reading_encrypted_streams_then_should_report_same_projection_on_every_node(
    harness: &mut TestHarness,
) {
    let client = harness.root_client_for_node(0).await.unwrap();
    let mut created = Vec::new();
    for (name, seed) in STREAMS {
        let details = client
            .create_stream(
                name,
                &StreamCreateOptions {
                    encryption_key: seed.map(stream_key),
                    ..StreamCreateOptions::default()
                },
            )
            .await
            .unwrap();
        created.push(projection(&details.options));
    }
    let kek = ServerKeyContext::from_config(CONFIG_KEY, &[], true)
        .unwrap()
        .kek_fingerprint()
        .unwrap();
    for node in 0..harness.cluster_size() {
        let client = harness.root_client_for_node(node).await.unwrap();
        for ((name, _), created) in STREAMS.iter().zip(&created) {
            let deadline = Instant::now() + CONVERGE_TIMEOUT;
            let details = loop {
                if let Ok(Some(details)) =
                    client.get_stream(&Identifier::named(name).unwrap()).await
                {
                    break details;
                }
                assert!(Instant::now() < deadline, "node {node} never listed {name}");
                sleep(POLL_INTERVAL).await;
            };
            assert_eq!(
                &projection(&details.options),
                created,
                "node {node}, {name}"
            );
            assert_eq!(details.encryption_kek_fingerprints(), vec![kek.clone()]);
        }
    }
}

async fn seed_streams(client: &IggyClient, payload_tag: &str) {
    for (name, seed) in STREAMS {
        client
            .create_stream(
                name,
                &StreamCreateOptions {
                    encryption_key: seed.map(stream_key),
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
    }
    assert!(send_to_all_streams(client, payload_tag).await);
}

async fn send_to_all_streams(client: &IggyClient, payload_tag: &str) -> bool {
    for (name, _) in STREAMS {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload(name, payload_tag).into())
                .build()
                .unwrap(),
        ];
        if client
            .send_messages(
                &Identifier::named(name).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

/// Whether every stream's messages poll back and include one tagged `payload_tag`.
async fn all_streams_poll(client: &IggyClient, payload_tag: &str) -> bool {
    for (name, _) in STREAMS {
        let Ok(polled) = client
            .poll_messages(
                &Identifier::named(name).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                100,
                false,
            )
            .await
        else {
            return false;
        };
        let expected = payload(name, payload_tag);
        if !polled
            .messages
            .iter()
            .any(|message| message.payload.as_ref() == expected.as_bytes())
        {
            return false;
        }
    }
    true
}

async fn connect_any(harness: &TestHarness, nodes: &[usize]) -> Option<IggyClient> {
    for &node in nodes {
        if let Ok(builder) = harness.node(node).tcp_client()
            && let Ok(client) = builder.with_root_login().connect().await
        {
            return Some(client);
        }
    }
    None
}

fn projection(options: &ResourceOptions) -> Vec<(String, Option<String>)> {
    [
        OptionKey::EncryptionKeySource,
        OptionKey::EncryptionKeyFingerprint,
        OptionKey::EncryptionKekFingerprint,
    ]
    .into_iter()
    .map(|key| {
        (
            key.to_string(),
            options
                .get(&HeaderKey::from(key))
                .and_then(|option| option.value.as_str().ok().map(str::to_owned)),
        )
    })
    .collect()
}

fn stream_key(seed: u8) -> StreamEncryptionKey {
    StreamEncryptionKey::try_from([seed; 32].as_slice()).unwrap()
}

fn payload(stream: &str, tag: &str) -> String {
    format!("{tag}-{stream}")
}
