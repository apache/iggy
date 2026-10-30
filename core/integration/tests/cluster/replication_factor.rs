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

//! A topic's replication factor bounds the nodes that store each partition,
//! while a client connected to any node still reaches every partition.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::time::Duration;

use super::partition_primary_routing::partition_route;
use super::register_forwarding::connect_without_login;
use crate::server::http_client::{HttpClient, node_url};
use iggy::prelude::*;
use iggy_common::{HeaderKey, HeaderValue, topic_option_keys};
use integration::harness::TestHarness;
use integration::iggy_harness;
use tokio::time::{sleep, timeout};

const STREAM_NAME: &str = "replication-factor-stream";
const TOPIC_NAME: &str = "replication-factor-topic";
const PARTITIONS: u32 = 3;
const POLL_BUDGET: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const METRICS_BUDGET: Duration = Duration::from_secs(60);

async fn root_client(harness: &TestHarness, node: usize) -> TcpClient {
    let client = connect_without_login(harness.node(node).tcp_addr().unwrap()).await;
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_a_factor_of_one_when_writing_through_any_node_should_store_each_partition_once(
    harness: &mut TestHarness,
) {
    let writer = root_client(harness, 0).await;
    writer
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    writer
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                replication_factor: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();

    let stream_id = writer.get_stream(&stream).await.unwrap().unwrap().id;
    let details = writer.get_topic(&stream, &topic).await.unwrap().unwrap();
    let factor = details
        .options
        .get(&HeaderKey::from_str(topic_option_keys::REPLICATION_FACTOR).unwrap())
        .expect("the topic reports its replication factor");
    assert_eq!(factor.value, HeaderValue::from(1u32));
    assert!(factor.explicit);

    let mut primaries = BTreeSet::new();
    for partition_id in 0..PARTITIONS {
        let route = partition_route(&writer, &stream, &topic, partition_id).await;
        primaries.insert(route.primary_replica);
    }
    assert_eq!(
        primaries.len(),
        harness.cluster_size(),
        "one partition on each node"
    );

    let reader = root_client(harness, 2).await;
    for partition_id in 0..PARTITIONS {
        let payload = format!("stored-once-{partition_id}");
        let mut messages = vec![IggyMessage::from_str(&payload).unwrap()];
        writer
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
        let polled = timeout(POLL_BUDGET, async {
            loop {
                let polled = reader
                    .poll_messages(
                        &stream,
                        &topic,
                        Some(partition_id),
                        &Consumer::default(),
                        &PollingStrategy::offset(0),
                        1,
                        false,
                    )
                    .await
                    .unwrap();
                if !polled.messages.is_empty() {
                    break polled;
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("partition {partition_id} never served its message"));
        assert_eq!(polled.messages[0].payload.as_ref(), payload.as_bytes());

        let route = partition_route(&reader, &stream, &topic, partition_id).await;
        let directory = format!(
            "streams/{stream_id}/topics/{}/partitions/{partition_id}",
            details.id
        );
        let holders: Vec<usize> = (0..harness.cluster_size())
            .filter(|node| harness.node(*node).data_path().join(&directory).exists())
            .collect();
        assert_eq!(
            holders,
            [usize::from(route.primary_replica)],
            "partition {partition_id} is stored only on its primary"
        );
    }
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_factor_the_cluster_cannot_hold_when_creating_should_refuse(
    harness: &mut TestHarness,
) {
    let client = root_client(harness, 0).await;
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    for factor in [0, 4] {
        let error = client
            .create_topic(
                &stream,
                TOPIC_NAME,
                &TopicCreateOptions {
                    replication_factor: Some(factor),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            IggyError::InvalidReplicationFactor,
            "factor {factor} on three nodes"
        );
    }
}

/// `metric`'s value summed over every shard of every listed node.
async fn summed_gauge(sessions: &[HttpClient], metric: &str) -> i64 {
    let mut total = 0;
    for session in sessions {
        let text = session.get("/metrics").await.text().await.unwrap();
        total += text
            .lines()
            .filter(|line| line.starts_with(&format!("{metric}{{")))
            .filter_map(|line| line.rsplit(' ').next()?.parse::<i64>().ok())
            .sum::<i64>();
    }
    total
}

async fn await_replication_gauges(sessions: &[HttpClient], expected: (i64, i64)) {
    timeout(METRICS_BUDGET, async {
        loop {
            let observed = (
                summed_gauge(sessions, "partitions_under_replicated").await,
                summed_gauge(sessions, "partitions_below_write_quorum").await,
            );
            if observed == expected {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("replication gauges never reached {expected:?}"));
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_a_member_down_when_scraping_metrics_should_report_its_partitions_under_replicated(
    harness: &mut TestHarness,
) {
    const DOWN: usize = 2;
    let client = root_client(harness, 0).await;
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    client
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                replication_factor: Some(3),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    for partition_id in 0..PARTITIONS {
        partition_route(&client, &stream, &topic, partition_id).await;
    }
    let survivors = [
        HttpClient::login_root_no_redirect(node_url(harness, 0)).await,
        HttpClient::login_root_no_redirect(node_url(harness, 1)).await,
    ];
    await_replication_gauges(&survivors, (0, 0)).await;

    harness.kill_node(DOWN).unwrap();
    // Every partition keeps two of its three copies: short of its factor, still
    // a write quorum.
    await_replication_gauges(&survivors, (i64::from(PARTITIONS), 0)).await;

    harness.restart_node(DOWN).unwrap();
    await_replication_gauges(&survivors, (0, 0)).await;
}

/// Every login below must succeed within this; ended sessions free their
/// slots only as fast as finalization releases them.
const LOGIN_BUDGET: Duration = Duration::from_secs(30);

async fn login_within_budget(harness: &TestHarness, node: usize) -> TcpClient {
    timeout(LOGIN_BUDGET, async {
        loop {
            let client = connect_without_login(harness.node(node).tcp_addr().unwrap()).await;
            if client
                .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
                .await
                .is_ok()
            {
                break client;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("a login must not wait on a partition that lacks its member quorum")
}

async fn send_within_budget(
    client: &TcpClient,
    stream: &Identifier,
    topic: &Identifier,
    partition_id: u32,
    payload: &str,
) {
    timeout(POLL_BUDGET, async {
        loop {
            let mut messages = vec![IggyMessage::from_str(payload).unwrap()];
            if client
                .send_messages(
                    stream,
                    topic,
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .is_ok()
            {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("partition {partition_id} never accepted {payload}"));
}

#[iggy_harness(
    cluster_nodes = 3,
    server(metadata.clients_table_max = "8", sharding.cpu_allocation = "2")
)]
async fn given_an_rf1_node_down_when_sessions_churn_should_keep_admitting_logins(
    harness: &mut TestHarness,
) {
    const DOWN: usize = 2;
    const ROUNDS: usize = 24;
    let admin = root_client(harness, 0).await;
    admin
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    admin
        .create_topic(
            &stream,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                replication_factor: Some(1),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut stranded = None;
    for partition_id in 0..PARTITIONS {
        let route = partition_route(&admin, &stream, &topic, partition_id).await;
        if usize::from(route.primary_replica) == DOWN {
            stranded = Some(partition_id);
        }
    }
    let stranded = stranded.expect("placement gives every node one partition");

    // This session's receipt lives only on the node about to go down, so its
    // retirement there can only be owed.
    let writer = root_client(harness, 0).await;
    send_within_budget(&writer, &stream, &topic, stranded, "before-outage").await;
    harness.kill_node(DOWN).unwrap();
    writer.logout_user().await.unwrap();
    drop(writer);

    for _ in 0..ROUNDS {
        let client = login_within_budget(harness, 0).await;
        client.logout_user().await.unwrap();
    }

    harness.restart_node(DOWN).unwrap();
    let returned = [HttpClient::login_root_no_redirect(node_url(harness, DOWN)).await];
    timeout(METRICS_BUDGET, async {
        while summed_gauge(&returned, "retirement_debts_paid_total").await == 0 {
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the returning primary pays the retirement it owes");
    let returning = login_within_budget(harness, 0).await;
    send_within_budget(&returning, &stream, &topic, stranded, "after-outage").await;
    let polled = timeout(POLL_BUDGET, async {
        loop {
            if let Ok(polled) = returning
                .poll_messages(
                    &stream,
                    &topic,
                    Some(stranded),
                    &Consumer::default(),
                    &PollingStrategy::offset(0),
                    2,
                    false,
                )
                .await
                && polled.messages.len() == 2
            {
                break polled;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the returning partition serves both messages once its debt is paid");
    assert_eq!(polled.messages[1].payload.as_ref(), b"after-outage");
    timeout(METRICS_BUDGET, async {
        while summed_gauge(&returned, "partitions_owing_retirement").await != 0 {
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("no partition keeps refusing sessions once its debt is paid");
}
