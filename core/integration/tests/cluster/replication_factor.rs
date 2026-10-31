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

use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::time::Duration;

use super::partition_primary_routing::partition_route;
use super::register_forwarding::connect_without_login;
use crate::server::http_client::{HttpClient, node_url};
use iggy::prelude::*;
use iggy_common::{HeaderKey, HeaderValue, topic_option_keys};
use integration::harness::{TestHarness, TestServerConfig, resolve_config_paths};
use integration::iggy_harness;
use serial_test::parallel;
use tokio::time::{sleep, timeout};

const STREAM_NAME: &str = "replication-factor-stream";
const TOPIC_NAME: &str = "replication-factor-topic";
const WITNESS_TOPIC_NAME: &str = "replication-factor-witness";
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
                        partition_id,
                        &Consumer::default(),
                        &PollingStrategy::offset(0),
                        PollOptions {
                            count: 1,
                            ..PollOptions::default()
                        },
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
    send_within(client, stream, topic, partition_id, payload, POLL_BUDGET).await;
}

async fn send_within(
    client: &TcpClient,
    stream: &Identifier,
    topic: &Identifier,
    partition_id: u32,
    payload: &str,
    budget: Duration,
) {
    timeout(budget, async {
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

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_a_factor_of_one_when_one_reader_fetches_every_partition_should_return_each_message(
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
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let mut expected = BTreeSet::new();
    for partition_id in 0..PARTITIONS {
        partition_route(&writer, &stream, &topic, partition_id).await;
        let payload = format!("partition-{partition_id}");
        send_within_budget(&writer, &stream, &topic, partition_id, &payload).await;
        expected.insert(payload);
    }

    // One fetch names partitions that live on three different nodes.
    let client = IggyClient::new(ClientWrapper::Tcp(root_client(harness, 0).await));
    let mut reader = client
        .reader(
            &stream,
            &topic,
            &Consumer::new(Identifier::named("every-partition").unwrap()),
        )
        .partitions(0..PARTITIONS)
        .start(PollingStrategy::first())
        .build()
        .await
        .unwrap();
    let mut read = BTreeSet::new();
    timeout(POLL_BUDGET, async {
        while read.len() < expected.len() {
            let batch = reader.next().await.unwrap();
            for message in batch.messages {
                read.insert(String::from_utf8(message.payload.to_vec()).unwrap());
            }
        }
    })
    .await
    .expect("the reader reaches every partition");
    assert_eq!(read, expected);
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
                    stranded,
                    &Consumer::default(),
                    &PollingStrategy::offset(0),
                    PollOptions {
                        count: 2,
                        ..PollOptions::default()
                    },
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

/// The partitions of the topic, once every one of them lists all its members
/// in sync, read through `node`.
async fn await_in_sync(
    client: &TcpClient,
    stream: &Identifier,
    topic: &Identifier,
    expected: impl Fn(&Partition) -> Vec<u8>,
) -> Vec<Partition> {
    timeout(POLL_BUDGET, async {
        loop {
            let partitions = client
                .get_topic(stream, topic)
                .await
                .unwrap()
                .unwrap()
                .partitions;
            if partitions
                .iter()
                .all(|partition| partition.in_sync_members == expected(partition))
            {
                return partitions;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("in-sync members never reached the expected lists"))
}

#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_no_factor_when_creating_on_the_default_setting_should_store_every_partition_on_every_node(
    harness: &mut TestHarness,
) {
    let client = root_client(harness, 0).await;
    let specs = client.describe_options(OptionsScope::Topic).await.unwrap();
    let replication_factor = specs
        .iter()
        .find(|spec| spec.key == "replication_factor")
        .unwrap();
    assert_eq!(replication_factor.default_value, 3_u32.to_le_bytes());

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
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let partitions = await_in_sync(&client, &stream, &topic, |partition| {
        partition.members.clone()
    })
    .await;
    for partition in &partitions {
        let mut members = partition.members.clone();
        members.sort_unstable();
        assert_eq!(members, [0, 1, 2], "partition {}", partition.id);
    }
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        cluster.default_replication_factor = "2",
        sharding.cpu_allocation = "2"
    )
)]
async fn given_a_configured_default_factor_when_a_member_goes_down_should_report_it_out_of_sync(
    harness: &mut TestHarness,
) {
    const DOWN: u8 = 2;
    let client = root_client(harness, 0).await;
    let specs = client.describe_options(OptionsScope::Topic).await.unwrap();
    let replication_factor = specs
        .iter()
        .find(|spec| spec.key == "replication_factor")
        .unwrap();
    assert_eq!(replication_factor.default_value, 2_u32.to_le_bytes());

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
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let details = client.get_topic(&stream, &topic).await.unwrap().unwrap();
    let factor = details
        .options
        .get(&HeaderKey::from_str(topic_option_keys::REPLICATION_FACTOR).unwrap())
        .expect("the topic stores the factor it was given");
    assert_eq!(factor.value, HeaderValue::from(2_u32));

    let partitions = await_in_sync(&client, &stream, &topic, |partition| {
        partition.members.clone()
    })
    .await;
    let distinct: BTreeSet<u8> = partitions
        .iter()
        .flat_map(|partition| partition.members.iter().copied())
        .collect();
    assert!(distinct.len() > 1, "the members spread over the nodes");
    for partition in &partitions {
        assert_eq!(partition.members.len(), 2, "partition {}", partition.id);
    }
    assert!(
        partitions
            .iter()
            .any(|partition| partition.members.contains(&DOWN)),
        "three partitions of two members each use every node of three"
    );

    harness.kill_node(usize::from(DOWN)).unwrap();
    await_in_sync(&client, &stream, &topic, |partition| {
        partition
            .members
            .iter()
            .copied()
            .filter(|member| *member != DOWN)
            .collect()
    })
    .await;
}

/// Past the clock skew a node allows between a partition's creation and the
/// start of its own storage, so a node that comes back empty knows it lost
/// the partition rather than never having had it.
const STORAGE_EPOCH_SKEW: Duration = Duration::from_secs(11);
/// Rejoining on empty storage takes a metadata catch-up, an election and a
/// partition state transfer before a write commits again.
const REJOIN_BUDGET: Duration = Duration::from_secs(120);
/// Consecutive polls, 5 seconds, a check must pass: past the 3 second probe
/// timeout, after which a new metadata primary narrows every in-sync list to
/// the nodes it hears.
const STEADY_POLLS: u32 = 25;

/// Waits until partition 0 of `topic` lists in-sync members that pass `check`
/// on [`STEADY_POLLS`] polls in a row.
async fn await_steady_in_sync(
    client: &TcpClient,
    topic: &Identifier,
    check: impl Fn(&[u8]) -> bool,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    timeout(REJOIN_BUDGET, async {
        let mut passed = 0;
        while passed < STEADY_POLLS {
            let in_sync = client
                .get_topic(&stream, topic)
                .await
                .ok()
                .flatten()
                .map(|details| details.partitions[0].in_sync_members.clone());
            passed = if in_sync.is_some_and(|in_sync| check(&in_sync)) {
                passed + 1
            } else {
                0
            };
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the in-sync list of {topic} never settled"));
}

async fn await_steady_replication_gauges(sessions: &[HttpClient], expected: (i64, i64)) {
    timeout(REJOIN_BUDGET, async {
        let mut passed = 0;
        while passed < STEADY_POLLS {
            let observed = (
                summed_gauge(sessions, "partitions_under_replicated").await,
                summed_gauge(sessions, "partitions_below_write_quorum").await,
            );
            passed = if observed == expected { passed + 1 } else { 0 };
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("replication gauges never settled at {expected:?}"));
}

async fn create_factor_two_topic(client: &TcpClient, durability: Durability) {
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                replication_factor: Some(2),
                durability,
                consumer_offset_durability: durability,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
}

/// Replacing one member's disk at a time with `--fresh` is the documented
/// procedure. On a factor-2 topic whose writes wait for the disk, the member
/// that kept its data must still elect and copy the history back.
#[iggy_harness(cluster_nodes = 3, server(sharding.cpu_allocation = "2"))]
async fn given_a_factor_of_two_when_its_primary_returns_on_empty_storage_should_serve_again(
    harness: &mut TestHarness,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let admin = root_client(harness, 0).await;
    create_factor_two_topic(&admin, Durability::Persisted).await;
    let primary = usize::from(
        partition_route(&admin, &stream, &topic, 0)
            .await
            .primary_replica,
    );
    send_within_budget(&admin, &stream, &topic, 0, "before-wipe").await;
    drop(admin);
    sleep(STORAGE_EPOCH_SKEW).await;

    harness.restart_node_from_clean_slate(primary).unwrap();
    let survivor = (0..3).find(|node| *node != primary).unwrap();
    let client = login_within_budget(harness, survivor).await;
    let polled = timeout(REJOIN_BUDGET, async {
        let mut sent = false;
        loop {
            if !sent {
                let mut messages = vec![IggyMessage::from_str("after-wipe").unwrap()];
                sent = client
                    .send_messages(
                        &stream,
                        &topic,
                        &Partitioning::partition_id(0),
                        &mut messages,
                    )
                    .await
                    .is_ok();
            }
            if sent
                && let Ok(polled) = client
                    .poll_messages(
                        &stream,
                        &topic,
                        0,
                        &Consumer::default(),
                        &PollingStrategy::offset(0),
                        PollOptions {
                            count: 2,
                            ..PollOptions::default()
                        },
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
    .expect("the member that kept its data elects and the partition serves again");
    let payloads: Vec<_> = polled
        .messages
        .iter()
        .map(|message| message.payload.as_ref().to_vec())
        .collect();
    assert_eq!(payloads, [b"before-wipe".to_vec(), b"after-wipe".to_vec()]);
}

/// Cut off long enough that a primary hearing no member steps down: past the
/// 5 second `heartbeat_timeout`.
const STEP_DOWN_ISOLATION: Duration = Duration::from_secs(8);
/// Views the pair goes through before the rebuilt member must have led one.
const STEP_DOWN_ROUNDS: usize = 6;

/// A member rebuilt by a state transfer after it came back on empty storage
/// must elect like any other. Restarting the other member moves the pair to
/// new views until the rebuilt member leads one. It is then cut off from its
/// peers until it steps down, and the pair must elect again after the heal.
#[tokio::test(flavor = "multi_thread")]
#[parallel]
async fn given_a_member_rebuilt_by_state_transfer_when_each_leader_steps_down_should_elect_again() {
    let mut harness = relayed_cluster();
    harness.start().await.expect("start the cluster");
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let admin = root_client(&harness, 0).await;
    create_factor_two_topic(&admin, Durability::Persisted).await;
    let route = partition_route(&admin, &stream, &topic, 0).await;
    let rebuilt = route.primary_replica;
    let members = admin
        .get_topic(&stream, &topic)
        .await
        .unwrap()
        .unwrap()
        .partitions[0]
        .members
        .clone();
    let outsider = (0..3)
        .find(|node| !members.contains(node))
        .expect("a factor of two leaves one of three nodes out");
    send_within_budget(&admin, &stream, &topic, 0, "before-wipe").await;
    drop(admin);
    sleep(STORAGE_EPOCH_SKEW).await;

    harness
        .restart_node_from_clean_slate(usize::from(rebuilt))
        .unwrap();
    let client = login_within_budget(&harness, usize::from(outsider)).await;
    // Both members ack every write, so this commits only once the transfer
    // gave the rebuilt member the history.
    send_within(&client, &stream, &topic, 0, "after-wipe", REJOIN_BUDGET).await;

    let mut view = route.view;
    let mut rebuilt_led = false;
    for round in 0..STEP_DOWN_ROUNDS {
        let route = timeout(REJOIN_BUDGET, async {
            loop {
                let route = partition_route(&client, &stream, &topic, 0).await;
                if route.view > view {
                    break route;
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("the pair's new view reaches the outsider");
        view = route.view;
        let primary = usize::from(route.primary_replica);
        if route.primary_replica == rebuilt {
            harness.isolate_node(primary).unwrap();
            sleep(STEP_DOWN_ISOLATION).await;
            harness.heal_node(primary).unwrap();
            send_within(&client, &stream, &topic, 0, "stepped-down", REJOIN_BUDGET).await;
            rebuilt_led = true;
            break;
        }
        harness.restart_node(primary).unwrap();
        send_within(
            &client,
            &stream,
            &topic,
            0,
            &format!("round-{round}"),
            REJOIN_BUDGET,
        )
        .await;
    }
    assert!(rebuilt_led, "node {rebuilt} never led the pair");
}

fn relayed_cluster() -> TestHarness {
    let overrides = HashMap::from([("sharding.cpu_allocation".to_owned(), "2".to_owned())]);
    TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(resolve_config_paths(&overrides).expect("valid server config paths"))
                .build(),
        )
        .cluster_nodes(3)
        .peer_relays()
        .build()
        .expect("build the harness")
}

/// Both members of a partition come back on empty storage. Neither serves
/// it, since that would restart it at offset 0, and deleting the topic, the
/// way to accept the loss, must complete and free its name.
#[iggy_harness(cluster_nodes = 5, server(sharding.cpu_allocation = "2"))]
async fn given_a_factor_of_two_when_both_members_return_on_empty_storage_should_let_its_topic_be_deleted_and_recreated(
    harness: &mut TestHarness,
) {
    const NODES: usize = 5;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let creator = root_client(harness, 0).await;
    create_factor_two_topic(&creator, Durability::Replicated).await;
    let members = creator
        .get_topic(&stream, &topic)
        .await
        .unwrap()
        .unwrap()
        .partitions[0]
        .members
        .clone();
    assert_eq!(members.len(), 2);
    send_within_budget(&creator, &stream, &topic, 0, "lost").await;
    let witness = Identifier::named(WITNESS_TOPIC_NAME).unwrap();
    creator
        .create_topic(
            &stream,
            WITNESS_TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                replication_factor: Some(u32::try_from(NODES).unwrap()),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    drop(creator);
    let outsider = (0..NODES)
        .find(|node| !members.contains(&u8::try_from(*node).unwrap()))
        .unwrap();
    let admin = root_client(harness, outsider).await;
    sleep(STORAGE_EPOCH_SKEW).await;

    for &member in &members {
        harness.stop_node(usize::from(member)).unwrap();
    }
    for &member in &members {
        harness
            .restart_node_from_clean_slate(usize::from(member))
            .unwrap();
    }
    for &member in &members {
        drop(login_within_budget(harness, usize::from(member)).await);
    }
    // The metadata primary lists a node in sync only while it hears the node,
    // so the partition every node stores shows when it hears every node again.
    await_steady_in_sync(&admin, &witness, |in_sync| in_sync.len() == NODES).await;
    await_steady_in_sync(&admin, &topic, <[u8]>::is_empty).await;
    let mut sessions = Vec::with_capacity(NODES);
    for node in 0..NODES {
        sessions.push(HttpClient::login_root_no_redirect(node_url(harness, node)).await);
    }
    // Neither member leads the lost pair, whether or not the metadata primary
    // still holds the last report from before the wipe.
    await_steady_replication_gauges(&sessions, (1, 1)).await;

    timeout(REJOIN_BUDGET, async {
        loop {
            let _ = admin.delete_topic(&stream, &topic).await;
            if matches!(admin.get_topic(&stream, &topic).await, Ok(None)) {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the delete completes once both members retired their lost copies");
    create_factor_two_topic_in_stream(&admin).await;
    send_within_budget(&admin, &stream, &topic, 0, "recreated").await;
}

async fn create_factor_two_topic_in_stream(client: &TcpClient) {
    timeout(POLL_BUDGET, async {
        loop {
            let created = client
                .create_topic(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    TOPIC_NAME,
                    &TopicCreateOptions {
                        partitions_count: Some(1),
                        replication_factor: Some(2),
                        ..TopicCreateOptions::default()
                    },
                )
                .await;
            if created.is_ok() {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("the freed name takes a new topic");
}
