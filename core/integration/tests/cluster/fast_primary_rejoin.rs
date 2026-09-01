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

//! Same-handle continuity when the primary leaves and comes back.
//!
//! Two sibling scenarios share one setup and differ only in the rejoin step.
//! A producing client is pinned to the primary, a follower restarts and
//! progress is required, then the primary goes away. In the baseline the
//! primary stays down and the client must settle on a survivor. In the
//! fast-rejoin case the primary is started again immediately, so the old
//! endpoint accepts TCP and answers metadata while the partition group's
//! primaryship may live elsewhere. The same client object must reach a
//! confirmed write and read it back in both cases: an endpoint that can hold
//! a session hostage without being able to admit the write is a routing
//! failure, not a durability failure.

use iggy::prelude::*;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::{IggyMessagesBatch, SendMessagesConfirmations};
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::time::sleep;

use crate::server::http_client::HttpClient;

const STREAM_NAME: &str = "rejoin-stream";
const TOPIC_NAME: &str = "rejoin-topic";
const PARTITION_ID: u32 = 0;

/// Acks the pinned producer must capture before any disruption, so the
/// session is warm and mid-stream rather than freshly connected.
const WARM_ACKS: usize = 20;

/// Budget for reaching the warm ack count against the healthy cluster.
const WARMUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Budget for one confirmed send after a disruption. Covers an election, a
/// reconnect sweep, and a leader recheck, while staying far below the point
/// where repeated full response timeouts would read as progress.
const RESUME_BUDGET: Duration = Duration::from_secs(30);

/// Budget for reading the resumed record back through the same handle.
const READBACK_BUDGET: Duration = Duration::from_secs(10);

const RETRY_PAUSE: Duration = Duration::from_millis(250);

fn build_message(payload: &str) -> IggyMessage {
    IggyMessage::builder()
        .payload(payload.to_owned().into())
        .build()
        .expect("build message")
}

/// Create the stream and an eagerly flushed single-partition topic, so every
/// confirmed send is durable and each scenario is purely a routing question.
async fn create_stream_and_topic(harness: &TestHarness) {
    let setup_client = harness.tcp_root_client().await.unwrap();
    setup_client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        message_expiry: Some(IggyExpiry::NeverExpire),
        messages_required_to_save: Some(1),
        enforce_fsync: Some(true),
        ..TopicCreateOptions::default()
    };
    setup_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &options,
        )
        .await
        .expect("create topic");
}

/// Connect one client pinned to the current primary's own endpoint, the way a
/// leader-aware SDK ends up connected to whichever node answers as leader.
async fn pinned_producer(
    harness: &TestHarness,
    leader: usize,
    transport: TransportProtocol,
    reestablish_after: Option<IggyDuration>,
) -> (IggyClient, String) {
    let node = harness.node(leader);
    let primary_endpoint = match transport {
        TransportProtocol::Tcp => node.tcp_addr(),
        TransportProtocol::Quic => node.quic_addr(),
        TransportProtocol::WebSocket => node.websocket_addr(),
        TransportProtocol::Http => panic!("HTTP does not expose a persistent Iggy client"),
    }
    .unwrap_or_else(|| panic!("leader exposes a {transport} endpoint"))
    .to_string();
    let builder = match transport {
        TransportProtocol::Tcp => node.tcp_client(),
        TransportProtocol::Quic => node.quic_client(),
        TransportProtocol::WebSocket => node.websocket_client(),
        TransportProtocol::Http => panic!("HTTP does not expose a persistent Iggy client"),
    }
    .unwrap_or_else(|error| panic!("leader exposes a {transport} client: {error}"));
    let builder = match reestablish_after {
        Some(duration) => builder.with_reestablish_after(duration),
        None => builder,
    };
    let producer = builder
        .with_reconnecting_root_login()
        .connect()
        .await
        .expect("connect the producer to the primary");
    assert_eq!(
        producer.get_connection_info().await.server_address,
        primary_endpoint,
        "the producer must be pinned to the node this test disrupts, or it proves nothing"
    );
    (producer, primary_endpoint)
}

/// Drive confirmed sends until `count` acks land, within `budget`.
async fn require_acked_sends(
    producer: &IggyClient,
    label: &str,
    count: usize,
    budget: Duration,
) -> Vec<(u64, String)> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);
    let deadline = tokio::time::Instant::now() + budget;
    let mut acked = 0usize;
    let mut confirmations = Vec::with_capacity(count);
    let mut attempt = 0usize;
    let mut last_error: Option<IggyError> = None;
    while acked < count {
        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "{label}: only {acked}/{count} confirmed sends within {budget:?} \
             ({attempt} attempts, last error: {last_error:?})"
        );
        let payload = format!("{label}-{attempt:05}");
        let mut messages = vec![build_message(&payload)];
        attempt += 1;
        let send = producer.send_messages(&stream, &topic, &partitioning, &mut messages);
        match tokio::time::timeout(deadline - now, send).await {
            Ok(Ok(response)) => {
                let confirmation = response
                    .confirmations
                    .first()
                    .unwrap_or_else(|| panic!("{label}: the VSR server confirms every send"));
                confirmations.push((confirmation.base_offset, payload));
                acked += 1;
            }
            Ok(Err(error)) => {
                last_error = Some(error);
                sleep(RETRY_PAUSE).await;
            }
            Err(_elapsed) => {
                panic!(
                    "{label}: an attempt outlived the whole {budget:?} budget \
                     ({acked}/{count} acked, {attempt} attempts, last error: {last_error:?})"
                );
            }
        }
    }
    confirmations
}

/// Poll the topic through the same client and require every confirmed record
/// at its assigned offset, including the write confirmed after failover.
async fn require_readback(producer: &IggyClient, expected: &[(u64, String)]) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let deadline = tokio::time::Instant::now() + READBACK_BUDGET;
    let last_offset = expected
        .iter()
        .map(|(offset, _)| *offset)
        .max()
        .expect("at least one confirmed record");
    let count = u32::try_from(last_offset + 1).expect("test offsets fit in u32");
    let mut last_missing = Vec::new();
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the same handle must read every confirmed record after the failover \
             (last missing offsets: {last_missing:?})"
        );
        match producer
            .poll_messages(
                &stream,
                &topic,
                Some(PARTITION_ID),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                count,
                false,
            )
            .await
        {
            Ok(polled) => {
                last_missing = expected
                    .iter()
                    .filter(|(offset, payload)| {
                        !polled.messages.iter().any(|message| {
                            message.header.offset == *offset
                                && message.payload.as_ref() == payload.as_bytes()
                        })
                    })
                    .map(|(offset, _)| *offset)
                    .collect();
                if last_missing.is_empty() {
                    return;
                }
                sleep(RETRY_PAUSE).await;
            }
            Err(_) => sleep(RETRY_PAUSE).await,
        }
    }
}

/// Shared prologue: topic, pinned producer, warm acks, follower restart, and
/// required progress after the follower is back. Returns the producer and the
/// primary it is pinned to.
async fn warmed_producer_past_follower_restart(
    harness: &mut TestHarness,
    transport: TransportProtocol,
    reestablish_after: Option<IggyDuration>,
) -> (IggyClient, usize, String, Vec<(u64, String)>) {
    create_stream_and_topic(harness).await;
    let leader = disk::leader_node_index(harness).await;
    let (producer, primary_endpoint) =
        pinned_producer(harness, leader, transport, reestablish_after).await;
    let mut acked = require_acked_sends(&producer, "warmup", WARM_ACKS, WARMUP_TIMEOUT).await;

    let follower = (0..harness.cluster_size())
        .find(|index| *index != leader)
        .expect("a three-node cluster has a follower");
    harness
        .restart_node(follower)
        .expect("restart the follower with its data intact");
    acked.extend(require_acked_sends(&producer, "post-follower-restart", 1, RESUME_BUDGET).await);

    (producer, leader, primary_endpoint, acked)
}

async fn require_resume_after_primary_stop(
    harness: &mut TestHarness,
    transport: TransportProtocol,
) {
    let (producer, leader, primary_endpoint, mut acked) =
        warmed_producer_past_follower_restart(harness, transport, None).await;

    harness.stop_node(leader).expect("stop the primary");

    acked.extend(require_acked_sends(&producer, "post-primary-stop", 1, RESUME_BUDGET).await);
    assert_ne!(
        producer.get_connection_info().await.server_address,
        primary_endpoint,
        "the send that resumed must have landed on a survivor"
    );
    require_readback(&producer, &acked).await;
}

async fn require_resume_after_fast_primary_rejoin(
    harness: &mut TestHarness,
    transport: TransportProtocol,
) {
    let (producer, leader, _primary_endpoint, mut acked) =
        warmed_producer_past_follower_restart(harness, transport, None).await;

    harness
        .restart_node(leader)
        .expect("restart the primary with its data intact");

    acked.extend(require_acked_sends(&producer, "post-primary-restart", 1, RESUME_BUDGET).await);
    require_readback(&producer, &acked).await;
}

async fn require_resume_after_fast_primary_rejoin_with_dead_roster_hop(
    harness: &mut TestHarness,
    transport: TransportProtocol,
) {
    let (producer, leader, _primary_endpoint, mut acked) =
        warmed_producer_past_follower_restart(harness, transport, None).await;
    let dead_hop = (leader + 1) % harness.cluster_size();
    harness
        .stop_node(dead_hop)
        .expect("stop the first roster hop after the original primary");
    harness
        .restart_node(leader)
        .expect("restart the primary with its data intact");

    acked.extend(require_acked_sends(&producer, "post-dead-roster-hop", 1, RESUME_BUDGET).await);
    require_readback(&producer, &acked).await;
}

/// Baseline sibling: the primary stops gracefully and stays down. The same
/// client must settle on a survivor, write, and read back.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_pinned_producer_when_its_primary_stops_and_stays_down_should_resume(
    harness: &mut TestHarness,
) {
    require_resume_after_primary_stop(harness, TransportProtocol::Tcp).await;
}

/// Fast-rejoin sibling: the primary stops gracefully and is started again
/// immediately, so its endpoint answers TCP and metadata as a rejoining
/// follower while the group primaryship settles elsewhere. The same client
/// must still reach a confirmed write and read the stream back.
#[iggy_harness(cluster_nodes = 3)]
async fn given_a_pinned_producer_when_its_primary_restarts_quickly_should_resume(
    harness: &mut TestHarness,
) {
    require_resume_after_fast_primary_rejoin(harness, TransportProtocol::Tcp).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_zero_cooldown_tcp_producer_when_replay_lands_on_a_partition_backup_should_resume(
    harness: &mut TestHarness,
) {
    let (producer, leader, _primary_endpoint, mut acked) = warmed_producer_past_follower_restart(
        harness,
        TransportProtocol::Tcp,
        Some(IggyDuration::from(0u64)),
    )
    .await;

    harness
        .restart_node(leader)
        .expect("restart the primary with its data intact");

    acked.extend(require_acked_sends(&producer, "zero-cooldown-replay", 1, RESUME_BUDGET).await);
    require_readback(&producer, &acked).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_quic_producer_when_its_primary_restarts_quickly_should_resume(
    harness: &mut TestHarness,
) {
    require_resume_after_fast_primary_rejoin(harness, TransportProtocol::Quic).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_quic_producer_when_its_first_roster_hop_is_down_should_reach_the_partition_primary(
    harness: &mut TestHarness,
) {
    require_resume_after_fast_primary_rejoin_with_dead_roster_hop(harness, TransportProtocol::Quic)
        .await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_websocket_producer_when_its_primary_restarts_quickly_should_resume(
    harness: &mut TestHarness,
) {
    require_resume_after_fast_primary_rejoin(harness, TransportProtocol::WebSocket).await;
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_a_websocket_producer_when_its_first_roster_hop_is_down_should_reach_the_partition_primary(
    harness: &mut TestHarness,
) {
    require_resume_after_fast_primary_rejoin_with_dead_roster_hop(
        harness,
        TransportProtocol::WebSocket,
    )
    .await;
}

/// The stateless HTTP transport has no client-side partition routing. After
/// the old primary rejoins as a backup, its listener must walk the bounded
/// server roster for acknowledged partition writes.
#[iggy_harness(
    cluster_nodes = 3,
    server(
        http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
        http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
    )
)]
async fn given_http_writes_on_a_rejoined_backup_when_the_primary_moved_should_forward_once(
    harness: &mut TestHarness,
) {
    let (producer, leader, primary_endpoint, mut acked) =
        warmed_producer_past_follower_restart(harness, TransportProtocol::Tcp, None).await;
    harness
        .restart_node(leader)
        .expect("restart the primary with its data intact");

    acked.extend(require_acked_sends(&producer, "settled-primary", 1, RESUME_BUDGET).await);
    assert_ne!(
        producer.get_connection_info().await.server_address,
        primary_endpoint,
        "the HTTP target must be a partition backup for this test"
    );

    let http_addr = harness
        .node(leader)
        .http_addr()
        .expect("the restarted node exposes HTTP");
    let http = HttpClient::login_root_no_redirect(format!("http://{http_addr}")).await;
    let payload = "http-after-primary-rejoin".to_owned();
    let message = build_message(&payload);
    let messages = vec![message];
    let body = SendMessages {
        metadata_length: 0,
        stream_id: Identifier::default(),
        topic_id: Identifier::default(),
        partitioning: Partitioning::partition_id(PARTITION_ID),
        batch: IggyMessagesBatch::from(&messages),
    };
    let response = http
        .client
        .post(http.url(&format!(
            "/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/messages"
        )))
        .bearer_auth(&http.token)
        .json(&body)
        .send()
        .await
        .expect("forwarded HTTP produce");
    assert_eq!(response.status(), StatusCode::CREATED);
    let confirmation: SendMessagesConfirmations =
        response.json().await.expect("decode confirmations");
    let confirmation = confirmation
        .confirmations
        .first()
        .expect("acknowledged HTTP produce has a confirmation");
    acked.push((confirmation.base_offset, payload));

    let offsets_path = format!("/streams/{STREAM_NAME}/topics/{TOPIC_NAME}/consumer-offsets");
    let consumer_id = Identifier::numeric(1).expect("valid consumer id");
    let store = StoreConsumerOffset {
        consumer: Consumer::new(consumer_id),
        partition_id: Some(PARTITION_ID),
        offset: 0,
    };
    let response = http
        .client
        .put(http.url(&offsets_path))
        .bearer_auth(&http.token)
        .json(&store)
        .send()
        .await
        .expect("forwarded HTTP offset store");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = http
        .client
        .delete(http.url(&format!("{offsets_path}/1?partition_id={PARTITION_ID}")))
        .bearer_auth(&http.token)
        .send()
        .await
        .expect("forwarded HTTP offset delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    require_readback(&producer, &acked).await;
}
