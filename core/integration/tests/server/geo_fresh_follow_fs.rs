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

//! A consumer in `dc-b` of the shadow of a geo topic of `dc-a`, which reads
//! the batches `dc-a` proved in its fresh objects before any manifest lists
//! them, and goes on into the long-term segments once one does.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iggy::prelude::*;
use integration::harness::{GeoClusterPair, GeoSide};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
/// How long the shadow may take to serve its first message: a follower
/// that starts after the origin's first objects waits for a manifest.
const WARM_UP_DEADLINE: Duration = Duration::from_secs(120);
/// How long a test may take to read what it sent.
const READ_DEADLINE: Duration = Duration::from_secs(120);
/// How long a manifest may take to list what was sent.
const PUBLISH_DEADLINE: Duration = Duration::from_secs(90);
/// The pause between two sends of the steady load.
const SEND_EVERY: Duration = Duration::from_millis(20);
/// The sends whose lag the steady test measures.
const MEASURED_SENDS: u64 = 250;
/// What the steady test allows a message to take from its send to its
/// read in the other cluster: one upload window, one upload and a probe
/// interval come to a fraction of it, and a manifest poll takes ten times
/// as long.
const LAG_BOUND: Duration = Duration::from_secs(3);
/// How often each manifest of the peer is listed in the steady test, so
/// that only the fresh objects can bring a message within the bound.
const SLOW_MANIFEST_POLL: &str = "30s";
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[tokio::test]
async fn given_steady_sends_to_a_geo_topic_when_a_consumer_follows_its_shadow_should_read_each_within_one_upload_window_and_one_probe()
 {
    let mut pair = GeoClusterPair::builder()
        .extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", SLOW_MANIFEST_POLL)
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    let (client_a, shadow) = create_and_follow(&pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let progress = Arc::new(Progress::default());
    let reads = tokio::spawn(read_with_lag(client_b, shadow, Arc::clone(&progress)));
    let mut index = 0;
    let warm_up = Instant::now() + WARM_UP_DEADLINE;
    while progress.read.load(Ordering::Acquire) == 0 {
        send_timed(&client_a, index).await;
        index += 1;
        assert!(
            Instant::now() < warm_up && !reads.is_finished(),
            "dc-b served none of {index} messages"
        );
        sleep(SEND_EVERY).await;
    }

    let measured_from = index;
    for _ in 0..MEASURED_SENDS {
        send_timed(&client_a, index).await;
        index += 1;
        sleep(SEND_EVERY).await;
    }
    progress.sent.store(index, Ordering::Release);
    let lags = reads.await.expect("the reader finishes");

    let measured: Vec<Duration> = lags
        .iter()
        .filter(|(index, _)| *index >= measured_from)
        .map(|(_, lag)| *lag)
        .collect();
    assert_eq!(measured.len() as u64, MEASURED_SENDS);
    let mut sorted = measured.clone();
    sorted.sort_unstable();
    let percentile = |share: usize| sorted[(sorted.len() - 1) * share / 100];
    eprintln!(
        "shadow lag over {} sends: p50 {:?}, p95 {:?}, max {:?}",
        sorted.len(),
        percentile(50),
        percentile(95),
        percentile(100)
    );
    assert!(
        percentile(100) < LAG_BOUND,
        "a message took {:?} to reach dc-b, p50 {:?}",
        percentile(100),
        percentile(50)
    );
    let metrics = scrape(&HttpClient::login_root(pair.b().harness()).await).await;
    assert!(
        sum(
            &metrics,
            "geo_peer_fresh_requests_total",
            "request=\"probe\""
        ) > 0
    );
    assert_eq!(sum(&metrics, "geo_shadow_fresh_conflicts_total", ""), 0);
}

#[tokio::test]
async fn given_a_consumer_of_a_shadow_topic_when_the_fresh_batches_it_reads_become_long_term_should_read_each_message_once_in_order()
 {
    let mut pair = GeoClusterPair::builder()
        .extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s")
        .build()
        .expect("the pair");
    pair.start().await.expect("both clusters boot");
    let (client_a, shadow) = create_and_follow(&pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let consumer = Consumer::new(Identifier::named("follower").expect("a consumer name"));

    send(&client_a, 0..50).await;
    let mut read = read_next(&client_b, shadow, &consumer, 3, 50).await;
    send(&client_a, 50..100).await;
    let published = wait_for_floor(&pair, 99).await;
    // The manifest poll of dc-b takes a second, and its commit a moment.
    sleep(Duration::from_secs(3)).await;
    send(&client_a, 100..150).await;
    read.extend(read_next(&client_b, shadow, &consumer, 3, 100).await);

    let expected: Vec<String> = (0..150).map(payload).collect();
    assert_eq!(read, expected, "each message once, in the origin's order");
    let from_start = read_from_offset_zero(&client_b, shadow, 7, 150).await;
    assert_eq!(
        from_start, expected,
        "a reader from the start goes through the segments of the floor at {published} and on into the fresh batches"
    );
    let metrics = scrape(&HttpClient::login_root(pair.b().harness()).await).await;
    assert_eq!(sum(&metrics, "geo_shadow_fresh_conflicts_total", ""), 0);
}

/// The ids of the shadow topic in `dc-b` of the geo topic of `dc-a`.
#[derive(Debug, Clone, Copy)]
struct Shadow {
    stream_id: u32,
    topic_id: u32,
}

/// Create the geo topic of one partition in `dc-a`, wait until `dc-b`
/// adopts it, and return a client of `dc-a` and the shadow.
async fn create_and_follow(pair: &GeoClusterPair) -> (IggyClient, Shadow) {
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let topic = pair
        .a()
        .create_geo_topic(&client_a, &named(STREAM), TOPIC, 1)
        .await
        .expect("dc-a takes the geo topic");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let stream = pair
        .cluster(GeoSide::B)
        .wait_for_shadow_topic(&client_b, STREAM, topic.id)
        .await
        .expect("dc-b adopts the geo topic");
    (
        client_a,
        Shadow {
            stream_id: stream.id,
            topic_id: topic.id,
        },
    )
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}

fn numeric(id: u32) -> Identifier {
    Identifier::numeric(id).expect("a valid identifier")
}

fn payload(index: u64) -> String {
    format!("message-{index}")
}

fn now_micros() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_micros(),
    )
    .expect("a time in microseconds")
}

/// How far the steady test got: the messages read, and all that were
/// sent once the sender is done.
struct Progress {
    read: AtomicU64,
    sent: AtomicU64,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            read: AtomicU64::new(0),
            sent: AtomicU64::new(u64::MAX),
        }
    }
}

/// Send the message `index`, which carries when it was sent.
async fn send_timed(client: &IggyClient, index: u64) {
    let mut payload = index.to_le_bytes().to_vec();
    payload.extend_from_slice(&now_micros().to_le_bytes());
    let mut messages = [IggyMessage::builder()
        .payload(payload.into())
        .build()
        .expect("a message")];
    client
        .send_messages(
            &named(STREAM),
            &named(TOPIC),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("dc-a takes the send");
}

/// Read the shadow from offset 0 until every message the sender sent came,
/// each once and in order, and return how long each took from its send to
/// its read.
async fn read_with_lag(
    client: IggyClient,
    shadow: Shadow,
    progress: Arc<Progress>,
) -> Vec<(u64, Duration)> {
    let mut lags = Vec::new();
    let deadline = Instant::now() + WARM_UP_DEADLINE + READ_DEADLINE;
    while (lags.len() as u64) < progress.sent.load(Ordering::Acquire) {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(lags.len() as u64),
                PollOptions {
                    count: 100,
                    ..PollOptions::default()
                },
            )
            .await;
        let read_at = now_micros();
        if let Ok(polled) = polled {
            for message in &polled.messages {
                let bytes = message.payload.as_ref();
                let index = u64::from_le_bytes(bytes[..8].try_into().expect("an index"));
                let sent_at = u64::from_le_bytes(bytes[8..16].try_into().expect("a time"));
                assert_eq!(
                    (message.header.offset, index),
                    (lags.len() as u64, lags.len() as u64),
                    "each message once, at its offset"
                );
                lags.push((
                    index,
                    Duration::from_micros(read_at.saturating_sub(sent_at)),
                ));
            }
            progress.read.store(lags.len() as u64, Ordering::Release);
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} messages",
            lags.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    lags
}

/// Send the messages `indexes` to the geo topic of `dc-a`, one a send.
async fn send(client: &IggyClient, indexes: std::ops::Range<u64>) {
    for index in indexes {
        let mut messages = [IggyMessage::builder()
            .payload(payload(index).into_bytes().into())
            .build()
            .expect("a message")];
        client
            .send_messages(
                &named(STREAM),
                &named(TOPIC),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("dc-a takes the send");
    }
}

/// Poll the next messages of `consumer`, `count` at a time and committing
/// what it read, until `wanted` arrived.
async fn read_next(
    client: &IggyClient,
    shadow: Shadow,
    consumer: &Consumer,
    count: u32,
    wanted: usize,
) -> Vec<String> {
    let mut read = Vec::new();
    let deadline = Instant::now() + READ_DEADLINE;
    while read.len() < wanted {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                consumer,
                &PollingStrategy::next(),
                PollOptions {
                    count,
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await;
        if let Ok(polled) = polled {
            read.extend(
                polled
                    .messages
                    .iter()
                    .map(|message| String::from_utf8_lossy(&message.payload).into_owned()),
            );
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} of {wanted} messages",
            read.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read
}

/// Every message of the shadow from offset 0, `count` a poll, until
/// `wanted` came.
async fn read_from_offset_zero(
    client: &IggyClient,
    shadow: Shadow,
    count: u32,
    wanted: usize,
) -> Vec<String> {
    let mut read = Vec::new();
    let deadline = Instant::now() + READ_DEADLINE;
    while read.len() < wanted {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(read.len() as u64),
                PollOptions {
                    count,
                    ..PollOptions::default()
                },
            )
            .await;
        if let Ok(polled) = polled {
            for message in &polled.messages {
                assert_eq!(message.header.offset, read.len() as u64);
                read.push(String::from_utf8_lossy(&message.payload).into_owned());
            }
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} of {wanted} messages from the start",
            read.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read
}

/// Wait until the manifest of the geo partition in the bucket of `dc-a`
/// lists every offset up to `through`, and return its floor.
async fn wait_for_floor(pair: &GeoClusterPair, through: u64) -> u64 {
    let deadline = Instant::now() + PUBLISH_DEADLINE;
    loop {
        let floor = pair
            .a()
            .bucket()
            .manifests()
            .await
            .expect("read the manifests of dc-a")
            .iter()
            .filter_map(|manifest| manifest.floor)
            .max();
        if let Some(floor) = floor.filter(|floor| *floor >= through) {
            return floor;
        }
        assert!(
            Instant::now() < deadline,
            "no manifest of dc-a lists offset {through}: {floor:?}"
        );
        sleep(Duration::from_millis(250)).await;
    }
}

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(response.status().is_success(), "/metrics scrape failed");
    response.text().await.expect("the metrics text")
}

/// The sum of the series of `name` whose labels hold `label`.
fn sum(exposition: &str, name: &str, label: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| line.starts_with(name) && line.contains(label))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .map(|value| value as u64)
        .sum()
}
