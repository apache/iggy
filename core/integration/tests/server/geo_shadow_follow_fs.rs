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

//! A shadow topic in `dc-b` of a geo topic of `dc-a`, which serves what the
//! manifests of `dc-a` list and nothing else: a message reaches a consumer in
//! `dc-b` once a long-term segment holds it and `dc-b` read a manifest that
//! lists that segment. Nothing in `dc-b` reads the fresh objects of `dc-a`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::KeyLayout;
use integration::harness::{Bucket, BucketEvent, GeoClusterPair, GeoClusterPairBuilder, GeoSide};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
/// How long the shadow may take to serve its first message.
const WARM_UP_DEADLINE: Duration = Duration::from_secs(120);
/// How long a test may take to read what it sent.
const READ_DEADLINE: Duration = Duration::from_secs(120);
/// The pause between two sends of the steady load.
const SEND_EVERY: Duration = Duration::from_millis(50);
/// How long the steady load runs after the shadow served its first message.
const MEASURE_FOR: Duration = Duration::from_secs(60);
/// The longest wait between two rewrites of a partition, the slowest step
/// from a commit to a stored manifest.
const REWRITE_INTERVAL_MAX: Duration = Duration::from_secs(10);
/// What a message may take beyond one rewrite interval and one manifest
/// poll: the upload and commit of the segment, the manifest, its read and
/// its commit in `dc-b`, and the reader's next poll.
const LAG_SLACK: Duration = Duration::from_secs(10);
/// How long the request test watches the bucket of `dc-a` through `dc-b`.
const REQUEST_WINDOW: Duration = Duration::from_secs(30);
/// `[geo] manifest_poll_interval` at its default.
const DEFAULT_MANIFEST_POLL: Duration = Duration::from_secs(5);
/// How long a new watch takes to report the directories it opened itself
/// when it started, which the request test leaves out.
const WATCH_SETTLE: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[tokio::test]
async fn given_steady_sends_to_a_geo_topic_when_a_consumer_follows_its_shadow_should_read_each_message_only_once_a_manifest_lists_it()
 {
    let pair = started_pair(GeoClusterPair::builder()).await;
    let lags = measure_lag(&pair).await;
    lags.report("defaults");
    lags.assert_within(REWRITE_INTERVAL_MAX + DEFAULT_MANIFEST_POLL + LAG_SLACK);
}

#[tokio::test]
async fn given_a_one_second_manifest_poll_when_a_consumer_follows_a_shadow_under_steady_sends_should_read_each_message_only_once_a_manifest_lists_it()
 {
    let pair =
        started_pair(GeoClusterPair::builder().extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s"))
            .await;
    let lags = measure_lag(&pair).await;
    lags.report("manifest_poll_interval = 1 s");
    lags.assert_within(REWRITE_INTERVAL_MAX + Duration::from_secs(1) + LAG_SLACK);
}

#[tokio::test]
async fn given_a_consumer_of_a_shadow_topic_when_manifests_move_the_floor_should_read_each_message_once_in_order()
 {
    let pair =
        started_pair(GeoClusterPair::builder().extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s"))
            .await;
    let (client_a, shadow) = create_and_follow(&pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let consumer = Consumer::new(Identifier::named("follower").expect("a consumer name"));

    send(&client_a, 0..50).await;
    let mut read = read_next(&pair, &client_b, shadow, &consumer, 3, 50).await;
    send(&client_a, 50..150).await;
    read.extend(read_next(&pair, &client_b, shadow, &consumer, 3, 100).await);

    let expected: Vec<String> = (0..150).map(payload).collect();
    assert_eq!(read, expected, "each message once, in the origin's order");
    let from_start = read_from_offset_zero(&client_b, shadow, 7, 150).await;
    assert_eq!(
        from_start, expected,
        "a reader from the start goes through every segment the manifests listed"
    );
}

#[tokio::test]
async fn given_a_shadow_without_consumers_when_its_origin_produces_should_send_the_peer_store_only_manifest_listings_and_reads()
 {
    let pair = started_pair(GeoClusterPair::builder().peer_reads_through_mirror(GeoSide::B)).await;
    let mirror = pair
        .b()
        .peer_mirror()
        .expect("dc-b reads dc-a through a mirror");
    let (client_a, _shadow) = create_and_follow(&pair).await;
    let producing = Arc::new(AtomicBool::new(true));
    let producer = tokio::spawn(produce_until_stopped(client_a, Arc::clone(&producing)));
    let http_b = HttpClient::login_root(pair.b().harness()).await;
    let deadline = Instant::now() + WARM_UP_DEADLINE;
    while manifest_requests(&scrape(&http_b).await).1 == 0 {
        assert!(
            Instant::now() < deadline && !producer.is_finished(),
            "dc-b read no manifest of dc-a"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let watch = mirror.watch().expect("a watch on the mirror");
    sleep(WATCH_SETTLE).await;
    let started = std::time::Instant::now();
    let (lists_before, gets_before) = manifest_requests(&scrape(&http_b).await);
    sleep(REQUEST_WINDOW).await;
    let (lists_after, gets_after) = manifest_requests(&scrape(&http_b).await);
    let reads = watch.reads();
    let lost = watch
        .changes()
        .into_iter()
        .filter(
            |change| matches!(change, BucketEvent::Change { kind, .. } if kind == "events lost"),
        )
        .count();
    drop(watch);
    producing.store(false, Ordering::Release);
    let sent = producer.await.expect("the producer finishes");

    let layout = pair.a().bucket().layout();
    let key_of = |path: &Path| {
        path.strip_prefix(mirror.path())
            .ok()
            .and_then(Path::to_str)
            .map(str::to_owned)
    };
    let mut opens = Opens::default();
    for event in &reads {
        if let BucketEvent::Read { at, paths } = event
            && *at >= started
        {
            for path in paths {
                opens.count(layout, key_of(path).as_deref());
            }
        }
    }
    let (lists, gets) = (lists_after - lists_before, gets_after - gets_before);
    let manifests_stored = mirror
        .copies()
        .iter()
        .filter(|copy| copy.at >= started)
        .filter_map(|copy| key_of(&copy.path))
        .filter(|key| layout.parse_manifest_key(key).is_some())
        .count() as u64;
    eprintln!(
        "{sent} sends in about {REQUEST_WINDOW:?}: dc-a stored {manifests_stored} manifests, \
         dc-b listed {lists} times and read {gets} manifests; opens of its bucket: {opens:?}"
    );
    assert_eq!(lost, 0, "the watch lost no event");
    assert_eq!(
        (opens.fresh, opens.segments, opens.recovery),
        (0, 0, 0),
        "no fresh object, segment or recovery record of dc-a is read: {opens:?}"
    );
    let polls = REQUEST_WINDOW.as_secs() / DEFAULT_MANIFEST_POLL.as_secs();
    assert!(
        lists + 1 >= polls && lists <= polls + 1,
        "one listing per manifest poll: {lists} in {REQUEST_WINDOW:?}"
    );
    assert!(
        gets >= 1 && gets <= lists + 1 && gets <= manifests_stored + 1,
        "a manifest is read only when it changed: {gets} reads, {lists} listings, \
         {manifests_stored} stored"
    );
    assert!(
        opens.manifest_files > 0 && opens.manifest_files <= lists + gets + 2,
        "each listing and each read opens the manifest once: {opens:?}"
    );
}

/// The ids of the shadow topic in `dc-b` of the geo topic of `dc-a`.
#[derive(Debug, Clone, Copy)]
struct Shadow {
    stream_id: u32,
    topic_id: u32,
}

/// What `dc-b` opened under its mirror of the bucket of `dc-a`, by what it
/// holds.
#[derive(Debug, Default)]
struct Opens {
    /// The manifests of the stream: its directories, listed.
    manifest_directories: u64,
    /// The manifest files, opened by a listing for their header or by a
    /// read.
    manifest_files: u64,
    fresh: u64,
    segments: u64,
    recovery: u64,
    /// The rest, which no partition owns: the identity, topic and positions
    /// objects and the directories above them, by the first two parts of
    /// their keys under the root.
    other: BTreeMap<String, u64>,
}

impl Opens {
    /// Count an open of `key`, the path under the mirror, `None` for one
    /// outside it.
    fn count(&mut self, layout: &KeyLayout, key: Option<&str>) {
        let Some(key) = key else {
            *self.other.entry(String::new()).or_default() += 1;
            return;
        };
        if key
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with('.'))
        {
            // A scratch file of the mirror.
            return;
        }
        if layout.parse_manifest_key(key).is_some() {
            self.manifest_files += 1;
            return;
        }
        let Some(rest) = key
            .strip_prefix(layout.root())
            .and_then(|rest| rest.strip_prefix('/'))
        else {
            *self.other.entry(key.to_owned()).or_default() += 1;
            return;
        };
        let parts: Vec<&str> = rest.split('/').collect();
        let is_id = |part: &&str| part.parse::<u64>().is_ok();
        match parts.as_slice() {
            ["meta", "fresh", ..] => self.fresh += 1,
            ["meta", "recovery", ..] => self.recovery += 1,
            ["meta", id, ..] if is_id(id) => self.manifest_directories += 1,
            [id, ..] if is_id(id) => self.segments += 1,
            _ => {
                let place = parts.iter().take(2).copied().collect::<Vec<_>>().join("/");
                *self.other.entry(place).or_default() += 1;
            }
        }
    }
}

/// How long each message of the steady load took from its acknowledgement
/// in `dc-a` to its read in `dc-b`.
struct Lags(Vec<Duration>);

impl Lags {
    fn percentile(&self, share: usize) -> Duration {
        self.0[(self.0.len() - 1) * share / 100]
    }

    fn report(&self, setting: &str) {
        eprintln!(
            "shadow lag at {setting} over {} sends: p50 {:?}, p95 {:?}, max {:?}",
            self.0.len(),
            self.percentile(50),
            self.percentile(95),
            self.percentile(100)
        );
    }

    fn assert_within(&self, bound: Duration) {
        assert!(
            self.percentile(100) < bound,
            "a message took {:?} to reach dc-b, more than {bound:?}, p50 {:?}",
            self.percentile(100),
            self.percentile(50)
        );
    }
}

async fn started_pair(builder: GeoClusterPairBuilder) -> GeoClusterPair {
    let mut pair = builder.build().expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Send to the geo topic of `dc-a` every [`SEND_EVERY`] while a consumer in
/// `dc-b` reads its shadow from offset 0, and return how long each message
/// sent after the first read took.
async fn measure_lag(pair: &GeoClusterPair) -> Lags {
    let (client_a, shadow) = create_and_follow(pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let acked = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(Mutex::new(None));
    let reader = tokio::spawn(read_and_check(
        client_b,
        pair.a().bucket().clone(),
        shadow,
        Arc::clone(&acked),
        Arc::clone(&sent),
    ));
    let warm_up = Instant::now() + WARM_UP_DEADLINE;
    let mut index = 0;
    while first_read(&acked).is_none() {
        send_one(&client_a, index, &acked).await;
        index += 1;
        assert!(
            Instant::now() < warm_up && !reader.is_finished(),
            "dc-b served none of {index} messages"
        );
        sleep(SEND_EVERY).await;
    }
    let measured_from = index;
    let measure_until = Instant::now() + MEASURE_FOR;
    while Instant::now() < measure_until {
        send_one(&client_a, index, &acked).await;
        index += 1;
        sleep(SEND_EVERY).await;
    }
    *sent.lock().unwrap_or_else(PoisonError::into_inner) = Some(index);
    let read_at = reader.await.expect("the reader finishes");
    let acked = acked.lock().unwrap_or_else(PoisonError::into_inner);
    let mut lags: Vec<Duration> = (measured_from..index)
        .map(|index| {
            let index = usize::try_from(index).expect("an index");
            read_at[index].saturating_duration_since(acked[index].0)
        })
        .collect();
    lags.sort_unstable();
    Lags(lags)
}

/// The acknowledgement of each message, and whether it was read.
type Acked = Arc<Mutex<Vec<(Instant, bool)>>>;

/// The index of the first message read, `None` before one was.
fn first_read(acked: &Acked) -> Option<usize> {
    acked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .position(|(_, read)| *read)
}

/// Send the message `index` and record when `dc-a` acknowledged it.
async fn send_one(client: &IggyClient, index: u64, acked: &Acked) {
    send(client, index..index + 1).await;
    acked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((Instant::now(), false));
}

/// Read the shadow from offset 0 until every message that was sent came,
/// each once and in order, and return when each one was read. Each poll
/// serves no offset past the floor of the newest manifest in the bucket of
/// `dc-a`: the shadow serves only what a manifest it read lists.
async fn read_and_check(
    client: IggyClient,
    bucket_a: Bucket,
    shadow: Shadow,
    acked: Acked,
    sent: Arc<Mutex<Option<u64>>>,
) -> Vec<Instant> {
    let mut read_at: Vec<Instant> = Vec::new();
    let deadline = Instant::now() + WARM_UP_DEADLINE + MEASURE_FOR + READ_DEADLINE;
    let done = |read: usize| {
        sent.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|sent| read as u64 >= sent)
    };
    while !done(read_at.len()) {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(read_at.len() as u64),
                PollOptions {
                    count: 100,
                    ..PollOptions::default()
                },
            )
            .await;
        let now = Instant::now();
        if let Ok(polled) = polled
            && let Some(last) = polled.messages.last()
        {
            let floor = origin_floor(&bucket_a).await;
            assert!(
                floor.is_some_and(|floor| last.header.offset <= floor),
                "dc-b served offset {} above the floor {floor:?} of every manifest of dc-a",
                last.header.offset
            );
            let mut acked = acked.lock().unwrap_or_else(PoisonError::into_inner);
            for message in &polled.messages {
                let index = read_at.len();
                assert_eq!(
                    (message.header.offset, payload_of(message)),
                    (index as u64, payload(index as u64)),
                    "each message once, at its offset"
                );
                if let Some(entry) = acked.get_mut(index) {
                    entry.1 = true;
                }
                read_at.push(now);
            }
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} messages",
            read_at.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read_at
}

/// The highest floor of the manifests in `bucket`.
async fn origin_floor(bucket: &Bucket) -> Option<u64> {
    bucket
        .manifests()
        .await
        .expect("read the manifests of dc-a")
        .iter()
        .filter_map(|manifest| manifest.floor)
        .max()
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

/// Send to the geo topic of `dc-a`, one message at a time, until `producing`
/// is cleared, and return how many it sent.
async fn produce_until_stopped(client: IggyClient, producing: Arc<AtomicBool>) -> u64 {
    let mut index = 0;
    while producing.load(Ordering::Acquire) {
        send(&client, index..index + 1).await;
        index += 1;
    }
    index
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

fn payload_of(message: &IggyMessage) -> String {
    String::from_utf8_lossy(&message.payload).into_owned()
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
/// what it read, until `wanted` arrived. No poll serves an offset past the
/// floor of the newest manifest of `dc-a`.
async fn read_next(
    pair: &GeoClusterPair,
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
        if let Ok(polled) = polled
            && let Some(last) = polled.messages.last()
        {
            let floor = origin_floor(pair.a().bucket()).await;
            assert!(
                floor.is_some_and(|floor| last.header.offset <= floor),
                "dc-b served offset {} above the floor {floor:?} of every manifest of dc-a",
                last.header.offset
            );
            read.extend(polled.messages.iter().map(payload_of));
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
                read.push(payload_of(message));
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

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(response.status().is_success(), "/metrics scrape failed");
    response.text().await.expect("the metrics text")
}

/// The listings of the manifests of `dc-a` that `dc-b` sent, and the reads.
fn manifest_requests(exposition: &str) -> (u64, u64) {
    let sum = |request: &str| {
        exposition
            .lines()
            .filter(|line| {
                line.starts_with("geo_peer_manifest_requests_total")
                    && line.contains(&format!("request=\"{request}\""))
            })
            .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
            .map(|value| value as u64)
            .sum()
    };
    (sum("list"), sum("get"))
}
