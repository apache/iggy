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

//! Geo topics under faults of the object store and of the nodes.
//!
//! The scenario bodies take clients, node addresses and a view of the
//! bucket, never a harness, so a harness of one cluster drives them here
//! and a harness of two clusters can drive the same bodies against a peer.
//!
//! Every send carries its own index in its payload, and a [`Ledger`] keeps
//! how each send was answered. A read of the partition then shows whether
//! each acknowledged send is there exactly once and in order, and whether a
//! refused one is absent.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use iggy::prelude::*;
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::consensus::{Command, Operation, RequestHeader, read_size_field};
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::requests::messages::send_messages::{RawMessage, SendMessagesEncoder};
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{HEADER_SIZE, WireDecode, WireEncode, WireIdentifier, WirePartitioning};
use iggy_object_store::s3::S3Store;
use iggy_object_store::{
    ErrorClass, FsStore, KeyLayout, ListRequest, LongTermKey, ObjectStore, PartitionManifest,
    SegmentObject,
};
use integration::harness::floci::FlociBucket;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use crate::server::raw_tcp;

pub const PARTITION_ID: u32 = 0;
/// The bucket prefix of a server that sets none.
pub const STORE_PREFIX: &str = "iggy";
/// How long a cluster may take to accept a geo topic, to recover from a
/// fault, and to rewrite what it committed into long-term segments.
pub const SETTLE_DEADLINE: Duration = Duration::from_secs(90);
pub const POLL_INTERVAL: Duration = Duration::from_millis(200);
pub const MESSAGE_BYTES: usize = 1024;
const GEO_TOPIC: &str = "geo";
const NORMAL_TOPIC: &str = "normal";
/// Sends to each topic before, during and after a store fault.
const SENDS_PER_PHASE: u64 = 10;
/// Geo sends made, and refused, while the store is down.
const REFUSED_SENDS: u64 = 3;
/// The client of the raw probes, which the SDK would pick at random.
const PROBE_CLIENT_ID: u128 = 0x6E0_FA17;
const READ_BATCH: u32 = 100;
/// How long a raw send waits for its answer: past the deadline of a fresh
/// upload, after which the server gives up an upload that hangs.
const RAW_REPLY_WAIT: Duration = Duration::from_secs(30);

/// The stream of a scenario with its geo topic and its normal topic.
pub struct Topics {
    pub stream: Identifier,
    pub geo: Identifier,
    pub normal: Identifier,
    stream_name: String,
}

/// How a send was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Acked,
    /// Refused before the partition took it, so it is never stored.
    Refused,
    /// No answer tells whether it is stored, as when its node stopped.
    Uncertain,
}

/// The answer to every send of one topic, by the index its payload carries.
#[derive(Debug, Default)]
pub struct Ledger {
    answers: Vec<Answer>,
}

/// A store fault that a scenario switches on and off.
pub trait StoreFault {
    fn begin(&mut self);
    fn end(&mut self);
}

/// An `fs` store that is down: its root becomes a symlink to itself, so
/// every call under it fails. Unlike a missing directory, a loop is an
/// error that a retry can get past, as the errors of a store that is down
/// are, so the node keeps passing its identity check.
pub struct FsStoreOutage {
    root: PathBuf,
    parked: PathBuf,
    down: bool,
}

/// A view of a bucket from outside the cluster, by key.
#[derive(Debug, Clone)]
pub enum Bucket {
    Fs(PathBuf),
    S3(FlociBucket),
}

/// A geo partition's manifest and the objects that stood beside it.
pub struct SettledBucket {
    pub manifest: PartitionManifest,
    pub objects: BTreeMap<String, Bytes>,
}

/// One raw client session that sends to a geo partition and reads the
/// answer of exactly one attempt, which the SDK would retry.
pub struct RawSender {
    stream: TcpStream,
    session: u64,
    context: PartitionContext,
    stream_name: String,
}

impl Topics {
    /// Creates `stream` with a one-partition geo topic, once the cluster
    /// accepts one, and a one-partition normal topic.
    pub async fn create(client: &IggyClient, stream: &str) -> Self {
        let topics = Self {
            stream: Identifier::named(stream).expect("stream identifier"),
            geo: Identifier::named(GEO_TOPIC).expect("geo topic identifier"),
            normal: Identifier::named(NORMAL_TOPIC).expect("normal topic identifier"),
            stream_name: stream.to_owned(),
        };
        client
            .create_stream(stream, &StreamCreateOptions::default())
            .await
            .expect("create stream");
        let geo = TopicCreateOptions {
            partitions_count: Some(1),
            topic_type: Some(TopicType::Geo),
            ..TopicCreateOptions::default()
        };
        let deadline = Instant::now() + SETTLE_DEADLINE;
        loop {
            match client.create_topic(&topics.stream, GEO_TOPIC, &geo).await {
                Ok(_) => break,
                Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                    sleep(POLL_INTERVAL).await;
                }
                Err(error) => panic!("the cluster never accepted a geo topic: {error}"),
            }
        }
        let normal = TopicCreateOptions {
            partitions_count: Some(1),
            ..TopicCreateOptions::default()
        };
        client
            .create_topic(&topics.stream, NORMAL_TOPIC, &normal)
            .await
            .expect("create the normal topic");
        topics
    }

    /// The stream and topic ids that the keys of the geo topic's objects
    /// carry.
    pub async fn geo_prefix_ids(&self, client: &IggyClient) -> (u64, u64) {
        let stream = client
            .get_stream(&self.stream)
            .await
            .expect("get stream")
            .expect("the stream exists");
        let topic = client
            .get_topic(&self.stream, &self.geo)
            .await
            .expect("get topic")
            .expect("the geo topic exists");
        (u64::from(stream.id), u64::from(topic.id))
    }

    /// The replica that leads the geo partition now.
    pub async fn geo_primary(&self, client: &IggyClient) -> u8 {
        let request = GetPartitionRouteRequest {
            stream_id: WireIdentifier::named(&self.stream_name).expect("stream identifier"),
            topic_id: WireIdentifier::named(GEO_TOPIC).expect("topic identifier"),
            partition_id: PARTITION_ID,
        }
        .to_bytes();
        let deadline = Instant::now() + SETTLE_DEADLINE;
        loop {
            match client
                .send_binary_request(GET_PARTITION_ROUTE_CODE, request.clone())
                .await
            {
                Ok(reply) => {
                    return PartitionRouteResponse::decode_from(&reply)
                        .expect("decode the route")
                        .primary_replica;
                }
                Err(IggyError::TransientNotAccepted) if Instant::now() < deadline => {
                    sleep(POLL_INTERVAL).await;
                }
                Err(error) => panic!("the geo partition has no route: {error}"),
            }
        }
    }
}

impl Ledger {
    /// Sends the next message, `bytes` long, and records how it was
    /// answered.
    pub async fn send(
        &mut self,
        client: &IggyClient,
        stream: &Identifier,
        topic: &Identifier,
        bytes: usize,
    ) -> Answer {
        self.send_batch(client, stream, topic, 1, bytes).await
    }

    /// Sends the next `count` messages, each `bytes` long, in one send, and
    /// records how it was answered for each.
    pub async fn send_batch(
        &mut self,
        client: &IggyClient,
        stream: &Identifier,
        topic: &Identifier,
        count: u64,
        bytes: usize,
    ) -> Answer {
        let first = self.answers.len() as u64;
        let mut messages: Vec<_> = (first..first + count)
            .map(|index| message(index, bytes))
            .collect();
        let answer = match client
            .send_messages(
                stream,
                topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
        {
            Ok(_) => Answer::Acked,
            // The SDK ends a send this way only when no attempt was admitted.
            Err(IggyError::TransientNotAccepted) => Answer::Refused,
            Err(_) => Answer::Uncertain,
        };
        self.answers.extend(iter::repeat_n(answer, messages.len()));
        answer
    }

    /// Sends the next message through `sender` once, and returns the status
    /// of the reply. Only a refusal tells a raw send's fate from its status,
    /// so any other answer counts as uncertain.
    pub async fn send_raw(&mut self, sender: &mut RawSender) -> u32 {
        let index = self.answers.len() as u64;
        let status = sender.send_once(index).await;
        self.answers
            .push(if status == IggyError::TransientNotAccepted.as_code() {
                Answer::Refused
            } else {
                Answer::Uncertain
            });
        status
    }

    pub fn count(&self, answer: Answer) -> usize {
        self.answers.iter().filter(|seen| **seen == answer).count()
    }

    /// Checks that `read`, the indexes in the partition in offset order,
    /// holds every acknowledged send once and in the order they were sent,
    /// and no refused one.
    pub fn assert_read(&self, read: &[u64]) {
        for pair in read.windows(2) {
            assert!(
                pair[0] < pair[1],
                "send {} sits after send {}: the partition repeats or reorders sends",
                pair[1],
                pair[0]
            );
        }
        let held: BTreeSet<u64> = read.iter().copied().collect();
        for (index, answer) in self.answers.iter().enumerate() {
            let index = index as u64;
            match answer {
                Answer::Acked => assert!(
                    held.contains(&index),
                    "acknowledged send {index} is missing: {}",
                    self.describe(read)
                ),
                Answer::Refused => assert!(
                    !held.contains(&index),
                    "refused send {index} was stored: {}",
                    self.describe(read)
                ),
                Answer::Uncertain => {}
            }
        }
        let sent = self.answers.len() as u64;
        assert!(
            read.iter().all(|index| *index < sent),
            "the partition holds a send that was never made: {}",
            self.describe(read)
        );
    }

    fn describe(&self, read: &[u64]) -> String {
        format!(
            "{} sends ({} acknowledged, {} refused, {} uncertain), {} read",
            self.answers.len(),
            self.count(Answer::Acked),
            self.count(Answer::Refused),
            self.count(Answer::Uncertain),
            read.len()
        )
    }
}

impl FsStoreOutage {
    /// The outage of the `fs` store in `store`, not begun yet.
    pub fn new(store: &Path) -> Self {
        Self {
            root: store.join(STORE_PREFIX),
            parked: store.join(format!("{STORE_PREFIX}.outage")),
            down: false,
        }
    }
}

impl StoreFault for FsStoreOutage {
    fn begin(&mut self) {
        std::fs::rename(&self.root, &self.parked).expect("park the store root");
        std::os::unix::fs::symlink(STORE_PREFIX, &self.root).expect("loop the store root");
        self.down = true;
    }

    fn end(&mut self) {
        std::fs::remove_file(&self.root).expect("remove the loop");
        std::fs::rename(&self.parked, &self.root).expect("bring the store root back");
        self.down = false;
    }
}

impl Drop for FsStoreOutage {
    fn drop(&mut self) {
        if self.down {
            let _ = std::fs::remove_file(&self.root);
            let _ = std::fs::rename(&self.parked, &self.root);
        }
    }
}

impl Bucket {
    /// Every object in the bucket, by key, with its bytes. An object deleted
    /// while the listing runs is left out.
    pub async fn objects(&self) -> BTreeMap<String, Bytes> {
        let bucket = self.clone();
        on_compio(move || {
            compio::runtime::Runtime::new()
                .expect("a compio runtime")
                .block_on(async move {
                    match bucket {
                        Self::Fs(root) => read_objects(&FsStore::new(root)).await,
                        Self::S3(bucket) => {
                            read_objects(&S3Store::new(bucket.s3_config()).expect("the S3 store"))
                                .await
                        }
                    }
                })
        })
        .await
    }

    /// Waits until the bucket holds a manifest of the geo partition with
    /// the ids `stream_id` and `topic_id` that covers every offset up to
    /// `through`, and returns it with the objects beside it.
    pub async fn await_rewritten(
        &self,
        stream_id: u64,
        topic_id: u64,
        through: u64,
    ) -> SettledBucket {
        let deadline = Instant::now() + SETTLE_DEADLINE;
        loop {
            let objects = self.objects().await;
            let manifest = geo_manifest(&objects, stream_id, topic_id);
            if let Some(manifest) = manifest.as_ref()
                && manifest.floor.is_some_and(|floor| floor >= through)
            {
                return SettledBucket {
                    manifest: manifest.clone(),
                    objects,
                };
            }
            assert!(
                Instant::now() < deadline,
                "no manifest of {stream_id}/{topic_id} covers offset {through}: {:?}",
                manifest.map(|manifest| manifest.floor)
            );
            sleep(POLL_INTERVAL).await;
        }
    }
}

impl SettledBucket {
    /// Checks that the manifest lists one run of long-term segments from
    /// offset 0 to its floor, and that the bucket holds both objects of each,
    /// the `.log` with the bytes the manifest hashed.
    ///
    /// The fresh objects left cannot be checked one by one: their keys name
    /// no offsets. A fresh object is kept while a placed batch above the
    /// long-term floor names it, and the floor of a settled partition is at
    /// its last committed offset, so no batch names any fresh object left.
    /// Each is one that the sweep deletes once its grace has passed.
    pub fn assert_long_term_log(&self) {
        let layout = KeyLayout::new(STORE_PREFIX).expect("the default prefix");
        let manifest = &self.manifest;
        assert!(
            manifest.segments_keyed_by_both_offsets,
            "a geo partition's manifest lists long-term segments"
        );
        let mut next = 0;
        for segment in &manifest.segments {
            assert_eq!(
                segment.start_offset, next,
                "the long-term log has a gap or an overlap before {segment:?}"
            );
            for object in SegmentObject::ALL {
                let key = layout.long_term_key(&LongTermKey {
                    prefix: manifest.prefix,
                    start_offset: segment.start_offset,
                    end_offset: segment.end_offset,
                    object,
                });
                let held = self
                    .objects
                    .get(&key)
                    .unwrap_or_else(|| panic!("the manifest lists {key}, which the bucket lacks"));
                if object == SegmentObject::Log {
                    assert_eq!(held.len() as u64, segment.size, "{key}");
                    assert_eq!(
                        blake3::hash(held).as_bytes(),
                        &segment.hash,
                        "{key} holds other bytes than the manifest lists"
                    );
                }
            }
            next = segment.end_offset + 1;
        }
        assert_eq!(
            manifest.floor,
            next.checked_sub(1),
            "the manifest's floor is not the end of its last segment"
        );
        let fresh = layout.fresh_prefix();
        for key in self.objects.keys().filter(|key| key.starts_with(&fresh)) {
            assert!(
                layout.parse_fresh_key(key).is_some(),
                "{key} is no fresh object that a sweep deletes"
            );
        }
    }
}

impl RawSender {
    /// Signs in as root on a connection of its own to `node`.
    pub async fn connect(node: SocketAddr, client: &IggyClient, topics: &Topics) -> Self {
        let context =
            raw_tcp::partition_context(client, &topics.stream, &topics.geo, PARTITION_ID).await;
        let mut stream = raw_tcp::connect_to(node).await;
        let session = raw_tcp::register_root(&mut stream, PROBE_CLIENT_ID).await;
        Self {
            stream,
            session,
            context,
            stream_name: topics.stream_name.clone(),
        }
    }

    /// Sends message `index` in one attempt, and returns the status of the
    /// reply: the code of a refusal, or 0 for a send the partition took.
    async fn send_once(&mut self, index: u64) -> u32 {
        let payload = payload(index, MESSAGE_BYTES);
        let messages = [RawMessage {
            id: u128::from(index) + 1,
            origin_timestamp: 0,
            headers: None,
            payload: &payload,
        }];
        let stream_id = WireIdentifier::named(&self.stream_name).expect("stream identifier");
        let topic_id = WireIdentifier::named(GEO_TOPIC).expect("topic identifier");
        let partitioning = WirePartitioning::PartitionId(PARTITION_ID);
        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut body = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut body, &stream_id, &topic_id, &partitioning, &messages)
            .expect("encode the send");
        // A refused request was never admitted, so the next one may take
        // its number again.
        let mut header: RequestHeader = raw_tcp::request_header(
            Operation::SendMessages,
            PROBE_CLIENT_ID,
            self.session,
            1,
            body.len(),
        );
        self.context.stamp(&mut header);
        raw_tcp::write_frame(&mut self.stream, &header, &body).await;
        let mut reply = [0u8; HEADER_SIZE];
        timeout(RAW_REPLY_WAIT, self.stream.read_exact(&mut reply))
            .await
            .expect("a geo send is answered")
            .expect("read the reply header");
        assert_eq!(
            raw_tcp::frame_command(&reply),
            Command::Reply as u8,
            "a geo send is answered with a reply, not an eviction"
        );
        let size = read_size_field(&reply).expect("the reply's size") as usize;
        let mut rest = vec![0u8; size.saturating_sub(HEADER_SIZE)];
        timeout(RAW_REPLY_WAIT, self.stream.read_exact(&mut rest))
            .await
            .expect("the reply's body arrives")
            .expect("read the reply body");
        raw_tcp::reply_status(&reply)
    }
}

/// Produces to the geo topic and to the normal topic of `topics` before,
/// during and after `fault`. While the store is down, each geo send through
/// `raw` must be refused with [`IggyError::TransientNotAccepted`], and the
/// normal topic must take and serve its sends. Returns the answers to the
/// geo sends.
pub async fn produce_through_store_fault(
    client: &IggyClient,
    raw: &mut RawSender,
    topics: &Topics,
    fault: &mut dyn StoreFault,
) -> Ledger {
    let mut geo = Ledger::default();
    let mut normal = Ledger::default();
    for _ in 0..SENDS_PER_PHASE {
        assert_eq!(
            geo.send(client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
        assert_eq!(
            normal
                .send(client, &topics.stream, &topics.normal, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }

    fault.begin();
    for _ in 0..REFUSED_SENDS {
        let status = geo.send_raw(raw).await;
        assert_eq!(
            status,
            IggyError::TransientNotAccepted.as_code(),
            "a geo send to a store that is down must be refused as retryable"
        );
    }
    for _ in 0..SENDS_PER_PHASE {
        assert_eq!(
            normal
                .send(client, &topics.stream, &topics.normal, MESSAGE_BYTES)
                .await,
            Answer::Acked,
            "a normal topic takes sends while the store is down"
        );
    }
    let read = read_indexes(client, &topics.stream, &topics.normal)
        .await
        .expect("a normal topic serves polls while the store is down");
    normal.assert_read(&read);
    fault.end();

    // The first send after the store returns can still meet an upload
    // that failed before it did.
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while geo
        .send(client, &topics.stream, &topics.geo, MESSAGE_BYTES)
        .await
        != Answer::Acked
    {
        assert!(
            Instant::now() < deadline,
            "the geo topic takes no send after the store returned"
        );
        sleep(POLL_INTERVAL).await;
    }
    for _ in 1..SENDS_PER_PHASE {
        assert_eq!(
            geo.send(client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }
    geo
}

/// Sends to the geo topic of `topics`, one message of `bytes` at a time,
/// until `interrupt` is done and `after` more sends were acknowledged.
/// `interrupt` starts once `before` sends were acknowledged.
pub async fn produce_around(
    client: &IggyClient,
    topics: &Topics,
    bytes: usize,
    before: usize,
    after: usize,
    interrupt: impl Future<Output = ()>,
) -> Ledger {
    let acked = Cell::new(0);
    let acked_at_interrupt = Cell::new(None);
    let mut ledger = Ledger::default();
    let producer = async {
        let deadline = Instant::now() + 2 * SETTLE_DEADLINE;
        while acked_at_interrupt
            .get()
            .is_none_or(|at: usize| acked.get() < at + after)
        {
            if ledger
                .send(client, &topics.stream, &topics.geo, bytes)
                .await
                == Answer::Acked
            {
                acked.set(acked.get() + 1);
            }
            assert!(
                Instant::now() < deadline,
                "the geo topic took {} sends in {:?}",
                acked.get(),
                2 * SETTLE_DEADLINE
            );
        }
    };
    let interrupter = async {
        while acked.get() < before {
            sleep(Duration::from_millis(1)).await;
        }
        interrupt.await;
        acked_at_interrupt.set(Some(acked.get()));
    };
    tokio::join!(producer, interrupter);
    ledger
}

/// Reads the geo partition back, waits until long-term segments in `bucket`
/// hold all of it, and reads it again from them.
pub async fn assert_settled(
    client: &IggyClient,
    topics: &Topics,
    ledger: &Ledger,
    bucket: &Bucket,
) {
    let read = read_indexes(client, &topics.stream, &topics.geo)
        .await
        .expect("read the geo topic");
    ledger.assert_read(&read);
    let last = (read.len() as u64)
        .checked_sub(1)
        .expect("the geo topic holds messages");
    let (stream_id, topic_id) = topics.geo_prefix_ids(client).await;
    let settled = bucket.await_rewritten(stream_id, topic_id, last).await;
    settled.assert_long_term_log();
    let reread = read_indexes(client, &topics.stream, &topics.geo)
        .await
        .expect("read the geo topic from its long-term segments");
    assert_eq!(reread, read, "the long-term segments hold other messages");
}

/// Reads the partition of `topic` from its start, and returns the index
/// that each message carries, in offset order. Each message is checked
/// against the payload of its index.
pub async fn read_indexes(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Result<Vec<u64>, IggyError> {
    let mut indexes = Vec::new();
    let mut offset = 0;
    loop {
        let polled = client
            .poll_messages(
                stream,
                topic,
                PARTITION_ID,
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                PollOptions {
                    count: READ_BATCH,
                    ..PollOptions::default()
                },
            )
            .await?;
        if polled.messages.is_empty() {
            return Ok(indexes);
        }
        for message in &polled.messages {
            assert_eq!(
                message.header.offset, offset,
                "the partition skips an offset"
            );
            let bytes = message.payload.as_ref();
            let index = bytes
                .get(..8)
                .and_then(|head| head.try_into().ok())
                .map(u64::from_le_bytes)
                .unwrap_or_else(|| panic!("the message at offset {offset} carries no index"));
            assert_eq!(
                bytes,
                payload(index, bytes.len()).as_slice(),
                "the payload at offset {offset}"
            );
            indexes.push(index);
            offset += 1;
        }
    }
}

pub fn message(index: u64, bytes: usize) -> IggyMessage {
    IggyMessage::builder()
        .payload(payload(index, bytes).into())
        .build()
        .expect("build message")
}

fn payload(index: u64, bytes: usize) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; bytes.max(8)];
    payload[..8].copy_from_slice(&index.to_le_bytes());
    payload
}

/// The newest manifest in `objects` of partition [`PARTITION_ID`] of the
/// topic `topic_id` in the stream `stream_id`.
pub fn geo_manifest(
    objects: &BTreeMap<String, Bytes>,
    stream_id: u64,
    topic_id: u64,
) -> Option<PartitionManifest> {
    objects
        .iter()
        .filter(|(key, _)| key.ends_with("/manifest.bin"))
        .filter_map(|(_, bytes)| PartitionManifest::decode(bytes).ok())
        .filter(|manifest| {
            manifest.prefix.stream_id == stream_id
                && manifest.prefix.topic_id == topic_id
                && manifest.prefix.partition_id == u64::from(PARTITION_ID)
        })
        .max_by_key(|manifest| (manifest.prefix.created_revision, manifest.floor))
}

async fn read_objects<S: ObjectStore>(store: &S) -> BTreeMap<String, Bytes> {
    let mut objects = BTreeMap::new();
    let mut next: Option<String> = None;
    loop {
        let request = match next.as_deref() {
            Some(continuation) => ListRequest::new("").after(continuation),
            None => ListRequest::new(""),
        };
        let page = store.list(&request).await.expect("list the bucket");
        for listed in page.objects {
            if listed.size == 0 {
                objects.insert(listed.key, Bytes::new());
                continue;
            }
            match store.get_range(&listed.key, 0..listed.size).await {
                Ok(range) => {
                    objects.insert(listed.key, range.bytes);
                }
                Err(error) if error.class() == ErrorClass::NotFound => {}
                Err(error) => panic!("read {}: {error}", listed.key),
            }
        }
        match page.next {
            Some(continuation) => next = Some(continuation),
            None => return objects,
        }
    }
}

/// Runs `work` on a thread of its own, as the store clients run on compio
/// and not on the test's runtime.
async fn on_compio<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        let _ = sender.send(work());
    });
    receiver.await.expect("the store thread answers")
}
