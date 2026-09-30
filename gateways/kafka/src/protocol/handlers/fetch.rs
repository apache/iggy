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

//! Fetch (API key 1).

use std::collections::{HashMap, HashSet};
use std::future::{Future, poll_fn};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    IGGY_MESSAGE_HEADER_SIZE, IggyMessage, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE, PolledMessages,
};
use kafka_protocol::indexmap::IndexSet;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::records::Record;
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use crate::bridge::iggy_bridge::{FetchSlot, PartitionProbe};
use crate::bridge::topic_map::validate_kafka_topic_name;
use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_FETCH, ApiVersionRange, ConnectionState, ERROR_FETCH_SESSION_ID_NOT_FOUND,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_OFFSET_OUT_OF_RANGE, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE, UNKNOWN_OFFSET,
};
use crate::protocol::bounds_guard::validate_fetch_shape;
use crate::protocol::handlers::{
    CODEC_OFF_WORKER_BYTES, COPY_OFF_WORKER_BYTES, decode_guarded, decode_request, encode_message,
    off_worker, respond_or_close,
};
use crate::protocol::probe_board::{PROBE_INTERVAL, Snapshot, TopicProbed, recent};
use crate::records::{
    BATCH_HEADER_BYTES, RecordCodecError, encode_batch, from_iggy, record_size_bound,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_FETCH,
    min_version: 4,
    max_version: 12,
};

/// Longest wait. Leaves 2 s for the last probe and read, so a long wait does not end in 6.
const MAX_WAIT: Duration = REQUEST_DEADLINE.saturating_sub(Duration::from_secs(2));
/// Most topics one request loads a new probe for, per round, so a request that names many costs
/// Iggy this many `get_topic` calls per round at most.
const MAX_ROUND_TOPICS: usize = 100;
/// How long a partition with no offset past 0 answers 6 instead of 1. Iggy reports that shape
/// while it loads or fences a partition, so a 1 then resets a consumer that is in range.
///
/// A purge reads the same. Seek consumers to 0 after one: writes that pass a consumer's old offset
/// make it skip the records below.
const RELOAD_GRACE: Duration = Duration::from_secs(30);

/// Most messages one partition read takes from Iggy, over all its polls.
const MAX_POLL_COUNT: u32 = 1000;
/// Most messages one poll asks for. Iggy caps a poll by count, not by bytes, so this caps what one
/// poll can load: about 1 GB of the largest messages. A byte cap in Iggy would lift it.
const MAX_PAGE_COUNT: u32 = 16;
/// The largest message Iggy stores, header included.
const LARGEST_MESSAGE_BYTES: u64 =
    MAX_PAYLOAD_SIZE as u64 + MAX_USER_HEADERS_SIZE as u64 + IGGY_MESSAGE_HEADER_SIZE as u64;
// The server answers an empty poll when the reply passes its u32 frame size. The consumer would
// then stall.
const _: () = assert!(MAX_PAGE_COUNT as u64 * LARGEST_MESSAGE_BYTES < u32::MAX as u64);

/// First version with sessions and a top-level error code.
const SESSION_MIN_VERSION: i16 = 7;
/// The epochs of a full request. Any other epoch continues a session.
const INITIAL_EPOCH: i32 = 0;
const FINAL_EPOCH: i32 = -1;

/// Without a bridge, the stub. With one, records from Iggy.
///
/// `connection` keeps the partitions that answered its Fetches -1, so its retries skip Iggy for a
/// while.
pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_FETCH,
        api_version,
        body,
        |v, b| decode(state, v, b),
        encode_error_response,
        "Fetch",
    );
    let request = match decoded {
        Ok(request) => request,
        Err(outcome) => return outcome,
    };
    let Some(bridge) = &state.bridge else {
        return respond_or_close(encode_response(api_version, &request), "Fetch");
    };
    // Kafka answers an unknown session the same way. The client then sends a full request.
    if !is_full(api_version, &request) {
        tracing::debug!(
            session_id = request.session_id,
            session_epoch = request.session_epoch,
            "Fetch continues a session this gateway never opened"
        );
        return respond_or_close(
            encode_error_response(api_version, ERROR_FETCH_SESSION_ID_NOT_FOUND),
            "Fetch",
        );
    }

    let (responses, slot) = fetch(bridge, state, &connection.stuck_offsets, &request).await;
    let record_bytes: usize = responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.records.as_ref().map_or(0, Bytes::len))
        .sum();
    // The records are encoded already, so this is a plain copy.
    let encoded = off_worker(record_bytes >= COPY_OFF_WORKER_BYTES, || {
        encode_inner(api_version, responses, ERROR_NONE)
    });
    // The slot covers the read and the encode. A slow socket must not hold it.
    drop(slot);
    respond_or_close(encoded, "Fetch")
}

/// Well-formed Fetch response. Uses top-level `error_code` at v7+, or a single
/// placeholder topic/partition with per-partition `error_code` below v7.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    if version >= SESSION_MIN_VERSION {
        return encode_inner(version, Vec::new(), error_code);
    }
    // No top-level error field below v7; the error surfaces on the placeholder partition instead.
    let topics = vec![
        FetchableTopicResponse::default().with_partitions(vec![partition_response(0, error_code)]),
    ];
    encode_inner(version, topics, ERROR_NONE)
}

/// Stub: discard the payload and return a retriable error so clients don't mistake "no real
/// data yet" for a genuinely empty partition (same philosophy as Produce).
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &FetchRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| partition_response(p.partition, ERROR_NOT_LEADER_OR_FOLLOWER))
                        .collect(),
                )
        })
        .collect();
    encode_inner(version, topics, ERROR_NONE)
}

fn decode(state: &GatewayState, version: i16, body: Bytes) -> Result<FetchRequest> {
    decode_guarded::<FetchRequest>(version, body, |v, b| {
        validate_fetch_shape(v, b, state.max_frame_size)
    })
}

/// Whether `request` names every partition it wants. Anything else continues a session, and
/// this gateway opens none: it always answers `session_id` 0.
const fn is_full(version: i16, request: &FetchRequest) -> bool {
    version < SESSION_MIN_VERSION || matches!(request.session_epoch, INITIAL_EPOCH | FINAL_EPOCH)
}

/// Reads every requested partition once. When that finds nothing, waits for records up to
/// `max_wait_ms` and reads again.
///
/// Returns the read slot too, if the read took one. Keep it until the response is encoded.
async fn fetch(
    bridge: &Arc<IggyBridge>,
    state: &GatewayState,
    stuck_offsets: &Mutex<StuckOffsets>,
    request: &FetchRequest,
) -> (Vec<FetchableTopicResponse>, Option<FetchSlot>) {
    let started = Instant::now();
    let give_up_at = started + REQUEST_DEADLINE;
    let fetch = Fetch::new(bridge, state, stuck_offsets, request, give_up_at);
    // A shared probe will do: an offset past it goes to a poll, not straight to a 1.
    let since = recent(started);
    let mut probes = fetch.probe_all(since).await;
    let (mut answers, mut slot) = fetch.read(&probes).await;
    if waits(request, &answers) {
        // No slot while it waits.
        drop(slot);
        let held: Vec<bool> = answers
            .iter()
            .map(|answer| retried_at_once(answer.error_code))
            .collect();
        let wait_until = started + fetch.max_wait;
        probes = fetch
            .wait(probes, &held, request.min_bytes, since, wait_until)
            .await;
        // Kafka reads again when the wait ends, too.
        (answers, slot) = fetch.read(&probes).await;
    }
    (group(request, &fetch.wanted, answers), slot)
}

/// Kafka answers at once when a partition fails, when the request allows no wait, or when the
/// records read reach `min_bytes`. Otherwise it waits.
///
/// A 6 or a -1 waits too. See [`retried_at_once`].
fn waits(request: &FetchRequest, answers: &[PartitionData]) -> bool {
    let min_bytes = usize::try_from(request.min_bytes).unwrap_or(0);
    let record_bytes: usize = answers
        .iter()
        .map(|answer| answer.records.as_ref().map_or(0, Bytes::len))
        .sum();
    request.max_wait_ms > 0
        && !answers.is_empty()
        && record_bytes < min_bytes
        && answers
            .iter()
            .all(|answer| answer.error_code == ERROR_NONE || retried_at_once(answer.error_code))
}

/// Codes the Java consumer fetches again at once after. Sent at once, they make it spin, so they
/// wait out the request instead.
const fn retried_at_once(code: i16) -> bool {
    matches!(
        code,
        ERROR_NOT_LEADER_OR_FOLLOWER | ERROR_UNKNOWN_SERVER_ERROR
    )
}

fn wait_time(max_wait_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(max_wait_ms).unwrap_or(0)).min(MAX_WAIT)
}

/// When a -1 hold that starts at `now` ends: after the request's wait, but at least
/// [`PROBE_INTERVAL`] on. A consumer with a short wait then reads a stuck partition at most ten
/// times a second.
fn hold_until(now: Instant, max_wait: Duration) -> Instant {
    now + max_wait.max(PROBE_INTERVAL)
}

/// The topics to load a new probe for: each one whose newest probe started before `since`, the
/// oldest first, at most [`MAX_ROUND_TOPICS`].
fn stale_topics(snapshots: &[Option<Arc<Snapshot>>], since: Instant) -> Vec<usize> {
    let started = |topic: usize| snapshots[topic].as_ref().map(|snapshot| snapshot.started);
    let mut stale: Vec<usize> = (0..snapshots.len())
        .filter(|&topic| started(topic).is_none_or(|started| started < since))
        .collect();
    // No probe sorts first, then the oldest, so each topic gets its turn.
    stale.sort_by_key(|&topic| started(topic));
    stale.truncate(MAX_ROUND_TOPICS);
    stale
}

/// The topics wait round `turn` probes: the next [`MAX_ROUND_TOPICS`] of `count`, in turn.
fn round(turn: usize, count: usize) -> impl Iterator<Item = usize> {
    let start = turn.saturating_mul(MAX_ROUND_TOPICS) % count.max(1);
    (start..start + MAX_ROUND_TOPICS.min(count)).map(move |topic| topic % count)
}

/// Waits for the next gateway write to any of the topics behind `writes`, then returns the topics
/// written, at most [`MAX_ROUND_TOPICS`], each with the time of its write, and marks them seen.
/// Pends while no write can come.
async fn next_writes(writes: &mut [watch::Receiver<Option<Instant>>]) -> Vec<(usize, Instant)> {
    let first = {
        let mut changes: Vec<_> = writes
            .iter_mut()
            .map(|write| Some(Box::pin(write.changed())))
            .collect();
        poll_fn(|context| {
            for (topic, slot) in changes.iter_mut().enumerate() {
                let Some(change) = slot else { continue };
                match change.as_mut().poll(context) {
                    // `changed` marks this write seen.
                    Poll::Ready(Ok(())) => return Poll::Ready(topic),
                    // Only a dropped board ends the channel. The board keeps a watched topic.
                    Poll::Ready(Err(_)) => *slot = None,
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        })
        .await
    };
    let mut written = vec![(first, written_at(&mut writes[first]))];
    written.extend(
        writes
            .iter_mut()
            .enumerate()
            .filter(|(topic, write)| *topic != first && write.has_changed().unwrap_or(false))
            .take(MAX_ROUND_TOPICS - 1)
            .map(|(topic, write)| (topic, written_at(write))),
    );
    written
}

/// When the newest write that `write` shows ended. Marks it seen.
fn written_at(write: &mut watch::Receiver<Option<Instant>>) -> Instant {
    let seen = *write.borrow_and_update();
    seen.unwrap_or_else(Instant::now)
}

/// The topics behind `writes` whose last gateway write ended at `since` or later, each with the
/// time of that write, at most [`MAX_ROUND_TOPICS`].
fn writes_since(
    writes: &[watch::Receiver<Option<Instant>>],
    since: Instant,
) -> Vec<(usize, Instant)> {
    writes
        .iter()
        .enumerate()
        .filter_map(|(topic, write)| {
            let written = (*write.borrow())?;
            (written >= since).then_some((topic, written))
        })
        .take(MAX_ROUND_TOPICS)
        .collect()
}

/// One request: what it asks for, and what bounds it.
struct Fetch<'a> {
    bridge: &'a Arc<IggyBridge>,
    state: &'a GatewayState,
    /// The connection's, not the gateway's. See [`ConnectionState`].
    stuck_offsets: &'a Mutex<StuckOffsets>,
    wanted: Vec<Wanted<'a>>,
    /// Each topic to probe once, in request order.
    topics: Vec<&'a str>,
    response_bytes: usize,
    give_up_at: Instant,
    /// The request's wait, at most [`MAX_WAIT`].
    max_wait: Duration,
}

impl<'a> Fetch<'a> {
    fn new(
        bridge: &'a Arc<IggyBridge>,
        state: &'a GatewayState,
        stuck_offsets: &'a Mutex<StuckOffsets>,
        request: &'a FetchRequest,
        give_up_at: Instant,
    ) -> Self {
        let (wanted, topics) = wanted(request);
        Self {
            bridge,
            state,
            stuck_offsets,
            wanted,
            topics,
            response_bytes: response_bytes(request.max_bytes, state.max_frame_size),
            give_up_at,
            max_wait: wait_time(request.max_wait_ms),
        }
    }

    /// Each topic's probe, started at `since` or later.
    ///
    /// Loads at most [`MAX_ROUND_TOPICS`] new probes, the oldest first. Other topics keep an older
    /// probe, or answer 6 without one. So does a topic not probed before `give_up_at`.
    async fn probe_all(&self, since: Instant) -> Probes {
        let board = &self.state.probe_board;
        let mut snapshots: Vec<_> = self
            .topics
            .iter()
            .map(|&topic| board.newest(topic))
            .collect();
        for topic in stale_topics(&snapshots, since) {
            let probe = self.probe_topic(self.topics[topic], since, self.give_up_at);
            if let Some(snapshot) = probe.await {
                snapshots[topic] = Some(snapshot);
            }
        }
        snapshots
            .into_iter()
            .map(|snapshot| {
                snapshot.map_or(Err(ERROR_NOT_LEADER_OR_FOLLOWER), |snapshot| {
                    readable(&snapshot)
                })
            })
            .collect()
    }

    /// `topic`'s probe from the board, started at `since` or later. `None` if none comes by
    /// `deadline`.
    async fn probe_topic(
        &self,
        topic: &str,
        since: Instant,
        deadline: Instant,
    ) -> Option<Arc<Snapshot>> {
        let load = || {
            let (bridge, name) = (Arc::clone(self.bridge), topic.to_owned());
            async move {
                let probe = bridge.probe(&name).await;
                probe.map(Arc::new).map_err(|error| refusal(&name, &error))
            }
        };
        self.state
            .probe_board
            .probe(topic, since, deadline, load)
            .await
    }

    /// The probe of partition `index` of `topic`, started at `since` or later.
    ///
    /// Never the last good probe of a failed refresh: [`empty_poll`] proves nothing from a probe
    /// taken before the poll, and a stale one would answer an in-range offset 1.
    async fn probe_partition(
        &self,
        topic: &str,
        index: u32,
        since: Instant,
    ) -> Option<PartitionProbe> {
        let snapshot = self.probe_topic(topic, since, self.give_up_at).await?;
        snapshot.probe.as_ref().ok()?.get(index)
    }

    /// Probes until the bytes waiting reach `min_bytes`, a partition fails, or `wait_until`
    /// passes. Holds no slot while it waits.
    ///
    /// Each [`PROBE_INTERVAL`], a round probes the next [`MAX_ROUND_TOPICS`] topics. A gateway
    /// write to a topic probes it at once. A topic not probed keeps its last probe.
    ///
    /// `held` marks partitions that answered 6 or -1. They wait it out and end no wait.
    ///
    /// `probed_since` is the oldest start of a fresh probe in hand. A write that ended after it may
    /// not show in the probes, and subscribing marks it seen, so those topics probe at once. An
    /// older probe, kept when a refresh failed, is stale by the first round.
    async fn wait(
        &self,
        mut probes: Probes,
        held: &[bool],
        min_bytes: i32,
        probed_since: Instant,
        wait_until: Instant,
    ) -> Probes {
        if held.iter().all(|&held| held) {
            sleep_until(wait_until).await;
            return self.probe_all(recent(Instant::now())).await;
        }
        let min_bytes = u64::try_from(min_bytes).unwrap_or(0);
        let board = &self.state.probe_board;
        let mut writes: Vec<_> = self
            .topics
            .iter()
            .map(|&topic| board.writes(topic))
            .collect();
        let mut missed = writes_since(&writes, probed_since);
        let mut turn = 0;
        // Kept across write wakes, so steady writes to one topic do not stop the rounds.
        let mut round_at = (Instant::now() + PROBE_INTERVAL).min(wait_until);
        while Instant::now() < wait_until {
            let written = if missed.is_empty() {
                tokio::select! {
                    () = sleep_until(round_at) => None,
                    written = next_writes(&mut writes) => Some(written),
                }
            } else {
                Some(std::mem::take(&mut missed))
            };
            let timed = written.is_none();
            // A write needs a probe that started after it. A round needs one newer than the last.
            let due = written.unwrap_or_else(|| {
                turn += 1;
                let since = recent(Instant::now());
                round(turn - 1, self.topics.len())
                    .map(|topic| (topic, since))
                    .collect()
            });
            for (topic, since) in due {
                // A probe late for the round still lands on the board for the next Fetch.
                let probe = self.probe_topic(self.topics[topic], since, wait_until);
                if let Some(snapshot) = probe.await {
                    probes[topic] = readable(&snapshot);
                }
            }
            if timed {
                round_at = (Instant::now() + PROBE_INTERVAL).min(wait_until);
            }
            if ready(&self.wanted, held, &probes, self.response_bytes, min_bytes) {
                break;
            }
        }
        probes
    }

    /// One answer per requested partition, in request order, and the read slot, if taken.
    async fn read(&self, probes: &Probes) -> (Vec<PartitionData>, Option<FetchSlot>) {
        Reader::new(self).read_all(probes).await
    }
}

/// One requested partition.
struct Wanted<'a> {
    /// Its topic entry in the request.
    entry: usize,
    topic: &'a str,
    partition: i32,
    fetch_offset: i64,
    max_bytes: usize,
    /// Where its probe is, or the code it answers without asking Iggy.
    probe_at: core::result::Result<ProbeAt, i16>,
}

/// Where a partition's probe is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeAt {
    /// Its topic's place in `Fetch::topics`.
    topic: usize,
    index: u32,
}

/// Each requested partition once, in request order, and each topic to probe once. A repeat
/// keeps the first entry.
///
/// The Java consumer drops a whole response that leaves out a partition it asked for.
fn wanted(request: &FetchRequest) -> (Vec<Wanted<'_>>, Vec<&str>) {
    let total = request
        .topics
        .iter()
        .map(|topic| topic.partitions.len())
        .sum();
    let mut seen = HashSet::with_capacity(total);
    let mut wanted = Vec::with_capacity(total);
    let mut topics = IndexSet::new();
    for (entry, topic) in request.topics.iter().enumerate() {
        let name = topic.topic.as_str();
        // A name Kafka refuses answers 3 here, so it never takes a place on the probe board.
        let named = validate_kafka_topic_name("kafka_topic", name)
            .map_err(|_| ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        for partition in &topic.partitions {
            if !seen.insert((name, partition.partition)) {
                continue;
            }
            let probe_at = named
                .and_then(|()| {
                    u32::try_from(partition.partition).map_err(|_| ERROR_UNKNOWN_TOPIC_OR_PARTITION)
                })
                .map(|index| ProbeAt {
                    topic: topics.insert_full(name).0,
                    index,
                });
            wanted.push(Wanted {
                entry,
                topic: name,
                partition: partition.partition,
                fetch_offset: partition.fetch_offset,
                max_bytes: usize::try_from(partition.partition_max_bytes).unwrap_or(0),
                probe_at,
            });
        }
    }
    (wanted, topics.into_iter().collect())
}

/// Record bytes the whole response may carry: `max_bytes`, at most one frame.
fn response_bytes(max_bytes: i32, max_frame_size: usize) -> usize {
    usize::try_from(max_bytes).unwrap_or(0).min(max_frame_size)
}

/// Per topic in `Fetch::topics`, its probe, or one code for the whole topic.
type Probes = Vec<TopicProbed>;

/// The probe to read with. A refresh that failed with a code the consumer retries at once gives
/// way to the last good probe, so a blip in `get_topic` leaves the polls to decide rather than
/// answering 6 or -1 for partitions that hold records.
fn readable(snapshot: &Snapshot) -> TopicProbed {
    match (&snapshot.probe, &snapshot.last_good) {
        (Err(code), Some(good)) if retried_at_once(*code) => Ok(Arc::clone(good)),
        (probe, _) => probe.clone(),
    }
}

/// Whether a partition now fails, as an error ends Kafka's wait too, or the bytes waiting reach
/// `min_bytes`. Skips `held` partitions.
fn ready(
    wanted: &[Wanted<'_>],
    held: &[bool],
    probes: &Probes,
    response_bytes: usize,
    min_bytes: u64,
) -> bool {
    let mut waiting = 0u64;
    for (want, _) in wanted.iter().zip(held).filter(|&(_, &held)| !held) {
        match position(want, probes) {
            Position::Refused(_) => return true,
            // Past the end of this probe, or no message kept. A newer probe, or the read after
            // the wait, decides.
            Position::CaughtUp(_) | Position::Unsure { .. } => {}
            Position::Behind { offset, probe, .. } => {
                let budget = want.max_bytes.min(response_bytes);
                waiting = waiting.saturating_add(bytes_waiting(offset, probe, budget));
            }
        }
    }
    waiting >= min_bytes
}

/// Estimated bytes past `offset`, at most `budget`. Kafka's delayed fetch counts the same way.
fn bytes_waiting(offset: u64, probe: PartitionProbe, budget: usize) -> u64 {
    probe
        .high_watermark
        .saturating_sub(offset)
        .saturating_mul(probe.average_size)
        .min(u64::try_from(budget).unwrap_or(u64::MAX))
}

/// Where a requested offset stands against the probe.
#[derive(Debug, PartialEq, Eq)]
enum Position {
    /// Answer this code, with no records.
    Refused(i16),
    /// At the high watermark. Answer 0, with no records.
    CaughtUp(PartitionProbe),
    /// Records wait in partition `index` from `offset` on.
    Behind {
        index: u32,
        offset: u64,
        probe: PartitionProbe,
    },
    /// The probe cannot place `offset`. One poll decides.
    Unsure {
        index: u32,
        offset: u64,
        probe: PartitionProbe,
    },
}

fn position(want: &Wanted<'_>, probes: &Probes) -> Position {
    let at = match want.probe_at {
        Ok(at) => at,
        Err(code) => return Position::Refused(code),
    };
    // `Fetch::probe_all` probes every topic in `Fetch::topics`, in order.
    let probe = match &probes[at.topic] {
        Ok(topic) => match topic.get(at.index) {
            Some(probe) => probe,
            None => return Position::Refused(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
        },
        Err(code) => return Position::Refused(*code),
    };
    if probe.is_loading() {
        return Position::Refused(ERROR_NOT_LEADER_OR_FOLLOWER);
    }
    // Below the oldest retained offset is in range: Iggy reads from the first retained message,
    // and Kafka consumers accept the gap.
    match u64::try_from(want.fetch_offset) {
        Ok(offset) if offset == probe.high_watermark => Position::CaughtUp(probe),
        // Retention can remove every message, and a server with no stats reads the same. Past
        // the end, a shared probe can predate records the consumer already read.
        Ok(offset) if probe.messages_count == 0 || offset > probe.high_watermark => {
            Position::Unsure {
                index: at.index,
                offset,
                probe,
            }
        }
        Ok(offset) => Position::Behind {
            index: at.index,
            offset,
            probe,
        },
        Err(_) => Position::Refused(ERROR_OFFSET_OUT_OF_RANGE),
    }
}

/// Reads partitions in request order, as Kafka does from v3, and spends one byte budget on them.
struct Reader<'a> {
    fetch: &'a Fetch<'a>,
    bytes_left: usize,
    /// No partition has answered with records yet, so the next one returns at least one, whatever
    /// the limits say (KIP-74). A record larger than every limit would stall the consumer
    /// otherwise.
    owes_first_record: bool,
    /// Taken at the first poll, and freed before a probe that confirms an empty one. A poll given
    /// up on keeps it until the poll ends.
    slot: Option<FetchSlot>,
}

impl<'a> Reader<'a> {
    const fn new(fetch: &'a Fetch<'a>) -> Self {
        Self {
            fetch,
            bytes_left: fetch.response_bytes,
            owes_first_record: true,
            slot: None,
        }
    }

    /// One answer per requested partition, in request order, and the slot, if taken.
    async fn read_all(mut self, probes: &Probes) -> (Vec<PartitionData>, Option<FetchSlot>) {
        let fetch = self.fetch;
        let mut answers = Vec::with_capacity(fetch.wanted.len());
        for want in &fetch.wanted {
            let answer = match position(want, probes) {
                Position::Refused(code) => refused(want.partition, code),
                Position::CaughtUp(probe) => {
                    served(want.partition, probe.high_watermark, Bytes::new())
                }
                Position::Behind { index, offset, .. } | Position::Unsure { index, offset, .. }
                    if held(
                        fetch.stuck_offsets,
                        want.topic,
                        index,
                        offset,
                        Instant::now(),
                    ) =>
                {
                    refused(want.partition, ERROR_UNKNOWN_SERVER_ERROR)
                }
                Position::Behind {
                    index,
                    offset,
                    probe,
                } => {
                    let polled = Polled::new(offset, probe.high_watermark);
                    self.read(want, index, offset, probe, polled).await
                }
                Position::Unsure {
                    index,
                    offset,
                    probe,
                } => self.settle(want, index, offset, probe).await,
            };
            answers.push(answer);
        }
        (answers, self.slot)
    }

    /// Polls one partition that has records waiting, and answers with what its budget holds.
    async fn read(
        &mut self,
        want: &Wanted<'_>,
        index: u32,
        offset: u64,
        probe: PartitionProbe,
        mut polled: Polled,
    ) -> PartitionData {
        let budget = want.max_bytes.min(self.bytes_left);
        // Unless a record is owed, skip a poll that likely brings back nothing that fits.
        let average = usize::try_from(probe.average_size).unwrap_or(usize::MAX);
        if self.owes_first_record || budget >= average.max(1) {
            while let Some(count) = next_page(&polled, budget, probe.average_size) {
                let asked = Instant::now();
                let Some(page) = self.poll(want.topic, index, polled.next, count).await else {
                    break;
                };
                match page {
                    // The probe showed records, yet the first poll found none.
                    Ok(page) if page.messages.is_empty() && polled.messages.is_empty() => {
                        return self
                            .after_empty_poll(want, index, offset, probe, asked)
                            .await;
                    }
                    Ok(page) => polled.add(page, count),
                    Err(error) if polled.messages.is_empty() => {
                        return self.refuse(want, index, offset, &error);
                    }
                    // Serve what the earlier polls brought.
                    Err(_) => break,
                }
            }
        }

        let keep_one = self.owes_first_record;
        let encoded = off_worker(polled.bytes >= CODEC_OFF_WORKER_BYTES, || {
            encode_records(&polled.messages, offset, budget, keep_one)
        });
        match encoded {
            Ok(batch) => {
                self.bytes_left = self.bytes_left.saturating_sub(batch.len());
                self.owes_first_record &= batch.is_empty();
                served(want.partition, polled.high_watermark, batch)
            }
            Err(error) => {
                let stuck = self.fetch.stuck_offsets;
                let now = Instant::now();
                let first = hold(stuck, want.topic, index, offset, now, self.fetch.max_wait);
                log_stuck(first, want.topic, index, offset, &error);
                refused(want.partition, ERROR_UNKNOWN_SERVER_ERROR)
            }
        }
    }

    /// Reads a partition the probe cannot place, with a poll at `offset`.
    ///
    /// - Records: serve them. The probe missed them.
    /// - Nothing, and Iggy's commit ends just before `offset`: caught up.
    /// - Nothing else: a probe taken after the poll decides. See [`empty_poll`].
    async fn settle(
        &mut self,
        want: &Wanted<'_>,
        index: u32,
        offset: u64,
        probe: PartitionProbe,
    ) -> PartitionData {
        // Sized as a read's first page, since an empty answer costs the same round trip.
        let budget = want.max_bytes.min(self.bytes_left);
        let count = poll_count(budget, probe.average_size, u64::from(MAX_PAGE_COUNT));
        let asked = Instant::now();
        match self.poll(want.topic, index, offset, count).await {
            // Never below the offset, or the consumer reports negative lag.
            None => served(
                want.partition,
                probe.high_watermark.max(offset),
                Bytes::new(),
            ),
            Some(Err(error)) => self.refuse(want, index, offset, &error),
            Some(Ok(page)) if !page.messages.is_empty() => {
                let mut polled = Polled::new(offset, probe.high_watermark);
                polled.add(page, count);
                self.read(want, index, offset, probe, polled).await
            }
            Some(Ok(page)) if caught_up(offset, page.current_offset) => {
                served(want.partition, offset, Bytes::new())
            }
            Some(Ok(_)) => {
                self.after_empty_poll(want, index, offset, probe, asked)
                    .await
            }
        }
    }

    /// Answers an empty poll at `offset`, made at `asked`, from `before` and a probe taken after
    /// it. See [`empty_poll`].
    async fn after_empty_poll(
        &mut self,
        want: &Wanted<'_>,
        index: u32,
        offset: u64,
        before: PartitionProbe,
        asked: Instant,
    ) -> PartitionData {
        // The probe can wait behind other Iggy calls until the deadline, and every Fetch needs a
        // slot to poll. The next poll takes one again.
        self.slot = None;
        let after = self.fetch.probe_partition(want.topic, index, asked).await;
        match empty_poll(offset, before, after) {
            EmptyPoll::OutOfRange { high_watermark } => {
                // No offset past 0: maybe a partition that Iggy loads or fences.
                let not_ready = &self.fetch.state.not_ready;
                let loading = high_watermark <= 1
                    && spell(not_ready, want.topic, index, Instant::now()) < RELOAD_GRACE;
                let code = if loading {
                    ERROR_NOT_LEADER_OR_FOLLOWER
                } else {
                    ERROR_OFFSET_OUT_OF_RANGE
                };
                refused(want.partition, code)
            }
            EmptyPoll::NotReady => {
                let spell = spell(
                    &self.fetch.state.not_ready,
                    want.topic,
                    index,
                    Instant::now(),
                );
                log_not_ready(spell.is_zero(), want.topic, index, offset);
                refused(want.partition, ERROR_NOT_LEADER_OR_FOLLOWER)
            }
            EmptyPoll::Unproven => {
                let high_watermark =
                    after.map_or(before.high_watermark, |after| after.high_watermark);
                served(want.partition, high_watermark.max(offset), Bytes::new())
            }
        }
    }

    /// The code for `error`. A -1 also holds the partition, so reads skip Iggy until the hold ends.
    fn refuse(
        &self,
        want: &Wanted<'_>,
        index: u32,
        offset: u64,
        error: &BridgeError,
    ) -> PartitionData {
        let code = refusal(want.topic, error);
        if code == ERROR_UNKNOWN_SERVER_ERROR {
            hold(
                self.fetch.stuck_offsets,
                want.topic,
                index,
                offset,
                Instant::now(),
                self.fetch.max_wait,
            );
        }
        refused(want.partition, code)
    }

    /// Polls one page, in the read slot. `None` once `give_up_at` passes.
    async fn poll(
        &mut self,
        topic: &str,
        index: u32,
        offset: u64,
        count: u32,
    ) -> Option<core::result::Result<PolledMessages, BridgeError>> {
        let give_up_at = self.fetch.give_up_at;
        if Instant::now() >= give_up_at {
            return None;
        }
        let bridge = self.fetch.bridge;
        let (page, slot) = bridge
            .poll(
                self.take_slot().await?,
                topic,
                index,
                offset,
                count,
                give_up_at,
            )
            .await?;
        self.slot = Some(slot);
        Some(page)
    }

    /// The read slot, taken at the first poll. `None` once `give_up_at` passes.
    async fn take_slot(&mut self) -> Option<FetchSlot> {
        match self.slot.take() {
            Some(slot) => Some(slot),
            None => self.fetch.bridge.fetch_slot(self.fetch.give_up_at).await,
        }
    }
}

/// Whether an empty poll at `offset` proves the consumer is at the end: Iggy's commit counter,
/// `current_offset`, ends just before it. A 0 proves nothing, since an empty partition and a failed
/// read send 0 too.
const fn caught_up(offset: u64, current_offset: u64) -> bool {
    current_offset > 0 && current_offset.saturating_add(1) == offset
}

/// What an empty poll proves.
#[derive(Debug, PartialEq, Eq)]
enum EmptyPoll {
    /// The offset is past `high_watermark`, the end after the poll.
    OutOfRange { high_watermark: u64 },
    /// Records remain, yet the poll found none: a read fault, or a partition Iggy loads. Retry.
    NotReady,
    /// Nothing. Serve no records, and the consumer asks again.
    Unproven,
}

/// Decides an empty poll at `offset` from the probe before it and one taken after it.
///
/// Iggy also answers empty on a read fault, so only `after` proves anything. No message kept below
/// the end proves no gap: a cluster replica reads so after a crash or a failed install, while its
/// peers keep the records. So the consumer waits for the next write instead of moving past them.
const fn empty_poll(
    offset: u64,
    before: PartitionProbe,
    after: Option<PartitionProbe>,
) -> EmptyPoll {
    let Some(after) = after else {
        return EmptyPoll::Unproven;
    };
    if after.is_loading() {
        EmptyPoll::NotReady
    } else if offset > after.high_watermark {
        EmptyPoll::OutOfRange {
            high_watermark: after.high_watermark,
        }
    } else if after.messages_count > 0
        && offset < before.high_watermark
        && offset < after.high_watermark
    {
        EmptyPoll::NotReady
    } else {
        EmptyPoll::Unproven
    }
}

/// Per Kafka topic, then partition, so a lookup takes a `&str` and allocates nothing.
///
/// A new key first sweeps out the entries its caller no longer needs, once the map has doubled
/// since the last sweep. Sweeps then cost O(1) per new key.
#[derive(Debug)]
pub(crate) struct PartitionMap<T> {
    by_topic: HashMap<String, HashMap<u32, T>>,
    /// Entries across every topic.
    len: usize,
    /// The size at which the next new key sweeps first.
    sweep_at: usize,
}

/// No sweep below this many entries.
const MIN_SWEEP: usize = 1024;

impl<T> Default for PartitionMap<T> {
    fn default() -> Self {
        Self {
            by_topic: HashMap::new(),
            len: 0,
            sweep_at: MIN_SWEEP,
        }
    }
}

impl<T> PartitionMap<T> {
    fn get(&self, topic: &str, partition: u32) -> Option<&T> {
        self.by_topic.get(topic)?.get(&partition)
    }

    fn get_mut(&mut self, topic: &str, partition: u32) -> Option<&mut T> {
        self.by_topic.get_mut(topic)?.get_mut(&partition)
    }

    /// Stores `value` for `partition` of `topic`, and returns the value it replaces. A new key may
    /// first sweep out every entry that `live` rejects.
    fn insert(
        &mut self,
        topic: &str,
        partition: u32,
        value: T,
        live: impl FnMut(&T) -> bool,
    ) -> Option<T> {
        if let Some(entry) = self.get_mut(topic, partition) {
            return Some(std::mem::replace(entry, value));
        }
        if self.len >= self.sweep_at {
            self.sweep(live);
        }
        match self.by_topic.get_mut(topic) {
            Some(partitions) => {
                partitions.insert(partition, value);
            }
            None => {
                self.by_topic
                    .insert(topic.to_owned(), HashMap::from([(partition, value)]));
            }
        }
        self.len += 1;
        None
    }

    fn sweep(&mut self, mut live: impl FnMut(&T) -> bool) {
        self.by_topic.retain(|_, partitions| {
            partitions.retain(|_, value| live(value));
            !partitions.is_empty()
        });
        self.len = self.by_topic.values().map(HashMap::len).sum();
        // Twice what is left, so sweeps cost O(1) per new key.
        self.sweep_at = self.len.saturating_mul(2).max(MIN_SWEEP);
    }
}

/// Per Kafka topic and partition, a spell of answers that found it not ready.
pub(crate) type Spells = PartitionMap<Spell>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Spell {
    first: Instant,
    last: Instant,
}

/// How long `partition` of `topic` reads as not ready, as of `now`. Zero for a new spell. A spell
/// ends once the partition goes [`RELOAD_GRACE`] without reading so.
fn spell(spells: &Mutex<Spells>, topic: &str, partition: u32, now: Instant) -> Duration {
    let mut spells = spells.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(spell) = spells.get_mut(topic, partition) else {
        let fresh = Spell {
            first: now,
            last: now,
        };
        spells.insert(topic, partition, fresh, |spell| {
            now.saturating_duration_since(spell.last) < RELOAD_GRACE
        });
        return Duration::ZERO;
    };
    if now.saturating_duration_since(spell.last) >= RELOAD_GRACE {
        spell.first = now;
    }
    spell.last = now;
    let first = spell.first;
    drop(spells);
    now.saturating_duration_since(first)
}

/// Logs a partition whose records Iggy did not return: `warn!` when a spell starts, `debug!` while
/// it lasts.
fn log_not_ready(first: bool, topic: &str, partition: u32, offset: u64) {
    if first {
        tracing::warn!(
            kafka_topic = topic,
            partition,
            fetch_offset = offset,
            "Fetch found no records where Iggy reports some"
        );
    } else {
        tracing::debug!(
            kafka_topic = topic,
            partition,
            fetch_offset = offset,
            "Fetch still finds no records where Iggy reports some"
        );
    }
}

/// Per Kafka topic, then partition, where a partition last answered one connection -1.
///
/// Each connection keeps its own. A consumer reads one offset per partition, so a map holds one
/// stop per partition, and a stop spares Iggy that consumer's retries without taking another
/// consumer's read.
pub(crate) type StuckOffsets = PartitionMap<Stuck>;

/// A partition answered -1 at `offset`. Until `until`, it answers -1 there without a read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Stuck {
    offset: u64,
    until: Instant,
}

/// Holds `partition` of `topic` at `offset` for a wait of `max_wait` that starts at `now`. `true`
/// if it did not stop there before.
///
/// A sweep forgets a stop once its hold ended [`MAX_WAIT`] ago. A consumer still stuck there asks
/// again within its own wait, so it keeps the stop and logs no second `warn!`.
fn hold(
    stuck: &Mutex<StuckOffsets>,
    topic: &str,
    partition: u32,
    offset: u64,
    now: Instant,
    max_wait: Duration,
) -> bool {
    let stop = Stuck {
        offset,
        until: hold_until(now, max_wait),
    };
    let before = stuck.lock().unwrap_or_else(PoisonError::into_inner).insert(
        topic,
        partition,
        stop,
        |held| now.saturating_duration_since(held.until) < MAX_WAIT,
    );
    before.is_none_or(|before| before.offset != offset)
}

/// Whether `partition` of `topic` answered -1 at `offset` and its hold runs at `now`.
fn held(
    stuck: &Mutex<StuckOffsets>,
    topic: &str,
    partition: u32,
    offset: u64,
    now: Instant,
) -> bool {
    stuck
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(topic, partition)
        .is_some_and(|held| held.offset == offset && now < held.until)
}

/// Logs a stored message that does not map: `warn!` the first time a partition stops at an offset,
/// `debug!` while it stays there. The consumer asks for that offset again until someone acts.
fn log_stuck(first: bool, topic: &str, partition: u32, offset: u64, error: &RecordCodecError) {
    if first {
        tracing::warn!(
            %error,
            kafka_topic = topic,
            partition,
            fetch_offset = offset,
            "Fetch cannot map a stored message"
        );
    } else {
        tracing::debug!(
            %error,
            kafka_topic = topic,
            partition,
            fetch_offset = offset,
            "Fetch still cannot map a stored message"
        );
    }
}

/// What one partition read polled so far.
struct Polled {
    messages: Vec<IggyMessage>,
    /// Where the next page starts.
    next: u64,
    /// Stored bytes of `messages`, and of the largest one.
    bytes: usize,
    largest: usize,
    high_watermark: u64,
    /// A page came back short, so nothing more waits.
    done: bool,
}

impl Polled {
    const fn new(offset: u64, high_watermark: u64) -> Self {
        Self {
            messages: Vec::new(),
            next: offset,
            bytes: 0,
            largest: 0,
            high_watermark,
            done: false,
        }
    }

    fn add(&mut self, page: PolledMessages, count: u32) {
        self.done = page.messages.len() < usize::try_from(count).unwrap_or(usize::MAX);
        let Some(last) = page.messages.last() else {
            return;
        };
        self.next = last.header.offset.saturating_add(1);
        self.high_watermark = self
            .high_watermark
            .max(page.current_offset.saturating_add(1));
        for message in &page.messages {
            let size = stored_bytes(message);
            self.bytes = self.bytes.saturating_add(size);
            self.largest = self.largest.max(size);
        }
        self.messages.extend(page.messages);
    }
}

/// Messages to ask for in the next poll of one partition read, or `None` when it is done.
///
/// The first poll trusts the partition's average message size, the later ones the largest
/// message seen so far. Large messages after small ones then cost one short poll at most.
fn next_page(polled: &Polled, budget: usize, average_size: u64) -> Option<u32> {
    let waiting = polled.high_watermark.saturating_sub(polled.next);
    let taken = u32::try_from(polled.messages.len()).unwrap_or(u32::MAX);
    let room = MAX_POLL_COUNT
        .saturating_sub(taken)
        .min(u32::try_from(waiting).unwrap_or(u32::MAX));
    if polled.done || room == 0 {
        return None;
    }
    if polled.messages.is_empty() {
        return Some(poll_count(budget, average_size, waiting).min(MAX_PAGE_COUNT));
    }
    let fits = budget.saturating_sub(polled.bytes) / polled.largest.max(1);
    let count = u32::try_from(fits)
        .unwrap_or(u32::MAX)
        .min(MAX_PAGE_COUNT)
        .min(room);
    (count > 0).then_some(count)
}

/// Messages to poll for about `budget` bytes at `average_size` bytes each.
///
/// At least one, since a record past the budget may be owed. At most what waits, and
/// [`MAX_POLL_COUNT`].
fn poll_count(budget: usize, average_size: u64, waiting: u64) -> u32 {
    let fits = u64::try_from(budget).unwrap_or(u64::MAX) / average_size.max(1);
    let count = fits.min(waiting).clamp(1, u64::from(MAX_POLL_COUNT));
    u32::try_from(count).unwrap_or(MAX_POLL_COUNT)
}

/// Bytes Iggy stores for `message`, header included, as the probe's average counts them.
fn stored_bytes(message: &IggyMessage) -> usize {
    IGGY_MESSAGE_HEADER_SIZE
        + message.payload.len()
        + message.user_headers.as_ref().map_or(0, Bytes::len)
}

/// `messages` from `offset` on, as one batch of at most `budget` bytes.
///
/// `keep_one` keeps the first record even past `budget`. A message that does not map ends the
/// batch before it, and fails it when it comes first.
fn encode_records(
    messages: &[IggyMessage],
    offset: u64,
    budget: usize,
    keep_one: bool,
) -> core::result::Result<Bytes, RecordCodecError> {
    let mut records: Vec<Record> = Vec::with_capacity(messages.len());
    let mut size = BATCH_HEADER_BYTES;
    for message in messages
        .iter()
        .filter(|message| message.header.offset >= offset)
    {
        let Ok(kafka_offset) = i64::try_from(message.header.offset) else {
            break;
        };
        let record = match from_iggy(message, kafka_offset) {
            Ok(record) => record,
            Err(error) if records.is_empty() => return Err(error),
            // The next Fetch starts at this message and reports it.
            Err(_) => break,
        };
        size = size.saturating_add(record_size_bound(&record));
        if size > budget && !(keep_one && records.is_empty()) {
            break;
        }
        records.push(record);
    }
    encode_batch(&mut records)
}

/// The Fetch code for a bridge error.
///
/// The Java consumer retries 3, 6 and -1, and throws on 29 on purpose. It throws on other codes
/// too, so the rest fold into these.
const fn fetch_code(error: &BridgeError) -> i16 {
    match error.to_kafka_error_code() {
        code @ (ERROR_UNKNOWN_TOPIC_OR_PARTITION
        | ERROR_NOT_LEADER_OR_FOLLOWER
        | ERROR_TOPIC_AUTHORIZATION_FAILED) => code,
        // A read changes nothing, so an unknown outcome is a plain retry.
        ERROR_REQUEST_TIMED_OUT => ERROR_NOT_LEADER_OR_FOLLOWER,
        // No topic has a name Kafka refuses.
        ERROR_INVALID_TOPIC_EXCEPTION => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        _ => ERROR_UNKNOWN_SERVER_ERROR,
    }
}

/// [`fetch_code`], logged at a level that matches who can fix it.
fn refusal(kafka_topic: &str, error: &BridgeError) -> i16 {
    let code = fetch_code(error);
    if code == ERROR_UNKNOWN_SERVER_ERROR {
        // The client sees only -1, so this log is the one place the cause shows.
        tracing::error!(%error, kafka_topic, "Iggy refused a Fetch read");
    } else {
        // The code names the cause: the client retries a 3 or a 6, and stops on a 29.
        tracing::debug!(%error, kafka_topic, "Iggy refused a Fetch read");
    }
    code
}

/// Answers under their request topic entries. An entry left with no partition is dropped.
fn group(
    request: &FetchRequest,
    wanted: &[Wanted<'_>],
    answers: Vec<PartitionData>,
) -> Vec<FetchableTopicResponse> {
    let mut responses: Vec<FetchableTopicResponse> = Vec::new();
    let mut open = None;
    for (want, answer) in wanted.iter().zip(answers) {
        if open != Some(want.entry) {
            open = Some(want.entry);
            responses.push(
                FetchableTopicResponse::default()
                    .with_topic(request.topics[want.entry].topic.clone()),
            );
        }
        if let Some(response) = responses.last_mut() {
            response.partitions.push(answer);
        }
    }
    responses
}

/// A partition answered with `code`: no records, and no offset worth trusting.
fn refused(partition: i32, code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(code)
        .with_high_watermark(UNKNOWN_OFFSET)
        .with_last_stable_offset(UNKNOWN_OFFSET)
        .with_log_start_offset(UNKNOWN_OFFSET)
        .with_aborted_transactions(None)
        .with_records(Some(Bytes::new()))
}

/// A partition answered with `records`, which may be empty.
///
/// No transactions, so the last stable offset is the high watermark. The log start stays unknown.
fn served(partition: i32, high_watermark: u64, records: Bytes) -> PartitionData {
    let high_watermark = i64::try_from(high_watermark).unwrap_or(UNKNOWN_OFFSET);
    PartitionData::default()
        .with_partition_index(partition)
        .with_high_watermark(high_watermark)
        .with_last_stable_offset(high_watermark)
        .with_log_start_offset(UNKNOWN_OFFSET)
        .with_aborted_transactions(None)
        .with_records(Some(records))
}

fn encode_inner(
    version: i16,
    topics: Vec<FetchableTopicResponse>,
    top_level_error: i16,
) -> Result<Bytes> {
    let resp = FetchResponse::default()
        .with_error_code(top_level_error)
        .with_responses(topics);
    // Only a size hint. If the size fails, the encode fails and says why.
    let capacity = resp.compute_size(version).unwrap_or(0);
    encode_message(&resp, version, capacity)
}

fn partition_response(partition: i32, error_code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_last_stable_offset(0)
        .with_log_start_offset(0)
        .with_records(None)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use iggy::prelude::{HeaderKey, HeaderValue, IggyError};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
        RecordBatchDecoder, TimestampType,
    };

    use super::*;
    use crate::bridge::iggy_bridge::TopicProbe;
    use crate::protocol::probe_board::ProbeBoard;
    use crate::records::{MAPPING_VERSION, TimestampWindow, VERSION_HEADER, to_iggy};

    const CREATE_TIME: i64 = 1_700_000_000_123;
    /// Every partition code Fetch sends. The Java consumer throws on 29 on purpose, and on 1
    /// when it has no reset policy.
    const CONSUMER_CODES: [i16; 6] = [0, 1, 3, 6, 29, -1];

    const fn probe(high_watermark: u64, average_size: u64) -> PartitionProbe {
        PartitionProbe {
            high_watermark,
            average_size,
            messages_count: if average_size == 0 { 0 } else { high_watermark },
        }
    }

    /// A message at `offset` holding `value`, as Produce stores it.
    fn stored(offset: u64, value: &[u8]) -> IggyMessage {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp: CREATE_TIME,
            key: None,
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        };
        let window = TimestampWindow::of(std::slice::from_ref(&record));
        let mut message = to_iggy(&record, window).unwrap();
        message.header.offset = offset;
        message
    }

    /// A message that claims a mapping version this build does not read.
    fn unmappable(offset: u64) -> IggyMessage {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from(VERSION_HEADER).unwrap(),
            HeaderValue::try_from(&[MAPPING_VERSION + 1][..]).unwrap(),
        );
        let mut message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();
        message.header.offset = offset;
        message
    }

    fn offsets(mut batch: Bytes) -> Vec<i64> {
        RecordBatchDecoder::decode_all(&mut batch)
            .unwrap()
            .into_iter()
            .flat_map(|set| set.records)
            .map(|record| record.offset)
            .collect()
    }

    /// Partition `partition` of the first topic probed.
    fn want(partition: i32, fetch_offset: i64) -> Wanted<'static> {
        Wanted {
            entry: 0,
            topic: "a",
            partition,
            fetch_offset,
            max_bytes: 1024 * 1024,
            probe_at: u32::try_from(partition)
                .map(|index| ProbeAt { topic: 0, index })
                .map_err(|_| ERROR_UNKNOWN_TOPIC_OR_PARTITION),
        }
    }

    /// One topic, probed with `found`.
    fn probes(found: &[(u32, PartitionProbe)]) -> Probes {
        vec![Ok(Arc::new(found.iter().copied().collect()))]
    }

    fn request(topics: &[(&'static str, &[i32])]) -> FetchRequest {
        FetchRequest::default().with_topics(
            topics
                .iter()
                .map(|(name, partitions)| {
                    FetchTopic::default()
                        .with_topic(TopicName(StrBytes::from_static_str(name)))
                        .with_partitions(
                            partitions
                                .iter()
                                .map(|&partition| {
                                    FetchPartition::default().with_partition(partition)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
    }

    #[test]
    fn given_a_budget_when_counting_should_poll_what_fits_at_the_average_size() {
        assert_eq!(poll_count(1000, 100, 50), 10);
        assert_eq!(poll_count(1000, 100, 3), 3, "never more than waits");
        assert_eq!(
            poll_count(usize::MAX, 1, u64::MAX),
            MAX_POLL_COUNT,
            "never more than the cap"
        );
        assert_eq!(poll_count(10, 100, 50), 1, "one, in case it is owed");
        assert_eq!(poll_count(0, 100, 50), 1);
        assert_eq!(
            poll_count(500, 0, 50),
            50,
            "no retained message to measure reads as one byte each"
        );
    }

    /// A page of messages from `first` on, one per payload size.
    fn page(first: u64, sizes: &[usize], current_offset: u64) -> PolledMessages {
        let messages: Vec<IggyMessage> = sizes
            .iter()
            .zip(first..)
            .map(|(&size, offset)| {
                let mut message = IggyMessage::builder()
                    .payload(Bytes::from(vec![b'v'; size]))
                    .build()
                    .unwrap();
                message.header.offset = offset;
                message
            })
            .collect();
        PolledMessages {
            partition_id: 0,
            current_offset,
            count: u32::try_from(messages.len()).unwrap(),
            messages,
        }
    }

    #[test]
    fn given_a_new_read_when_sizing_the_first_page_should_use_the_average_up_to_the_cap() {
        let mib = 1024 * 1024;
        assert_eq!(next_page(&Polled::new(0, 10_000), mib, 100), Some(16));
        assert_eq!(next_page(&Polled::new(0, 3), mib, 100), Some(3));
        assert_eq!(
            next_page(&Polled::new(0, 100), 10, 100),
            Some(1),
            "one, in case it is owed"
        );
        assert_eq!(next_page(&Polled::new(5, 5), mib, 100), None, "none waits");
    }

    #[test]
    fn given_small_messages_when_sizing_later_pages_should_grow_to_the_cap() {
        let mut polled = Polled::new(0, 10_000);
        polled.add(page(0, &[100; 16], 15), 16);
        assert_eq!(polled.next, 16);
        assert_eq!(next_page(&polled, 1024 * 1024, 100), Some(MAX_PAGE_COUNT));
    }

    #[test]
    fn given_a_large_message_when_sizing_the_next_page_should_ask_for_what_fits() {
        let mut polled = Polled::new(0, 10_000);
        polled.add(page(0, &[100, 100, 400_000], 2), 3);
        assert_eq!(next_page(&polled, 1024 * 1024, 100), Some(1));
        assert_eq!(next_page(&polled, 500_000, 100), None, "no room left");
    }

    #[test]
    fn given_a_short_page_when_sizing_the_next_page_should_stop() {
        let mut polled = Polled::new(0, 10_000);
        polled.add(page(0, &[100; 3], 20_000), 16);
        assert_eq!(polled.high_watermark, 20_001, "the reply moves it up");
        assert_eq!(next_page(&polled, 1024 * 1024, 100), None);
    }

    #[test]
    fn given_the_count_cap_when_sizing_the_next_page_should_not_pass_it() {
        let mut polled = Polled::new(0, 10_000);
        polled.add(page(0, &[1; 990], 989), 990);
        assert_eq!(next_page(&polled, usize::MAX, 1), Some(10));
    }

    #[test]
    fn given_max_bytes_when_sizing_the_response_should_keep_it_within_one_frame() {
        let frame = 8 * 1024 * 1024;
        assert_eq!(response_bytes(52_428_800, frame), frame);
        assert_eq!(response_bytes(1000, frame), 1000);
        assert_eq!(response_bytes(-1, frame), 0);
    }

    #[test]
    fn given_an_offset_when_placed_against_the_probe_should_follow_the_error_table() {
        let found = probe(5, 10);
        let probes = probes(&[(0, found)]);
        let at = |partition, fetch_offset| position(&want(partition, fetch_offset), &probes);
        assert_eq!(at(0, -1), Position::Refused(ERROR_OFFSET_OUT_OF_RANGE));
        assert_eq!(
            at(0, 6),
            Position::Unsure {
                index: 0,
                offset: 6,
                probe: found
            },
            "a poll decides, not a probe that may be old"
        );
        assert_eq!(at(0, 5), Position::CaughtUp(found));
        assert_eq!(
            at(0, 0),
            Position::Behind {
                index: 0,
                offset: 0,
                probe: found
            }
        );
        assert_eq!(
            at(1, 0),
            Position::Refused(ERROR_UNKNOWN_TOPIC_OR_PARTITION)
        );
        assert_eq!(
            at(-1, 0),
            Position::Refused(ERROR_UNKNOWN_TOPIC_OR_PARTITION)
        );
        assert_eq!(
            position(&want(0, 0), &vec![Err(ERROR_NOT_LEADER_OR_FOLLOWER)]),
            Position::Refused(ERROR_NOT_LEADER_OR_FOLLOWER)
        );
    }

    #[test]
    fn given_a_partition_that_keeps_no_message_when_placed_should_poll_before_answering() {
        let emptied = probe(42, 0);
        let probes = probes(&[(0, emptied)]);
        let at = |fetch_offset| position(&want(0, fetch_offset), &probes);
        let unsure = |offset| Position::Unsure {
            index: 0,
            offset,
            probe: emptied,
        };
        assert_eq!(at(42), Position::CaughtUp(emptied));
        assert_eq!(at(10), unsure(10), "maybe a gap, maybe a stats miss");
        assert_eq!(at(50), unsure(50), "maybe past the end, maybe a stats miss");
        assert_eq!(at(-1), Position::Refused(ERROR_OFFSET_OUT_OF_RANGE));
        assert!(
            !ready(&[want(0, 10)], &[false], &probes, 1024, 1),
            "no message kept proves no gap, so the wait goes on"
        );
    }

    #[test]
    fn given_an_offset_past_the_probe_when_waiting_should_let_a_newer_probe_decide() {
        let probes = probes(&[(0, probe(5, 10)), (1, probe(42, 0))]);
        assert!(
            !ready(&[want(0, 6)], &[false], &probes, 1024, 1),
            "maybe caught up on an old probe, so the wait goes on"
        );
        assert!(
            !ready(&[want(1, 50)], &[false], &probes, 1024, 1),
            "no message kept, past the probe: the same"
        );
    }

    #[tokio::test]
    async fn given_a_gateway_write_when_waiting_should_wake_for_that_topic() {
        let board = ProbeBoard::default();
        let mut writes = vec![board.writes("a"), board.writes("b"), board.writes("c")];
        let quiet = tokio::time::timeout(Duration::from_millis(20), next_writes(&mut writes));
        assert!(quiet.await.is_err(), "no write, no wake");
        let before = Instant::now();
        board.wrote("c");
        board.wrote("a");
        board.wrote("not waited on");
        let after = Instant::now();
        let written = tokio::time::timeout(Duration::from_secs(1), next_writes(&mut writes))
            .await
            .expect("the write wakes the wait");
        let topics: Vec<usize> = written.iter().map(|&(topic, _)| topic).collect();
        assert_eq!(topics, vec![0, 2]);
        assert!(
            written.iter().all(|&(_, at)| before <= at && at <= after),
            "each with the time of its write, not of the wake"
        );
        let again = tokio::time::timeout(Duration::from_millis(20), next_writes(&mut writes));
        assert!(again.await.is_err(), "each write is seen once");
    }

    #[test]
    fn given_a_write_before_the_wait_subscribed_when_waiting_should_still_probe_that_topic() {
        let board = ProbeBoard::default();
        let early = board.writes("b");
        let probed_since = Instant::now();
        board.wrote("b");
        let writes = vec![board.writes("a"), board.writes("b")];
        assert!(
            early.has_changed().unwrap(),
            "a receiver from before the write wakes"
        );
        assert!(
            !writes[1].has_changed().unwrap(),
            "one from after it does not"
        );

        let missed = writes_since(&writes, probed_since);
        let topics: Vec<usize> = missed.iter().map(|&(topic, _)| topic).collect();
        assert_eq!(topics, vec![1]);
        assert!(
            writes_since(&writes, Instant::now()).is_empty(),
            "a write before the probes shows in them"
        );
    }

    #[test]
    fn given_a_failed_refresh_when_read_should_fall_back_only_on_codes_retried_at_once() {
        let good = Arc::new(TopicProbe::default());
        let failed = |code, last_good: Option<&Arc<TopicProbe>>| Snapshot {
            started: Instant::now(),
            probe: Err(code),
            last_good: last_good.cloned(),
        };
        for code in [ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNKNOWN_SERVER_ERROR] {
            let probe = readable(&failed(code, Some(&good)));
            assert!(
                probe.is_ok_and(|probe| Arc::ptr_eq(&probe, &good)),
                "{code}"
            );
        }
        for code in [
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ERROR_TOPIC_AUTHORIZATION_FAILED,
        ] {
            assert_eq!(readable(&failed(code, Some(&good))), Err(code), "{code}");
        }
        assert_eq!(
            readable(&failed(ERROR_NOT_LEADER_OR_FOLLOWER, None)),
            Err(ERROR_NOT_LEADER_OR_FOLLOWER),
            "no good probe to fall back to"
        );
    }

    #[test]
    fn given_many_stale_topics_when_probing_should_load_the_oldest_first_up_to_the_cap() {
        let now = Instant::now();
        let snapshot = |age| {
            Some(Arc::new(Snapshot {
                started: now - Duration::from_millis(age),
                probe: Ok(Arc::new(TopicProbe::default())),
                last_good: None,
            }))
        };
        let snapshots = [snapshot(500), None, snapshot(10), snapshot(900)];
        assert_eq!(
            stale_topics(&snapshots, now - Duration::from_millis(100)),
            vec![1, 3, 0],
            "none first, then the oldest; a new one waits"
        );
        let many = vec![None; MAX_ROUND_TOPICS + 5];
        assert_eq!(stale_topics(&many, now).len(), MAX_ROUND_TOPICS);
    }

    #[test]
    fn given_a_partition_mid_reload_when_placed_should_answer_6() {
        let reloading = PartitionProbe {
            high_watermark: 1,
            average_size: 10,
            messages_count: 50,
        };
        let probes = probes(&[(0, reloading)]);
        for fetch_offset in [0, 1, 500] {
            assert_eq!(
                position(&want(0, fetch_offset), &probes),
                Position::Refused(ERROR_NOT_LEADER_OR_FOLLOWER)
            );
        }
    }

    #[test]
    fn given_an_empty_poll_when_decided_should_trust_only_a_probe_taken_after_it() {
        let before = probe(42, 0);
        let empty = |high_watermark| Some(probe(high_watermark, 0));
        let past = |high_watermark| EmptyPoll::OutOfRange { high_watermark };
        assert_eq!(
            empty_poll(10, before, empty(42)),
            EmptyPoll::Unproven,
            "no message kept: retention, or a replica that lost what its peers keep"
        );
        assert_eq!(empty_poll(10, before, empty(44)), EmptyPoll::Unproven);
        assert_eq!(
            empty_poll(10, before, Some(probe(42, 100))),
            EmptyPoll::NotReady,
            "messages kept, so the poll failed"
        );
        assert_eq!(empty_poll(10, before, None), EmptyPoll::Unproven);
        assert_eq!(empty_poll(50, before, empty(42)), past(42));
        assert_eq!(
            empty_poll(43, before, empty(44)),
            EmptyPoll::Unproven,
            "a new message, not counted yet"
        );
        assert_eq!(empty_poll(44, before, empty(44)), EmptyPoll::Unproven);
    }

    #[test]
    fn given_an_empty_first_poll_when_decided_should_not_read_a_stats_miss_as_a_gap() {
        let before = probe(42, 100);
        let miss = Some(probe(0, 0));
        assert_eq!(
            empty_poll(10, before, miss),
            EmptyPoll::OutOfRange { high_watermark: 0 },
            "(0, 0) is a stats miss or a load: the grace decides"
        );
        assert_eq!(
            empty_poll(10, before, Some(probe(42, 100))),
            EmptyPoll::NotReady,
            "records sat there before the poll and still do"
        );
        assert_eq!(
            empty_poll(10, before, Some(probe(42, 0))),
            EmptyPoll::Unproven,
            "no message kept is no gap: serve nothing"
        );
        let loading = PartitionProbe {
            high_watermark: 1,
            average_size: 10,
            messages_count: 50,
        };
        assert_eq!(empty_poll(10, before, Some(loading)), EmptyPoll::NotReady);
        assert_eq!(
            empty_poll(50, probe(42, 100), Some(probe(42, 100))),
            EmptyPoll::OutOfRange { high_watermark: 42 }
        );
        assert_eq!(
            empty_poll(50, probe(42, 100), Some(probe(60, 100))),
            EmptyPoll::Unproven,
            "records came after the poll"
        );
    }

    #[test]
    fn given_a_partition_not_ready_when_it_stays_so_should_count_one_spell() {
        let spells = Mutex::default();
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        assert_eq!(spell(&spells, "a", 0, at(0)), Duration::ZERO, "a new spell");
        assert_eq!(spell(&spells, "a", 0, at(10)), Duration::from_secs(10));
        assert_eq!(
            spell(&spells, "a", 1, at(10)),
            Duration::ZERO,
            "per partition"
        );
        assert_eq!(spell(&spells, "a", 0, at(35)), Duration::from_secs(35));
        assert!(
            spell(&spells, "a", 0, at(40)) >= RELOAD_GRACE,
            "past the grace: 1"
        );
        assert_eq!(
            spell(&spells, "a", 0, at(40) + RELOAD_GRACE),
            Duration::ZERO,
            "a quiet grace ends the spell"
        );
    }

    #[test]
    fn given_bridge_errors_when_mapped_should_send_only_codes_the_consumer_knows() {
        let table = [
            (BridgeError::Timeout, ERROR_NOT_LEADER_OR_FOLLOWER),
            (
                BridgeError::Iggy(IggyError::TransientNotCommitted),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
            (
                BridgeError::Iggy(IggyError::Disconnected),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
            (
                BridgeError::Iggy(IggyError::Unauthenticated),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
            (
                BridgeError::Iggy(IggyError::Unauthorized),
                ERROR_TOPIC_AUTHORIZATION_FAILED,
            ),
            (
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    "orders".to_string(),
                    "kafka".to_string(),
                )),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                BridgeError::PartitionOutOfRange {
                    topic: "orders".to_string(),
                    partition: 5,
                    partitions_count: 1,
                },
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                BridgeError::InvalidKafkaTopicName {
                    kafka_topic: "bad name".to_string(),
                    reason: "space".to_string(),
                },
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                BridgeError::Iggy(IggyError::InvalidCredentials),
                ERROR_UNKNOWN_SERVER_ERROR,
            ),
            (
                BridgeError::PartitionCountMismatch {
                    topic: "orders".to_string(),
                    existing: 1,
                    requested: 2,
                },
                ERROR_UNKNOWN_SERVER_ERROR,
            ),
        ];
        for (error, expected) in table {
            let code = fetch_code(&error);
            assert_eq!(code, expected, "{error}");
            assert!(
                CONSUMER_CODES.contains(&code),
                "{code} makes the Java consumer throw"
            );
        }
    }

    #[test]
    fn given_session_fields_when_checked_should_accept_only_a_full_request() {
        let with = |session_id, session_epoch| {
            request(&[])
                .with_session_id(session_id)
                .with_session_epoch(session_epoch)
        };
        assert!(is_full(6, &with(7, 3)), "no sessions before v7");
        assert!(is_full(7, &with(0, INITIAL_EPOCH)));
        assert!(is_full(12, &with(0, FINAL_EPOCH)));
        assert!(
            is_full(12, &with(5, INITIAL_EPOCH)),
            "closes session 5 and asks for a new one"
        );
        assert!(!is_full(7, &with(5, 1)));
        assert!(!is_full(12, &with(5, 9)));
        assert!(!is_full(12, &with(0, -2)));
    }

    #[test]
    fn given_a_record_past_every_limit_when_owed_should_still_return_it() {
        let messages = [stored(0, &[b'a'; 1024]), stored(1, &[b'b'; 1024])];

        let owed = encode_records(&messages, 0, 10, true).unwrap();
        assert_eq!(offsets(owed), vec![0], "exactly the first record");

        let not_owed = encode_records(&messages, 0, 10, false).unwrap();
        assert!(not_owed.is_empty(), "no records rather than a cut batch");
    }

    #[test]
    fn given_a_budget_when_encoding_should_stop_before_passing_it() {
        let messages: Vec<IggyMessage> =
            (0..10).map(|offset| stored(offset, &[b'v'; 100])).collect();

        let batch = encode_records(&messages, 0, 500, false).unwrap();
        assert!(batch.len() <= 500, "{} bytes", batch.len());
        let served = offsets(batch);
        assert!(!served.is_empty() && served.len() < 10);
        assert_eq!(served, (0..).take(served.len()).collect::<Vec<i64>>());
    }

    #[test]
    fn given_messages_below_the_fetch_offset_when_encoding_should_skip_them() {
        let messages = [stored(3, b"a"), stored(4, b"b"), stored(5, b"c")];
        let batch = encode_records(&messages, 4, usize::MAX, false).unwrap();
        assert_eq!(offsets(batch), vec![4, 5]);
    }

    #[test]
    fn given_an_unmappable_message_when_it_comes_first_should_fail_the_partition() {
        let messages = [unmappable(0), stored(1, b"b")];
        assert!(matches!(
            encode_records(&messages, 0, usize::MAX, true),
            Err(RecordCodecError::MappingVersion(_))
        ));
    }

    #[test]
    fn given_an_unmappable_message_after_others_should_serve_the_ones_before_it() {
        let messages = [
            stored(0, b"a"),
            stored(1, b"b"),
            unmappable(2),
            stored(3, b"d"),
        ];
        let batch = encode_records(&messages, 0, usize::MAX, true).unwrap();
        assert_eq!(
            offsets(batch),
            vec![0, 1],
            "the next Fetch starts at the bad one"
        );
    }

    #[test]
    fn given_a_partition_named_twice_when_answered_should_answer_it_once() {
        let request = request(&[("a", &[0, 1]), ("b", &[0]), ("a", &[1, 2])]);
        let (wanted, _) = wanted(&request);
        let named: Vec<(&str, i32)> = wanted
            .iter()
            .map(|want| (want.topic, want.partition))
            .collect();
        assert_eq!(named, vec![("a", 0), ("a", 1), ("b", 0), ("a", 2)]);

        let answers = wanted
            .iter()
            .map(|want| served(want.partition, 0, Bytes::new()))
            .collect();
        let responses = group(&request, &wanted, answers);
        let grouped: Vec<(&str, Vec<i32>)> = responses
            .iter()
            .map(|topic| {
                (
                    topic.topic.as_str(),
                    topic.partitions.iter().map(|p| p.partition_index).collect(),
                )
            })
            .collect();
        assert_eq!(
            grouped,
            vec![("a", vec![0, 1]), ("b", vec![0]), ("a", vec![2])],
            "under their own request entries"
        );
    }

    #[test]
    fn given_repeated_topics_when_listed_should_probe_each_once_and_skip_negative_indexes() {
        let request = request(&[("a", &[2, -1, 0]), ("b", &[-1]), ("c", &[1]), ("a", &[1])]);
        let (wanted, topics) = wanted(&request);
        assert_eq!(topics, vec!["a", "c"], "b has no index to probe");
        let at = |topic, index| Ok(ProbeAt { topic, index });
        let negative = Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            wanted.iter().map(|want| want.probe_at).collect::<Vec<_>>(),
            vec![at(0, 2), negative, at(0, 0), negative, at(1, 1), at(0, 1)]
        );
    }

    #[test]
    fn given_a_name_kafka_refuses_when_listed_should_answer_3_without_a_probe() {
        let request = request(&[("bad name", &[0]), ("a", &[0])]);
        let (wanted, topics) = wanted(&request);
        assert_eq!(topics, vec!["a"]);
        assert_eq!(wanted[0].probe_at, Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION));
        assert_eq!(wanted[1].probe_at, Ok(ProbeAt { topic: 0, index: 0 }));
    }

    #[test]
    fn given_a_served_partition_when_built_should_report_no_transactions() {
        let answer = served(3, 42, Bytes::new());
        assert_eq!(answer.error_code, ERROR_NONE);
        assert_eq!(answer.high_watermark, 42);
        assert_eq!(answer.last_stable_offset, 42);
        assert_eq!(answer.log_start_offset, UNKNOWN_OFFSET);
        assert_eq!(answer.aborted_transactions, None);

        let refused = refused(3, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(refused.high_watermark, UNKNOWN_OFFSET);
        assert_eq!(refused.last_stable_offset, UNKNOWN_OFFSET);
    }

    #[test]
    fn given_a_minus_one_when_asked_again_should_hold_the_partition_until_the_wait_ends() {
        let stuck = Mutex::default();
        let now = Instant::now();
        let wait = Duration::from_millis(500);
        let until = now + wait;
        assert!(hold(&stuck, "a", 0, 7, now, wait), "a new stop");
        assert!(held(&stuck, "a", 0, 7, now));
        assert!(!held(&stuck, "a", 0, 7, until), "the hold ended");
        assert!(!held(&stuck, "a", 0, 8, now), "another offset");
        assert!(!held(&stuck, "a", 1, 7, now), "another partition");
        assert!(!held(&stuck, "b", 0, 7, now), "another topic");
        assert!(!hold(&stuck, "a", 0, 7, now, wait), "the same stop");
        assert!(
            hold(&stuck, "a", 0, 9, now, wait),
            "moved on, then stopped again"
        );
    }

    #[test]
    fn given_a_full_partition_map_when_a_new_key_comes_should_sweep_out_the_dead_entries() {
        let mut map = PartitionMap::default();
        for partition in 0..MIN_SWEEP - 1 {
            let partition = u32::try_from(partition).unwrap();
            assert_eq!(map.insert("dead", partition, false, |_| false), None);
        }
        assert_eq!(map.len, MIN_SWEEP - 1, "no sweep below MIN_SWEEP");
        map.insert("live", 0, true, |&live| live);

        assert_eq!(
            map.insert("live", 0, true, |&live| live),
            Some(true),
            "a replaced entry sweeps nothing"
        );
        assert_eq!(map.len, MIN_SWEEP);

        map.insert("new", 0, true, |&live| live);
        assert_eq!(map.get("dead", 0), None, "swept");
        assert!(
            !map.by_topic.contains_key("dead"),
            "an empty topic goes too"
        );
        assert_eq!(map.get("live", 0), Some(&true));
        assert_eq!(map.get("new", 0), Some(&true));
        assert_eq!(map.len, 2);
        assert_eq!(map.sweep_at, MIN_SWEEP);
    }

    #[test]
    fn given_many_stuck_partitions_when_they_sweep_should_forget_only_stops_past_the_longest_wait()
    {
        let stuck = Mutex::default();
        let start = Instant::now();
        let wait = Duration::from_millis(500);
        for partition in 0..MIN_SWEEP - 1 {
            let partition = u32::try_from(partition).unwrap();
            hold(&stuck, "gone", partition, 7, start, wait);
        }
        hold(&stuck, "retried", 0, 7, start + MAX_WAIT, wait);

        // "gone" ended its hold more than MAX_WAIT ago, "retried" one second ago.
        let now = start + MAX_WAIT + wait + Duration::from_secs(1);
        assert!(hold(&stuck, "new", 0, 7, now, wait), "a new stop sweeps");
        assert!(
            !hold(&stuck, "retried", 0, 7, now, wait),
            "a retried stop keeps its first warn"
        );
        assert!(
            hold(&stuck, "gone", 0, 7, now, wait),
            "a forgotten stop warns again"
        );
    }

    #[test]
    fn given_a_6_or_a_minus_one_in_hand_when_the_request_allows_a_wait_should_wait_it_out() {
        let request = request(&[]).with_max_wait_ms(500).with_min_bytes(1);
        for code in [ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNKNOWN_SERVER_ERROR] {
            let stuck = refused(0, code);
            assert!(waits(&request, std::slice::from_ref(&stuck)), "{code}");
            assert!(
                waits(&request, &[stuck.clone(), served(1, 5, Bytes::new())]),
                "{code}"
            );
            assert!(
                !waits(
                    &request,
                    &[stuck, served(1, 5, Bytes::from_static(b"batch"))]
                ),
                "records in hand"
            );
        }

        let wanted = [want(0, 10), want(1, 5)];
        let probes = |second| probes(&[(0, probe(20, 100)), (1, probe(second, 100))]);
        let held = [true, false];
        assert!(
            !ready(&wanted, &held, &probes(5), 1024, 1),
            "a held partition ends no wait"
        );
        assert!(ready(&wanted, &held, &probes(6), 1024, 1));
    }

    #[test]
    fn given_records_below_min_bytes_when_the_request_allows_a_wait_should_wait() {
        let request = request(&[]).with_max_wait_ms(500).with_min_bytes(100);
        let small = served(0, 5, Bytes::from_static(b"batch"));
        assert!(waits(&request, std::slice::from_ref(&small)));
        let enough = served(0, 5, Bytes::from(vec![0; 100]));
        assert!(!waits(&request, &[small, enough]), "min_bytes reached");
        assert!(
            !waits(
                &request,
                &[
                    served(0, 5, Bytes::new()),
                    refused(1, ERROR_UNKNOWN_TOPIC_OR_PARTITION)
                ]
            ),
            "an error in hand"
        );
    }

    #[test]
    fn given_nothing_found_when_the_request_allows_a_wait_should_wait() {
        let empty = [served(0, 5, Bytes::new())];
        let request = request(&[]).with_max_wait_ms(500).with_min_bytes(1);
        assert!(waits(&request, &empty));
        assert!(
            !waits(&request.clone().with_max_wait_ms(0), &empty),
            "no wait asked for"
        );
        assert!(
            !waits(&request.clone().with_min_bytes(0), &empty),
            "zero bytes are already there"
        );
        assert!(!waits(&request, &[]), "no partition to wait on");
        assert!(
            !waits(&request, &[served(0, 5, Bytes::from_static(b"batch"))]),
            "records in hand"
        );
        assert!(
            !waits(&request, &[refused(0, ERROR_UNKNOWN_TOPIC_OR_PARTITION)]),
            "an error in hand"
        );
    }

    #[test]
    fn given_new_messages_when_estimated_should_count_at_most_the_budget() {
        assert_eq!(bytes_waiting(8, probe(10, 100), 1024), 200);
        assert_eq!(
            bytes_waiting(0, probe(10, 100), 500),
            500,
            "capped at the budget"
        );
        assert_eq!(
            bytes_waiting(0, probe(10, 0), 500),
            0,
            "nothing retained to count"
        );
    }

    #[test]
    fn given_new_probes_when_waiting_should_stop_on_enough_bytes_or_a_failure() {
        let wanted = [want(0, 10)];
        let frame = 8 * 1024 * 1024;
        let at = |high_watermark| probes(&[(0, probe(high_watermark, 100))]);

        assert!(
            !ready(&wanted, &[false], &at(10), frame, 1),
            "still caught up"
        );
        assert!(
            ready(&wanted, &[false], &at(11), frame, 1),
            "one new message"
        );
        assert!(
            !ready(&wanted, &[false], &at(11), frame, 1000),
            "short of min_bytes"
        );
        assert!(
            ready(
                &wanted,
                &[false],
                &vec![Err(ERROR_NOT_LEADER_OR_FOLLOWER)],
                frame,
                1
            ),
            "a failed probe"
        );
        assert!(
            ready(&wanted, &[false], &probes(&[]), frame, 1),
            "the partition is gone"
        );
    }

    #[test]
    fn given_many_topics_when_waiting_should_probe_each_within_a_few_rounds() {
        let mut seen = HashSet::new();
        for turn in 0..3 {
            let topics: Vec<usize> = round(turn, 250).collect();
            assert_eq!(topics.len(), MAX_ROUND_TOPICS);
            seen.extend(topics);
        }
        assert_eq!(seen.len(), 250, "three rounds cover 250 topics");

        let mut few: Vec<usize> = round(7, 3).collect();
        few.sort_unstable();
        assert_eq!(few, vec![0, 1, 2], "each round covers a short list");
        assert_eq!(round(0, 0).count(), 0);
    }

    #[test]
    fn given_an_empty_poll_when_the_commit_ends_before_the_offset_should_read_as_caught_up() {
        assert!(caught_up(42, 41));
        assert!(!caught_up(42, 40), "a message may still wait");
        assert!(!caught_up(42, 42), "past the end");
        assert!(
            !caught_up(1, 0),
            "0 comes from an empty partition or a failed read too"
        );
        assert!(!caught_up(0, 0));
    }

    #[test]
    fn given_a_short_wait_when_a_minus_one_holds_should_hold_one_probe_interval_at_least() {
        let now = Instant::now();
        assert_eq!(hold_until(now, Duration::ZERO), now + PROBE_INTERVAL);
        assert_eq!(
            hold_until(now, Duration::from_millis(500)),
            now + Duration::from_millis(500)
        );
    }

    #[test]
    fn given_max_wait_when_converted_should_stay_between_zero_and_the_cap() {
        assert_eq!(wait_time(500), Duration::from_millis(500));
        assert_eq!(wait_time(-1), Duration::ZERO);
        assert_eq!(
            wait_time(i32::MAX),
            Duration::from_secs(18),
            "time left to read before the request deadline"
        );
    }
}
