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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    IGGY_MESSAGE_HEADER_SIZE, IggyMessage, MAX_PAYLOAD_SIZE, MAX_USER_HEADERS_SIZE, PolledMessages,
};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::records::Record;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::bridge::{BridgeError, IggyBridge, PartitionProbe};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_FETCH, ApiVersionRange, ERROR_FETCH_SESSION_ID_NOT_FOUND,
    ERROR_INVALID_TOPIC_EXCEPTION, ERROR_NONE, ERROR_NOT_LEADER_OR_FOLLOWER,
    ERROR_OFFSET_OUT_OF_RANGE, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_fetch_shape;
use crate::protocol::handlers::{
    decode_guarded, decode_request, encode_message, off_worker, respond_or_close,
};
use crate::protocol::probe_board::TopicProbed;
use crate::records::{
    BATCH_HEADER_BYTES, RecordCodecError, empty_batch, encode_batch, from_iggy, record_size_bound,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_FETCH,
    min_version: 4,
    max_version: 12,
};

/// Ceiling on one request, wait included, like Produce's. Fits the 25 s shutdown drain.
const REQUEST_DEADLINE: Duration = Duration::from_secs(20);
/// Longest wait. Leaves 2 s for the last probe and read, so a long wait does not end in 6.
const MAX_WAIT: Duration = REQUEST_DEADLINE.saturating_sub(Duration::from_secs(2));

/// How often a waiting request asks Iggy for new watermarks.
const PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// Most messages one partition read takes from Iggy, over all its polls.
const MAX_POLL_COUNT: u32 = 1000;
/// Most messages the first poll of a partition asks for.
const FIRST_PAGE_COUNT: u32 = 16;
/// Most messages any later poll asks for. Iggy caps a poll by count, not by bytes, so this caps
/// what one poll can load when large messages follow small ones.
const MAX_PAGE_COUNT: u32 = 64;
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

/// Kafka's "not known". The Java consumer skips an offset that holds it.
const UNKNOWN_OFFSET: i64 = -1;

/// Without a bridge, the stub. With one, records from Iggy.
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
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

    let (responses, slot) = fetch(bridge, state, &request).await;
    let record_bytes = responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.records.as_ref().map_or(0, Bytes::len))
        .sum();
    let encoded = off_worker(record_bytes, || {
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
    request: &FetchRequest,
) -> (Vec<FetchableTopicResponse>, Option<OwnedSemaphorePermit>) {
    let started = Instant::now();
    let give_up_at = started + REQUEST_DEADLINE;
    let fetch = Fetch::new(bridge, state, request, give_up_at);
    let mut probes = fetch.first_probe(started).await;
    let (mut answers, mut slot) = fetch.read(&probes).await;
    if waits(request, &answers) {
        // No slot while it waits.
        drop(slot);
        let held: Vec<bool> = answers
            .iter()
            .map(|answer| answer.error_code == ERROR_UNKNOWN_SERVER_ERROR)
            .collect();
        let wait_until = started + wait_time(request.max_wait_ms);
        probes = fetch
            .wait(probes, &held, request.min_bytes, wait_until)
            .await;
        (answers, slot) = fetch.read(&probes).await;
    }
    (group(request, &fetch.wanted, answers), slot)
}

/// Kafka answers at once with records or an error in hand, or when the request allows no wait.
///
/// A -1 waits too: the Java consumer retries it at once, so an answer at once makes it spin.
fn waits(request: &FetchRequest, answers: &[PartitionData]) -> bool {
    request.max_wait_ms > 0
        && request.min_bytes > 0
        && !answers.is_empty()
        && answers.iter().all(|answer| {
            answer.error_code == ERROR_UNKNOWN_SERVER_ERROR
                || (answer.error_code == ERROR_NONE
                    && answer.records.as_ref().is_none_or(Bytes::is_empty))
        })
}

/// The oldest start a shared probe can have and still count as new at `now`.
fn recent(now: Instant) -> Instant {
    now.checked_sub(PROBE_INTERVAL).unwrap_or(now)
}

fn wait_time(max_wait_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(max_wait_ms).unwrap_or(0)).min(MAX_WAIT)
}

/// One request: what it asks for, and what bounds it.
struct Fetch<'a> {
    bridge: &'a Arc<IggyBridge>,
    state: &'a GatewayState,
    wanted: Vec<Wanted<'a>>,
    /// Each topic to probe once, in request order.
    topics: Vec<&'a str>,
    response_bytes: usize,
    give_up_at: Instant,
}

impl<'a> Fetch<'a> {
    fn new(
        bridge: &'a Arc<IggyBridge>,
        state: &'a GatewayState,
        request: &'a FetchRequest,
        give_up_at: Instant,
    ) -> Self {
        let (wanted, topics) = wanted(request);
        Self {
            bridge,
            state,
            wanted,
            topics,
            response_bytes: response_bytes(request.max_bytes, state.max_frame_size),
            give_up_at,
        }
    }

    /// Probes for the first read. A shared probe up to [`PROBE_INTERVAL`] old will do, but never
    /// for a 1: a partition that looks past its end gets a probe that started with the request.
    async fn first_probe(&self, started: Instant) -> Probes {
        let probes = self.probe(recent(started)).await;
        let past_end = self.wanted.iter().any(|want| {
            want.fetch_offset >= 0
                && position(want, &probes) == Position::Refused(ERROR_OFFSET_OUT_OF_RANGE)
        });
        if past_end {
            return self.probe(started).await;
        }
        probes
    }

    /// Each topic's probe from the board, started at `since` or later. A topic not probed before
    /// `give_up_at` answers 6.
    async fn probe(&self, since: Instant) -> Probes {
        let mut probes = Vec::with_capacity(self.topics.len());
        for &topic in &self.topics {
            let (bridge, name) = (Arc::clone(self.bridge), topic.to_owned());
            let load = move || async move {
                let probe = bridge.probe(&name).await;
                probe.map(Arc::new).map_err(|error| refusal(&name, &error))
            };
            let snapshot = self
                .state
                .probe_board
                .probe(topic, since, self.give_up_at, load)
                .await;
            probes.push(
                snapshot.map_or(Err(ERROR_NOT_LEADER_OR_FOLLOWER), |snapshot| {
                    snapshot.probe.clone()
                }),
            );
        }
        probes
    }

    /// Probes every [`PROBE_INTERVAL`] until the bytes waiting reach `min_bytes`, a partition
    /// fails, or `wait_until` passes. Holds no slot while it waits.
    ///
    /// `held` marks partitions that answered -1. They wait it out and end no wait.
    async fn wait(
        &self,
        mut probes: Probes,
        held: &[bool],
        min_bytes: i32,
        wait_until: Instant,
    ) -> Probes {
        if held.iter().all(|&held| held) {
            sleep_until(wait_until).await;
            return self.probe(recent(Instant::now())).await;
        }
        let min_bytes = u64::try_from(min_bytes).unwrap_or(0);
        while Instant::now() < wait_until {
            sleep_until((Instant::now() + PROBE_INTERVAL).min(wait_until)).await;
            // Each round asks for a newer probe than the last. Fetches that wait share it.
            probes = self.probe(recent(Instant::now())).await;
            if ready(&self.wanted, held, &probes, self.response_bytes, min_bytes) {
                break;
            }
        }
        probes
    }

    /// One answer per requested partition, in request order, and the read slot, if taken.
    async fn read(&self, probes: &Probes) -> (Vec<PartitionData>, Option<OwnedSemaphorePermit>) {
        Reader::new(
            self.bridge,
            self.state,
            self.response_bytes,
            self.give_up_at,
        )
        .read_all(&self.wanted, probes)
        .await
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
    /// Where its probe is. `None` for a negative index, which answers 3 without asking Iggy.
    slot: Option<Slot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slot {
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
    let mut topics = Vec::new();
    let mut slots = HashMap::new();
    for (entry, topic) in request.topics.iter().enumerate() {
        let name = topic.topic.as_str();
        for partition in &topic.partitions {
            if !seen.insert((name, partition.partition)) {
                continue;
            }
            let slot = u32::try_from(partition.partition).ok().map(|index| Slot {
                topic: *slots.entry(name).or_insert_with(|| {
                    topics.push(name);
                    topics.len() - 1
                }),
                index,
            });
            wanted.push(Wanted {
                entry,
                topic: name,
                partition: partition.partition,
                fetch_offset: partition.fetch_offset,
                max_bytes: usize::try_from(partition.partition_max_bytes).unwrap_or(0),
                slot,
            });
        }
    }
    (wanted, topics)
}

/// Record bytes the whole response may carry: `max_bytes`, at most one frame.
fn response_bytes(max_bytes: i32, max_frame_size: usize) -> usize {
    usize::try_from(max_bytes).unwrap_or(0).min(max_frame_size)
}

/// Per topic in `Fetch::topics`, its probe, or one code for the whole topic.
type Probes = Vec<TopicProbed>;

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
            Position::Refused(_) | Position::Unsure { .. } => return true,
            Position::CaughtUp(_) => {}
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
    /// The partition keeps no message, so the probe cannot place `offset`. One poll decides.
    Unsure {
        index: u32,
        offset: u64,
        probe: PartitionProbe,
    },
}

fn position(want: &Wanted<'_>, probes: &Probes) -> Position {
    let Some(slot) = want.slot else {
        return Position::Refused(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };
    // `Fetch::probe` probes every topic in `Fetch::topics`, in order.
    let probe = match &probes[slot.topic] {
        Ok(topic) => match topic.get(slot.index) {
            Some(probe) => probe,
            None => return Position::Refused(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
        },
        Err(code) => return Position::Refused(*code),
    };
    // Below the oldest retained offset is in range: Iggy reads from the first retained message,
    // and Kafka consumers accept the gap.
    match u64::try_from(want.fetch_offset) {
        Ok(offset) if offset == probe.high_watermark => Position::CaughtUp(probe),
        // Retention can remove every message, and a server with no stats reads the same.
        Ok(offset) if probe.messages_count == 0 => Position::Unsure {
            index: slot.index,
            offset,
            probe,
        },
        Ok(offset) if offset < probe.high_watermark => Position::Behind {
            index: slot.index,
            offset,
            probe,
        },
        _ => Position::Refused(ERROR_OFFSET_OUT_OF_RANGE),
    }
}

/// Reads partitions in request order, as Kafka does from v3, and spends one byte budget on them.
struct Reader<'a> {
    bridge: &'a Arc<IggyBridge>,
    state: &'a GatewayState,
    give_up_at: Instant,
    bytes_left: usize,
    /// No partition has answered with records yet, so the next one returns at least one, whatever
    /// the limits say (KIP-74). A record larger than every limit would stall the consumer
    /// otherwise.
    owes_first_record: bool,
    /// Taken at the first poll. A poll given up on keeps it until the poll ends.
    slot: Option<OwnedSemaphorePermit>,
}

impl<'a> Reader<'a> {
    const fn new(
        bridge: &'a Arc<IggyBridge>,
        state: &'a GatewayState,
        response_bytes: usize,
        give_up_at: Instant,
    ) -> Self {
        Self {
            bridge,
            state,
            give_up_at,
            bytes_left: response_bytes,
            owes_first_record: true,
            slot: None,
        }
    }

    /// One answer per requested partition, in request order, and the slot, if taken.
    async fn read_all(
        mut self,
        wanted: &[Wanted<'_>],
        probes: &Probes,
    ) -> (Vec<PartitionData>, Option<OwnedSemaphorePermit>) {
        let mut answers = Vec::with_capacity(wanted.len());
        for want in wanted {
            let answer = match position(want, probes) {
                Position::Refused(code) => refused(want.partition, code),
                Position::CaughtUp(probe) => {
                    served(want.partition, probe.high_watermark, Bytes::new())
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
                let Some(page) = self.poll(want.topic, index, polled.next, count).await else {
                    break;
                };
                match page {
                    Ok(page) => polled.add(page, count),
                    Err(error) if polled.messages.is_empty() => {
                        return refused(want.partition, refusal(want.topic, &error));
                    }
                    // Serve what the earlier polls brought.
                    Err(_) => break,
                }
            }
        }

        match encode_polled(&polled.messages, offset, budget, self.owes_first_record) {
            Ok(batch) => {
                self.bytes_left = self.bytes_left.saturating_sub(batch.len());
                self.owes_first_record &= batch.is_empty();
                served(want.partition, polled.high_watermark, batch)
            }
            Err(error) => {
                log_stuck(&self.state.stuck_offsets, want.topic, index, offset, &error);
                refused(want.partition, ERROR_UNKNOWN_SERVER_ERROR)
            }
        }
    }

    /// Reads a partition the probe cannot place, with one poll at `offset`.
    ///
    /// - Records: serve them. The probe missed them.
    /// - Nothing, below the high watermark: an empty batch that ends there, so the consumer moves
    ///   past the gap.
    /// - Nothing, above: 1.
    async fn settle(
        &mut self,
        want: &Wanted<'_>,
        index: u32,
        offset: u64,
        probe: PartitionProbe,
    ) -> PartitionData {
        match self.poll(want.topic, index, offset, 1).await {
            None => served(want.partition, probe.high_watermark, Bytes::new()),
            Some(Err(error)) => refused(want.partition, refusal(want.topic, &error)),
            Some(Ok(page)) if !page.messages.is_empty() => {
                let mut polled = Polled::new(offset, probe.high_watermark);
                polled.add(page, 1);
                self.read(want, index, offset, probe, polled).await
            }
            Some(Ok(_)) if offset < probe.high_watermark => {
                let batch = empty_batch(
                    i64::try_from(offset).unwrap_or(i64::MAX),
                    i64::try_from(probe.high_watermark - 1).unwrap_or(i64::MAX),
                );
                self.bytes_left = self.bytes_left.saturating_sub(batch.len());
                served(want.partition, probe.high_watermark, batch)
            }
            Some(Ok(_)) => refused(want.partition, ERROR_OFFSET_OUT_OF_RANGE),
        }
    }

    /// Polls one page, in the read slot. `None` once `give_up_at` passes.
    async fn poll(
        &mut self,
        topic: &str,
        index: u32,
        offset: u64,
        count: u32,
    ) -> Option<core::result::Result<PolledMessages, BridgeError>> {
        if Instant::now() >= self.give_up_at {
            return None;
        }
        let bridge = Arc::clone(self.bridge);
        let (page, slot) = bridge
            .poll(
                self.take_slot().await?,
                topic,
                index,
                offset,
                count,
                self.give_up_at,
            )
            .await?;
        self.slot = Some(slot);
        Some(page)
    }

    /// The read slot, taken at the first poll. `None` once `give_up_at` passes.
    async fn take_slot(&mut self) -> Option<OwnedSemaphorePermit> {
        match self.slot.take() {
            Some(slot) => Some(slot),
            None => timeout_at(
                self.give_up_at,
                Arc::clone(&self.state.fetch_slots).acquire_owned(),
            )
            .await
            .ok()?
            .ok(),
        }
    }
}

/// Per Kafka topic and partition, the offset where a message that does not map stops Fetch.
type StuckOffsets = Mutex<HashMap<(String, u32), u64>>;

/// Logs a stored message that does not map: `warn!` the first time a partition stops at an offset,
/// `debug!` while it stays there. The consumer asks for that offset again until someone acts.
fn log_stuck(
    stuck: &StuckOffsets,
    topic: &str,
    partition: u32,
    offset: u64,
    error: &RecordCodecError,
) {
    if first_stop(stuck, topic, partition, offset) {
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

/// Whether `partition` of `topic` now stops at `offset` and did not before.
fn first_stop(stuck: &StuckOffsets, topic: &str, partition: u32, offset: u64) -> bool {
    stuck.lock().map_or(true, |mut stuck| {
        stuck.insert((topic.to_owned(), partition), offset) != Some(offset)
    })
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
        return Some(poll_count(budget, average_size, waiting).min(FIRST_PAGE_COUNT));
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

/// [`encode_records`], off the async worker for a large poll.
fn encode_polled(
    messages: &[IggyMessage],
    offset: u64,
    budget: usize,
    keep_one: bool,
) -> core::result::Result<Bytes, RecordCodecError> {
    let polled_bytes = messages.iter().map(stored_bytes).sum();
    off_worker(polled_bytes, || {
        encode_records(messages, offset, budget, keep_one)
    })
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
/// The Java consumer throws on any code but 0, 1, 3, 6, 29 and -1, so the rest fold into these.
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
        // Missing topic, lost connection: the client sees the code and retries.
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
    use crate::records::{MAPPING_VERSION, VERSION_HEADER, to_iggy};

    const CREATE_TIME: i64 = 1_700_000_000_123;
    /// Every partition code the Java consumer handles without throwing.
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
        let mut message = to_iggy(&record).unwrap();
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
            slot: u32::try_from(partition)
                .ok()
                .map(|index| Slot { topic: 0, index }),
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
        assert_eq!(at(0, 6), Position::Refused(ERROR_OFFSET_OUT_OF_RANGE));
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
            ready(&[want(0, 10)], &[false], &probes, 1024, 1),
            "a poll decides, so the wait ends"
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
        let slot = |topic, index| Some(Slot { topic, index });
        assert_eq!(
            wanted.iter().map(|want| want.slot).collect::<Vec<_>>(),
            vec![slot(0, 2), None, slot(0, 0), None, slot(1, 1), slot(0, 1)]
        );
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
    fn given_a_stuck_offset_when_logged_again_should_count_as_the_same_stop() {
        let stuck = StuckOffsets::default();
        assert!(first_stop(&stuck, "a", 0, 7));
        assert!(!first_stop(&stuck, "a", 0, 7), "still there");
        assert!(first_stop(&stuck, "a", 1, 7), "another partition");
        assert!(
            first_stop(&stuck, "a", 0, 9),
            "moved on, then stopped again"
        );
    }

    #[test]
    fn given_a_minus_one_in_hand_when_the_request_allows_a_wait_should_wait_it_out() {
        let request = request(&[]).with_max_wait_ms(500).with_min_bytes(1);
        let stuck = refused(0, ERROR_UNKNOWN_SERVER_ERROR);
        assert!(waits(&request, std::slice::from_ref(&stuck)));
        assert!(waits(
            &request,
            &[stuck.clone(), served(1, 5, Bytes::new())]
        ));
        assert!(
            !waits(
                &request,
                &[stuck, served(1, 5, Bytes::from_static(b"batch"))]
            ),
            "records in hand"
        );

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
