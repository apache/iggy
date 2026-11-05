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

//! `ListOffsets` (API key 2).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse, TopicName};
use tokio::time::Instant;

use crate::bridge::iggy_bridge::{FetchSlot, LocatedRecord, PartitionProbe, RecordLookup};
use crate::bridge::{BridgeError, IggyBridge};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_LIST_OFFSETS, ApiVersionRange, ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_REQUEST_TIMED_OUT,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    REQUEST_DEADLINE, UNKNOWN_OFFSET,
};
use crate::protocol::bounds_guard::validate_list_offsets_shape;
use crate::protocol::handlers::fetch::{Spells, sight_first, sight_loading};
use crate::protocol::handlers::{decode_guarded, decode_request, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_OFFSETS,
    min_version: 1,
    max_version: 7,
};

/// Cap on distinct topics one `ListOffsets` request resolves through the bridge in one pass.
///
/// `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode `DoS` ceiling, not a usability
/// recommendation: each distinct topic here costs one `partition_probes` call on the probe client
/// that the Fetch and `ListOffsets` probes of every Kafka connection share. The calls of one request
/// run one after another, so a large batch shares that client with other connections rather than
/// holding it for its whole duration. 100 keeps a worst-case batch's aggregate
/// bridge cost small relative to that shared resource while remaining generous for any real
/// consumer's offset lookup. A request naming more than this many distinct topics gets the first
/// 100 resolved and the rest answered [`ERROR_REQUEST_TIMED_OUT`] with no bridge call at all - a
/// client that retries only its still-erroring topics (the common case) narrows below the cap on
/// its own within a couple of retries, rather than resending the same oversized request forever.
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

/// KIP-79 sentinel: the offset of the next message that would be produced.
const LATEST_TIMESTAMP: i64 = -1;
/// KIP-79 sentinel: the offset of the first message still retained.
const EARLIEST_TIMESTAMP: i64 = -2;
/// KIP-734 sentinel, from v7. Kafka answers the record with the largest timestamp, this gateway
/// the last record appended.
const MAX_TIMESTAMP: i64 = -3;

/// Most of the request deadline one record lookup may take, so a partition that stalls leaves
/// time for the rest.
const LOOKUP_BUDGET: Duration = Duration::from_secs(REQUEST_DEADLINE.as_secs() / 4);

/// [`IggyBridge::partition_probes`]'s return type, spelled once for [`resolve_one_partition`].
type PartitionProbesResult = core::result::Result<
    Vec<(u32, core::result::Result<PartitionProbe, BridgeError>)>,
    BridgeError,
>;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let decoded = decode_request(
        API_KEY_LIST_OFFSETS,
        api_version,
        body,
        |v, b| decode(state, v, b),
        encode_error_response,
        "ListOffsets",
    );
    let req = match decoded {
        Ok(req) => req,
        Err(outcome) => return outcome,
    };
    let Some(bridge) = &state.bridge else {
        return respond_or_close(encode_response(api_version, &req), "ListOffsets");
    };

    // No `timeout_ms` below v10, so the ceiling is fixed. It bounds the sum of the topic calls.
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let resolved = resolve_all_topics(bridge, &state.loading, &req.topics, deadline).await;
    let topics = locate_records(bridge, &state.unproven_empty, resolved, deadline).await;
    let resp = ListOffsetsResponse::default().with_topics(topics);
    respond_or_close(encode_message(&resp, api_version, 256), "ListOffsets")
}

fn decode(state: &GatewayState, version: i16, body: Bytes) -> Result<ListOffsetsRequest> {
    decode_guarded::<ListOffsetsRequest>(version, body, |v, b| {
        validate_list_offsets_shape(v, b, state.max_frame_size)
    })
}

/// One topic's bridge-lookup outcome, decided once per distinct name in [`resolve_all_topics`]
/// and reused for every partition of that topic in [`resolve_one_partition`].
enum TopicLookup {
    /// A real `partition_probes` call was made and returned - or every partition requested was an
    /// invalid index and there was nothing to call `partition_probes` for, which is `Ok(vec![])`
    /// as far as [`resolve_one_partition`] is concerned (every partition in it fails its own
    /// `u32::try_from` before ever consulting this).
    Probed(PartitionProbesResult),
    /// Beyond [`MAX_BRIDGE_BACKED_TOPICS`], or the deadline elapsed before this topic's turn - no
    /// call was made. Answered [`ERROR_REQUEST_TIMED_OUT`] (retriable) rather than
    /// [`ERROR_INVALID_REQUEST`](crate::protocol::api::ERROR_INVALID_REQUEST) so a client's own
    /// per-topic retry narrows the batch on its own.
    NotAttempted,
}

/// Dedupes `requested` by topic name, merging every entry's partitions.
///
/// `order` preserves first-seen order so the topic cap in [`resolve_topic_lookups`] keeps a
/// deterministic prefix of the request rather than an arbitrary hash-order subset.
///
/// Every topic gets the `partition_probes` call, whatever its partitions ask for: it is what tells a
/// missing topic or partition apart, and what spares a lookup by time on an empty or loading
/// partition.
fn group_requested_topics(requested: &[ListOffsetsTopic]) -> (Vec<&str>, HashMap<&str, Vec<u32>>) {
    let mut order: Vec<&str> = Vec::new();
    let mut partitions_by_name: HashMap<&str, Vec<u32>> = HashMap::new();
    for topic in requested {
        let name = topic.name.as_str();
        if !partitions_by_name.contains_key(name) {
            order.push(name);
        }
        let entry = partitions_by_name.entry(name).or_default();
        let valid = topic
            .partitions
            .iter()
            .filter_map(|p| u32::try_from(p.partition_index).ok());
        entry.extend(valid);
    }
    for partitions in partitions_by_name.values_mut() {
        partitions.sort_unstable();
        partitions.dedup();
    }
    (order, partitions_by_name)
}

/// Resolves one [`TopicLookup`] per name in `order`: [`TopicLookup::NotAttempted`] beyond
/// [`MAX_BRIDGE_BACKED_TOPICS`] or once `deadline` has passed, otherwise a real `partition_probes`
/// call wrapped in [`tokio::time::timeout_at`] against `deadline` so a topic already resolved
/// when time runs out keeps its real answer. A topic with no *valid* partition index at all
/// skips the call - there is nothing `partition_probes` could tell us that would change any
/// partition's answer, since every one of them already fails its own index check.
///
/// A partition that reads as loading counts toward its `loading` spell, so the log shows one that
/// stays so. It answers 6 past the grace too: a high watermark read during a load is too low, and
/// a consumer that seeks to it reads those records again.
async fn resolve_topic_lookups<'a>(
    bridge: &IggyBridge,
    loading: &Mutex<Spells>,
    order: &[&'a str],
    partitions_by_name: &HashMap<&'a str, Vec<u32>>,
    deadline: Instant,
) -> HashMap<&'a str, TopicLookup> {
    let accepted: HashSet<&str> = order
        .iter()
        .take(MAX_BRIDGE_BACKED_TOPICS)
        .copied()
        .collect();
    if order.len() > MAX_BRIDGE_BACKED_TOPICS {
        // debug!, not warn!: the client controls how many topics it batches into one request and
        // the connection stays open, so a consumer stuck above the cap logs this every retry.
        tracing::debug!(
            distinct_topics = order.len(),
            max = MAX_BRIDGE_BACKED_TOPICS,
            "ListOffsets request exceeds the per-request topic cap; resolving the first {} and \
             answering the rest retriable",
            MAX_BRIDGE_BACKED_TOPICS
        );
    }

    let mut lookups: HashMap<&str, TopicLookup> = HashMap::new();
    let mut deadline_exceeded = false;
    for &name in order {
        if !accepted.contains(name) {
            lookups.insert(name, TopicLookup::NotAttempted);
            continue;
        }
        let partitions = partitions_by_name[name].as_slice();
        if partitions.is_empty() {
            lookups.insert(name, TopicLookup::Probed(Ok(Vec::new())));
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    deadline_secs = REQUEST_DEADLINE.as_secs(),
                    "ListOffsets request's aggregate bridge work exceeded its deadline; \
                     answering remaining topics retriable instead of starting new bridge calls"
                );
            }
            lookups.insert(name, TopicLookup::NotAttempted);
            continue;
        }

        let result = match tokio::time::timeout_at(
            deadline,
            bridge.partition_probes(name, partitions),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                deadline_exceeded = true;
                tracing::warn!(
                    topic = name,
                    deadline_secs = REQUEST_DEADLINE.as_secs(),
                    "ListOffsets bridge call for this topic exceeded the request's aggregate \
                     deadline; answering retriable instead of blocking further"
                );
                lookups.insert(name, TopicLookup::NotAttempted);
                continue;
            }
        };

        if let Err(call_err) = &result {
            let kafka_code = call_err.to_list_offsets_error_code();
            if kafka_code == ERROR_UNKNOWN_TOPIC_OR_PARTITION {
                // Client-caused (topic doesn't exist / isn't mapped): expected traffic, not
                // operator-actionable.
                tracing::debug!(topic = name, %call_err, "ListOffsets bridge lookup: topic not found");
            } else {
                tracing::error!(topic = name, %call_err, "ListOffsets bridge lookup failed");
            }
        }
        for (partition, probe) in result.iter().flatten() {
            if matches!(probe, Err(BridgeError::PartitionLoading { .. })) {
                sight_loading(loading, name, *partition, Instant::now());
            }
        }
        lookups.insert(name, TopicLookup::Probed(result));
    }
    lookups
}

/// One requested topic entry, its partitions answered or left for a record lookup.
struct ResolvedTopic {
    name: TopicName,
    partitions: Vec<PartitionAnswer>,
}

/// One requested partition after [`resolve_one_partition`].
#[derive(Debug)]
enum PartitionAnswer {
    Answered(ListOffsetsPartitionResponse),
    /// Its timestamp needs a record lookup, which [`locate_records`] runs.
    Pending(PendingLookup),
}

/// A record lookup still to run, with what the probe saw of its partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingLookup {
    /// The requested index, as the response echoes it.
    partition_index: i32,
    /// The same index, as Iggy takes it.
    partition: u32,
    lookup: RecordLookup,
    /// Messages the probe counted. 0 also when the server has no stats for the partition.
    retained: u64,
    high_watermark: u64,
}

/// Resolves every requested topic entry via [`group_requested_topics`] +
/// [`resolve_topic_lookups`], then stamps each requested partition with its topic's
/// [`TopicLookup`] outcome. `EARLIEST` and `LATEST` are answered from the watermark here. Every
/// other timestamp is left [`PartitionAnswer::Pending`] for [`locate_records`].
async fn resolve_all_topics(
    bridge: &IggyBridge,
    loading: &Mutex<Spells>,
    requested: &[ListOffsetsTopic],
    deadline: Instant,
) -> Vec<ResolvedTopic> {
    let (order, partitions_by_name) = group_requested_topics(requested);
    let lookups =
        resolve_topic_lookups(bridge, loading, &order, &partitions_by_name, deadline).await;

    requested
        .iter()
        .map(|topic| {
            // Always present: `order` (and so `lookups`) was built from exactly these same
            // requested topic names, just above.
            let lookup = lookups
                .get(topic.name.as_str())
                .expect("every requested topic name was resolved above");
            let partitions = topic
                .partitions
                .iter()
                .map(|requested| resolve_one_partition(requested, lookup))
                .collect();
            ResolvedTopic {
                name: topic.name.clone(),
                partitions,
            }
        })
        .collect()
}

/// `lookup` is the whole topic's resolution outcome: an errored [`TopicLookup::Probed`] is a
/// call-level failure (e.g. the mapped stream doesn't exist) applying to every partition alike;
/// the inner per-partition `Result` inside its `Ok` is [`BridgeError::PartitionOutOfRange`] for one
/// bad index among otherwise resolvable ones.
fn resolve_one_partition(
    requested: &ListOffsetsPartition,
    lookup: &TopicLookup,
) -> PartitionAnswer {
    let index = requested.partition_index;
    let Ok(partition_index) = u32::try_from(index) else {
        return error_answer(index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };

    let results = match lookup {
        TopicLookup::NotAttempted => {
            return error_answer(index, ERROR_REQUEST_TIMED_OUT);
        }
        TopicLookup::Probed(Err(call_err)) => {
            return error_answer(index, call_err.to_list_offsets_error_code());
        }
        TopicLookup::Probed(Ok(results)) => results,
    };

    // `results` preserves the order of the sorted, deduped partition list `resolve_all_topics`
    // passed to `partition_probes` (`IggyBridge::partition_probes` maps over its input in place),
    // so a binary search is correct here, not just faster than the linear scan this replaced.
    let Ok(found) = results.binary_search_by_key(&partition_index, |(id, _)| *id) else {
        return error_answer(index, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    };
    let (_, probe) = &results[found];

    let probe = match probe {
        Err(err) => return error_answer(index, err.to_list_offsets_error_code()),
        Ok(probe) => *probe,
    };
    let watermark = i64::try_from(probe.high_watermark).unwrap_or(i64::MAX);

    let answer = match requested.timestamp {
        LATEST_TIMESTAMP => offset_response(index, watermark),
        // Real only for a partition retention has never trimmed: Iggy tracks no rolling
        // low-watermark distinct from partition creation, so a `0` here for an older,
        // already-trimmed partition names a log-start offset that no longer exists - a real
        // consumer with `auto.offset.reset=earliest` would seek into a hole. Fetch there reads
        // from the oldest kept message, so the consumer skips the gap. Not fixable client-side;
        // needs the bridge to expose a real start offset.
        EARLIEST_TIMESTAMP => offset_response(index, 0),
        timestamp => {
            let lookup = match timestamp {
                MAX_TIMESTAMP => RecordLookup::Last,
                millis => RecordLookup::AppendedAtOrAfter(millis),
            };
            return PartitionAnswer::Pending(PendingLookup {
                partition_index: index,
                partition: partition_index,
                lookup,
                retained: probe.messages_count,
                high_watermark: probe.high_watermark,
            });
        }
    };
    PartitionAnswer::Answered(answer)
}

fn error_answer(partition: i32, error_code: i16) -> PartitionAnswer {
    PartitionAnswer::Answered(error_response(partition, error_code))
}

/// Kafka's answer when no record matches a timestamp: no error, and -1 for both fields.
fn not_found_response(partition: i32) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_timestamp(UNKNOWN_OFFSET)
        .with_offset(UNKNOWN_OFFSET)
}

/// Answers every partition [`resolve_all_topics`] left pending, one record lookup at a time
/// on one Fetch read slot, which each lookup reuses.
///
/// Looked up one by one rather than at once: a consumer resetting a group by time asks for every
/// partition in one request, and running those in parallel would take every read slot from the
/// consumers that are fetching. A lookup that cannot finish within [`LOOKUP_BUDGET`] or by
/// `deadline` answers [`ERROR_REQUEST_TIMED_OUT`], which the client retries. Repeated entries are
/// looked up once.
async fn locate_records(
    bridge: &Arc<IggyBridge>,
    unproven_empty: &Mutex<Spells>,
    topics: Vec<ResolvedTopic>,
    deadline: Instant,
) -> Vec<ListOffsetsTopicResponse> {
    let mut slot = None;
    let mut answered: HashMap<(TopicName, PendingLookup), ListOffsetsPartitionResponse> =
        HashMap::new();
    let mut responses = Vec::with_capacity(topics.len());
    for topic in topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let pending = match partition {
                PartitionAnswer::Answered(answer) => {
                    partitions.push(answer);
                    continue;
                }
                PartitionAnswer::Pending(pending) => pending,
            };
            let key = (topic.name.clone(), pending);
            let answer = if let Some(answer) = answered.get(&key) {
                answer.clone()
            } else {
                let answer = locate_one(
                    bridge,
                    unproven_empty,
                    &mut slot,
                    &topic.name,
                    pending,
                    deadline,
                )
                .await;
                answered.insert(key, answer.clone());
                answer
            };
            partitions.push(answer);
        }
        responses.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name)
                .with_partitions(partitions),
        );
    }
    drop(slot);
    responses
}

/// Answers one `pending` lookup, reusing `slot` or taking a new one into it.
async fn locate_one(
    bridge: &Arc<IggyBridge>,
    unproven_empty: &Mutex<Spells>,
    slot: &mut Option<FetchSlot>,
    topic: &str,
    pending: PendingLookup,
    request_deadline: Instant,
) -> ListOffsetsPartitionResponse {
    let PendingLookup {
        partition_index,
        partition,
        lookup,
        ..
    } = pending;
    let deadline = lookup_deadline(Instant::now(), request_deadline);
    let Some(found) = poll_record(bridge, slot, topic, partition, lookup, deadline).await else {
        return error_response(partition_index, ERROR_REQUEST_TIMED_OUT);
    };
    let last = match (&found, lookup) {
        (Ok(None), RecordLookup::AppendedAtOrAfter(_)) => {
            match poll_record(bridge, slot, topic, partition, RecordLookup::Last, deadline).await {
                Some(last) => Some(last),
                None => return error_response(partition_index, ERROR_REQUEST_TIMED_OUT),
            }
        }
        _ => None,
    };
    lookup_answer(unproven_empty, topic, pending, found, last.as_ref())
}

/// One [`IggyBridge::locate_record`] on `slot`, or on a new slot for the first lookup or after a
/// timed-out poll lost it.
/// `None` if `deadline` has passed, no slot frees, or no answer comes by `deadline`.
async fn poll_record(
    bridge: &Arc<IggyBridge>,
    slot: &mut Option<FetchSlot>,
    topic: &str,
    partition: u32,
    lookup: RecordLookup,
    deadline: Instant,
) -> Option<core::result::Result<Option<LocatedRecord>, BridgeError>> {
    // A poll started now would time out at once and drop the slot's client.
    if Instant::now() >= deadline {
        return None;
    }
    if slot.is_none() {
        // A poll that timed out frees its old slot as it stops, so for a moment the request can
        // hold two.
        *slot = bridge.fetch_slot(deadline).await;
    }
    let (located, back) = bridge
        .locate_record(slot.take()?, topic, partition, lookup, deadline)
        .await?;
    *slot = Some(back);
    Some(located)
}

fn lookup_deadline(now: Instant, request_deadline: Instant) -> Instant {
    request_deadline.min(now + LOOKUP_BUDGET)
}

/// The answer to one `pending` lookup, from what it `found` and, after an empty
/// `AppendedAtOrAfter`, the `last` record.
///
/// Iggy also polls empty on a read fault or before a partition seeds its commit point, and a node
/// that lags the probed one can serve a shorter tail. So "no record" needs the last record appended
/// before the time and reaching the probed high watermark, or, where the probe counted no message,
/// a last poll that is empty too. A last record short of the high watermark is no answer to
/// `Last` either. Anything else answers [`ERROR_NOT_LEADER_OR_FOLLOWER`] to retry.
fn lookup_answer(
    unproven_empty: &Mutex<Spells>,
    topic: &str,
    pending: PendingLookup,
    found: core::result::Result<Option<LocatedRecord>, BridgeError>,
    last: Option<&core::result::Result<Option<LocatedRecord>, BridgeError>>,
) -> ListOffsetsPartitionResponse {
    let PendingLookup {
        partition_index,
        partition,
        lookup,
        retained,
        high_watermark,
    } = pending;
    match found {
        Ok(Some(record)) if lookup == RecordLookup::Last && !reaches(&record, high_watermark) => {
            tracing::debug!(
                topic,
                partition,
                offset = record.offset,
                high_watermark,
                "ListOffsets last record falls short of the probed high watermark; answering \
                 retriable"
            );
            error_response(partition_index, ERROR_NOT_LEADER_OR_FOLLOWER)
        }
        Ok(Some(record)) => ListOffsetsPartitionResponse::default()
            .with_partition_index(partition_index)
            .with_timestamp(record.timestamp)
            .with_offset(record.offset),
        Err(error) => {
            tracing::warn!(topic, partition, %error, "ListOffsets record lookup by time failed");
            error_response(partition_index, retriable_code(&error))
        }
        Ok(None) => {
            let proven = match (lookup, last) {
                (_, Some(Err(error))) => {
                    tracing::warn!(topic, partition, %error, "ListOffsets last record lookup failed");
                    return error_response(partition_index, retriable_code(error));
                }
                (RecordLookup::Last, _) | (_, Some(Ok(None))) => retained == 0,
                (RecordLookup::AppendedAtOrAfter(millis), Some(Ok(Some(last)))) => {
                    last.appended_before(millis) && reaches(last, high_watermark)
                }
                (RecordLookup::AppendedAtOrAfter(_), None) => false,
            };
            if proven {
                return not_found_response(partition_index);
            }
            if sight_first(unproven_empty, topic, partition, Instant::now()) {
                tracing::warn!(
                    topic,
                    partition,
                    retained,
                    high_watermark,
                    ?lookup,
                    ?last,
                    "ListOffsets lookup polled empty without proof that no record matches; \
                     answering retriable"
                );
            } else {
                tracing::debug!(
                    topic,
                    partition,
                    retained,
                    high_watermark,
                    ?lookup,
                    ?last,
                    "ListOffsets lookup still polls empty without proof that no record matches"
                );
            }
            error_response(partition_index, ERROR_NOT_LEADER_OR_FOLLOWER)
        }
    }
}

/// Whether `record` is the last below `high_watermark` or beyond it.
fn reaches(record: &LocatedRecord, high_watermark: u64) -> bool {
    u64::try_from(record.offset).is_ok_and(|offset| offset.saturating_add(1) >= high_watermark)
}

/// `error`'s code, but 6 where it would be -1, which the `AdminClient` fails the partition on.
/// A rejected bridge login keeps -1, as in Fetch and Produce: retrying cannot fix it.
const fn retriable_code(error: &BridgeError) -> i16 {
    match error.to_list_offsets_error_code() {
        ERROR_UNKNOWN_SERVER_ERROR if !error.is_bridge_login_rejected() => {
            ERROR_NOT_LEADER_OR_FOLLOWER
        }
        code => code,
    }
}

fn offset_response(partition: i32, offset: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_timestamp(LATEST_TIMESTAMP)
        .with_offset(offset)
}

fn error_response(partition: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
        .with_timestamp(UNKNOWN_OFFSET)
        .with_offset(UNKNOWN_OFFSET)
}

/// Well-formed `ListOffsets` response with a single placeholder topic/partition.
///
/// `kafka_protocol` has no encodable representation for `ListOffsets` v0 (the legacy
/// `old_style_offsets` shape predates the schema this crate generates from); a v0 request now
/// falls through `super::unsupported_version_response`'s encode-failure path to `Close`
/// instead of the pre-migration downgraded response.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version` (always the
/// case for `version == 0`).
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        ListOffsetsTopicResponse::default()
            .with_partitions(vec![stub_partition_response(0, error_code)]),
    ];
    encode_inner(version, topics)
}

/// Stub: discard the payload and return a retriable error, matching Produce/Fetch - a genuine
/// offset lookup requires the same partition-leadership the stub doesn't have yet.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &ListOffsetsRequest) -> Result<Bytes> {
    let topics = req
        .topics
        .iter()
        .map(|topic| {
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|p| {
                            stub_partition_response(p.partition_index, ERROR_NOT_LEADER_OR_FOLLOWER)
                        })
                        .collect(),
                )
        })
        .collect();
    encode_inner(version, topics)
}

fn encode_inner(version: i16, topics: Vec<ListOffsetsTopicResponse>) -> Result<Bytes> {
    let resp = ListOffsetsResponse::default().with_topics(topics);
    encode_message(&resp, version, 256)
}

fn stub_partition_response(partition: i32, error_code: i16) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition)
        .with_error_code(error_code)
}

#[cfg(test)]
mod tests {
    use iggy::prelude::IggyError;

    use crate::protocol::api::ERROR_NONE;

    use super::*;

    /// The answer [`resolve_one_partition`] gives without a record lookup.
    fn resolved(
        requested: &ListOffsetsPartition,
        lookup: &TopicLookup,
    ) -> ListOffsetsPartitionResponse {
        match resolve_one_partition(requested, lookup) {
            PartitionAnswer::Answered(answer) => answer,
            PartitionAnswer::Pending(pending) => panic!("left for a lookup: {pending:?}"),
        }
    }

    fn pending(lookup: RecordLookup, retained: u64, high_watermark: u64) -> PendingLookup {
        PendingLookup {
            partition_index: 3,
            partition: 3,
            lookup,
            retained,
            high_watermark,
        }
    }

    fn partition(index: i32, timestamp: i64) -> ListOffsetsPartition {
        ListOffsetsPartition::default()
            .with_partition_index(index)
            .with_timestamp(timestamp)
    }

    /// Partitions that retain every message below their high watermark.
    fn ok_watermarks(
        entries: &[(u32, u64)],
    ) -> Vec<(u32, core::result::Result<PartitionProbe, BridgeError>)> {
        entries
            .iter()
            .map(|&(id, high_watermark)| {
                let probe = PartitionProbe {
                    high_watermark,
                    average_size: 0,
                    messages_count: high_watermark,
                };
                (id, Ok(probe))
            })
            .collect()
    }

    #[test]
    fn latest_resolves_to_the_watermark() {
        let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, 42)])));
        let resp = resolved(&partition(0, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 42);
    }

    #[test]
    fn earliest_resolves_to_zero_regardless_of_the_watermark() {
        let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, 42)])));
        let resp = resolved(&partition(0, EARLIEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_NONE);
        assert_eq!(resp.offset, 0);
    }

    #[test]
    fn a_timestamp_or_max_timestamp_is_left_for_a_record_lookup() {
        let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, 42)])));
        let expected = [
            (
                1_700_000_000_000,
                RecordLookup::AppendedAtOrAfter(1_700_000_000_000),
            ),
            (0, RecordLookup::AppendedAtOrAfter(0)),
            (MAX_TIMESTAMP, RecordLookup::Last),
            (-7, RecordLookup::AppendedAtOrAfter(-7)),
        ];
        for (requested, record_lookup) in expected {
            let answer = resolve_one_partition(&partition(0, requested), &lookup);
            let PartitionAnswer::Pending(pending) = answer else {
                panic!("timestamp {requested} answered without a lookup: {answer:?}");
            };
            let want = PendingLookup {
                partition_index: 0,
                partition: 0,
                lookup: record_lookup,
                retained: 42,
                high_watermark: 42,
            };
            assert_eq!(pending, want, "timestamp {requested}");
        }
    }

    /// Retention leaves a count of 0 below a high watermark, and so does a server without stats
    /// for the partition, so only a lookup tells whether records remain.
    #[test]
    fn a_timestamp_where_the_probe_counts_no_message_below_a_high_watermark_is_looked_up() {
        let uncounted = PartitionProbe {
            high_watermark: 42,
            average_size: 0,
            messages_count: 0,
        };
        let lookup = TopicLookup::Probed(Ok(vec![(0, Ok(uncounted))]));
        for requested in [1_700_000_000_000, MAX_TIMESTAMP] {
            let answer = resolve_one_partition(&partition(0, requested), &lookup);
            assert!(
                matches!(
                    answer,
                    PartitionAnswer::Pending(PendingLookup {
                        retained: 0,
                        high_watermark: 42,
                        ..
                    })
                ),
                "{requested}: {answer:?}"
            );
        }
    }

    #[test]
    fn an_uncounted_partition_whose_lookups_both_poll_empty_is_not_found() {
        // Every message expired: the probe and both polls agree nothing is retained.
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(time, 0, 42),
            Ok(None),
            Some(&Ok(None)),
        );
        assert_eq!(answer(&resp), (ERROR_NONE, -1, -1));
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(RecordLookup::Last, 0, 42),
            Ok(None),
            None,
        );
        assert_eq!(answer(&resp), (ERROR_NONE, -1, -1));
    }

    #[test]
    fn an_uncounted_partition_answers_the_record_a_lookup_finds() {
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let found = Ok(Some(record(17, REQUESTED_MILLIS + 5)));
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(time, 0, 42),
            found,
            None,
        );
        assert_eq!(answer(&resp), (ERROR_NONE, 17, REQUESTED_MILLIS + 5));
    }

    /// A server without stats for the partition reads a high watermark of 0 too, so only a
    /// lookup tells it from one that was never appended to.
    #[test]
    fn a_timestamp_on_a_partition_with_a_high_watermark_of_zero_is_looked_up() {
        let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, 0)])));
        for requested in [1_700_000_000_000, MAX_TIMESTAMP] {
            let answer = resolve_one_partition(&partition(0, requested), &lookup);
            assert!(
                matches!(
                    answer,
                    PartitionAnswer::Pending(PendingLookup {
                        retained: 0,
                        high_watermark: 0,
                        ..
                    })
                ),
                "timestamp {requested}: {answer:?}"
            );
        }
    }

    #[test]
    fn earliest_and_latest_never_answer_no_error_with_offset_minus_one() {
        // The async Java consumer dereferences a null for that entry and throws.
        for watermark in [0, 42] {
            let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, watermark)])));
            for requested in [EARLIEST_TIMESTAMP, LATEST_TIMESTAMP] {
                let resp = resolved(&partition(0, requested), &lookup);
                assert_eq!(resp.error_code, ERROR_NONE);
                assert!(resp.offset >= 0, "{requested} on watermark {watermark}");
            }
        }
    }

    /// The two answers the Java client turns into "no offset", which the async consumer then
    /// dereferences on an EARLIEST or LATEST reset: no error with offset -1, and 43
    /// (`OffsetFetcherUtils.handleListOffsetResponse`). A loading, unknown or timed-out partition
    /// answers a code the client retries instead.
    #[test]
    fn earliest_and_latest_never_answer_what_the_java_client_reads_as_no_offset() {
        const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 =
            kafka_protocol::ResponseError::UnsupportedForMessageFormat.code();
        let loading = BridgeError::PartitionLoading {
            topic: "orders".to_string(),
            partition: 0,
        };
        let lookups = [
            TopicLookup::Probed(Ok(ok_watermarks(&[(0, 0)]))),
            TopicLookup::Probed(Ok(ok_watermarks(&[(0, 42)]))),
            TopicLookup::Probed(Ok(vec![(0, Err(loading))])),
            TopicLookup::Probed(Ok(vec![])),
            TopicLookup::Probed(Err(BridgeError::Timeout)),
            TopicLookup::NotAttempted,
        ];
        for lookup in &lookups {
            for requested in [EARLIEST_TIMESTAMP, LATEST_TIMESTAMP] {
                let resp = resolved(&partition(0, requested), lookup);
                assert_ne!(resp.error_code, UNSUPPORTED_FOR_MESSAGE_FORMAT);
                assert!(
                    !(resp.error_code == ERROR_NONE && resp.offset == UNKNOWN_OFFSET),
                    "{requested} must not answer no error with offset -1"
                );
            }
        }
    }

    #[test]
    fn a_retriable_iggy_refusal_answers_retriable_at_every_list_offsets_error_site() {
        let refused = || BridgeError::Iggy(IggyError::HistoryUnavailable);
        for requested in [EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, REQUESTED_MILLIS] {
            let topic = TopicLookup::Probed(Err(refused()));
            let partition_level = TopicLookup::Probed(Ok(vec![(0, Err(refused()))]));
            for lookup in [&topic, &partition_level] {
                let resp = resolved(&partition(0, requested), lookup);
                assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER, "{requested}");
            }
        }
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            Err(refused()),
            None,
        );
        assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    /// The `AdminClient` fails the partition on -1 instead of retrying until its timeout.
    #[test]
    fn a_rejected_bridge_login_answers_unknown_server_error_from_a_record_lookup() {
        let rejected = || BridgeError::Iggy(IggyError::InvalidCredentials);
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let found = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            Err(rejected()),
            None,
        );
        let last = Some(Err(rejected()));
        let after_empty = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            Ok(None),
            last.as_ref(),
        );
        for resp in [found, after_empty] {
            assert_eq!(resp.error_code, ERROR_UNKNOWN_SERVER_ERROR);
        }
    }

    const REQUESTED_MILLIS: i64 = 1_700_000_000_000;

    fn record(offset: i64, appended_at_millis: i64) -> LocatedRecord {
        LocatedRecord {
            offset,
            timestamp: appended_at_millis,
            appended_at: u64::try_from(appended_at_millis).unwrap() * 1000,
        }
    }

    fn answer(response: &ListOffsetsPartitionResponse) -> (i16, i64, i64) {
        (response.error_code, response.offset, response.timestamp)
    }

    #[test]
    fn a_found_record_answers_its_offset_and_timestamp() {
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let found = Ok(Some(record(17, REQUESTED_MILLIS + 123)));
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            found,
            None,
        );
        assert_eq!(answer(&resp), (ERROR_NONE, 17, REQUESTED_MILLIS + 123));
    }

    #[test]
    fn a_failed_lookup_answers_its_error() {
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let failed = Err(BridgeError::Timeout);
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            failed,
            None,
        );
        assert_eq!(
            resp.error_code,
            BridgeError::Timeout.to_list_offsets_error_code()
        );
    }

    #[test]
    fn an_empty_poll_is_not_found_when_the_last_record_was_appended_before_the_time() {
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        for high_watermark in [41, 42] {
            let last = Some(Ok(Some(record(41, REQUESTED_MILLIS - 1))));
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(lookup, 42, high_watermark),
                Ok(None),
                last.as_ref(),
            );
            assert_eq!(answer(&resp), (ERROR_NONE, -1, -1), "{high_watermark}");
        }
    }

    /// The polled node can lag the probed one and serve a tail that ends before the time.
    #[test]
    fn an_empty_poll_whose_last_record_falls_short_of_the_high_watermark_answers_retriable() {
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let last = Some(Ok(Some(record(40, REQUESTED_MILLIS - 1))));
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(lookup, 42, 42),
            Ok(None),
            last.as_ref(),
        );
        assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn an_empty_poll_while_messages_are_retained_without_proof_answers_retriable() {
        let lookup = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let unproven = [
            // The last record matches the time, so the empty poll was a fault.
            Some(Ok(Some(record(41, REQUESTED_MILLIS)))),
            Some(Ok(Some(record(41, REQUESTED_MILLIS + 1)))),
            Some(Ok(None)),
            None,
        ];
        for last in unproven {
            let shown = format!("{last:?}");
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(lookup, 42, 42),
                Ok(None),
                last.as_ref(),
            );
            assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER, "{shown}");
        }
    }

    #[test]
    fn an_uncounted_partition_whose_last_record_matches_the_time_answers_retriable() {
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let last = Some(Ok(Some(record(41, REQUESTED_MILLIS + 1))));
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(time, 0, 42),
            Ok(None),
            last.as_ref(),
        );
        assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn a_failed_first_poll_answers_like_a_failed_last_poll() {
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        for (error, code) in [
            (BridgeError::Timeout, ERROR_REQUEST_TIMED_OUT),
            (
                BridgeError::InvalidConfig("bad".to_string()),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
        ] {
            let shown = format!("{error:?}");
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(time, 42, 42),
                Err(error),
                None,
            );
            assert_eq!(resp.error_code, code, "{shown}");
        }
    }

    #[test]
    fn a_failed_last_poll_answers_its_error_but_never_unknown_server_error() {
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        let failures = [
            (BridgeError::Timeout, ERROR_REQUEST_TIMED_OUT),
            (
                BridgeError::InvalidConfig("bad".to_string()),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
        ];
        for (error, code) in failures {
            let shown = format!("{error:?}");
            let last = Some(Err(error));
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(time, 42, 42),
                Ok(None),
                last.as_ref(),
            );
            assert_eq!(resp.error_code, code, "{shown}");
        }
    }

    /// The polled node can lag the probed one and serve a last record below its high watermark.
    #[test]
    fn a_last_record_short_of_the_high_watermark_answers_retriable() {
        for retained in [0, 42] {
            let short = Ok(Some(record(40, REQUESTED_MILLIS)));
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(RecordLookup::Last, retained, 42),
                short,
                None,
            );
            assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER, "{retained}");
            let reaching = Ok(Some(record(41, REQUESTED_MILLIS)));
            let resp = lookup_answer(
                &Mutex::default(),
                "orders",
                pending(RecordLookup::Last, retained, 42),
                reaching,
                None,
            );
            assert_eq!(
                answer(&resp),
                (ERROR_NONE, 41, REQUESTED_MILLIS),
                "{retained}"
            );
        }
    }

    #[test]
    fn an_empty_last_poll_while_messages_are_retained_answers_retriable() {
        let resp = lookup_answer(
            &Mutex::default(),
            "orders",
            pending(RecordLookup::Last, 42, 42),
            Ok(None),
            None,
        );
        assert_eq!(resp.error_code, ERROR_NOT_LEADER_OR_FOLLOWER);
    }

    #[test]
    fn each_lookup_gets_its_own_budget_within_the_request_deadline() {
        let now = Instant::now();
        assert_eq!(
            lookup_deadline(now, now + REQUEST_DEADLINE),
            now + LOOKUP_BUDGET
        );
        assert!(LOOKUP_BUDGET < REQUEST_DEADLINE);
        let near = now + LOOKUP_BUDGET / 2;
        assert_eq!(lookup_deadline(now, near), near);
    }

    #[tokio::test]
    async fn a_lookup_past_its_deadline_answers_retriable_without_using_a_read_slot() {
        let bridge = Arc::new(IggyBridge::offline());
        let client = Arc::new(
            iggy::prelude::IggyClientBuilder::new()
                .with_tcp()
                .with_server_address("127.0.0.1:1".to_string())
                .build()
                .expect("building a client dials nothing"),
        );
        bridge.hold_fetch_clients(&client).await;
        let held = bridge.held_fetch_clients().await;
        let later = Instant::now() + Duration::from_secs(5);
        let mut slot = bridge.fetch_slot(later).await;
        assert!(slot.is_some());
        let time = RecordLookup::AppendedAtOrAfter(REQUESTED_MILLIS);
        for lookup in [time, RecordLookup::Last] {
            let passed = Instant::now();
            let resp = locate_one(
                &bridge,
                &Mutex::default(),
                &mut slot,
                "orders",
                pending(lookup, 42, 42),
                passed,
            )
            .await;
            assert_eq!(resp.error_code, ERROR_REQUEST_TIMED_OUT, "{lookup:?}");
            assert!(slot.is_some(), "{lookup:?} kept its slot");
        }
        drop(slot);

        let passed = Instant::now();
        let topics = vec![ResolvedTopic {
            name: TopicName(kafka_protocol::protocol::StrBytes::from_static_str(
                "orders",
            )),
            partitions: vec![PartitionAnswer::Pending(pending(time, 42, 42))],
        }];
        let responses = locate_records(&bridge, &Mutex::default(), topics, passed).await;
        assert_eq!(
            responses[0].partitions[0].error_code,
            ERROR_REQUEST_TIMED_OUT
        );
        assert_eq!(
            bridge.held_fetch_clients().await,
            held,
            "every slot kept its client"
        );
    }

    #[test]
    fn a_nonexistent_topic_is_unknown_regardless_of_the_requested_timestamp() {
        // Existence is only established by the partition_probes call, so its error wins over any
        // record lookup the timestamp would otherwise ask for.
        let lookup = TopicLookup::Probed(Err(BridgeError::Timeout));
        let resp = resolved(&partition(0, 1_700_000_000_000), &lookup);
        assert_eq!(
            resp.error_code,
            BridgeError::Timeout.to_list_offsets_error_code()
        );
    }

    #[test]
    fn a_topic_beyond_the_cap_or_past_the_deadline_answers_retriable() {
        let resp = resolved(&partition(0, LATEST_TIMESTAMP), &TopicLookup::NotAttempted);
        assert_eq!(resp.error_code, ERROR_REQUEST_TIMED_OUT);
        assert_eq!(resp.offset, UNKNOWN_OFFSET);
    }

    #[test]
    fn a_negative_partition_index_is_rejected_without_consulting_the_bridge_result() {
        let lookup = TopicLookup::Probed(Ok(vec![]));
        let resp = resolved(&partition(-1, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_partition_specific_error_only_affects_that_partition() {
        let lookup = TopicLookup::Probed(Ok(vec![(
            0,
            Err(BridgeError::PartitionOutOfRange {
                topic: "orders".to_string(),
                partition: 0,
                partitions_count: 0,
            }),
        )]));
        let resp = resolved(&partition(0, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_call_level_error_applies_regardless_of_the_requested_timestamp() {
        let lookup = TopicLookup::Probed(Err(BridgeError::Timeout));
        let resp = resolved(&partition(0, EARLIEST_TIMESTAMP), &lookup);
        assert_eq!(
            resp.error_code,
            BridgeError::Timeout.to_list_offsets_error_code()
        );
    }

    #[test]
    fn a_partition_index_absent_from_a_sorted_result_is_rejected() {
        // Exercises the binary search on a multi-entry, sorted-by-index result - a single-entry
        // vec would pass a linear scan and a broken binary search alike.
        let lookup = TopicLookup::Probed(Ok(ok_watermarks(&[(0, 10), (2, 30), (5, 60)])));
        let resp = resolved(&partition(3, LATEST_TIMESTAMP), &lookup);
        assert_eq!(resp.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}
