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

//! Shadow streams: the geo topics of a peer cluster, served by this one.
//!
//! A peer publishes an object for each of its geo topics in its store. The
//! metadata primary of this cluster reads those objects and proposes a
//! [`ShadowTopicRequest`] for each change it finds, so every replica holds the
//! same shadow streams. A shadow stream is named
//! `__shadow_{origin}_{stream}`, and it holds the geo topics of one stream of
//! the peer with the peer's topic and partition ids. Its partitions read the
//! peer's log from the peer's store.
//!
//! A client reads a shadow stream like any other, and keeps consumer groups,
//! offsets and permissions on it. It cannot write to it or change it: the
//! applies of the client commands refuse that with
//! `IggyError::ShadowStreamReadOnly`. A client cannot give a stream a name
//! that starts with [`SHADOW_STREAM_PREFIX`] either.
//!
//! A shadow topic is `following` while its origin holds the topic, and
//! `detached` once the origin deleted it. The primary removes a detached
//! topic after a grace period, and a shadow stream once it holds no topic.
//! Whether a peer's store answers is the primary's own view, which it does
//! not replicate.
//!
//! The same operation takes back the geo topics of a cluster that lost every
//! disk, from its store, into the cluster rebuilt in its place: a restored
//! stream and topic are this cluster's own, with the ids and the revision
//! that the lost cluster gave them, so their objects keep their keys.
//!
//! An operator retires a peer that never returns with a
//! [`RetireGeoOriginRequest`]. This cluster then follows nothing of the
//! origin again, its shadow topics expire by their own retention, and a
//! client may delete its shadow streams and topics. The retirement names the
//! origin, so a datacenter that comes back needs another origin name.

use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::{
    WireDecode, WireEncode, read_bytes, read_u8, read_u32_le, read_u64_le, read_u128_le,
};
use iggy_binary_protocol::requests::streams::DeleteStreamRequest;
use iggy_binary_protocol::requests::system::RetireGeoOriginRequest;
use iggy_binary_protocol::requests::topics::{
    CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
};
use iggy_binary_protocol::{WireError, WireIdentifier, WireName};
use iggy_common::{GeoMode, IggyTimestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::change_log::{AppliedChange, change_actor, stream_body};
use crate::stm::StateHandler;
use crate::stm::result::{ApplyReply, ShadowTopicResult};
use crate::stm::stream::{FixedTopic, Stream, StreamsInner, admits_slab_key, create_topic};
use iggy_binary_protocol::primitives::metadata_changes::WireMetadataChange;
use server_common::sharding::MAX_STREAMS;

/// The start of every shadow stream's name. Clients cannot create or rename
/// a stream to a name that starts with it.
pub const SHADOW_STREAM_PREFIX: &str = "__shadow_";

/// The name of the shadow stream of the stream `stream_name` of the peer
/// `origin`. `None` when it would be longer than a stream name may be.
#[must_use]
pub fn shadow_stream_name(origin: &str, stream_name: &str) -> Option<String> {
    let name = format!("{SHADOW_STREAM_PREFIX}{origin}_{stream_name}");
    (name.len() <= iggy_binary_protocol::MAX_WIRE_NAME_LENGTH).then_some(name)
}

/// Whether `name` is kept for shadow streams.
#[must_use]
pub fn is_shadow_stream_name(name: &str) -> bool {
    name.starts_with(SHADOW_STREAM_PREFIX)
}

/// The stream of a peer that a shadow stream holds the geo topics of.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShadowStream {
    /// The peer's `geo.origin`.
    pub origin: String,
    /// The cluster lifetime that owned the peer's store when this cluster
    /// adopted the stream, as the store's identity object names it.
    pub cluster_uid: u128,
    /// The stream's id in the peer.
    pub stream_id: u64,
}

/// The incarnation of a peer's geo topic that a shadow topic serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowTopic {
    /// The revision that the peer created the topic's partitions at, as the
    /// key of the topic's object names it.
    pub created_revision: u64,
    /// When the peer wrote the topic's tombstone, in microseconds since the
    /// Unix epoch. `None` while the peer holds the topic.
    pub deleted_at: Option<u64>,
    /// The retention of the topic in the peer, as its object said at
    /// adoption. A geo topic keeps its options, so it does not change. It
    /// applies once the peer's origin retired.
    #[serde(default)]
    pub retention: ShadowRetention,
    /// How the peer writes the topic, as its object said at adoption. A
    /// read of the shadow topic reports it, and the topic as a geo topic,
    /// although it is stored as a normal one.
    #[serde(default)]
    pub geo_mode: GeoMode,
}

/// The retention of a peer's geo topic, as the topic's object writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowRetention {
    /// How long a message lives, in microseconds, `u64::MAX` when messages
    /// never expire.
    pub message_expiry: u64,
    /// The most bytes that the topic holds, `u64::MAX` without a limit.
    pub max_topic_size: u64,
}

impl Default for ShadowRetention {
    fn default() -> Self {
        Self {
            message_expiry: u64::MAX,
            max_topic_size: u64::MAX,
        }
    }
}

impl ShadowRetention {
    /// How long a message lives, in microseconds, `None` when messages never
    /// expire.
    #[must_use]
    pub const fn expiry_micros(&self) -> Option<u64> {
        match self.message_expiry {
            0 | u64::MAX => None,
            micros => Some(micros),
        }
    }

    /// The most bytes that the topic holds, `None` without a limit.
    #[must_use]
    pub const fn max_bytes(&self) -> Option<u64> {
        match self.max_topic_size {
            0 | u64::MAX => None,
            bytes => Some(bytes),
        }
    }
}

impl ShadowTopic {
    /// Whether the peer deleted the topic.
    #[must_use]
    pub const fn is_detached(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// One shadow topic, by its shadow stream and the peer's incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowTopicTarget {
    /// The shadow stream in this cluster.
    pub stream_id: u32,
    pub source: ShadowStream,
    /// The same in the peer and in this cluster.
    pub topic_id: u32,
    pub created_revision: u64,
}

/// What one `ShadowTopic` operation changes. Each one checks that its target
/// still is what the primary saw, so a stale proposal commits as a no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShadowTopicRequest {
    /// Create the empty shadow stream `name` for `source`.
    CreateStream {
        source: ShadowStream,
        name: WireName,
    },
    /// Rename the shadow stream after its peer stream.
    RenameStream {
        stream_id: u32,
        source: ShadowStream,
        name: WireName,
    },
    /// Remove a shadow stream that holds no topic.
    RemoveStream {
        stream_id: u32,
        source: ShadowStream,
    },
    /// Add a peer's topic to its shadow stream. `create` names the topic and
    /// carries its options and partitions, as a create of a topic does.
    AdoptTopic {
        target: ShadowTopicTarget,
        create: Box<CreateTopicWithAssignmentsRequest>,
        retention: ShadowRetention,
        geo_mode: GeoMode,
    },
    /// The peer deleted the topic at `deleted_at`.
    DetachTopic {
        target: ShadowTopicTarget,
        deleted_at: u64,
    },
    /// Remove the shadow topic and its partitions.
    RemoveTopic { target: ShadowTopicTarget },
    /// Create the stream `name` of this cluster at `source.stream_id`, the
    /// id the retired cluster `source.cluster_uid` gave it.
    RestoreStream {
        source: ShadowStream,
        name: WireName,
    },
    /// Create a geo topic of this cluster in the restored stream, at the
    /// topic id and with the partitions' revision of the retired cluster.
    /// `create` names the topic and carries its options and partitions.
    RestoreTopic {
        target: ShadowTopicTarget,
        create: Box<CreateTopicWithAssignmentsRequest>,
    },
}

const CREATE_STREAM: u8 = 1;
const RENAME_STREAM: u8 = 2;
const REMOVE_STREAM: u8 = 3;
const ADOPT_TOPIC: u8 = 4;
const DETACH_TOPIC: u8 = 5;
const REMOVE_TOPIC: u8 = 6;
const RESTORE_STREAM: u8 = 7;
const RESTORE_TOPIC: u8 = 8;

impl ShadowTopicRequest {
    const fn kind(&self) -> u8 {
        match self {
            Self::CreateStream { .. } => CREATE_STREAM,
            Self::RenameStream { .. } => RENAME_STREAM,
            Self::RemoveStream { .. } => REMOVE_STREAM,
            Self::AdoptTopic { .. } => ADOPT_TOPIC,
            Self::DetachTopic { .. } => DETACH_TOPIC,
            Self::RemoveTopic { .. } => REMOVE_TOPIC,
            Self::RestoreStream { .. } => RESTORE_STREAM,
            Self::RestoreTopic { .. } => RESTORE_TOPIC,
        }
    }

    const fn source(&self) -> &ShadowStream {
        match self {
            Self::CreateStream { source, .. }
            | Self::RenameStream { source, .. }
            | Self::RemoveStream { source, .. }
            | Self::RestoreStream { source, .. } => source,
            Self::AdoptTopic { target, .. }
            | Self::DetachTopic { target, .. }
            | Self::RemoveTopic { target }
            | Self::RestoreTopic { target, .. } => &target.source,
        }
    }
}

// The body, all integers little-endian, a name as its length in one byte
// and its bytes:
//
//   kind u8 | origin name | cluster_uid u128 | peer stream_id u64
//   create stream: name
//   rename stream: stream_id u32 | name
//   remove stream: stream_id u32
//   adopt topic:   stream_id u32 | topic_id u32 | created_revision u64
//                  | create length u32 | create | retention | geo_mode u8
//   detach topic:  stream_id u32 | topic_id u32 | created_revision u64
//                  | deleted_at u64
//   remove topic:  stream_id u32 | topic_id u32 | created_revision u64
//   restore stream: name
//   restore topic: stream_id u32 | topic_id u32 | created_revision u64
//                  | create length u32 | create
//
// A retention is message_expiry u64 | max_topic_size u64.
//
// A restore carries this cluster's origin, the retired cluster's
// cluster_uid and the stream's id there.

const TARGET_LEN: usize = 4 + 4 + 8;
const RETENTION_LEN: usize = 8 + 8;

impl WireEncode for ShadowTopicRequest {
    fn encoded_size(&self) -> usize {
        let source = self.source();
        let head = 1 + 1 + source.origin.len() + 16 + 8;
        head + match self {
            Self::CreateStream { name, .. } | Self::RestoreStream { name, .. } => {
                name.encoded_size()
            }
            Self::RenameStream { name, .. } => 4 + name.encoded_size(),
            Self::RemoveStream { .. } => 4,
            Self::AdoptTopic { create, .. } => {
                TARGET_LEN + 4 + create.encoded_size() + RETENTION_LEN + 1
            }
            Self::RestoreTopic { create, .. } => TARGET_LEN + 4 + create.encoded_size(),
            Self::DetachTopic { .. } => TARGET_LEN + 8,
            Self::RemoveTopic { .. } => TARGET_LEN,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        let source = self.source();
        buf.put_u8(self.kind());
        let origin = u8::try_from(source.origin.len()).expect("an origin fits a name");
        buf.put_u8(origin);
        buf.put_slice(source.origin.as_bytes());
        buf.put_u128_le(source.cluster_uid);
        buf.put_u64_le(source.stream_id);
        match self {
            Self::CreateStream { name, .. } | Self::RestoreStream { name, .. } => name.encode(buf),
            Self::RenameStream {
                stream_id, name, ..
            } => {
                buf.put_u32_le(*stream_id);
                name.encode(buf);
            }
            Self::RemoveStream { stream_id, .. } => buf.put_u32_le(*stream_id),
            Self::AdoptTopic {
                target,
                create,
                retention,
                geo_mode,
            } => {
                encode_create(target, create, buf);
                encode_retention(retention, buf);
                buf.put_u8(geo_mode_code(*geo_mode));
            }
            Self::RestoreTopic { target, create } => encode_create(target, create, buf),
            Self::DetachTopic { target, deleted_at } => {
                encode_target(target, buf);
                buf.put_u64_le(*deleted_at);
            }
            Self::RemoveTopic { target } => encode_target(target, buf),
        }
    }
}

fn encode_target(target: &ShadowTopicTarget, buf: &mut BytesMut) {
    buf.put_u32_le(target.stream_id);
    buf.put_u32_le(target.topic_id);
    buf.put_u64_le(target.created_revision);
}

fn encode_create(
    target: &ShadowTopicTarget,
    create: &CreateTopicWithAssignmentsRequest,
    buf: &mut BytesMut,
) {
    encode_target(target, buf);
    let length = u32::try_from(create.encoded_size()).expect("a create fits u32");
    buf.put_u32_le(length);
    create.encode(buf);
}

fn encode_retention(retention: &ShadowRetention, buf: &mut BytesMut) {
    buf.put_u64_le(retention.message_expiry);
    buf.put_u64_le(retention.max_topic_size);
}

const fn geo_mode_code(geo_mode: GeoMode) -> u8 {
    match geo_mode {
        GeoMode::Geo => 0,
        GeoMode::GeoTiered => 1,
    }
}

fn decode_geo_mode(buf: &[u8], position: usize) -> Result<GeoMode, WireError> {
    match read_u8(buf, position)? {
        0 => Ok(GeoMode::Geo),
        1 => Ok(GeoMode::GeoTiered),
        code => Err(WireError::Validation(
            format!("unknown geo mode {code} of a shadow topic").into(),
        )),
    }
}

fn decode_retention(buf: &[u8], position: usize) -> Result<ShadowRetention, WireError> {
    Ok(ShadowRetention {
        message_expiry: read_u64_le(buf, position)?,
        max_topic_size: read_u64_le(buf, position + 8)?,
    })
}

impl WireDecode for ShadowTopicRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let kind = read_u8(buf, 0)?;
        let (origin, consumed) = WireName::decode(slice_from(buf, 1)?)?;
        let mut position = 1 + consumed;
        let cluster_uid = read_u128_le(buf, position)?;
        position += 16;
        let stream_id = read_u64_le(buf, position)?;
        position += 8;
        let source = ShadowStream {
            origin: origin.as_str().to_owned(),
            cluster_uid,
            stream_id,
        };
        let request = match kind {
            CREATE_STREAM | RESTORE_STREAM => {
                let (name, consumed) = WireName::decode(slice_from(buf, position)?)?;
                position += consumed;
                if kind == CREATE_STREAM {
                    Self::CreateStream { source, name }
                } else {
                    Self::RestoreStream { source, name }
                }
            }
            RENAME_STREAM => {
                let stream_id = read_u32_le(buf, position)?;
                let (name, consumed) = WireName::decode(slice_from(buf, position + 4)?)?;
                position += 4 + consumed;
                Self::RenameStream {
                    stream_id,
                    source,
                    name,
                }
            }
            REMOVE_STREAM => {
                let stream_id = read_u32_le(buf, position)?;
                position += 4;
                Self::RemoveStream { stream_id, source }
            }
            ADOPT_TOPIC | DETACH_TOPIC | REMOVE_TOPIC | RESTORE_TOPIC => {
                let target = ShadowTopicTarget {
                    stream_id: read_u32_le(buf, position)?,
                    source,
                    topic_id: read_u32_le(buf, position + 4)?,
                    created_revision: read_u64_le(buf, position + 8)?,
                };
                position += TARGET_LEN;
                match kind {
                    ADOPT_TOPIC | RESTORE_TOPIC => {
                        let length = read_u32_le(buf, position)? as usize;
                        let bytes = read_bytes(buf, position + 4, length)?;
                        let create =
                            Box::new(CreateTopicWithAssignmentsRequest::decode_from(bytes)?);
                        position += 4 + length;
                        if kind == ADOPT_TOPIC {
                            let retention = decode_retention(buf, position)?;
                            let geo_mode = decode_geo_mode(buf, position + RETENTION_LEN)?;
                            position += RETENTION_LEN + 1;
                            Self::AdoptTopic {
                                target,
                                create,
                                retention,
                                geo_mode,
                            }
                        } else {
                            Self::RestoreTopic { target, create }
                        }
                    }
                    DETACH_TOPIC => {
                        let deleted_at = read_u64_le(buf, position)?;
                        position += 8;
                        Self::DetachTopic { target, deleted_at }
                    }
                    _ => Self::RemoveTopic { target },
                }
            }
            _ => {
                return Err(WireError::Validation(
                    format!("unknown shadow topic request kind {kind}").into(),
                ));
            }
        };
        Ok((request, position))
    }
}

fn slice_from(buf: &[u8], offset: usize) -> Result<&[u8], WireError> {
    buf.get(offset..).ok_or(WireError::UnexpectedEof {
        offset,
        need: 1,
        have: 0,
    })
}

/// A geo peer that never returns, which this cluster no longer follows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredOrigin {
    /// The cluster lifetimes that the shadow streams of the origin came
    /// from when it retired, in ascending order.
    pub cluster_uids: Vec<u128>,
    /// When the retirement committed, in microseconds since the Unix epoch.
    pub retired_at: u64,
}

impl StateHandler for RetireGeoOriginRequest {
    type State = StreamsInner;

    /// Retire the origin once. Retiring it again changes nothing, so a
    /// retried command keeps the first time.
    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        let origin = self.origin.as_str();
        if !state.retired_origins.contains_key(origin) {
            let cluster_uids: BTreeSet<u128> = state
                .items
                .iter()
                .filter_map(|(_, stream)| stream.shadow.as_ref())
                .filter(|source| source.origin == origin)
                .map(|source| source.cluster_uid)
                .collect();
            state.retired_origins.insert(
                origin.to_owned(),
                RetiredOrigin {
                    cluster_uids: cluster_uids.into_iter().collect(),
                    retired_at: timestamp.as_micros(),
                },
            );
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for ShadowTopicRequest {
    type State = StreamsInner;

    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        state.shadow_write = true;
        let reply = match self {
            Self::CreateStream { source, name } => create_stream(state, source, name, timestamp),
            Self::RenameStream {
                stream_id,
                source,
                name,
            } => rename_stream(state, *stream_id, source, name),
            Self::RemoveStream { stream_id, source } => {
                remove_stream(state, *stream_id, source, timestamp)
            }
            Self::AdoptTopic {
                target,
                create,
                retention,
                geo_mode,
            } => adopt_topic(state, target, create, *retention, *geo_mode, timestamp),
            Self::DetachTopic { target, deleted_at } => detach_topic(state, target, *deleted_at),
            Self::RemoveTopic { target } => remove_topic(state, target, timestamp),
            Self::RestoreStream { source, name } => restore_stream(state, source, name, timestamp),
            Self::RestoreTopic { target, create } => {
                restore_topic(state, target, create, timestamp)
            }
        };
        state.shadow_write = false;
        reply
    }
}

/// The shadow stream `stream_id` when it still shadows `source`.
fn shadow_stream_id(state: &StreamsInner, stream_id: u32, source: &ShadowStream) -> Option<usize> {
    let stream_id = stream_id as usize;
    state
        .items
        .get(stream_id)
        .is_some_and(|stream| stream.shadow.as_ref() == Some(source))
        .then_some(stream_id)
}

fn create_stream(
    state: &mut StreamsInner,
    source: &ShadowStream,
    name: &WireName,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    if !is_shadow_stream_name(name.as_str()) {
        return ApplyReply::err(ShadowTopicResult::InvalidStreamName);
    }
    let name: Arc<str> = Arc::from(name.as_str());
    if let Some(&existing) = state.index.get(&name) {
        return if state.items[existing].shadow.as_ref() == Some(source) {
            ApplyReply::ok(Bytes::new())
        } else {
            ApplyReply::err(ShadowTopicResult::StreamNameAlreadyExists)
        };
    }
    // One shadow stream per stream of a peer, whatever its name.
    if state
        .items
        .iter()
        .any(|(_, stream)| stream.shadow.as_ref() == Some(source))
    {
        return ApplyReply::err(ShadowTopicResult::StreamNameAlreadyExists);
    }
    let id = state.items.vacant_key();
    if !admits_slab_key(id, MAX_STREAMS) {
        return ApplyReply::err(ShadowTopicResult::TooManyStreams);
    }
    let mut stream = Stream::with_stats(name.clone(), timestamp, state.stats_registry.stream(id));
    stream.id = id;
    stream.created_op = state.apply_context.metadata_op;
    stream.shadow = Some(source.clone());
    let inserted = state.items.insert(Arc::new(stream));
    debug_assert_eq!(inserted, id, "vacant_key must match the insert slot");
    state.index.insert(name, id);
    state.created_index.insert((timestamp.as_micros(), id));
    let changes = state
        .items
        .get(id)
        .and_then(|stream| stream_body(stream))
        .map(|body| {
            AppliedChange::new(
                &WireMetadataChange::StreamCreated(body),
                change_actor(&state.apply_context),
            )
        })
        .into_iter()
        .collect();
    ApplyReply {
        changes,
        ..ApplyReply::ok(Bytes::new())
    }
}

fn rename_stream(
    state: &mut StreamsInner,
    stream_id: u32,
    source: &ShadowStream,
    name: &WireName,
) -> ApplyReply {
    let Some(stream_id) = shadow_stream_id(state, stream_id, source) else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    if !is_shadow_stream_name(name.as_str()) {
        return ApplyReply::err(ShadowTopicResult::InvalidStreamName);
    }
    let name: Arc<str> = Arc::from(name.as_str());
    match state.index.get(&name) {
        Some(&existing) if existing == stream_id => return ApplyReply::ok(Bytes::new()),
        Some(_) => return ApplyReply::err(ShadowTopicResult::StreamNameAlreadyExists),
        None => {}
    }
    let Some(stream) = state.items.make_mut(stream_id) else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    state.index.remove(&stream.name);
    stream.name = name.clone();
    state.index.insert(name, stream_id);
    let changes = stream_body(&state.items[stream_id])
        .map(|body| {
            AppliedChange::new(
                &WireMetadataChange::StreamUpdated(body),
                change_actor(&state.apply_context),
            )
        })
        .into_iter()
        .collect();
    ApplyReply {
        changes,
        ..ApplyReply::ok(Bytes::new())
    }
}

fn remove_stream(
    state: &mut StreamsInner,
    stream_id: u32,
    source: &ShadowStream,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    let Some(stream_id) = shadow_stream_id(state, stream_id, source) else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    if !state.items[stream_id].topics.is_empty() {
        return ApplyReply::err(ShadowTopicResult::StreamNotEmpty);
    }
    let Ok(wire_stream_id) = u32::try_from(stream_id) else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(wire_stream_id),
    }
    .apply(state, timestamp)
}

/// The shadow topic `target` names, as `(stream_id, topic_id)`, when it is
/// still the peer's incarnation `target` names.
fn shadow_topic_ids(state: &StreamsInner, target: &ShadowTopicTarget) -> Option<(usize, usize)> {
    let stream_id = shadow_stream_id(state, target.stream_id, &target.source)?;
    let topic_id = target.topic_id as usize;
    state.items[stream_id]
        .topics
        .get(topic_id)
        .and_then(|topic| topic.shadow)
        .is_some_and(|shadow| shadow.created_revision == target.created_revision)
        .then_some((stream_id, topic_id))
}

fn adopt_topic(
    state: &mut StreamsInner,
    target: &ShadowTopicTarget,
    create: &CreateTopicWithAssignmentsRequest,
    retention: ShadowRetention,
    geo_mode: GeoMode,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    if shadow_topic_ids(state, target).is_some() {
        return ApplyReply::ok(Bytes::new());
    }
    let Some(stream_id) = shadow_stream_id(state, target.stream_id, &target.source) else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    let shadow = ShadowTopic {
        created_revision: target.created_revision,
        deleted_at: None,
        retention,
        geo_mode,
    };
    create_topic(
        state,
        stream_id,
        Some(FixedTopic::Shadow {
            topic_id: target.topic_id as usize,
            shadow,
        }),
        create,
        timestamp,
    )
}

fn detach_topic(
    state: &mut StreamsInner,
    target: &ShadowTopicTarget,
    deleted_at: u64,
) -> ApplyReply {
    let Some((stream_id, topic_id)) = shadow_topic_ids(state, target) else {
        return ApplyReply::err(ShadowTopicResult::TopicNotFound);
    };
    // Read first: `make_mut` copies a stream that a checkpoint capture shares.
    let attached = state.items[stream_id]
        .topics
        .get(topic_id)
        .and_then(|topic| topic.shadow.as_ref())
        .is_some_and(|shadow| shadow.deleted_at.is_none());
    if attached
        && let Some(shadow) = state
            .items
            .make_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
            .and_then(|topic| topic.shadow.as_mut())
    {
        shadow.deleted_at = Some(deleted_at);
    }
    ApplyReply::ok(Bytes::new())
}

fn remove_topic(
    state: &mut StreamsInner,
    target: &ShadowTopicTarget,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    let Some((stream_id, topic_id)) = shadow_topic_ids(state, target) else {
        return ApplyReply::err(ShadowTopicResult::TopicNotFound);
    };
    let (Ok(stream_id), Ok(topic_id)) = (u32::try_from(stream_id), u32::try_from(topic_id)) else {
        return ApplyReply::err(ShadowTopicResult::TopicNotFound);
    };
    DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(stream_id),
        topic_id: WireIdentifier::numeric(topic_id),
    }
    .apply(state, timestamp)
}

/// Create the stream `name` of this cluster at the id the retired cluster
/// gave it. A stream that holds the id already with that name and no peer
/// is the one an earlier attempt restored.
fn restore_stream(
    state: &mut StreamsInner,
    source: &ShadowStream,
    name: &WireName,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    if is_shadow_stream_name(name.as_str()) {
        return ApplyReply::err(ShadowTopicResult::InvalidStreamName);
    }
    let Some(id) = usize::try_from(source.stream_id)
        .ok()
        .filter(|id| admits_slab_key(*id, MAX_STREAMS))
    else {
        return ApplyReply::err(ShadowTopicResult::InvalidStreamId);
    };
    let name: Arc<str> = Arc::from(name.as_str());
    if let Some(stream) = state.items.get(id) {
        return if stream.name == name && stream.shadow.is_none() {
            ApplyReply::ok(Bytes::new())
        } else {
            ApplyReply::err(ShadowTopicResult::InvalidStreamId)
        };
    }
    if state.index.contains_key(&name) {
        return ApplyReply::err(ShadowTopicResult::StreamNameAlreadyExists);
    }
    let mut stream = Stream::with_stats(name.clone(), timestamp, state.stats_registry.stream(id));
    stream.id = id;
    stream.created_op = state.apply_context.metadata_op;
    if !state.items.insert_at(id, Arc::new(stream)) {
        return ApplyReply::err(ShadowTopicResult::InvalidStreamId);
    }
    state.index.insert(name, id);
    state.created_index.insert((timestamp.as_micros(), id));
    let changes = state
        .items
        .get(id)
        .and_then(|stream| stream_body(stream))
        .map(|body| {
            AppliedChange::new(
                &WireMetadataChange::StreamCreated(body),
                change_actor(&state.apply_context),
            )
        })
        .into_iter()
        .collect();
    ApplyReply {
        changes,
        ..ApplyReply::ok(Bytes::new())
    }
}

/// Create the geo topic `target` names in its restored stream. A topic that
/// holds its id already with its revision is the one an earlier attempt
/// restored.
fn restore_topic(
    state: &mut StreamsInner,
    target: &ShadowTopicTarget,
    create: &CreateTopicWithAssignmentsRequest,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    let stream_id = target.stream_id as usize;
    let topic_id = target.topic_id as usize;
    let Some(stream) = state
        .items
        .get(stream_id)
        .filter(|stream| stream.shadow.is_none() && stream.id as u64 == target.source.stream_id)
    else {
        return ApplyReply::err(ShadowTopicResult::StreamNotFound);
    };
    if let Some(topic) = stream.topics.get(topic_id) {
        let restored = topic.recovered_from == Some(target.source.cluster_uid)
            && topic
                .partitions
                .first()
                .is_some_and(|partition| partition.created_revision == target.created_revision);
        return if restored {
            ApplyReply::ok(Bytes::new())
        } else {
            ApplyReply::err(ShadowTopicResult::InvalidTopicId)
        };
    }
    create_topic(
        state,
        stream_id,
        Some(FixedTopic::Restored {
            topic_id,
            created_revision: target.created_revision,
            retired_cluster_uid: target.source.cluster_uid,
        }),
        create,
        timestamp,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
    use crate::stm::result::{
        CreatePartitionsResult, CreateStreamResult, CreateTopicResult, DeletePartitionsResult,
        DeleteStreamResult, DeleteTopicResult, TruncatePartitionResult, UpdateStreamResult,
        UpdateTopicResult,
    };
    use crate::stm::snapshot::Snapshotable;
    use crate::stm::stream::{Streams, TruncatePartitionRequest};
    use iggy_binary_protocol::WireOptions;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::partitions::{
        CreatePartitionsRequest, CreatePartitionsWithAssignmentsRequest, DeletePartitionsRequest,
    };
    use iggy_binary_protocol::requests::streams::{
        CreateStreamRequest, UpdateStreamResolvedRequest,
    };
    use iggy_binary_protocol::requests::topics::{CreateTopicRequest, UpdateTopicResolvedRequest};
    use iggy_common::IggyError;

    const PEER: &str = "dc-b";
    const PEER_CLUSTER: u128 = 0x0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b;
    const PEER_STREAM: u64 = 7;
    const PEER_TOPIC: u32 = 3;
    const PEER_REVISION: u64 = 41;
    const SHADOW_NAME: &str = "__shadow_dc-b_orders";

    fn source() -> ShadowStream {
        ShadowStream {
            origin: PEER.to_owned(),
            cluster_uid: PEER_CLUSTER,
            stream_id: PEER_STREAM,
        }
    }

    fn apply<R: StateHandler<State = StreamsInner>>(
        inner: &mut StreamsInner,
        op: u64,
        request: &R,
    ) -> ApplyReply {
        inner.apply_context.metadata_op = op;
        request.apply(inner, IggyTimestamp::now())
    }

    fn name(name: &str) -> WireName {
        WireName::new(name).expect("a test name fits a wire name")
    }

    fn assignments(count: u32) -> Vec<CreatedPartitionAssignment> {
        (0..count)
            .map(|partition_id| CreatedPartitionAssignment {
                partition_id,
                consensus_group_id: 100 + u64::from(partition_id),
                initial_view: 0,
                members: Vec::new(),
            })
            .collect()
    }

    fn target(stream_id: u32) -> ShadowTopicTarget {
        ShadowTopicTarget {
            stream_id,
            source: source(),
            topic_id: PEER_TOPIC,
            created_revision: PEER_REVISION,
        }
    }

    fn adoption(stream_id: u32, partitions: u32) -> ShadowTopicRequest {
        ShadowTopicRequest::AdoptTopic {
            target: target(stream_id),
            create: Box::new(CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(stream_id),
                    partitions_count: partitions,
                    name: name("payments"),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: assignments(partitions),
            }),
            retention: RETENTION,
            geo_mode: GeoMode::Geo,
        }
    }

    /// The retention of the peer's topic: a day, and a gigabyte.
    const RETENTION: ShadowRetention = ShadowRetention {
        message_expiry: 86_400_000_000,
        max_topic_size: 1 << 30,
    };

    /// A shadow stream holding the peer's topic `PEER_TOPIC` with two
    /// partitions, after a plain stream of this cluster at id 0.
    fn shadowed() -> (StreamsInner, u32) {
        let mut inner = StreamsInner::new();
        let local = CreateStreamRequest {
            name: name("local"),
            options: WireOptions::empty(),
        };
        assert_eq!(apply(&mut inner, 1, &local).code, 0);
        let create = ShadowTopicRequest::CreateStream {
            source: source(),
            name: name(SHADOW_NAME),
        };
        assert_eq!(apply(&mut inner, 2, &create).code, 0);
        let stream_id = u32::try_from(inner.index[SHADOW_NAME]).unwrap();
        assert_eq!(apply(&mut inner, 3, &adoption(stream_id, 2)).code, 0);
        (inner, stream_id)
    }

    const ORIGIN: &str = "dc-a";
    const RETIRED_CLUSTER: u128 = 0x0a0a_0a0a_0a0a_0a0a_0a0a_0a0a_0a0a_0a0a;
    const RETIRED_STREAM: u32 = 5;
    const RETIRED_TOPIC: u32 = 2;
    const RETIRED_REVISION: u64 = 30;

    /// A stream of the retired cluster, which this cluster takes back.
    fn retired_source() -> ShadowStream {
        ShadowStream {
            origin: ORIGIN.to_owned(),
            cluster_uid: RETIRED_CLUSTER,
            stream_id: u64::from(RETIRED_STREAM),
        }
    }

    fn restoration(created_revision: u64, partitions: u32) -> ShadowTopicRequest {
        ShadowTopicRequest::RestoreTopic {
            target: ShadowTopicTarget {
                stream_id: RETIRED_STREAM,
                source: retired_source(),
                topic_id: RETIRED_TOPIC,
                created_revision,
            },
            create: Box::new(CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(RETIRED_STREAM),
                    partitions_count: partitions,
                    name: name("payments"),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: assignments(partitions),
            }),
        }
    }

    /// The stream `orders` of the retired cluster restored after a stream
    /// of this cluster at id 0.
    fn restored_stream() -> StreamsInner {
        let mut inner = StreamsInner::new();
        let local = CreateStreamRequest {
            name: name("local"),
            options: WireOptions::empty(),
        };
        assert_eq!(apply(&mut inner, 1, &local).code, 0);
        let restore = ShadowTopicRequest::RestoreStream {
            source: retired_source(),
            name: name("orders"),
        };
        assert_eq!(apply(&mut inner, 2, &restore).code, 0);
        inner
    }

    #[test]
    fn given_a_stream_and_topic_of_a_retired_cluster_when_restored_should_keep_their_ids_and_revision()
     {
        let mut inner = restored_stream();

        assert_eq!(
            apply(&mut inner, 3, &restoration(RETIRED_REVISION, 2)).code,
            0
        );

        let stream = &inner.items[RETIRED_STREAM as usize];
        assert_eq!(&*stream.name, "orders");
        assert_eq!(stream.shadow, None);
        assert_eq!(inner.index["orders"], RETIRED_STREAM as usize);
        let topic = stream.topics.get(RETIRED_TOPIC as usize).unwrap();
        assert_eq!(topic.shadow, None);
        assert_eq!(topic.recovered_from, Some(RETIRED_CLUSTER));
        assert_eq!(&*topic.name, "payments");
        let revisions: Vec<_> = topic
            .partitions
            .iter()
            .map(|partition| (partition.id, partition.created_revision))
            .collect();
        assert_eq!(revisions, [(0, RETIRED_REVISION), (1, RETIRED_REVISION)]);
        assert_eq!(inner.revision, RETIRED_REVISION);
        // A client's create after the restore goes on above its revision.
        let next = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: wire_id(RETIRED_STREAM),
                partitions_count: 1,
                name: name("later"),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: assignments(1),
        };
        assert_eq!(apply(&mut inner, 4, &next).code, 0);
        assert_eq!(inner.revision, RETIRED_REVISION + 1);
    }

    #[test]
    fn given_a_restore_that_committed_when_proposed_again_should_change_nothing() {
        let mut inner = restored_stream();
        assert_eq!(
            apply(&mut inner, 3, &restoration(RETIRED_REVISION, 2)).code,
            0
        );
        let again = ShadowTopicRequest::RestoreStream {
            source: retired_source(),
            name: name("orders"),
        };

        assert_eq!(apply(&mut inner, 4, &again).code, 0);
        assert_eq!(
            apply(&mut inner, 5, &restoration(RETIRED_REVISION, 2)).code,
            0
        );

        assert_eq!(inner.revision, RETIRED_REVISION);
        assert_eq!(inner.items[RETIRED_STREAM as usize].topics.len(), 1);
    }

    #[test]
    fn given_ids_or_a_revision_this_cluster_holds_when_restoring_should_refuse() {
        let mut inner = restored_stream();
        let taken_id = ShadowTopicRequest::RestoreStream {
            source: ShadowStream {
                stream_id: 0,
                ..retired_source()
            },
            name: name("orders-elsewhere"),
        };
        let taken_name = ShadowTopicRequest::RestoreStream {
            source: ShadowStream {
                stream_id: 9,
                ..retired_source()
            },
            name: name("local"),
        };
        let shadow_name = ShadowTopicRequest::RestoreStream {
            source: ShadowStream {
                stream_id: 9,
                ..retired_source()
            },
            name: name(SHADOW_NAME),
        };
        assert_eq!(
            apply(&mut inner, 3, &taken_id).code,
            u32::from(ShadowTopicResult::InvalidStreamId)
        );
        assert_eq!(
            apply(&mut inner, 4, &taken_name).code,
            u32::from(ShadowTopicResult::StreamNameAlreadyExists)
        );
        assert_eq!(
            apply(&mut inner, 5, &shadow_name).code,
            u32::from(ShadowTopicResult::InvalidStreamName)
        );
        assert_eq!(
            apply(&mut inner, 6, &restoration(RETIRED_REVISION, 1)).code,
            0
        );

        // The topic id holds another incarnation.
        assert_eq!(
            apply(&mut inner, 7, &restoration(RETIRED_REVISION + 1, 1)).code,
            u32::from(ShadowTopicResult::InvalidTopicId)
        );
        let ShadowTopicRequest::RestoreTopic { mut target, create } =
            restoration(RETIRED_REVISION, 1)
        else {
            unreachable!()
        };
        // A free topic id, at a revision this cluster passed.
        target.topic_id += 1;
        let mut create = create;
        create.request.name = name("payments-2");
        assert_eq!(
            apply(
                &mut inner,
                8,
                &ShadowTopicRequest::RestoreTopic { target, create }
            )
            .code,
            u32::from(ShadowTopicResult::InvalidTopicId)
        );
        assert_eq!(inner.revision, RETIRED_REVISION);
    }

    fn wire_id(id: u32) -> WireIdentifier {
        WireIdentifier::numeric(id)
    }

    fn read_only() -> u32 {
        IggyError::ShadowStreamReadOnly.as_code()
    }

    #[test]
    fn given_each_kind_when_encoded_should_decode_to_the_same_request() {
        let requests = [
            ShadowTopicRequest::CreateStream {
                source: source(),
                name: name(SHADOW_NAME),
            },
            ShadowTopicRequest::RenameStream {
                stream_id: 4,
                source: source(),
                name: name("__shadow_dc-b_renamed"),
            },
            ShadowTopicRequest::RemoveStream {
                stream_id: 4,
                source: source(),
            },
            adoption(4, 3),
            ShadowTopicRequest::DetachTopic {
                target: target(4),
                deleted_at: 1_700_000_000_000_000,
            },
            ShadowTopicRequest::RemoveTopic { target: target(4) },
            ShadowTopicRequest::RestoreStream {
                source: retired_source(),
                name: name("orders"),
            },
            restoration(RETIRED_REVISION, 2),
        ];
        for request in requests {
            let bytes = request.to_bytes();
            assert_eq!(bytes.len(), request.encoded_size());
            assert_eq!(ShadowTopicRequest::decode_from(&bytes).unwrap(), request);
            assert!(
                ShadowTopicRequest::decode_from(&bytes[..bytes.len() - 1]).is_err(),
                "a cut body is refused: {request:?}"
            );
        }
        assert!(ShadowTopicRequest::decode_from(&[9, 1, b'x']).is_err());
    }

    #[test]
    fn given_a_peer_topic_when_adopted_should_keep_its_topic_and_partition_ids() {
        let (inner, stream_id) = shadowed();

        let stream = &inner.items[stream_id as usize];
        assert_eq!(stream.shadow, Some(source()));
        let topic = stream
            .topics
            .get(PEER_TOPIC as usize)
            .expect("the topic sits at the peer's id");
        assert_eq!(topic.id, PEER_TOPIC as usize);
        assert_eq!(&*topic.name, "payments");
        assert_eq!(
            topic.shadow,
            Some(ShadowTopic {
                created_revision: PEER_REVISION,
                deleted_at: None,
                retention: RETENTION,
                geo_mode: GeoMode::Geo,
            })
        );
        let ids: Vec<usize> = topic
            .partitions
            .iter()
            .map(|partition| partition.id)
            .collect();
        assert_eq!(ids, [0, 1]);
        assert!(topic.skips_object_deletes());
        assert_eq!(stream.topics.vacant_key(), 0, "the ids below stay free");
    }

    #[test]
    fn given_an_adopted_topic_when_adopted_again_should_change_nothing() {
        let (mut inner, stream_id) = shadowed();
        let revision = inner.revision;

        assert_eq!(apply(&mut inner, 4, &adoption(stream_id, 2)).code, 0);

        assert_eq!(inner.revision, revision);
        assert_eq!(inner.items[stream_id as usize].topics.len(), 1);
    }

    #[test]
    fn given_another_incarnation_when_adopted_should_wait_for_the_old_one() {
        let (mut inner, stream_id) = shadowed();
        let ShadowTopicRequest::AdoptTopic {
            mut target,
            create,
            retention,
            geo_mode,
        } = adoption(stream_id, 2)
        else {
            unreachable!()
        };
        target.created_revision = PEER_REVISION + 1;

        let reply = apply(
            &mut inner,
            4,
            &ShadowTopicRequest::AdoptTopic {
                target,
                create,
                retention,
                geo_mode,
            },
        );

        assert_eq!(
            reply.code,
            u32::from(ShadowTopicResult::TopicNameAlreadyExists)
        );
    }

    #[test]
    fn given_a_stream_of_another_source_when_creating_the_same_name_should_refuse_it() {
        let (mut inner, _) = shadowed();
        let other = ShadowTopicRequest::CreateStream {
            source: ShadowStream {
                cluster_uid: PEER_CLUSTER + 1,
                ..source()
            },
            name: name(SHADOW_NAME),
        };

        assert_eq!(
            apply(&mut inner, 4, &other).code,
            u32::from(ShadowTopicResult::StreamNameAlreadyExists)
        );
        let again = ShadowTopicRequest::CreateStream {
            source: source(),
            name: name("__shadow_dc-b_second"),
        };
        assert_eq!(
            apply(&mut inner, 5, &again).code,
            u32::from(ShadowTopicResult::StreamNameAlreadyExists),
            "one shadow stream per peer stream"
        );
        let plain = ShadowTopicRequest::CreateStream {
            source: source(),
            name: name("orders"),
        };
        assert_eq!(
            apply(&mut inner, 6, &plain).code,
            u32::from(ShadowTopicResult::InvalidStreamName)
        );
    }

    #[test]
    fn given_a_client_stream_name_with_the_shadow_prefix_should_refuse_create_and_rename() {
        let mut inner = StreamsInner::new();
        let create = CreateStreamRequest {
            name: name("__shadow_mine"),
            options: WireOptions::empty(),
        };
        assert_eq!(
            apply(&mut inner, 1, &create).code,
            u32::from(CreateStreamResult::InvalidStreamName)
        );

        let local = CreateStreamRequest {
            name: name("local"),
            options: WireOptions::empty(),
        };
        assert_eq!(apply(&mut inner, 2, &local).code, 0);
        let rename = UpdateStreamResolvedRequest {
            stream_id: wire_id(0),
            name: Some(name("__shadow_mine")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        };
        assert_eq!(
            apply(&mut inner, 3, &rename).code,
            u32::from(UpdateStreamResult::InvalidStreamName)
        );
        assert_eq!(&*inner.items[0].name, "local");
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_creates_a_topic_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let create = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: wire_id(stream_id),
                partitions_count: 1,
                name: name("mine"),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: assignments(1),
        };
        assert_eq!(apply(&mut inner, 4, &create).code, read_only());
        assert_eq!(
            u32::from(CreateTopicResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_updates_a_topic_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let update = UpdateTopicResolvedRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
            name: Some(name("renamed")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        };
        assert_eq!(apply(&mut inner, 4, &update).code, read_only());
        assert_eq!(
            u32::from(UpdateTopicResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_deletes_a_topic_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let delete = DeleteTopicRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
        };
        assert_eq!(apply(&mut inner, 4, &delete).code, read_only());
        assert_eq!(
            u32::from(DeleteTopicResult::ShadowStreamReadOnly),
            read_only()
        );
        assert!(inner.lifecycle_intents.is_empty());
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_adds_partitions_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let create = CreatePartitionsWithAssignmentsRequest {
            request: CreatePartitionsRequest {
                stream_id: wire_id(stream_id),
                topic_id: wire_id(PEER_TOPIC),
                partitions_count: 1,
            },
            partitions: assignments(1),
        };
        assert_eq!(apply(&mut inner, 4, &create).code, read_only());
        assert_eq!(
            u32::from(CreatePartitionsResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_deletes_partitions_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let delete = DeletePartitionsRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
            partitions_count: 1,
        };
        assert_eq!(apply(&mut inner, 4, &delete).code, read_only());
        assert_eq!(
            u32::from(DeletePartitionsResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_deletes_segments_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let truncate = TruncatePartitionRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
            partition_id: 0,
            up_to_offset: 10,
            expected_history: None,
        };
        assert_eq!(apply(&mut inner, 4, &truncate).code, read_only());
        assert_eq!(
            u32::from(TruncatePartitionResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_updates_it_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let update = UpdateStreamResolvedRequest {
            stream_id: wire_id(stream_id),
            name: Some(name("renamed")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        };
        assert_eq!(apply(&mut inner, 4, &update).code, read_only());
        assert_eq!(
            u32::from(UpdateStreamResult::ShadowStreamReadOnly),
            read_only()
        );
    }

    #[test]
    fn given_a_shadow_stream_when_a_client_deletes_it_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        let delete = DeleteStreamRequest {
            stream_id: wire_id(stream_id),
        };
        assert_eq!(apply(&mut inner, 4, &delete).code, read_only());
        assert_eq!(
            u32::from(DeleteStreamResult::ShadowStreamReadOnly),
            read_only()
        );
        assert!(inner.items.contains(stream_id as usize));
    }

    fn retirement(origin: &str) -> RetireGeoOriginRequest {
        RetireGeoOriginRequest {
            origin: name(origin),
        }
    }

    #[test]
    fn given_an_origin_when_retired_should_record_its_clusters_and_keep_the_first_time() {
        let (mut inner, _) = shadowed();
        let first = IggyTimestamp::from(1_000_u64);
        assert_eq!(retirement(PEER).apply(&mut inner, first).code, 0);
        assert_eq!(
            retirement(PEER)
                .apply(&mut inner, IggyTimestamp::from(2_000_u64))
                .code,
            0
        );

        assert_eq!(
            inner.retired_origins.get(PEER),
            Some(&RetiredOrigin {
                cluster_uids: vec![PEER_CLUSTER],
                retired_at: 1_000,
            })
        );
    }

    #[test]
    fn given_an_origin_without_shadow_streams_when_retired_should_record_no_cluster() {
        let mut inner = StreamsInner::new();
        assert_eq!(apply(&mut inner, 1, &retirement("dc-c")).code, 0);

        let retired = &inner.retired_origins["dc-c"];
        assert_eq!(retired.cluster_uids, Vec::<u128>::new());
    }

    #[test]
    fn given_a_retired_origin_when_a_client_deletes_its_shadow_topic_should_accept_it() {
        let (mut inner, stream_id) = shadowed();
        assert_eq!(apply(&mut inner, 4, &retirement(PEER)).code, 0);
        let delete = DeleteTopicRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
        };

        assert_eq!(apply(&mut inner, 5, &delete).code, 0);
        assert_eq!(inner.lifecycle_intents.len(), 1);
    }

    #[test]
    fn given_a_retired_origin_when_a_client_deletes_its_shadow_stream_should_accept_it() {
        let (mut inner, stream_id) = shadowed();
        assert_eq!(apply(&mut inner, 4, &retirement(PEER)).code, 0);
        let delete = DeleteStreamRequest {
            stream_id: wire_id(stream_id),
        };

        assert_eq!(apply(&mut inner, 5, &delete).code, 0);
        assert_eq!(inner.lifecycle_intents.len(), 1);
    }

    #[test]
    fn given_a_retired_origin_when_a_client_writes_to_its_shadow_stream_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        assert_eq!(apply(&mut inner, 4, &retirement(PEER)).code, 0);
        let update = UpdateStreamResolvedRequest {
            stream_id: wire_id(stream_id),
            name: Some(name("renamed")),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        };

        assert_eq!(apply(&mut inner, 5, &update).code, read_only());
    }

    #[test]
    fn given_another_origin_retired_when_a_client_deletes_a_shadow_stream_should_refuse_it() {
        let (mut inner, stream_id) = shadowed();
        assert_eq!(apply(&mut inner, 4, &retirement("dc-c")).code, 0);
        let delete = DeleteStreamRequest {
            stream_id: wire_id(stream_id),
        };

        assert_eq!(apply(&mut inner, 5, &delete).code, read_only());
    }

    #[test]
    fn given_a_retired_origin_when_restored_from_a_snapshot_should_keep_it() {
        let (mut inner, _) = shadowed();
        assert_eq!(apply(&mut inner, 4, &retirement(PEER)).code, 0);
        let retired = inner.retired_origins.clone();
        let streams = Streams::from(inner);

        let restored = Streams::from_snapshot(streams.to_snapshot()).unwrap();

        assert_eq!(restored.retired_origins(), retired);
    }

    #[test]
    fn given_a_shadow_topic_when_a_client_creates_a_group_should_accept_it() {
        let (mut inner, stream_id) = shadowed();
        let create = CreateConsumerGroupRequest {
            stream_id: wire_id(stream_id),
            topic_id: wire_id(PEER_TOPIC),
            name: name("readers"),
        };
        assert_eq!(apply(&mut inner, 4, &create).code, 0);
        let topic = &inner.items[stream_id as usize].topics[PEER_TOPIC as usize];
        assert_eq!(topic.consumer_groups.len(), 1);
    }

    #[test]
    fn given_a_tombstone_when_detached_should_record_when_and_keep_the_first_time() {
        let (mut inner, stream_id) = shadowed();
        let detach = |deleted_at| ShadowTopicRequest::DetachTopic {
            target: target(stream_id),
            deleted_at,
        };

        assert_eq!(apply(&mut inner, 4, &detach(500)).code, 0);
        assert_eq!(apply(&mut inner, 5, &detach(900)).code, 0);

        let shadow = inner.items[stream_id as usize].topics[PEER_TOPIC as usize]
            .shadow
            .unwrap();
        assert!(shadow.is_detached());
        assert_eq!(shadow.deleted_at, Some(500));
    }

    #[test]
    fn given_a_stale_target_when_detaching_or_removing_should_change_nothing() {
        let (mut inner, stream_id) = shadowed();
        let stale = ShadowTopicTarget {
            created_revision: PEER_REVISION + 1,
            ..target(stream_id)
        };
        let detach = ShadowTopicRequest::DetachTopic {
            target: stale.clone(),
            deleted_at: 500,
        };
        let remove = ShadowTopicRequest::RemoveTopic { target: stale };

        assert_eq!(
            apply(&mut inner, 4, &detach).code,
            u32::from(ShadowTopicResult::TopicNotFound)
        );
        assert_eq!(
            apply(&mut inner, 5, &remove).code,
            u32::from(ShadowTopicResult::TopicNotFound)
        );
        assert!(
            inner.items[stream_id as usize].topics[PEER_TOPIC as usize]
                .shadow
                .is_some_and(|shadow| !shadow.is_detached())
        );
    }

    #[test]
    fn given_a_shadow_topic_when_removed_should_go_once_its_partitions_retire() {
        let (mut inner, stream_id) = shadowed();
        let remove = ShadowTopicRequest::RemoveTopic {
            target: target(stream_id),
        };
        let remove_stream = ShadowTopicRequest::RemoveStream {
            stream_id,
            source: source(),
        };

        assert_eq!(apply(&mut inner, 10, &remove).code, 0);
        assert_eq!(
            apply(&mut inner, 11, &remove_stream).code,
            u32::from(ShadowTopicResult::StreamNotEmpty),
            "the topic waits for its partitions"
        );
        let completions = (0..2)
            .map(|partition_id| LifecycleCompletion {
                topic_id: PEER_TOPIC,
                partition_id,
                partition_op: 1,
            })
            .collect();
        let complete = CompleteLifecycleRequest::new(10, stream_id, completions).unwrap();
        assert_eq!(apply(&mut inner, 12, &complete).code, 0);
        assert!(inner.items[stream_id as usize].topics.is_empty());

        assert_eq!(apply(&mut inner, 13, &remove_stream).code, 0);
        assert!(!inner.items.contains(stream_id as usize));
        assert!(!inner.index.contains_key(SHADOW_NAME));
    }

    #[test]
    fn given_a_shadow_stream_when_listing_streams_by_creation_should_list_it() {
        let (inner, stream_id) = shadowed();

        let listed: Vec<usize> = inner
            .streams_by_created(None, false)
            .map(|((_, id), _)| id)
            .collect();

        assert_eq!(listed, [0, stream_id as usize]);
    }

    #[test]
    fn given_a_renamed_peer_stream_when_renaming_should_move_the_name() {
        let (mut inner, stream_id) = shadowed();
        let rename = |name_text: &str| ShadowTopicRequest::RenameStream {
            stream_id,
            source: source(),
            name: name(name_text),
        };

        assert_eq!(
            apply(&mut inner, 4, &rename("__shadow_dc-b_billing")).code,
            0
        );
        assert_eq!(
            &*inner.items[stream_id as usize].name,
            "__shadow_dc-b_billing"
        );
        assert!(!inner.index.contains_key(SHADOW_NAME));
        assert_eq!(
            apply(&mut inner, 5, &rename("local")).code,
            u32::from(ShadowTopicResult::InvalidStreamName)
        );
    }

    #[test]
    fn given_shadow_streams_when_restored_from_a_snapshot_should_keep_their_records() {
        let (inner, stream_id) = shadowed();
        let streams = Streams::from(inner);

        let restored = Streams::from_snapshot(streams.to_snapshot()).unwrap();

        let (shadow, revision) = restored.read(|inner| {
            let stream = &inner.items[stream_id as usize];
            let topic = &stream.topics[PEER_TOPIC as usize];
            (
                stream.shadow.clone(),
                topic.shadow.map(|shadow| shadow.created_revision),
            )
        });
        assert_eq!(shadow, Some(source()));
        assert_eq!(revision, Some(PEER_REVISION));
    }

    #[test]
    fn given_a_restored_topic_when_restored_from_a_snapshot_should_keep_its_recovery_marker() {
        let mut inner = restored_stream();
        assert_eq!(
            apply(&mut inner, 3, &restoration(RETIRED_REVISION, 2)).code,
            0
        );
        let streams = Streams::from(inner);

        let restored = Streams::from_snapshot(
            Streams::from_snapshot(streams.to_snapshot())
                .unwrap()
                .to_snapshot(),
        )
        .unwrap();

        let (recovered_from, revisions) = restored.read(|inner| {
            let topic = &inner.items[RETIRED_STREAM as usize].topics[RETIRED_TOPIC as usize];
            (
                topic.recovered_from,
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.created_revision)
                    .collect::<Vec<_>>(),
            )
        });
        assert_eq!(recovered_from, Some(RETIRED_CLUSTER));
        assert_eq!(revisions, [RETIRED_REVISION, RETIRED_REVISION]);
    }
}
