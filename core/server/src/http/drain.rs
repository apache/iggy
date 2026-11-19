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

//! The drain of a consumer group before a planned failover: the node stores
//! the group's final positions in its bucket at once, so that another
//! datacenter resumes the group on the exact next message.
//!
//! The server cannot stop a client, so the operator stops the group's
//! consumers first. The drain waits until the group has no member left in
//! this cluster, has the partitions fold what the group committed, stores
//! the node's object of the topic's log and waits for the store to take it.
//!
//! It covers the partitions that this node leads. A backup cannot tell that
//! it applied the group's last commit and its fold, so the answer lists the
//! partitions led elsewhere, and the operator drains the group on each node.

use std::str::FromStr;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use consensus::MetadataHandle;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{Identifier, IggyDuration, IggyError, IggyTimestamp};
use iggy_object_store::{GeoLog, PositionValue};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};
use shard::bus_timeout;
use shard::geo_positions::{LogRound, local_logs};

use crate::http::error::{CustomError, ErrorResponse};
use crate::http::extractor::Identity;
use crate::http::reads::resolve_gate_topic_ids;
use crate::http::state::{HttpInner, HttpState};

/// How long a drain waits when the caller names no timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest wait a caller may ask for.
const MAX_TIMEOUT: Duration = Duration::from_mins(10);

/// How often the drain looks at the group's members again.
const MEMBERS_RECHECK: Duration = Duration::from_millis(100);

#[derive(Debug, Default, Deserialize)]
pub(in crate::http) struct DrainQuery {
    /// How long to wait for the group to stop and for the store, such as
    /// `30s`.
    #[serde(default)]
    timeout: Option<String>,
}

/// What a drain stored of the group.
#[derive(Debug, Serialize)]
pub(in crate::http) struct DrainedGroup {
    pub group: String,
    /// The group's position on each partition this node leads, as the node's
    /// bucket now holds it.
    pub positions: Vec<DrainedPosition>,
    /// The partitions of the topic that this node holds a replica of and
    /// another node leads. Drain the group on the node that leads them.
    pub not_led: Vec<u32>,
    /// The partitions of the topic that this node holds no replica of. Drain
    /// a node that holds them too.
    pub not_held: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub(in crate::http) struct DrainedPosition {
    pub partition_id: u32,
    pub reset: u64,
    /// The last offset the group consumed, `null` when it consumed none.
    pub offset: Option<u64>,
}

/// `POST /streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}/drain`:
/// store the group's final positions on the partitions this node leads in
/// its bucket at once, for a planned failover. 409 while the group still has
/// members here after the timeout, 504 when the store did not take the
/// positions in time.
pub(in crate::http) async fn drain_cg(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
    Query(query): Query<DrainQuery>,
) -> Response {
    SendWrapper::new(drain(
        state,
        identity,
        (stream_id, topic_id, group_id),
        query,
    ))
    .await
}

#[allow(clippy::future_not_send)]
async fn drain(
    state: HttpState,
    identity: Identity,
    (stream_id, topic_id, group_id): (String, String, String),
    query: DrainQuery,
) -> Response {
    let target = match Target::resolve(&state, &identity, &stream_id, &topic_id, &group_id) {
        Ok(target) => target,
        Err(refusal) => return refusal.into_response(),
    };
    let timeout = match query.timeout.as_deref().map(IggyDuration::from_str) {
        None => DEFAULT_TIMEOUT,
        Some(Ok(timeout)) => timeout.get_duration().min(MAX_TIMEOUT),
        Some(Err(_)) => return CustomError::Error(IggyError::InvalidCommand).into_response(),
    };
    let shard = &state.shard;
    let deadline = shard.bus.monotonic_micros().saturating_add(micros(timeout));
    while target.has_members(&state) {
        if shard.bus.monotonic_micros() >= deadline {
            return refused(
                StatusCode::CONFLICT,
                "the group still has members in this cluster; stop its consumers first",
            );
        }
        shard.bus.sleep(MEMBERS_RECHECK).await;
    }
    loop {
        let left = deadline.saturating_sub(shard.bus.monotonic_micros());
        if left == 0 {
            return refused(
                StatusCode::GATEWAY_TIMEOUT,
                "the bucket did not take the group's positions in time",
            );
        }
        let Some(round) = shard.geo_positions_round_now(IggyTimestamp::now()) else {
            return CustomError::Error(IggyError::FeatureUnavailable).into_response();
        };
        let Some(Ok(summary)) = bus_timeout(&shard.bus, Duration::from_micros(left), round).await
        else {
            continue;
        };
        let Some(found) = summary.logs.get(&target.log) else {
            return Json(target.drained(&LogRound::default())).into_response();
        };
        let folded = found
            .partitions
            .values()
            .all(|partition| !partition.unfolded.contains(&target.name));
        if folded && found.stored {
            return Json(target.drained(found)).into_response();
        }
    }
}

/// The group a drain is for, and the log of its topic.
struct Target {
    stream_id: usize,
    topic_id: usize,
    group_id: u64,
    name: String,
    log: GeoLog,
    partitions: Vec<u32>,
}

impl Target {
    fn resolve(
        state: &HttpInner,
        identity: &Identity,
        stream_id: &str,
        topic_id: &str,
        group_id: &str,
    ) -> Result<Self, Refusal> {
        let rejected = Refusal::Error;
        let stream = Identifier::from_str_value(stream_id).map_err(rejected)?;
        let topic = Identifier::from_str_value(topic_id).map_err(rejected)?;
        let group = Identifier::from_str_value(group_id)
            .and_then(|group| identifier_to_wire(&group))
            .map_err(rejected)?;
        let (stream_id, topic_id) =
            resolve_gate_topic_ids(state, &stream, &topic).ok_or(Refusal::NotFound)?;
        let metadata = &state.shard.plane.metadata().mux_stm;
        metadata
            .users()
            .authorize(|permissioner| {
                permissioner.store_consumer_offset(identity.user_id, stream_id, topic_id)
            })
            .map_err(rejected)?;
        let origin = state.server_config.geo.origin.clone();
        metadata.streams().read(|inner| {
            let topic = inner
                .items
                .get(stream_id)
                .and_then(|stream| stream.topics.get(topic_id))
                .ok_or(Refusal::NotFound)?;
            let (group_id, name) = topic
                .resolve_group_id(&group)
                .and_then(|group_id| {
                    let group = topic.consumer_groups.get(&group_id)?;
                    Some((group_id, group.name.to_string()))
                })
                .ok_or(Refusal::NotFound)?;
            let local = local_logs(inner, &origin)
                .into_iter()
                .find(|local| local.stream_id == stream_id && local.topic_id == topic_id)
                .ok_or(Refusal::NotGeo)?;
            Ok(Self {
                stream_id,
                topic_id,
                group_id,
                name,
                partitions: local.partitions.iter().map(|(id, _, _)| *id).collect(),
                log: local.log,
            })
        })
    }

    fn has_members(&self, state: &HttpInner) -> bool {
        state
            .shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| {
                inner
                    .items
                    .get(self.stream_id)
                    .and_then(|stream| stream.topics.get(self.topic_id))
                    .and_then(|topic| topic.consumer_groups.get(&self.group_id))
                    .is_some_and(|group| !group.members.is_empty())
            })
    }

    /// The answer from what a round found of the topic's log on this node.
    fn drained(&self, found: &LogRound) -> DrainedGroup {
        let mut drained = DrainedGroup {
            group: self.name.clone(),
            positions: Vec::new(),
            not_led: Vec::new(),
            not_held: Vec::new(),
        };
        for partition_id in &self.partitions {
            if let Some(partition) = found.partitions.get(partition_id) {
                if let Some(position) = partition.positions.get(&self.name) {
                    drained.positions.push(DrainedPosition {
                        partition_id: *partition_id,
                        reset: position.reset,
                        offset: match position.value {
                            PositionValue::Offset(offset) => Some(offset),
                            PositionValue::Unset | PositionValue::Deleted => None,
                        },
                    });
                }
            } else if found.not_led.contains(partition_id) {
                drained.not_led.push(*partition_id);
            } else {
                drained.not_held.push(*partition_id);
            }
        }
        drained
    }
}

/// Why a drain does not start.
enum Refusal {
    Error(IggyError),
    NotFound,
    NotGeo,
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        match self {
            Self::Error(error) => CustomError::Error(error).into_response(),
            Self::NotFound => CustomError::ResourceNotFound.into_response(),
            Self::NotGeo => refused(
                StatusCode::BAD_REQUEST,
                "only the groups of a geo topic or a shadow topic carry their positions",
            ),
        }
    }
}

fn refused(status: StatusCode, reason: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            id: u32::from(status.as_u16()),
            code: status
                .canonical_reason()
                .unwrap_or("error")
                .to_lowercase()
                .replace(' ', "_"),
            reason: reason.to_owned(),
            field: None,
        }),
    )
        .into_response()
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
