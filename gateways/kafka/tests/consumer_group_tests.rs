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

//! Consumer group coordination: `FindCoordinator`, `JoinGroup`, Heartbeat, `SyncGroup`.
//!
//! Requests go through `handle_request_bounded` against one shared `GatewayState`, because
//! `handle_request` builds a fresh coordinator per call and no two requests would ever see the
//! same group. Time is paused, so every rebalance and eviction deadline is reached by advancing
//! it rather than by sleeping.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::advance;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_FIND_COORDINATOR, API_KEY_HEARTBEAT, API_KEY_JOIN_GROUP, API_KEY_SYNC_GROUP,
    BrokerAdvertise, ERROR_GROUP_MAX_SIZE_REACHED, ERROR_ILLEGAL_GENERATION,
    ERROR_INCONSISTENT_GROUP_PROTOCOL, ERROR_INVALID_GROUP_ID, ERROR_INVALID_REQUEST,
    ERROR_INVALID_SESSION_TIMEOUT, ERROR_MEMBER_ID_REQUIRED, ERROR_NONE,
    ERROR_REBALANCE_IN_PROGRESS, ERROR_UNKNOWN_MEMBER_ID, GatewayState, handle_request_bounded,
};

use codec::Decoder;
use server::spawn_test_server_with_config;
use tcp::{build_request_frame, parse_response_payload, read_response_frame};
use wire::{
    JoinGroupParams, SyncGroupParams, build_find_coordinator_request, build_heartbeat_request,
    build_join_group_request, build_sync_group_request,
};

const GROUP: &str = "orders";
const JOIN_VERSION: i16 = 9;
const SYNC_VERSION: i16 = 5;
const HEARTBEAT_VERSION: i16 = 4;
const SESSION_TIMEOUT_MS: i32 = 10_000;
const REBALANCE_TIMEOUT_MS: i32 = 20_000;

// ── Fixtures ────────────────────────────────────────────────────────────────

fn test_state(config: GroupCoordinatorConfig) -> Arc<GatewayState> {
    Arc::new(GatewayState::new(
        BrokerAdvertise::default(),
        None,
        8 * 1024 * 1024,
        GroupCoordinator::new(config, CancellationToken::new()),
    ))
}

/// A coordinator that completes a new group's first join immediately, so tests that are not
/// about the initial rebalance delay do not have to step time past it.
fn immediate_config() -> GroupCoordinatorConfig {
    GroupCoordinatorConfig {
        initial_rebalance_delay: Duration::ZERO,
        ..GroupCoordinatorConfig::default()
    }
}

fn join_params<'a>(member_id: &'a str, metadata: &'a [(&'a str, &'a [u8])]) -> JoinGroupParams<'a> {
    JoinGroupParams {
        group_id: GROUP,
        session_timeout_ms: SESSION_TIMEOUT_MS,
        rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
        member_id,
        protocols: metadata,
        ..JoinGroupParams::default()
    }
}

async fn join(state: &GatewayState, version: i16, params: &JoinGroupParams<'_>) -> JoinResponse {
    let body = build_join_group_request(version, params);
    let response = handle_request_bounded(state, API_KEY_JOIN_GROUP, version, body)
        .await
        .expect_response("JoinGroup must answer");
    JoinResponse::decode(version, response)
}

async fn sync(state: &GatewayState, version: i16, params: &SyncGroupParams<'_>) -> SyncResponse {
    let body = build_sync_group_request(version, params);
    let response = handle_request_bounded(state, API_KEY_SYNC_GROUP, version, body)
        .await
        .expect_response("SyncGroup must answer");
    SyncResponse::decode(version, response)
}

async fn heartbeat(state: &GatewayState, generation_id: i32, member_id: &str) -> i16 {
    let body = build_heartbeat_request(HEARTBEAT_VERSION, GROUP, generation_id, member_id);
    let response = handle_request_bounded(state, API_KEY_HEARTBEAT, HEARTBEAT_VERSION, body)
        .await
        .expect_response("Heartbeat must answer");
    let mut decoder = Decoder::new(response);
    decoder.read_i32().unwrap(); // throttle_time_ms
    let error = decoder.read_i16().unwrap();
    decoder.read_tagged_fields().unwrap();
    assert_eq!(
        decoder.remaining(),
        0,
        "Heartbeat response has trailing bytes"
    );
    error
}

/// Claim a member id, then join with it. Returns the id and the second join's answer, which is
/// `None` while the group's join barrier is still open.
async fn claim_member_id(state: &GatewayState, metadata: &'static [u8]) -> String {
    let protocols: &[(&str, &[u8])] = &[("range", metadata)];
    let claimed = join(state, JOIN_VERSION, &join_params("", protocols)).await;
    assert_eq!(claimed.error, ERROR_MEMBER_ID_REQUIRED);
    claimed.member_id
}

/// Two members through one full rebalance: generation 2, leader first, both awaiting `SyncGroup`.
async fn two_member_group(state: &Arc<GatewayState>) -> (String, String) {
    let leader_protocols: &[(&str, &[u8])] = &[("range", b"leader-subscription")];

    let leader = claim_member_id(state, b"leader-subscription").await;
    let first = join(state, JOIN_VERSION, &join_params(&leader, leader_protocols)).await;
    assert_eq!(first.error, ERROR_NONE);
    assert_eq!(first.generation_id, 1);

    let follower = claim_member_id(state, b"follower-subscription").await;
    let parked = {
        let state = Arc::clone(state);
        let follower = follower.clone();
        tokio::spawn(async move {
            let protocols: &[(&str, &[u8])] = &[("range", b"follower-subscription")];
            join(&state, JOIN_VERSION, &join_params(&follower, protocols)).await
        })
    };
    yield_to_parked().await;

    let rejoined = join(state, JOIN_VERSION, &join_params(&leader, leader_protocols)).await;
    assert_eq!(rejoined.error, ERROR_NONE);
    assert_eq!(rejoined.generation_id, 2);

    let follower_join = parked.await.expect("parked JoinGroup task");
    assert_eq!(follower_join.error, ERROR_NONE);
    assert_eq!(follower_join.generation_id, 2);

    (leader, follower)
}

/// Let a task that is about to park reach its park. Paused time turns this into one scheduler
/// turn plus a 1 ms clock step, not a real wait.
async fn yield_to_parked() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

// ── Response decoding ───────────────────────────────────────────────────────

#[derive(Debug)]
struct JoinResponse {
    error: i16,
    generation_id: i32,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: String,
    member_id: String,
    members: Vec<(String, Bytes)>,
}

impl JoinResponse {
    fn decode(version: i16, body: Bytes) -> Self {
        let flexible = version >= 6;
        let mut decoder = Decoder::new(body);
        if version >= 2 {
            decoder.read_i32().unwrap(); // throttle_time_ms
        }
        let error = decoder.read_i16().unwrap();
        let generation_id = decoder.read_i32().unwrap();
        let protocol_type = if version >= 7 {
            decoder.read_compact_nullable_string().unwrap()
        } else {
            None
        };
        let protocol_name = read_nullable(&mut decoder, flexible);
        let leader = read_nullable(&mut decoder, flexible).expect("leader is not nullable");
        if version >= 9 {
            decoder.read_bool().unwrap(); // skip_assignment
        }
        let member_id = read_nullable(&mut decoder, flexible).expect("member id is not nullable");

        let count = read_array_count(&mut decoder, flexible);
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let id = read_nullable(&mut decoder, flexible).expect("member id is not nullable");
            if version >= 5 {
                read_nullable(&mut decoder, flexible); // group_instance_id
            }
            let metadata = read_bytes_field(&mut decoder, flexible);
            if flexible {
                decoder.read_tagged_fields().unwrap();
            }
            members.push((id, metadata));
        }
        if flexible {
            decoder.read_tagged_fields().unwrap();
        }
        assert_eq!(
            decoder.remaining(),
            0,
            "JoinGroup v{version} response has trailing bytes"
        );

        Self {
            error,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            member_id,
            members,
        }
    }

    fn metadata_of(&self, member_id: &str) -> Option<&Bytes> {
        self.members
            .iter()
            .find(|(id, _)| id == member_id)
            .map(|(_, metadata)| metadata)
    }
}

#[derive(Debug)]
struct SyncResponse {
    error: i16,
    protocol_name: Option<String>,
    assignment: Bytes,
}

impl SyncResponse {
    fn decode(version: i16, body: Bytes) -> Self {
        let mut decoder = Decoder::new(body);
        if version >= 1 {
            decoder.read_i32().unwrap(); // throttle_time_ms
        }
        let error = decoder.read_i16().unwrap();
        let protocol_name = if version >= 5 {
            decoder.read_compact_nullable_string().unwrap(); // protocol_type
            decoder.read_compact_nullable_string().unwrap()
        } else {
            None
        };
        let assignment = read_bytes_field(&mut decoder, version >= 4);
        if version >= 4 {
            decoder.read_tagged_fields().unwrap();
        }
        assert_eq!(
            decoder.remaining(),
            0,
            "SyncGroup v{version} response has trailing bytes"
        );
        Self {
            error,
            protocol_name,
            assignment,
        }
    }
}

fn read_nullable(decoder: &mut Decoder, flexible: bool) -> Option<String> {
    if flexible {
        decoder.read_compact_nullable_string().unwrap()
    } else {
        decoder.read_nullable_string().unwrap()
    }
}

fn read_array_count(decoder: &mut Decoder, flexible: bool) -> usize {
    if flexible {
        usize::try_from(decoder.read_varint().unwrap() - 1).expect("count fits usize")
    } else {
        usize::try_from(decoder.read_i32().unwrap()).expect("count fits usize")
    }
}

fn read_bytes_field(decoder: &mut Decoder, flexible: bool) -> Bytes {
    if flexible {
        decoder.read_compact_nullable_bytes().unwrap()
    } else {
        decoder.read_nullable_bytes().unwrap()
    }
    .expect("bytes field is not nullable")
}

// ── JoinGroup ───────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn given_an_empty_member_id_when_joining_at_v9_should_require_a_member_id() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let response = join(&state, 9, &join_params("", protocols)).await;

    assert_eq!(response.error, ERROR_MEMBER_ID_REQUIRED);
    assert!(!response.member_id.is_empty());
    assert_eq!(response.generation_id, -1);
    assert!(response.leader.is_empty());
    assert!(response.members.is_empty());
    assert_eq!(
        response.protocol_name, None,
        "v9 encodes an absent protocol name as null"
    );
}

/// `protocol_name` only became nullable on the wire at v7. Below that a client reads it as a
/// non-nullable string, so an error response has to carry an empty one.
#[tokio::test(start_paused = true)]
async fn given_an_empty_member_id_when_joining_at_v6_should_send_an_empty_protocol_name() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let response = join(&state, 6, &join_params("", protocols)).await;

    assert_eq!(response.error, ERROR_MEMBER_ID_REQUIRED);
    assert_eq!(response.protocol_name.as_deref(), Some(""));
}

/// KIP-394 arrived in v4, so an older client never sees `MEMBER_ID_REQUIRED` and is admitted on
/// its first request.
#[tokio::test(start_paused = true)]
async fn given_an_empty_member_id_when_joining_at_v3_should_admit_the_member_directly() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let response = join(&state, 3, &join_params("", protocols)).await;

    assert_eq!(response.error, ERROR_NONE);
    assert_eq!(response.generation_id, 1);
    assert!(!response.member_id.is_empty());
    assert_eq!(response.leader, response.member_id);
    assert_eq!(response.protocol_name.as_deref(), Some("range"));
    assert_eq!(response.members.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn given_two_members_when_both_join_should_send_the_roster_only_to_the_leader() {
    let state = test_state(immediate_config());
    let leader_protocols: &[(&str, &[u8])] = &[("range", b"leader-subscription")];

    let leader = claim_member_id(&state, b"leader-subscription").await;
    let first = join(
        &state,
        JOIN_VERSION,
        &join_params(&leader, leader_protocols),
    )
    .await;
    assert_eq!(first.generation_id, 1);
    assert_eq!(first.members.len(), 1);

    let follower = claim_member_id(&state, b"follower-subscription").await;
    let parked = {
        let state = Arc::clone(&state);
        let follower = follower.clone();
        tokio::spawn(async move {
            let protocols: &[(&str, &[u8])] = &[("range", b"follower-subscription")];
            join(&state, JOIN_VERSION, &join_params(&follower, protocols)).await
        })
    };
    yield_to_parked().await;
    assert!(
        !parked.is_finished(),
        "the follower must wait for the leader to rejoin"
    );

    let rejoined = join(
        &state,
        JOIN_VERSION,
        &join_params(&leader, leader_protocols),
    )
    .await;
    let follower_join = parked.await.expect("parked JoinGroup task");

    assert_eq!(rejoined.generation_id, 2);
    assert_eq!(rejoined.leader, leader);
    assert_eq!(rejoined.members.len(), 2);
    assert_eq!(
        rejoined.metadata_of(&leader).map(Bytes::as_ref),
        Some(b"leader-subscription".as_slice())
    );
    assert_eq!(
        rejoined.metadata_of(&follower).map(Bytes::as_ref),
        Some(b"follower-subscription".as_slice()),
        "the leader must receive each member's own subscription bytes, verbatim"
    );
    assert_eq!(follower_join.generation_id, 2);
    assert_eq!(follower_join.leader, leader);
    assert!(
        follower_join.members.is_empty(),
        "a follower runs no assignor and must not receive the roster"
    );
    assert_eq!(follower_join.protocol_type.as_deref(), Some("consumer"));
}

// ── SyncGroup ───────────────────────────────────────────────────────────────

/// Acceptance criterion: two consumers in one group receive a disjoint assignment. The gateway's
/// share of that is the relay - each member is handed back exactly the bytes the leader filed
/// under its id, and nothing else.
#[tokio::test(start_paused = true)]
async fn given_a_leader_assignment_when_syncing_should_deliver_each_member_its_own_blob() {
    let state = test_state(immediate_config());
    let (leader, follower) = two_member_group(&state).await;

    let parked = {
        let state = Arc::clone(&state);
        let follower = follower.clone();
        tokio::spawn(async move {
            sync(
                &state,
                SYNC_VERSION,
                &SyncGroupParams {
                    group_id: GROUP,
                    generation_id: 2,
                    member_id: &follower,
                    ..SyncGroupParams::default()
                },
            )
            .await
        })
    };
    yield_to_parked().await;
    assert!(
        !parked.is_finished(),
        "a follower must wait for the leader's assignment"
    );

    let leader_blob: &[u8] = b"partitions-0-1";
    let follower_blob: &[u8] = b"partitions-2-3";
    let assignments: &[(&str, &[u8])] = &[
        (leader.as_str(), leader_blob),
        (follower.as_str(), follower_blob),
    ];
    let leader_sync = sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 2,
            member_id: &leader,
            assignments,
            ..SyncGroupParams::default()
        },
    )
    .await;
    let follower_sync = parked.await.expect("parked SyncGroup task");

    assert_eq!(leader_sync.error, ERROR_NONE);
    assert_eq!(follower_sync.error, ERROR_NONE);
    assert_eq!(leader_sync.assignment.as_ref(), leader_blob);
    assert_eq!(follower_sync.assignment.as_ref(), follower_blob);
    assert_ne!(leader_sync.assignment, follower_sync.assignment);
    assert_eq!(leader_sync.protocol_name.as_deref(), Some("range"));
}

#[tokio::test(start_paused = true)]
async fn given_a_member_the_leader_omitted_when_syncing_should_return_an_empty_assignment() {
    let state = test_state(immediate_config());
    let (leader, follower) = two_member_group(&state).await;

    let assignments: &[(&str, &[u8])] = &[(leader.as_str(), b"everything")];
    let leader_sync = sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 2,
            member_id: &leader,
            assignments,
            ..SyncGroupParams::default()
        },
    )
    .await;
    let follower_sync = sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 2,
            member_id: &follower,
            ..SyncGroupParams::default()
        },
    )
    .await;

    assert_eq!(leader_sync.assignment.as_ref(), b"everything");
    assert_eq!(follower_sync.error, ERROR_NONE);
    assert!(follower_sync.assignment.is_empty());
}

#[tokio::test(start_paused = true)]
async fn given_a_wrong_protocol_name_when_syncing_at_v5_should_return_inconsistent_group_protocol()
{
    let state = test_state(immediate_config());
    let (leader, _) = two_member_group(&state).await;

    let response = sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 2,
            member_id: &leader,
            protocol_type: Some("consumer"),
            protocol_name: Some("sticky"),
            ..SyncGroupParams::default()
        },
    )
    .await;

    assert_eq!(response.error, ERROR_INCONSISTENT_GROUP_PROTOCOL);
    assert!(response.assignment.is_empty());
}

#[tokio::test(start_paused = true)]
async fn given_a_stale_generation_when_syncing_should_return_illegal_generation() {
    let state = test_state(immediate_config());
    let (leader, _) = two_member_group(&state).await;

    let response = sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 1,
            member_id: &leader,
            ..SyncGroupParams::default()
        },
    )
    .await;

    assert_eq!(response.error, ERROR_ILLEGAL_GENERATION);
}

// ── Heartbeat ───────────────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn given_a_stable_group_when_a_new_member_joins_should_answer_heartbeat_rebalance_in_progress()
 {
    let state = test_state(immediate_config());
    let leader_protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let leader = claim_member_id(&state, b"sub").await;
    join(
        &state,
        JOIN_VERSION,
        &join_params(&leader, leader_protocols),
    )
    .await;
    sync(
        &state,
        SYNC_VERSION,
        &SyncGroupParams {
            group_id: GROUP,
            generation_id: 1,
            member_id: &leader,
            assignments: &[],
            ..SyncGroupParams::default()
        },
    )
    .await;
    assert_eq!(heartbeat(&state, 1, &leader).await, ERROR_NONE);

    let follower = claim_member_id(&state, b"sub").await;
    let parked = {
        let state = Arc::clone(&state);
        let follower = follower.clone();
        tokio::spawn(async move {
            let protocols: &[(&str, &[u8])] = &[("range", b"sub")];
            join(&state, JOIN_VERSION, &join_params(&follower, protocols)).await
        })
    };
    yield_to_parked().await;

    assert_eq!(
        heartbeat(&state, 1, &leader).await,
        ERROR_REBALANCE_IN_PROGRESS,
        "a stable member learns about a rebalance through its heartbeat"
    );

    let rejoined = join(
        &state,
        JOIN_VERSION,
        &join_params(&leader, leader_protocols),
    )
    .await;
    let follower_join = parked.await.expect("parked JoinGroup task");
    assert_eq!(rejoined.generation_id, 2);
    assert_eq!(follower_join.generation_id, 2);
}

/// Acceptance criterion: a heartbeat timeout evicts the member and triggers a rebalance.
#[tokio::test(start_paused = true)]
async fn given_a_missed_heartbeat_when_the_session_expires_should_evict_and_bump_the_generation() {
    let state = test_state(immediate_config());
    let leader_protocols: &[(&str, &[u8])] = &[("range", b"leader-subscription")];
    let (leader, follower) = two_member_group(&state).await;

    // Both sessions are still alive here; the leader refreshes its own, the follower goes quiet.
    advance(Duration::from_secs(6)).await;
    assert_eq!(heartbeat(&state, 2, &leader).await, ERROR_NONE);

    // Past the follower's deadline but not the leader's refreshed one.
    advance(Duration::from_secs(5)).await;
    assert_eq!(
        heartbeat(&state, 2, &leader).await,
        ERROR_REBALANCE_IN_PROGRESS,
        "the expired follower must be evicted and a rebalance started"
    );

    let rejoined = join(
        &state,
        JOIN_VERSION,
        &join_params(&leader, leader_protocols),
    )
    .await;
    assert_eq!(rejoined.error, ERROR_NONE);
    assert_eq!(rejoined.generation_id, 3);
    assert_eq!(rejoined.members.len(), 1);
    assert_eq!(rejoined.metadata_of(&follower), None);

    assert_eq!(
        heartbeat(&state, 2, &follower).await,
        ERROR_UNKNOWN_MEMBER_ID,
        "the evicted member must be told to rejoin from scratch"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_stale_generation_when_heartbeating_should_return_illegal_generation() {
    let state = test_state(immediate_config());
    let (leader, _) = two_member_group(&state).await;

    assert_eq!(
        heartbeat(&state, 1, &leader).await,
        ERROR_ILLEGAL_GENERATION
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_unknown_group_when_heartbeating_should_return_unknown_member_id() {
    let state = test_state(immediate_config());

    assert_eq!(heartbeat(&state, 1, "ghost").await, ERROR_UNKNOWN_MEMBER_ID);
}

/// Every member's session lapsed while nobody was talking to the group. The next joiner evicts
/// them on arrival instead of waiting out a rebalance window for members that will never rejoin:
/// the group it lands in is brand new, so its generation is 1 and not 2.
#[tokio::test(start_paused = true)]
async fn given_every_member_expired_when_a_new_member_joins_should_start_a_fresh_group() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let first = join(&state, 3, &join_params("", protocols)).await;
    assert_eq!(first.generation_id, 1);

    advance(Duration::from_secs(11)).await;
    let second = join(&state, 3, &join_params("", protocols)).await;

    assert_eq!(second.error, ERROR_NONE);
    assert_eq!(second.generation_id, 1);
    assert_eq!(second.leader, second.member_id);
    assert_eq!(second.members.len(), 1);
    assert_ne!(second.member_id, first.member_id);
}

// ── Rejected requests ───────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn given_a_session_timeout_out_of_range_when_joining_should_return_invalid_session_timeout() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let too_short = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            session_timeout_ms: 1_000,
            ..join_params("", protocols)
        },
    )
    .await;
    let too_long = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            session_timeout_ms: 3_600_000,
            ..join_params("", protocols)
        },
    )
    .await;

    assert_eq!(too_short.error, ERROR_INVALID_SESSION_TIMEOUT);
    assert_eq!(too_long.error, ERROR_INVALID_SESSION_TIMEOUT);
}

/// 246 bytes is what an Iggy name leaves for a group id once the `kafka.cg.` offset-key prefix
/// is accounted for, so a longer one could never have its offsets committed.
#[tokio::test(start_paused = true)]
async fn given_an_out_of_range_group_id_when_joining_should_return_invalid_group_id() {
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];
    let longest = "g".repeat(246);
    let too_long = "g".repeat(247);

    let empty = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            group_id: "",
            ..join_params("", protocols)
        },
    )
    .await;
    let oversized = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            group_id: &too_long,
            ..join_params("", protocols)
        },
    )
    .await;
    let accepted = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            group_id: &longest,
            ..join_params("", protocols)
        },
    )
    .await;

    assert_eq!(empty.error, ERROR_INVALID_GROUP_ID);
    assert_eq!(oversized.error, ERROR_INVALID_GROUP_ID);
    assert_eq!(accepted.error, ERROR_MEMBER_ID_REQUIRED);
}

#[tokio::test(start_paused = true)]
async fn given_a_conflicting_protocol_type_when_joining_should_return_inconsistent_group_protocol()
{
    let state = test_state(immediate_config());
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];
    join(&state, 3, &join_params("", protocols)).await;

    let response = join(
        &state,
        JOIN_VERSION,
        &JoinGroupParams {
            protocol_type: "connect",
            ..join_params("", protocols)
        },
    )
    .await;

    assert_eq!(response.error, ERROR_INCONSISTENT_GROUP_PROTOCOL);
}

#[tokio::test(start_paused = true)]
async fn given_no_shared_protocol_when_joining_should_return_inconsistent_group_protocol() {
    let state = test_state(immediate_config());
    let first: &[(&str, &[u8])] = &[("range", b"sub")];
    let second: &[(&str, &[u8])] = &[("sticky", b"sub")];
    join(&state, 3, &join_params("", first)).await;

    let response = join(&state, 3, &join_params("", second)).await;

    assert_eq!(response.error, ERROR_INCONSISTENT_GROUP_PROTOCOL);
}

#[tokio::test(start_paused = true)]
async fn given_a_group_at_its_size_cap_when_joining_should_return_group_max_size_reached() {
    let state = test_state(GroupCoordinatorConfig {
        max_members_per_group: 1,
        ..immediate_config()
    });
    let protocols: &[(&str, &[u8])] = &[("range", b"sub")];

    let admitted = join(&state, 3, &join_params("", protocols)).await;
    let rejected = join(&state, 3, &join_params("", protocols)).await;

    assert_eq!(admitted.error, ERROR_NONE);
    assert_eq!(rejected.error, ERROR_GROUP_MAX_SIZE_REACHED);
}

#[tokio::test(start_paused = true)]
async fn given_an_oversized_subscription_when_joining_should_return_invalid_request() {
    let state = test_state(GroupCoordinatorConfig {
        max_member_blob_bytes: 8,
        ..immediate_config()
    });
    let protocols: &[(&str, &[u8])] = &[("range", b"a-subscription-over-eight-bytes")];

    let response = join(&state, 3, &join_params("", protocols)).await;

    assert_eq!(response.error, ERROR_INVALID_REQUEST);
}

// ── FindCoordinator ─────────────────────────────────────────────────────────

async fn find_coordinator(
    state: &GatewayState,
    version: i16,
    keys: &[&str],
    key_type: i8,
) -> Decoder {
    let body = build_find_coordinator_request(version, keys, key_type);
    let response = handle_request_bounded(state, API_KEY_FIND_COORDINATOR, version, body)
        .await
        .expect_response("FindCoordinator must answer");
    Decoder::new(response)
}

#[tokio::test(start_paused = true)]
async fn given_a_group_key_when_finding_the_coordinator_at_v0_should_return_this_broker() {
    let state = test_state(immediate_config());
    let broker = BrokerAdvertise::default();

    let mut decoder = find_coordinator(&state, 0, &[GROUP], 0).await;

    assert_eq!(decoder.read_i16().unwrap(), ERROR_NONE);
    assert_eq!(decoder.read_i32().unwrap(), 1, "node id");
    assert_eq!(decoder.read_nullable_string().unwrap(), Some(broker.host));
    assert_eq!(decoder.read_i32().unwrap(), broker.port);
    assert_eq!(decoder.remaining(), 0);
}

#[tokio::test(start_paused = true)]
async fn given_several_keys_when_finding_the_coordinator_at_v4_should_return_one_entry_each() {
    let state = test_state(immediate_config());
    let keys = ["alpha", "beta", "gamma"];

    let mut decoder = find_coordinator(&state, 4, &keys, 0).await;

    decoder.read_i32().unwrap(); // throttle_time_ms
    assert_eq!(read_array_count(&mut decoder, true), keys.len());
    for key in keys {
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap().as_deref(),
            Some(key)
        );
        assert_eq!(decoder.read_i32().unwrap(), 1, "node id");
        decoder.read_compact_nullable_string().unwrap(); // host
        decoder.read_i32().unwrap(); // port
        assert_eq!(decoder.read_i16().unwrap(), ERROR_NONE);
        assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
        decoder.read_tagged_fields().unwrap();
    }
    decoder.read_tagged_fields().unwrap();
    assert_eq!(decoder.remaining(), 0);
}

/// Transactions are out of scope permanently, so the answer is a terminal error rather than a
/// retriable one a transactional producer would spin on forever.
#[tokio::test(start_paused = true)]
async fn given_a_transaction_key_type_when_finding_the_coordinator_should_return_invalid_request() {
    let state = test_state(immediate_config());

    let mut decoder = find_coordinator(&state, 1, &["txn"], 1).await;

    decoder.read_i32().unwrap(); // throttle_time_ms
    assert_eq!(decoder.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    assert!(decoder.read_nullable_string().unwrap().is_some());
    assert_eq!(decoder.read_i32().unwrap(), -1, "node id");
    assert_eq!(decoder.read_nullable_string().unwrap().as_deref(), Some(""));
    assert_eq!(decoder.read_i32().unwrap(), -1, "port");
    assert_eq!(decoder.remaining(), 0);
}

// ── Over a real TCP listener ────────────────────────────────────────────────

/// The in-process tests drive one `GatewayState` directly. This one proves the connection loop
/// itself parks and resumes: a follower's `SyncGroup` sits unanswered on its own socket until
/// the leader's arrives on another.
#[tokio::test]
async fn given_two_tcp_clients_when_they_join_and_sync_should_each_receive_their_own_assignment() {
    let (addr, _shutdown) = spawn_test_server_with_config(GatewayConfig {
        group: immediate_config(),
        ..GatewayConfig::default()
    })
    .await;

    let mut leader_stream = TcpStream::connect(addr).await.expect("connect leader");
    let mut follower_stream = TcpStream::connect(addr).await.expect("connect follower");

    let leader_protocols: &[(&str, &[u8])] = &[("range", b"leader-subscription")];
    let follower_protocols: &[(&str, &[u8])] = &[("range", b"follower-subscription")];

    let leader = tcp_join(&mut leader_stream, &join_params("", leader_protocols))
        .await
        .member_id;
    let first = tcp_join(&mut leader_stream, &join_params(&leader, leader_protocols)).await;
    assert_eq!(first.generation_id, 1);

    let follower = tcp_join(&mut follower_stream, &join_params("", follower_protocols))
        .await
        .member_id;
    // The follower's rejoin blocks until the leader rejoins, so only the request is written here.
    write_request(
        &mut follower_stream,
        API_KEY_JOIN_GROUP,
        JOIN_VERSION,
        2,
        &build_join_group_request(JOIN_VERSION, &join_params(&follower, follower_protocols)),
    )
    .await;

    let rejoined = tcp_join(&mut leader_stream, &join_params(&leader, leader_protocols)).await;
    assert_eq!(rejoined.generation_id, 2);
    assert_eq!(rejoined.members.len(), 2);
    let follower_join = read_join(&mut follower_stream).await;
    assert_eq!(follower_join.generation_id, 2);
    assert!(follower_join.members.is_empty());

    write_request(
        &mut follower_stream,
        API_KEY_SYNC_GROUP,
        SYNC_VERSION,
        3,
        &build_sync_group_request(
            SYNC_VERSION,
            &SyncGroupParams {
                group_id: GROUP,
                generation_id: 2,
                member_id: &follower,
                ..SyncGroupParams::default()
            },
        ),
    )
    .await;

    let leader_blob: &[u8] = b"partitions-0-1";
    let follower_blob: &[u8] = b"partitions-2-3";
    let assignments: &[(&str, &[u8])] = &[
        (leader.as_str(), leader_blob),
        (follower.as_str(), follower_blob),
    ];
    write_request(
        &mut leader_stream,
        API_KEY_SYNC_GROUP,
        SYNC_VERSION,
        4,
        &build_sync_group_request(
            SYNC_VERSION,
            &SyncGroupParams {
                group_id: GROUP,
                generation_id: 2,
                member_id: &leader,
                assignments,
                ..SyncGroupParams::default()
            },
        ),
    )
    .await;

    let leader_sync = read_sync(&mut leader_stream).await;
    let follower_sync = read_sync(&mut follower_stream).await;

    assert_eq!(leader_sync.assignment.as_ref(), leader_blob);
    assert_eq!(follower_sync.assignment.as_ref(), follower_blob);
}

async fn write_request(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &Bytes,
) {
    let frame = build_request_frame(
        api_key,
        api_version,
        correlation_id,
        Some("consumer-group-test"),
        body,
    );
    stream.write_all(&frame).await.expect("write request");
}

async fn tcp_join(stream: &mut TcpStream, params: &JoinGroupParams<'_>) -> JoinResponse {
    write_request(
        stream,
        API_KEY_JOIN_GROUP,
        JOIN_VERSION,
        1,
        &build_join_group_request(JOIN_VERSION, params),
    )
    .await;
    read_join(stream).await
}

async fn read_join(stream: &mut TcpStream) -> JoinResponse {
    let payload = read_response_frame(stream, 8 * 1024 * 1024).await;
    let (_, body) = parse_response_payload(API_KEY_JOIN_GROUP, JOIN_VERSION, payload);
    JoinResponse::decode(JOIN_VERSION, body)
}

async fn read_sync(stream: &mut TcpStream) -> SyncResponse {
    let payload = read_response_frame(stream, 8 * 1024 * 1024).await;
    let (_, body) = parse_response_payload(API_KEY_SYNC_GROUP, SYNC_VERSION, payload);
    SyncResponse::decode(SYNC_VERSION, body)
}
