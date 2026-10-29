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

//! End-to-end tests of the reader on fake replicas.

use std::iter::{once, repeat_n};
use std::sync::Arc;
use std::task::{Context, Waker};
use std::time::Duration;

use iggy_binary_protocol::codes::{
    CREATE_CONSUMER_GROUP_CODE, FETCH_MESSAGES_CODE, GET_CONSUMER_GROUP_CODE,
    JOIN_CONSUMER_GROUP_CODE, LOGIN_REGISTER_CODE, POLL_MESSAGES_DEFERRED_CODE,
    STORE_CONSUMER_OFFSET_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::consensus::EvictionReason;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::messages::{DeferredPollMessagesRequest, FetchMessagesRequest};
use iggy_binary_protocol::responses::consumer_groups::{
    ConsumerGroupDetailsResponse, ConsumerGroupResponse, SyncConsumerGroupResponse,
};
use iggy_binary_protocol::responses::messages::{FetchMessagesResponse, PartitionRouteResponse};
use iggy_binary_protocol::{WireDecode, WireEncode, WireName};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    BinaryTransport, Client, Consumer, ConsumerOffsetClient, DiagnosticEvent, IggyDuration,
    IggyError, PollOptions, PolledMessages, PollingStrategy, ServerCommitPolicy, UserClient,
};
use tokio::task::yield_now;
use tokio::time::{Instant, sleep, timeout};

use super::fixtures::{
    CONSUMER_ID, GROUP_ID, STREAM_ID, Section, TOPIC_ID, builder_on, committing, fetch_body, group,
    identifier, partition_and_offsets, refused, served, stored_offsets,
};
use super::{ORDERING, PartitionReader, ReadError};
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::reader_builder::ReaderBuilder;
use crate::links::OPERATION_TIMEOUT;
use crate::links::fake_cluster::{
    Answer, FakeCluster, Received, TEST_BUDGET, connected, endpoint, eventually,
};
use crate::tcp::tcp_client::TcpClient;

const REPLICAS: u8 = 3;
const METADATA_PRIMARY: u8 = 0;
/// The primary of every partition until a test moves one.
const PRIMARY: u8 = 1;
const MOVED_PRIMARY: u8 = 2;
const FIRST_VIEW: u32 = 0;
const MOVED_VIEW: u32 = 1;
const INCARNATION: u64 = 1;
const METADATA_OP: u64 = 1;
const PARTITION: u32 = 1;
const OTHER_PARTITION: u32 = 0;
const PARTITIONS_COUNT: u32 = PARTITION + 1;
const GROUP_NAME: &str = "group";
const GROUP_GENERATION: u64 = 1;
const USERNAME: &str = "iggy";
/// The commit of each scripted metadata reply.
const COMMIT_OP: u64 = 1;
const RETRY_INTERVAL: Duration = Duration::from_millis(10);
/// Long enough for every refresh and retry of a reader, and below the deadline of a read.
const POLL_TIMEOUT: Duration = Duration::from_secs(2);
/// The unanswered reads after the scripted ones. A reader that reads ahead then waits instead
/// of spinning on the empty replies of the fake replicas, which stops the paused clock.
const SILENT_READS: usize = 8;
const GROUP_REFRESHES: usize = 16;

#[tokio::test(start_paused = true)]
async fn given_reader_options_when_reading_should_send_them_in_each_request() {
    const COUNT: u32 = 10;
    const MIN_COUNT: u32 = 3;
    const MAX_WAIT: Duration = Duration::from_secs(2);
    const MAX_BYTES: u32 = 256 * 1024;
    let options = PollOptions {
        count: COUNT,
        max_wait: IggyDuration::from(MAX_WAIT),
        min_count: MIN_COUNT,
        max_bytes: MAX_BYTES,
        server_commit: ServerCommitPolicy::Disabled,
    };
    let wait_us = options.max_wait.as_micros();
    let request_timeout_us = options.request_timeout().as_micros();
    let fetch_records_limit =
        MAX_BYTES - u32::try_from(FetchMessagesResponse::head_size(1)).unwrap();
    for (start, code, max_bytes) in [
        (
            PollingStrategy::next(),
            FETCH_MESSAGES_CODE,
            fetch_records_limit,
        ),
        (
            PollingStrategy::last(),
            POLL_MESSAGES_DEFERRED_CODE,
            MAX_BYTES,
        ),
    ] {
        let (client, cluster) = connect_to_three_replicas().await;
        cluster.script(
            &endpoint(PRIMARY),
            code,
            repeat_n(Answer::Silence, SILENT_READS),
        );
        let _reader = standalone(client, [PARTITION])
            .start(start)
            .options(options)
            .build()
            .await
            .unwrap();

        eventually("the first read arrives", || cluster.count(code) == 1).await;

        let body = &cluster.received(code)[0].body;
        let sent = if code == FETCH_MESSAGES_CODE {
            let (request, _) = FetchMessagesRequest::decode(body).unwrap();
            assert!(
                request
                    .targets
                    .iter()
                    .all(|target| target.max_bytes == max_bytes),
                "the whole reply fits the reservation of one reply"
            );
            (
                request.count,
                request.wait_us,
                request.min_count,
                request.max_bytes,
                request.request_timeout_us,
            )
        } else {
            let (request, _) = DeferredPollMessagesRequest::decode(body).unwrap();
            (
                request.poll.count,
                request.wait_us,
                request.min_count,
                request.max_bytes,
                request.request_timeout_us,
            )
        };
        assert_eq!(
            sent,
            (COUNT, wait_us, MIN_COUNT, max_bytes, request_timeout_us),
            "{start:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_group_member_when_reading_should_fetch_the_partitions_of_a_primary_with_one_request()
 {
    const OFFSET: u64 = 7;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[OTHER_PARTITION, PARTITION]);
    script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);
    let mut reader = committing(member(client)).build().await.unwrap();

    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();

    assert_eq!(partition_and_offsets(&batch), (PARTITION, vec![OFFSET]));
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert_eq!(fetches.len(), 1, "one committing fetch per primary");
    let request = fetch_request(&fetches[0]);
    assert!(request.auto_commit);
    assert_eq!(request.count, reader.config.options.count);
    assert_eq!(partitions_of(&fetches[0]), [OTHER_PARTITION, PARTITION]);
    let records_limit = reader.config.options.max_bytes
        - u32::try_from(FetchMessagesResponse::head_size(request.targets.len())).unwrap();
    assert_eq!(request.max_bytes, records_limit);
    assert!(
        request
            .targets
            .iter()
            .all(|target| target.max_bytes == records_limit),
        "the whole reply fits the reservation of one reply"
    );
    assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 0);
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_store_of_a_second_target_when_a_committing_member_fetches_should_leave_its_partition_out()
 {
    const OFFSET: u64 = 7;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[OTHER_PARTITION, PARTITION]);
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [
            Answer::Refuse(IggyError::TransientNotCommitted),
            Answer::Refuse(IggyError::RequestTooOld),
        ],
    );
    script_fetches(&cluster, [served_answer(OTHER_PARTITION, OFFSET)]);
    let stored = client
        .store_consumer_offset(
            &group(),
            &identifier(STREAM_ID),
            &identifier(TOPIC_ID),
            Some(PARTITION),
            OFFSET,
        )
        .await;
    let store = cluster.received(STORE_CONSUMER_OFFSET_CODE)[0]
        .header
        .request;
    assert_eq!(stored, Err(IggyError::OperationUncertain(store)));
    let mut reader = committing(member(client)).build().await.unwrap();

    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();
    let blocked = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();

    assert_eq!(
        partition_and_offsets(&batch),
        (OTHER_PARTITION, vec![OFFSET]),
        "the other target of the group fetches on"
    );
    assert_eq!(
        blocked.unwrap_err(),
        ReadError::new(
            Some(PARTITION),
            IggyError::PartitionLaneBlocked(PARTITION, store)
        ),
        "a replay of the store after a commit of its partition would move the offset back"
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert!(
        !fetches.is_empty()
            && fetches
                .iter()
                .all(|fetch| partitions_of(fetch) == [OTHER_PARTITION]),
        "no fetch reads the blocked partition"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_missing_group_when_building_should_return_the_join_error() {
    let (client, _cluster) = connect_to_three_replicas().await;

    let built = member(client).build().await;

    assert!(
        matches!(built, Err(IggyError::ConsumerGroupNameNotFound(..))),
        "{built:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_join_when_building_again_should_keep_it_until_the_caller_resolves_it() {
    // Each build looks the group up, the blocked one too.
    const BUILDS: usize = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), BUILDS),
    );
    cluster.script(
        &metadata_primary,
        JOIN_CONSUMER_GROUP_CODE,
        [Answer::Silence, Answer::Commit(COMMIT_OP)],
    );
    let client = IggyRwLock::new(ClientWrapper::Tcp(client));
    let reader = || {
        ReaderBuilder::new(
            client.clone(),
            identifier(STREAM_ID),
            identifier(TOPIC_ID),
            group(),
            None,
        )
    };

    let lost = reader().build().await.err();
    let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[0].header.request;
    assert_eq!(lost, Some(IggyError::OperationUncertain(op)));
    assert_eq!(
        reader().build().await.err(),
        Some(IggyError::LaneBlocked(op)),
        "the client keeps the join of the failed build"
    );
    assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 1);

    client
        .read()
        .await
        .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
        .await
        .unwrap();
    reader().build().await.unwrap();

    let joins = cluster.received(JOIN_CONSUMER_GROUP_CODE);
    assert_eq!(joins.len(), 3);
    assert_eq!(
        joins[1].header.request, op,
        "the resolution replays the lost join"
    );
    assert_ne!(joins[2].header.request, op);
}

#[tokio::test(start_paused = true)]
async fn given_a_lost_membership_when_the_join_fails_should_report_it_for_the_whole_reader() {
    let (client, cluster) = connect_to_three_replicas().await;
    // Only the join of `build()` finds the group.
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        [existing_group()],
    );
    let mut reader = member(client).build().await.unwrap();
    forget_membership(&reader);

    let refused = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();

    assert!(
        matches!(
            refused,
            Err(ReadError {
                partition_id: None,
                error: IggyError::ConsumerGroupNameNotFound(..),
                ..
            })
        ),
        "{refused:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_member_unknown_to_the_server_when_refreshing_should_join_again_without_an_error() {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), GROUP_REFRESHES),
    );
    let not_member = IggyError::ConsumerGroupMemberNotFound(
        u32::default(),
        identifier(GROUP_ID),
        identifier(TOPIC_ID),
    );
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        SYNC_CONSUMER_GROUP_CODE,
        once(Answer::Refuse(not_member)).chain(repeat_n(assignment(&[PARTITION]), GROUP_REFRESHES)),
    );
    script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);
    let mut reader = member(client).build().await.unwrap();

    let read = timeout(TEST_BUDGET, reader.next()).await.unwrap();

    assert!(
        matches!(&read, Ok(batch) if partition_and_offsets(batch) == (PARTITION, vec![OFFSET])),
        "the refresh joins again instead of an error: {read:?}"
    );
    assert_eq!(cluster.count(JOIN_CONSUMER_GROUP_CODE), 2);
}

#[tokio::test(start_paused = true)]
async fn given_an_uncertain_group_mutation_when_the_refresh_joins_again_should_resolve_it_and_read()
{
    const OFFSET: u64 = 2;
    for mutation in [CREATE_CONSUMER_GROUP_CODE, JOIN_CONSUMER_GROUP_CODE] {
        let (client, cluster) = connect_to_three_replicas().await;
        let creates = mutation == CREATE_CONSUMER_GROUP_CODE;
        let metadata_primary = endpoint(METADATA_PRIMARY);
        let group_at_rejoin = if creates {
            Answer::Body(Vec::new())
        } else {
            existing_group()
        };
        cluster.script(
            &metadata_primary,
            GET_CONSUMER_GROUP_CODE,
            [existing_group(), group_at_rejoin, existing_group()],
        );
        // The join of `build()` goes through.
        let earlier_mutations = usize::from(!creates);
        if !creates {
            cluster.script(&metadata_primary, mutation, [Answer::Commit(COMMIT_OP)]);
        }
        cluster.script(
            &metadata_primary,
            mutation,
            [Answer::Silence, Answer::Commit(COMMIT_OP)],
        );
        cluster.script(
            &metadata_primary,
            SYNC_CONSUMER_GROUP_CODE,
            repeat_n(assignment(&[PARTITION]), GROUP_REFRESHES),
        );
        script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);
        let mut reader = member(client).create_group(true).build().await.unwrap();
        forget_membership(&reader);

        let uncertain = timeout(TEST_BUDGET, reader.next()).await.unwrap();
        let read = timeout(TEST_BUDGET, reader.next()).await.unwrap();

        let requests = cluster.received(mutation);
        let (lost, replayed) = (
            &requests[earlier_mutations],
            &requests[earlier_mutations + 1],
        );
        let op = lost.header.request;
        assert_eq!(
            uncertain.unwrap_err(),
            ReadError::new(None, IggyError::OperationUncertain(op)),
            "mutation {mutation}"
        );
        assert!(read.is_ok(), "mutation {mutation}: {read:?}");
        assert_eq!(
            (replayed.header.request, &replayed.body),
            (op, &lost.body),
            "the next join resolves the mutation by its replay, mutation {mutation}"
        );
        assert_eq!(
            reader
                .config
                .client
                .read()
                .await
                .resolve_operation(op, (Instant::now() + OPERATION_TIMEOUT).into_std())
                .await,
            Ok(()),
            "mutation {mutation}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn given_a_live_session_when_a_replica_refuses_a_fetch_with_stale_client_should_fetch_again_without_a_new_sign_in()
 {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [Answer::Detached, served_answer(PARTITION, OFFSET)],
    );
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();

    let refused = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();

    assert_eq!(
        refused.unwrap_err(),
        ReadError::new(None, IggyError::StaleClient)
    );
    next_batch(&mut reader).await;
    assert!((2..=3).contains(&cluster.count(FETCH_MESSAGES_CODE)));
    assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
}

#[tokio::test(start_paused = true)]
async fn given_a_lost_route_link_when_a_fetch_outlasts_it_should_fetch_again_once_the_link_is_back()
{
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();
    let primary = endpoint(PRIMARY);
    cluster.down.lock().unwrap().insert(primary.clone());
    cluster.drop_connections(&primary);

    let lost = timeout(POLL_TIMEOUT, reader.next()).await;
    assert!(
        lost.is_err(),
        "the driver retries a fetch that no replica admitted: {lost:?}"
    );
    cluster.down.lock().unwrap().clear();
    script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);

    next_batch(&mut reader).await;
    assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
}

#[tokio::test(start_paused = true)]
async fn given_an_evicted_fetch_when_the_client_signs_in_again_should_wait_for_signed_in_and_keep_reading_enabled()
 {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();
    *cluster.session.lock().unwrap() = None;
    cluster
        .held_registers
        .lock()
        .unwrap()
        .insert(USERNAME.to_owned());
    script_fetches(
        &cluster,
        [
            Answer::Evict(EvictionReason::NoSession),
            served_answer(PARTITION, OFFSET),
        ],
    );
    let shared = Arc::clone(&reader.shared);

    let evicted = timeout(POLL_TIMEOUT, reader.next()).await;
    assert!(
        evicted.is_err(),
        "reading waits for the new session: {evicted:?}"
    );
    eventually("the client signs in again", || {
        cluster.count(LOGIN_REGISTER_CODE) == 2
    })
    .await;
    eventually("the reader sees the sign-out", || {
        !shared.can_poll.load(ORDERING)
    })
    .await;
    let parked = timeout(POLL_TIMEOUT, reader.next()).await;
    assert!(parked.is_err(), "{parked:?}");
    assert_eq!(cluster.count(FETCH_MESSAGES_CODE), 1);
    cluster.held_registers.lock().unwrap().clear();
    eventually("the reader sees the sign-in", || {
        shared.can_poll.load(ORDERING)
    })
    .await;

    next_batch(&mut reader).await;
    assert!(
        shared.can_poll.load(ORDERING),
        "a late error of the evicted fetch keeps reading enabled"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_lost_committing_fetch_reply_when_closing_should_skip_the_rewind_of_its_partition()
{
    const DELIVERED: u64 = 4;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [served_answer(PARTITION, DELIVERED), Answer::Silence],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    let delivered = reader.next().await.unwrap();
    assert_eq!(
        partition_and_offsets(&delivered),
        (PARTITION, vec![DELIVERED])
    );

    let lost = reader.next().await;
    let op = cluster.received(FETCH_MESSAGES_CODE)[1].header.request;
    let closed = reader.close().await;

    assert_eq!(
        lost.unwrap_err(),
        ReadError::new(Some(PARTITION), IggyError::FetchProgressUncertain(op))
    );
    assert_eq!(closed, Err(IggyError::PartitionLaneBlocked(PARTITION, op)));
    assert!(
        stored_offsets(&cluster).is_empty(),
        "the server can store the offset of the lost reply after any store of the close"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_in_flight_when_closing_should_rewind_over_its_batch() {
    const FIRST: u64 = 5;
    const REPLY_DELAY: Duration = Duration::from_millis(100);
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.hold(FETCH_MESSAGES_CODE);
    script_fetches(&cluster, [served_answer(PARTITION, FIRST)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    let request_timeout = request_timeout(&reader);
    let started = Instant::now();

    let (closed, ()) = tokio::join!(reader.close(), async {
        sleep(REPLY_DELAY).await;
        cluster.release(FETCH_MESSAGES_CODE);
    });

    closed.unwrap();
    assert!(
        started.elapsed() < request_timeout,
        "the close ends with the fetch, not at its deadline"
    );
    assert_eq!(
        stored_offsets(&cluster),
        [FIRST - 1],
        "the server committed a batch that the application never got"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_without_a_reply_when_closing_should_wait_for_its_deadline() {
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, []);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    let op = cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
    let request_timeout = request_timeout(&reader);
    let started = Instant::now();

    let closed = reader.close().await;

    assert!(
        started.elapsed() >= request_timeout,
        "the server can commit until the deadline of the fetch, so the close waits for it"
    );
    assert_eq!(closed, Err(IggyError::PartitionLaneBlocked(PARTITION, op)));
    assert!(stored_offsets(&cluster).is_empty());
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_in_flight_when_the_reader_drops_should_return_at_once_and_block_its_partition()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, []);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    let op = cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
    let request_timeout = request_timeout(&reader);
    let client = reader.config.client.clone();
    let dropped = Instant::now();

    drop(reader);

    let builder = ReaderBuilder::new(
        client,
        identifier(STREAM_ID),
        identifier(TOPIC_ID),
        Consumer::new(identifier(CONSUMER_ID)),
        None,
    )
    .partitions([PARTITION]);
    let mut next_reader = committing(builder).build().await.unwrap();
    let blocked = timeout(POLL_TIMEOUT, next_reader.next()).await.unwrap();
    assert_eq!(
        blocked.unwrap_err(),
        ReadError::new(
            Some(PARTITION),
            IggyError::PartitionLaneBlocked(PARTITION, op)
        ),
        "the server can still commit the dropped fetch"
    );
    assert!(
        dropped.elapsed() < request_timeout,
        "the drop blocks the partition before the deadline of its fetch"
    );
    assert_eq!(cluster.count(FETCH_MESSAGES_CODE), 1);
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_in_flight_when_the_epoch_changes_should_hand_over_its_batch() {
    const FIRST: u64 = 5;
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.hold(FETCH_MESSAGES_CODE);
    script_fetches(&cluster, [served_answer(PARTITION, FIRST)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;

    reader.shared.epoch.fetch_add(1, ORDERING);
    cluster.release(FETCH_MESSAGES_CODE);
    let read = timeout(TEST_BUDGET, reader.next()).await;

    assert!(
        matches!(&read, Ok(Ok(batch)) if partition_and_offsets(batch) == (PARTITION, vec![FIRST])),
        "the server committed this batch, so a new epoch must not drop it: {read:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_signed_out_client_when_reads_fail_with_unauthenticated_should_back_off_between_attempts()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    client.logout_user().await.unwrap();
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();
    let unauthenticated = ReadError::new(None, IggyError::Unauthenticated);
    assert_eq!(reader.next().await.unwrap_err(), unauthenticated);
    let started = Instant::now();

    let refused = reader.next().await;

    assert_eq!(refused.unwrap_err(), unauthenticated);
    assert!(started.elapsed() >= RETRY_INTERVAL);
    assert_eq!(cluster.count(FETCH_MESSAGES_CODE), 0);
}

#[tokio::test(start_paused = true)]
async fn given_a_signed_in_client_when_a_late_disconnected_event_arrives_should_keep_reading() {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    // The events can make the first batch stale, so a second one follows it.
    script_fetches(
        &cluster,
        [
            served_answer(PARTITION, OFFSET),
            served_answer(PARTITION, OFFSET + 1),
        ],
    );
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();

    publish(
        &reader,
        &[DiagnosticEvent::SignedIn, DiagnosticEvent::Disconnected],
    )
    .await;
    sleep(RETRY_INTERVAL).await;
    let read = timeout(POLL_TIMEOUT, reader.next()).await;

    assert!(matches!(read, Ok(Ok(_))), "{read:?}");
    assert!(reader.shared.can_poll.load(ORDERING));
}

#[tokio::test(start_paused = true)]
async fn given_a_group_member_when_a_fetch_finds_its_session_ended_should_sign_in_and_join_again() {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    script_fetches(
        &cluster,
        [
            Answer::Refuse(IggyError::StaleClient),
            served_answer(PARTITION, OFFSET),
        ],
    );
    let mut reader = member(client).build().await.unwrap();

    let refused = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();

    assert_eq!(
        refused.unwrap_err(),
        ReadError::new(None, IggyError::StaleClient)
    );
    next_batch(&mut reader).await;
    assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
    assert!((2..=3).contains(&cluster.count(JOIN_CONSUMER_GROUP_CODE)));
}

#[tokio::test(start_paused = true)]
async fn given_an_unknown_committing_outcome_when_seeking_should_wait_for_the_abandon_and_read_from_the_new_position()
 {
    const SEEK_OFFSET: u64 = 9;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [Answer::Silence, served_answer(PARTITION, SEEK_OFFSET)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    let lost = reader.next().await;
    let op = cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
    assert_eq!(
        lost.unwrap_err(),
        ReadError::new(Some(PARTITION), IggyError::FetchProgressUncertain(op))
    );
    let stopped = timeout(POLL_TIMEOUT, reader.next()).await;
    assert!(stopped.is_err(), "the partition stops: {stopped:?}");
    let seek_start = PollingStrategy::offset(SEEK_OFFSET);

    let blocked = reader.seek(PARTITION, seek_start).await;
    let abandoned = reader
        .config
        .client
        .read()
        .await
        .abandon_operation(op)
        .await;
    let sought = reader.seek(PARTITION, seek_start).await;
    let resumed = timeout(TEST_BUDGET, reader.next()).await.unwrap().unwrap();

    assert_eq!(
        blocked,
        Err(IggyError::PartitionLaneBlocked(PARTITION, op)),
        "the server can still commit the lost fetch"
    );
    assert_eq!(abandoned, Ok(()));
    assert_eq!(sought, Ok(()));
    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, vec![SEEK_OFFSET])
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert_eq!(fetches.len(), 2);
    assert_eq!(
        targets_of(&fetches[1]),
        [(PARTITION, WirePollingStrategy::offset(SEEK_OFFSET))]
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_partition_refused_beside_a_served_one_when_a_fetch_completes_should_deliver_the_served_batch()
 {
    const OFFSET: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[OTHER_PARTITION, PARTITION]);
    let not_owned = IggyError::ConsumerGroupPartitionNotOwned(u128::default(), PARTITION);
    script_fetches(
        &cluster,
        [sections_answer(&[
            served(OTHER_PARTITION, &[OFFSET], OFFSET + 1),
            refused(PARTITION, &not_owned),
        ])],
    );
    let mut reader = member(client).build().await.unwrap();

    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();
    let after = timeout(POLL_TIMEOUT, reader.next()).await;

    assert_eq!(
        partition_and_offsets(&batch),
        (OTHER_PARTITION, vec![OFFSET]),
        "a partition that the member no longer owns keeps the batch of another one"
    );
    assert!(
        after.is_err(),
        "the member resyncs a partition that it does not own without an error: {after:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_partition_without_history_when_read_should_stop_it_until_a_seek() {
    const OFFSET: u64 = 3;
    const SEEK_OFFSET: u64 = 11;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [
            sections_answer(&[
                served(OTHER_PARTITION, &[OFFSET], OFFSET + 1),
                refused(PARTITION, &IggyError::HistoryUnavailable),
            ]),
            Answer::Silence,
            served_answer(PARTITION, SEEK_OFFSET),
        ],
    );
    let mut reader = standalone(client, [OTHER_PARTITION, PARTITION])
        .build()
        .await
        .unwrap();

    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();
    let stopped = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();
    eventually("the next fetch starts", || {
        cluster.count(FETCH_MESSAGES_CODE) == 2
    })
    .await;
    reader
        .seek(PARTITION, PollingStrategy::offset(SEEK_OFFSET))
        .await
        .unwrap();
    let resumed = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();

    assert_eq!(
        partition_and_offsets(&batch),
        (OTHER_PARTITION, vec![OFFSET]),
        "a refused partition keeps the batch of another one"
    );
    assert_eq!(
        stopped.unwrap_err(),
        ReadError::new(Some(PARTITION), IggyError::HistoryUnavailable)
    );
    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, vec![SEEK_OFFSET])
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert_eq!(
        partitions_of(&fetches[1]),
        [OTHER_PARTITION],
        "a stopped partition reads nothing"
    );
    assert_eq!(
        targets_of(&fetches[2]),
        [(PARTITION, WirePollingStrategy::offset(SEEK_OFFSET))]
    );
}

#[tokio::test(start_paused = true)]
async fn given_two_partitions_of_one_primary_when_one_moves_should_fetch_both_with_one_request_and_regroup()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    // The reply waits until the route moved, so the refresh that it starts sees the move.
    cluster.hold(FETCH_MESSAGES_CODE);
    script_fetches(
        &cluster,
        [sections_answer(&[refused(
            PARTITION,
            &IggyError::NotPrimary,
        )])],
    );
    cluster.script(
        &endpoint(MOVED_PRIMARY),
        FETCH_MESSAGES_CODE,
        repeat_n(Answer::Silence, SILENT_READS),
    );
    let _reader = standalone(client, [OTHER_PARTITION, PARTITION])
        .build()
        .await
        .unwrap();
    eventually("the first fetch arrives", || {
        cluster.count(FETCH_MESSAGES_CODE) == 1
    })
    .await;
    let first = cluster.received(FETCH_MESSAGES_CODE).remove(0);

    *cluster.route.lock().unwrap() = Some(route_to(MOVED_PRIMARY, MOVED_VIEW));
    cluster.release(FETCH_MESSAGES_CODE);

    assert_eq!(
        (first.endpoint.clone(), partitions_of(&first)),
        (endpoint(PRIMARY), vec![OTHER_PARTITION, PARTITION]),
        "one fetch reads the partitions of one primary"
    );
    eventually("the moved partition fetches from its new primary", || {
        cluster.received(FETCH_MESSAGES_CODE).iter().any(|fetch| {
            fetch.endpoint == endpoint(MOVED_PRIMARY) && partitions_of(fetch).contains(&PARTITION)
        })
    })
    .await;
}

/// A signed-in client of three ready replicas. Replica 0 is the metadata primary, and
/// `PRIMARY` leads every partition.
async fn connect_to_three_replicas() -> (TcpClient, Arc<FakeCluster>) {
    let cluster = FakeCluster::new(REPLICAS);
    *cluster.route.lock().unwrap() = Some(route_to(PRIMARY, FIRST_VIEW));
    let links = connected(&cluster, usize::from(REPLICAS)).await;
    (TcpClient::from_links(links), cluster)
}

fn route_to(primary: u8, view: u32) -> PartitionRouteResponse {
    PartitionRouteResponse {
        incarnation: INCARNATION,
        metadata_op: METADATA_OP,
        view,
        partitions_count: PARTITIONS_COUNT,
        primary_replica: primary,
    }
}

/// A standalone reader of `partitions` whose refused partitions retry after `RETRY_INTERVAL`.
fn standalone(client: TcpClient, partitions: impl IntoIterator<Item = u32>) -> ReaderBuilder {
    builder_on(client, Consumer::new(identifier(CONSUMER_ID)))
        .partitions(partitions)
        .retry_interval(RETRY_INTERVAL)
}

/// A group reader whose refused partitions retry after `RETRY_INTERVAL`.
fn member(client: TcpClient) -> ReaderBuilder {
    builder_on(client, group()).retry_interval(RETRY_INTERVAL)
}

/// The deadline of each read of `reader` after its start.
fn request_timeout(reader: &PartitionReader) -> Duration {
    reader.config.options.request_timeout().get_duration()
}

/// Makes the next refresh join again, as after the loss of the session that joined.
fn forget_membership(reader: &PartitionReader) {
    reader
        .shared
        .membership
        .as_ref()
        .expect("a group reader has a membership")
        .forget();
}

/// Scripts the joins of a group of `GROUP_REFRESHES` refreshes that assign `partitions`.
fn script_group(cluster: &FakeCluster, partitions: &[u32]) {
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), GROUP_REFRESHES),
    );
    cluster.script(
        &metadata_primary,
        SYNC_CONSUMER_GROUP_CODE,
        repeat_n(assignment(partitions), GROUP_REFRESHES),
    );
}

fn existing_group() -> Answer {
    Answer::Body(
        ConsumerGroupDetailsResponse {
            group: ConsumerGroupResponse {
                id: GROUP_ID,
                partitions_count: PARTITIONS_COUNT,
                members_count: 0,
                name: WireName::new(GROUP_NAME).unwrap(),
            },
            members: Vec::new(),
        }
        .to_bytes()
        .to_vec(),
    )
}

fn assignment(partitions: &[u32]) -> Answer {
    Answer::Body(
        SyncConsumerGroupResponse {
            generation: GROUP_GENERATION,
            partitions: partitions.to_vec(),
        }
        .to_bytes()
        .to_vec(),
    )
}

/// Scripts `answers` for the next fetches on `PRIMARY`, then `SILENT_READS` fetches without a
/// reply.
fn script_fetches(cluster: &FakeCluster, answers: impl IntoIterator<Item = Answer>) {
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        answers
            .into_iter()
            .chain(repeat_n(Answer::Silence, SILENT_READS)),
    );
}

fn sections_answer(sections: &[Section]) -> Answer {
    Answer::Body(fetch_body(sections).to_vec())
}

/// A fetch reply that serves one message of `partition` at `offset`.
fn served_answer(partition: u32, offset: u64) -> Answer {
    sections_answer(&[served(partition, &[offset], offset + 1)])
}

fn fetch_request(fetch: &Received) -> FetchMessagesRequest {
    FetchMessagesRequest::decode(&fetch.body).unwrap().0
}

fn targets_of(fetch: &Received) -> Vec<(u32, WirePollingStrategy)> {
    fetch_request(fetch)
        .targets
        .into_iter()
        .map(|target| (target.partition_id, target.strategy))
        .collect()
}

fn partitions_of(fetch: &Received) -> Vec<u32> {
    targets_of(fetch)
        .into_iter()
        .map(|(partition, _)| partition)
        .collect()
}

/// Asks `reader` for a batch until its fetch number `fetches` reaches `cluster`, then leaves
/// that fetch in flight.
async fn start_fetch(reader: &mut PartitionReader, cluster: &FakeCluster, fetches: usize) {
    let mut next = Box::pin(reader.next());
    timeout(TEST_BUDGET, async {
        while cluster.count(FETCH_MESSAGES_CODE) < fetches {
            assert!(
                next.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Takes errors until a batch arrives.
async fn next_batch(reader: &mut PartitionReader) -> PolledMessages {
    let mut last_error = None;
    let read = timeout(TEST_BUDGET, async {
        loop {
            match reader.next().await {
                Ok(batch) => return batch,
                Err(error) => last_error = Some(error),
            }
        }
    })
    .await;
    read.unwrap_or_else(|_| panic!("no batch arrived, last error: {last_error:?}"))
}

/// Publishes `events` in order on the client of `reader`.
async fn publish(reader: &PartitionReader, events: &[DiagnosticEvent]) {
    let client = reader.config.client.read().await;
    let ClientWrapper::Tcp(client) = &*client else {
        panic!("a test reader runs on a TCP client");
    };
    for event in events {
        client.publish_event(*event).await;
    }
}
