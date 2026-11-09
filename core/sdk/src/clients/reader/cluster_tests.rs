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

use bytes::Bytes;
use iggy_binary_protocol::codes::{
    CREATE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_OFFSET_CODE, FETCH_MESSAGES_CODE,
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_OFFSET_CODE, GET_PARTITION_ROUTE_CODE, GET_STREAMS_CODE,
    JOIN_CONSUMER_GROUP_CODE, LEAVE_CONSUMER_GROUP_CODE, LOGIN_REGISTER_CODE,
    POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::consensus::EvictionReason;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, GetConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::{DeferredPollMessagesRequest, FetchMessagesRequest};
use iggy_binary_protocol::responses::consumer_groups::{
    ConsumerGroupDetailsResponse, ConsumerGroupResponse, SyncConsumerGroupResponse,
};
use iggy_binary_protocol::responses::messages::{FetchMessagesResponse, PartitionRouteResponse};
use iggy_binary_protocol::{WireDecode, WireEncode, WireName};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    BinaryTransport, CommitPolicy, Consumer, ConsumerGroupClient, ConsumerOffsetClient,
    DiagnosticEvent, Identifier, IggyDuration, IggyError, IggyTimestamp, PollOptions,
    PolledMessages, PollingStrategy, TopicClient, UserClient,
};
use tokio::sync::Notify;
use tokio::task::yield_now;
use tokio::time::{Instant, sleep, timeout};

use super::fixtures::{
    CONSUMER_ID, CONTEXT, GROUP_ID, STREAM_ID, Section, TOPIC_ID, builder_on, committing,
    fetch_body, group, identifier, lookup_answer, partition_and_offsets, polled_answer_in, refused,
    served, stored_offsets,
};
use super::worker::{ASSIGNMENT_REFRESH_INTERVAL, MAX_LASTING_REFUSALS};
use super::{ORDERING, PartitionReader, ReadError};
use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::reader_builder::ReaderBuilder;
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
/// The first batch of a partition that holds the messages before [`PARTITION_END`].
const FIRST_BATCH: [u64; 2] = [0, 1];
const SECOND_BATCH_START: u64 = FIRST_BATCH.len() as u64;
const PARTITION_END: u64 = 4;

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
        commit: CommitPolicy::Manual,
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
    script_initial_next(&cluster, &[(OTHER_PARTITION, OFFSET), (PARTITION, OFFSET)]);
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
    assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 2);
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
    script_initial_next(
        &cluster,
        &[(OTHER_PARTITION, OFFSET), (PARTITION, OFFSET + 1)],
    );
    let mut reader = committing(member(client)).build().await.unwrap();

    // The lane refuses the first fetch of both targets before it leaves, so the refusal of the
    // blocked partition arrives before the batch of the other one.
    let blocked = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();
    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();

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

/// A build abandons a join with an unknown outcome on the client at once, so the next build
/// joins again instead of finding the metadata lane blocked.
#[tokio::test(start_paused = true)]
async fn given_an_uncertain_join_when_building_again_should_abandon_it_and_join_again() {
    const BUILDS: usize = 2;
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
    assert!(
        !client.read().await.retains_operation(op).await,
        "the failed build abandons its join"
    );

    reader().build().await.unwrap();

    let joins = cluster.received(JOIN_CONSUMER_GROUP_CODE);
    assert_eq!(joins.len(), BUILDS);
    assert_ne!(joins[1].header.request, op, "the build sends a new join");
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

/// A join after a lost session has an unknown outcome, and the reader closes before the next
/// refresh. The join abandoned it at once, so it does not block the leave.
#[tokio::test(start_paused = true)]
async fn given_a_join_with_an_unknown_outcome_when_closing_should_abandon_it_and_leave() {
    let (client, cluster) = connect_to_three_replicas().await;
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), 2),
    );
    cluster.script(
        &metadata_primary,
        JOIN_CONSUMER_GROUP_CODE,
        [Answer::Commit(COMMIT_OP), Answer::Silence],
    );
    let mut reader = member(client).build().await.unwrap();
    forget_membership(&reader);
    let lost = timeout(TEST_BUDGET, reader.next()).await.unwrap();
    let op = cluster.received(JOIN_CONSUMER_GROUP_CODE)[1].header.request;
    assert!(
        matches!(
            &lost,
            Err(ReadError {
                error: IggyError::OperationUncertain(uncertain),
                ..
            }) if *uncertain == op
        ),
        "{lost:?}"
    );

    let closed = reader.close().await;

    assert_eq!(closed, Ok(()));
    assert_eq!(
        cluster.count(JOIN_CONSUMER_GROUP_CODE),
        2,
        "the close does not resolve the join"
    );
    assert_eq!(cluster.count(LEAVE_CONSUMER_GROUP_CODE), 1);
}

/// The leave of the close has an unknown outcome, which blocks the metadata writes of the
/// client. The close abandons it, so a later metadata write of the application on the same
/// client goes out.
#[tokio::test(start_paused = true)]
async fn given_a_leave_with_an_unknown_outcome_when_the_client_writes_metadata_after_the_close_should_find_its_lane_free()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        [existing_group()],
    );
    cluster.script(
        &metadata_primary,
        LEAVE_CONSUMER_GROUP_CODE,
        [Answer::Silence],
    );
    let client = IggyRwLock::new(ClientWrapper::Tcp(client));
    let reader = ReaderBuilder::new(
        client.clone(),
        identifier(STREAM_ID),
        identifier(TOPIC_ID),
        group(),
        None,
    )
    .build()
    .await
    .unwrap();

    let closed = reader.close().await;
    let joined = client
        .read()
        .await
        .join_consumer_group(
            &identifier(STREAM_ID),
            &identifier(TOPIC_ID),
            &identifier(GROUP_ID),
        )
        .await;

    assert_eq!(closed, Ok(()));
    assert_eq!(cluster.count(LEAVE_CONSUMER_GROUP_CODE), 1);
    assert_eq!(joined, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn given_a_failed_refresh_when_the_caller_asks_again_at_once_should_wait_its_retry_interval()
{
    const CALLS: u32 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    // Only the join of `build()` finds the group.
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        [existing_group()],
    );
    let mut reader = member(client).build().await.unwrap();
    forget_membership(&reader);
    timeout(POLL_TIMEOUT, reader.next())
        .await
        .unwrap()
        .unwrap_err();
    let refreshes = cluster.count(GET_CONSUMER_GROUP_CODE);
    let started = Instant::now();

    for _ in 0..CALLS {
        timeout(POLL_TIMEOUT, reader.next())
            .await
            .unwrap()
            .unwrap_err();
    }

    assert!(started.elapsed() >= RETRY_INTERVAL * CALLS);
    assert_eq!(
        cluster.count(GET_CONSUMER_GROUP_CODE),
        refreshes + usize::try_from(CALLS).unwrap(),
        "one refresh per failed call"
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
async fn given_a_join_whose_partition_becomes_pollable_later_when_reading_should_read_before_the_refresh_interval()
 {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), GROUP_REFRESHES),
    );
    // The partition log persists the new owner only after the first sync.
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        SYNC_CONSUMER_GROUP_CODE,
        once(assignment(&[])).chain(repeat_n(
            assignment_at(GROUP_GENERATION + 1, &[PARTITION]),
            GROUP_REFRESHES,
        )),
    );
    script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);
    let mut reader = member(client).build().await.unwrap();
    let started = Instant::now();

    let read = timeout(TEST_BUDGET, reader.next()).await.unwrap();

    assert!(
        matches!(&read, Ok(batch) if partition_and_offsets(batch) == (PARTITION, vec![OFFSET])),
        "{read:?} after {:?}: lookups {}, joins {}, syncs {}",
        started.elapsed(),
        cluster.count(GET_CONSUMER_GROUP_CODE),
        cluster.count(JOIN_CONSUMER_GROUP_CODE),
        cluster.count(SYNC_CONSUMER_GROUP_CODE)
    );
    assert!(
        started.elapsed() < ASSIGNMENT_REFRESH_INTERVAL,
        "the member asks again soon after the join, but read after {:?}",
        started.elapsed()
    );
}

/// A handover installs the owner of a partition in a metadata op after the join, and a sync
/// lists the partition once its replica applied that op. So a sync that changes the assignment
/// raises the metadata watermark to its commit, and a primary that has not applied the handover
/// refuses a later read as not accepted, not as not owned. A sync that changes nothing keeps the
/// watermark, so the reads of a settled member never wait for unrelated metadata.
#[tokio::test(start_paused = true)]
async fn given_syncs_at_rising_commits_when_reading_metadata_should_wait_only_for_the_syncs_that_changed_the_assignment()
 {
    const ADDED: u64 = 5;
    const UNCHANGED: u64 = ADDED + 1;
    const EXTENDED: u64 = UNCHANGED + 1;
    const NOT_A_MEMBER: u64 = EXTENDED + 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let answers = [
        assignment_at_commit(ADDED, GROUP_GENERATION, &[PARTITION]),
        assignment_at_commit(UNCHANGED, GROUP_GENERATION, &[PARTITION]),
        assignment_at_commit(
            EXTENDED,
            GROUP_GENERATION + 1,
            &[OTHER_PARTITION, PARTITION],
        ),
        Answer::CommitBody(NOT_A_MEMBER, Vec::new()),
    ];
    let syncs = answers.len();
    cluster.script(
        &endpoint(METADATA_PRIMARY),
        SYNC_CONSUMER_GROUP_CODE,
        answers,
    );
    let (stream, topic, group) = (
        identifier(STREAM_ID),
        identifier(TOPIC_ID),
        identifier(GROUP_ID),
    );
    let mut watermarks = Vec::with_capacity(syncs);

    for _ in 0..syncs {
        client
            .sync_consumer_group(&stream, &topic, &group)
            .await
            .unwrap();
        client
            .send_raw_with_response(GET_STREAMS_CODE, Bytes::new())
            .await
            .unwrap();
        let reads = cluster.received(GET_STREAMS_CODE);
        watermarks.push(reads.last().unwrap().header.minimum_metadata_op);
    }

    assert_eq!(watermarks, [ADDED, ADDED, EXTENDED, EXTENDED]);
}

#[tokio::test(start_paused = true)]
async fn given_an_assignment_that_stops_changing_when_refreshing_should_back_off_to_the_refresh_interval()
 {
    const SETTLED: Duration = Duration::from_secs(2);
    const INTERVALS: u32 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    script_fetches(&cluster, []);
    let _reader = member(client).build().await.unwrap();
    sleep(SETTLED).await;
    let settled = cluster.count(SYNC_CONSUMER_GROUP_CODE);

    sleep(ASSIGNMENT_REFRESH_INTERVAL * INTERVALS).await;

    let refreshes = cluster.count(SYNC_CONSUMER_GROUP_CODE) - settled;
    assert!(
        refreshes <= usize::try_from(INTERVALS).unwrap() + 1,
        "one refresh per interval once the assignment settles, but {refreshes}"
    );
}

/// A store refused as not owned can follow a rebalance that the member did not see yet, so the
/// member asks for its assignment at once instead of at the next refresh.
#[tokio::test(start_paused = true)]
async fn given_a_commit_refused_as_not_owned_when_its_partition_stays_should_refresh_the_assignment_at_once()
 {
    const OFFSET: u64 = 2;
    const SETTLED: Duration = Duration::from_secs(3);
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    let (mut section, records) = served(PARTITION, &[OFFSET], OFFSET + 1);
    section.context = CONTEXT;
    script_fetches(&cluster, [sections_answer(&[(section, records)])]);
    let mut reader = member(client).build().await.unwrap();
    let checkpoint = next_batch(&mut reader).await.checkpoint().unwrap();
    sleep(SETTLED).await;
    let settled = cluster.count(SYNC_CONSUMER_GROUP_CODE);
    eventually("a refresh at the settled interval", || {
        cluster.count(SYNC_CONSUMER_GROUP_CODE) > settled
    })
    .await;
    let refreshes = cluster.count(SYNC_CONSUMER_GROUP_CODE);
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Refuse(IggyError::ConsumerGroupPartitionNotOwned(
            0, PARTITION,
        ))],
    );
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [polled_answer_in(PARTITION, &[], CONTEXT)],
    );

    reader.commit(checkpoint).await.unwrap();
    sleep(ASSIGNMENT_REFRESH_INTERVAL / 2).await;

    assert_eq!(cluster.count(SYNC_CONSUMER_GROUP_CODE), refreshes + 1);
}

/// A refresh abandons a create or a join with an unknown outcome at once. The next refresh looks
/// the group up again, finds the group that the lost create made, and joins anew.
#[tokio::test(start_paused = true)]
async fn given_an_uncertain_group_mutation_when_the_refresh_joins_again_should_abandon_it_and_read()
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
        // Auto-creation creates a group by its name only.
        let mut reader = builder_on(
            client,
            Consumer::group(Identifier::named(GROUP_NAME).unwrap()),
        )
        .retry_interval(RETRY_INTERVAL)
        .create_group(true)
        .build()
        .await
        .unwrap();
        forget_membership(&reader);

        let uncertain = timeout(TEST_BUDGET, reader.next()).await.unwrap();
        let read = timeout(TEST_BUDGET, reader.next()).await.unwrap();

        let requests = cluster.received(mutation);
        let op = requests[earlier_mutations].header.request;
        assert_eq!(
            uncertain.unwrap_err(),
            ReadError::new(None, IggyError::OperationUncertain(op)),
            "mutation {mutation}"
        );
        assert!(read.is_ok(), "mutation {mutation}: {read:?}");
        assert!(
            requests[earlier_mutations + 1..]
                .iter()
                .all(|later| later.header.request != op),
            "the next join replays nothing, mutation {mutation}"
        );
        assert!(
            !reader
                .config
                .client
                .read()
                .await
                .retains_operation(op)
                .await,
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
        matches!(&evicted, Ok(Err(error))
            if *error == ReadError::new(Some(PARTITION), IggyError::TransientNotCommitted)),
        "a noncommitting fetch that lost its session ends without a stored offset: {evicted:?}"
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

/// A committing fetch whose reply is lost may have stored its offset. The next read from `Next`
/// starts past the delivered messages, so the reader reads the skipped ones by offset without a
/// commit and hands them over, without an error and without an abandon by the application.
#[tokio::test(start_paused = true)]
async fn given_a_lost_committing_fetch_reply_when_the_next_read_skips_offsets_should_deliver_them_by_offset_without_a_commit()
 {
    const DELIVERED: u64 = 4;
    const SKIPPED: [u64; 3] = [5, 6, 7];
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [
            served_answer(PARTITION, DELIVERED),
            Answer::Silence,
            served_answer(PARTITION, SKIPPED[2]),
        ],
    );
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [
            polled_answer(PARTITION, &[DELIVERED]),
            polled_answer(PARTITION, &SKIPPED),
        ],
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

    let caught_up = timeout(request_timeout(&reader) * 3, reader.next())
        .await
        .expect("the reader reads on after a lost reply")
        .expect("a lost reply is no error of the reader");

    assert_eq!(
        partition_and_offsets(&caught_up),
        (PARTITION, SKIPPED.to_vec())
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert_eq!(
        targets_of(&fetches[2])
            .into_iter()
            .map(|(partition, strategy)| (partition, strategy.kind))
            .collect::<Vec<_>>(),
        [(PARTITION, WirePollingStrategy::next().kind)],
        "an OnRead reader reads from the stored offset, which the server fences"
    );
    let catch_up = DeferredPollMessagesRequest::decode_from(
        &cluster.received(POLL_MESSAGES_DEFERRED_CODE)[1].body,
    )
    .unwrap();
    assert_eq!(
        (
            catch_up.poll.strategy,
            catch_up.poll.count,
            catch_up.poll.auto_commit
        ),
        (WirePollingStrategy::offset(SKIPPED[0]), 3, false)
    );
    reader.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn given_the_first_committing_next_reply_is_lost_when_reading_should_recover_its_messages() {
    const SKIPPED: [u64; 3] = [5, 6, 7];
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [Answer::Silence, served_answer(PARTITION, SKIPPED[2])],
    );
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [
            polled_answer(PARTITION, &SKIPPED[..1]),
            polled_answer(PARTITION, &SKIPPED),
        ],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .prefetch_messages(1)
        .build()
        .await
        .unwrap();

    let caught_up = timeout(request_timeout(&reader) * 3, reader.next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        partition_and_offsets(&caught_up),
        (PARTITION, SKIPPED.to_vec())
    );
    reader.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn given_the_first_committing_next_reply_is_lost_when_closing_should_rewind_to_the_resolved_start()
 {
    for first in [0, 5] {
        let (client, cluster) = connect_to_three_replicas().await;
        script_fetches(&cluster, [Answer::Silence]);
        script_initial_next(&cluster, &[(PARTITION, first)]);
        // The server committed the read whose reply was lost.
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, Some(first))],
        );
        let mut reader = committing(standalone(client, [PARTITION]))
            .build()
            .await
            .unwrap();
        start_fetch(&mut reader, &cluster, 1).await;
        reader.close().await.unwrap();
        if first == 0 {
            assert_eq!(
                cluster.count(iggy_binary_protocol::codes::DELETE_CONSUMER_OFFSET_CODE),
                1
            );
            assert!(stored_offsets(&cluster).is_empty());
        } else {
            assert_eq!(stored_offsets(&cluster), [first - 1]);
        }
    }
}

/// A lost read may never reach the server, and the server refuses a delete of a missing offset.
#[tokio::test(start_paused = true)]
async fn given_a_lost_first_read_at_offset_0_without_a_stored_offset_when_closing_should_send_no_delete()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, [Answer::Silence]);
    script_initial_next(&cluster, &[(PARTITION, 0)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;

    reader.close().await.unwrap();

    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION)], vec![]));
    assert!(stored_offsets(&cluster).is_empty());
}

/// The next reader resumes after the stored offset whatever its start, so the close moves the
/// offset before the batch that the application never got.
#[tokio::test(start_paused = true)]
async fn given_an_offset_start_and_an_undelivered_batch_when_closing_should_rewind_before_the_batch()
 {
    for start in [0, SECOND_BATCH_START] {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut server = ServerOffset::default();
        cluster.script(
            &endpoint(PRIMARY),
            FETCH_MESSAGES_CODE,
            [server.read(&[start, start + 1])],
        );
        let mut reader = committing(standalone(client, [PARTITION]))
            .start(PollingStrategy::offset(start))
            .build()
            .await
            .unwrap();
        start_fetch(&mut reader, &cluster, 1).await;
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [server.lookup()],
        );
        cluster.script(
            &endpoint(PRIMARY),
            DELETE_CONSUMER_OFFSET_CODE,
            [server.delete()],
        );

        reader.close().await.unwrap();

        if start == 0 {
            assert_eq!(
                offset_resets(&cluster),
                (vec![Some(PARTITION); 2], vec![Some(PARTITION)]),
                "the start and the close look the offset up, and the close deletes it"
            );
            assert!(stored_offsets(&cluster).is_empty());
        } else {
            assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION)], vec![]));
            assert_eq!(stored_offsets(&cluster), [start - 1]);
        }
    }
}

/// A read from an explicit offset learns the history of its partition only from its reply, so
/// the close moves the offset before the start of a lost first read without a history.
#[tokio::test(start_paused = true)]
async fn given_an_offset_start_whose_first_reply_is_lost_when_closing_should_rewind_before_the_start()
 {
    for start in [0, SECOND_BATCH_START] {
        let cluster = close_after_a_lost_first_read(PollingStrategy::offset(start), start).await;
        // A start without a history names no known message, so the close looks the offset up.
        assert_rewound_before(&cluster, start, 2, &format!("offset {start}"));
    }
}

/// A read from past the end of the partition stores nothing, so the close leaves the offset.
#[tokio::test(start_paused = true)]
async fn given_an_offset_start_past_the_end_whose_reply_is_lost_when_closing_should_store_nothing()
{
    const PAST_END: u64 = PARTITION_END + 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let server = ServerOffset::default();
    script_fetches(&cluster, [Answer::Silence]);
    // The offset before the start lies past the last message.
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [server.store(PAST_END - 1)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(PAST_END))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;

    let closed = reader.close().await;

    assert_eq!(closed, Ok(()));
    assert!(stored_offsets(&cluster).is_empty());
    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION); 2], vec![]));
}

/// The check of a `Last` or timestamp start resolves it to the offset of its first message.
#[tokio::test(start_paused = true)]
async fn given_a_last_or_timestamp_start_whose_first_reply_is_lost_when_closing_should_rewind_to_the_resolved_start()
 {
    const TIMESTAMP_US: u64 = 1_000;
    for start in [
        PollingStrategy::last(),
        PollingStrategy::timestamp(IggyTimestamp::from(TIMESTAMP_US)),
    ] {
        for resolved in [0, SECOND_BATCH_START] {
            let cluster = close_after_a_lost_first_read(start, resolved).await;
            // The check names a known message, so only the delete at 0 looks the offset up again.
            let lookups = if resolved == 0 { 2 } else { 1 };
            assert_rewound_before(
                &cluster,
                resolved,
                lookups,
                &format!("{start} at {resolved}"),
            );
        }
    }
}

/// The seek stores the offset before its target without a history, and the close stores it
/// again after the lost first read from the target.
#[tokio::test(start_paused = true)]
async fn given_a_seek_without_a_history_whose_first_reply_is_lost_when_closing_should_store_before_it_again()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, [Answer::Silence]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    reader
        .seek(PARTITION, PollingStrategy::offset(SECOND_BATCH_START))
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;

    reader.close().await.unwrap();

    assert_eq!(stored_offsets(&cluster), [SECOND_BATCH_START - 1; 2]);
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_in_flight_when_closing_should_rewind_over_its_batch() {
    const FIRST: u64 = 5;
    const REPLY_DELAY: Duration = Duration::from_millis(100);
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.hold(FETCH_MESSAGES_CODE);
    script_fetches(&cluster, [served_answer(PARTITION, FIRST)]);
    script_initial_next(&cluster, &[(PARTITION, FIRST)]);
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
    script_initial_next(&cluster, &[(PARTITION, 0)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    let op = cluster.received(FETCH_MESSAGES_CODE)[0].header.request;
    let request_timeout = request_timeout(&reader);
    let reader_client = reader.config.client.clone();
    let started = Instant::now();

    let closed = reader.close().await;

    assert!(
        started.elapsed() >= request_timeout,
        "the server can commit until the deadline of the fetch, so the close waits for it"
    );
    closed.unwrap();
    assert!(
        !reader_client.read().await.retains_operation(op).await,
        "the close abandons the lost fetch, so the client reads the partition again"
    );
    assert!(stored_offsets(&cluster).is_empty(), "nothing was delivered");
}

#[tokio::test(start_paused = true)]
async fn given_a_committing_fetch_in_flight_when_the_reader_drops_should_return_at_once_and_block_its_partition()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, []);
    script_initial_next(&cluster, &[(PARTITION, 0)]);
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
    script_initial_next(&cluster, &[(PARTITION, FIRST)]);
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
async fn given_a_join_in_flight_when_the_reader_drops_should_let_it_finish_and_keep_metadata_calls_usable()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    let reader = member(client).build().await.unwrap();
    let client = reader.config.client.clone();
    cluster.hold(JOIN_CONSUMER_GROUP_CODE);
    forget_membership(&reader);
    eventually("the refresh joins again", || {
        cluster.count(JOIN_CONSUMER_GROUP_CODE) == 2
    })
    .await;

    drop(reader);
    cluster.release(JOIN_CONSUMER_GROUP_CODE);
    let deleted = client
        .read()
        .await
        .delete_topic(&identifier(STREAM_ID), &identifier(TOPIC_ID))
        .await;

    assert_eq!(
        deleted,
        Ok(()),
        "the join that the server received finished"
    );
}

#[tokio::test(start_paused = true)]
async fn given_a_join_in_flight_when_the_session_ends_should_join_again_on_the_next_refresh() {
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    // A fetch parks as on a server, so the refreshes run on time.
    script_fetches(&cluster, []);
    let reader = member(client).build().await.unwrap();
    cluster.hold(JOIN_CONSUMER_GROUP_CODE);
    forget_membership(&reader);
    eventually("the refresh joins again", || {
        cluster.count(JOIN_CONSUMER_GROUP_CODE) == 2
    })
    .await;

    forget_membership(&reader);
    cluster.release(JOIN_CONSUMER_GROUP_CODE);

    eventually("the next refresh joins for the new session", || {
        cluster.count(JOIN_CONSUMER_GROUP_CODE) == 3
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn given_a_partition_without_a_route_yet_when_refreshing_should_retry_without_an_error() {
    const OFFSET: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    let route = cluster.route.lock().unwrap().take();
    script_fetches(&cluster, [served_answer(PARTITION, OFFSET)]);
    let mut reader = standalone(client, [PARTITION]).build().await.unwrap();
    eventually("the refresh asks for the route", || {
        cluster.count(GET_PARTITION_ROUTE_CODE) > 0
    })
    .await;
    *cluster.route.lock().unwrap() = route;

    let read = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();

    assert!(
        matches!(&read, Ok(batch) if partition_and_offsets(batch) == (PARTITION, vec![OFFSET])),
        "a partition that is not routable yet is retried, not reported: {read:?}"
    );
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

/// The worker abandons a lost committing read itself, so a seek needs no abandon first.
#[tokio::test(start_paused = true)]
async fn given_a_lost_committing_reply_when_seeking_should_read_from_the_new_position() {
    const SEEK_OFFSET: u64 = 9;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [Answer::Silence, served_answer(PARTITION, SEEK_OFFSET)],
    );
    script_initial_next(&cluster, &[(PARTITION, 0)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    let seek_start = PollingStrategy::offset(SEEK_OFFSET);

    let sought = reader.seek(PARTITION, seek_start).await;
    let resumed = timeout(request_timeout(&reader) * 3, reader.next())
        .await
        .unwrap()
        .unwrap();

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

/// A seek to `Next` leaves the partition without a known start, which a poll resolves before a
/// committing fetch reads it, whatever start the reader was built with.
#[tokio::test(start_paused = true)]
async fn given_a_committing_reader_started_at_offset_0_when_a_partition_is_sought_to_next_should_read_it_again()
 {
    const RESUMED_OFFSET: u64 = 5;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(
        &cluster,
        [
            served_answer(PARTITION, 0),
            served_answer(PARTITION, RESUMED_OFFSET),
        ],
    );
    script_initial_next(&cluster, &[(PARTITION, RESUMED_OFFSET)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    let first = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();

    reader
        .seek(PARTITION, PollingStrategy::next())
        .await
        .unwrap();
    let resumed = timeout(POLL_TIMEOUT, reader.next()).await;

    assert_eq!(partition_and_offsets(&first), (PARTITION, vec![0]));
    let resumed = resumed
        .expect("the partition sought to next should be read again")
        .unwrap();
    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, vec![RESUMED_OFFSET])
    );
    assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
    reader.close().await.unwrap();
}

/// The commit of a read never moves the stored offset back, so only the delete lets the `Next`
/// reads after the first batch from offset 0 continue from there.
#[tokio::test(start_paused = true)]
async fn given_an_on_read_reader_past_its_first_batch_when_seeking_to_offset_0_should_replay_the_partition()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH), server.read_next()],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    let read = [next_batch(&mut reader).await, next_batch(&mut reader).await];
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [server.lookup()],
    );
    cluster.script(
        &endpoint(PRIMARY),
        DELETE_CONSUMER_OFFSET_CODE,
        [server.delete()],
    );

    let sought = reader.seek(PARTITION, PollingStrategy::first()).await;
    server.apply_deletes(&cluster);
    script_fetches(&cluster, [server.read(&FIRST_BATCH), server.read_next()]);
    let replayed = [next_batch(&mut reader).await, next_batch(&mut reader).await];

    let expected = [
        (PARTITION, FIRST_BATCH.to_vec()),
        (PARTITION, (SECOND_BATCH_START..PARTITION_END).collect()),
    ];
    assert_eq!(read.each_ref().map(partition_and_offsets), expected);
    assert_eq!(sought, Ok(()));
    assert_eq!(replayed.each_ref().map(partition_and_offsets), expected);
    assert_eq!(
        offset_resets(&cluster),
        (vec![Some(PARTITION); 2], vec![Some(PARTITION)]),
        "the start and the seek look the offset up"
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_on_read_partition_without_a_stored_offset_when_seeking_to_offset_0_should_send_the_lookup_and_no_delete()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [server.lookup()],
    );
    cluster.script(
        &endpoint(PRIMARY),
        DELETE_CONSUMER_OFFSET_CODE,
        [server.delete()],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();

    let sought = reader.seek(PARTITION, PollingStrategy::offset(0)).await;

    assert_eq!(sought, Ok(()));
    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION)], vec![]));
}

#[tokio::test(start_paused = true)]
async fn given_a_refused_delete_when_seeking_to_offset_0_should_fail_the_seek_and_read_on_from_the_old_position()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [server.lookup()],
    );
    cluster.script(
        &endpoint(PRIMARY),
        DELETE_CONSUMER_OFFSET_CODE,
        [Answer::CommitRejected(
            COMMIT_OP,
            IggyError::HistoryUnavailable,
        )],
    );

    let sought = reader.seek(PARTITION, PollingStrategy::first()).await;
    script_fetches(&cluster, [server.read_next()]);
    let resumed = next_batch(&mut reader).await;

    assert_eq!(sought, Err(IggyError::HistoryUnavailable));
    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, (SECOND_BATCH_START..PARTITION_END).collect())
    );
    let fetches = cluster.received(FETCH_MESSAGES_CODE);
    assert_eq!(
        targets_of(&fetches[1]),
        [(
            PARTITION,
            WirePollingStrategy {
                value: SECOND_BATCH_START,
                ..WirePollingStrategy::next()
            }
        )],
        "the reader reads on from its old position"
    );
}

/// A cancelled `next()` leaves demand behind, so the fetch loop reads again after an empty
/// reply. A read from the old position that started during the store would commit after it.
#[tokio::test(start_paused = true)]
async fn given_demand_left_by_a_cancelled_next_when_a_seek_stores_should_read_its_partition_only_after_the_store_and_from_the_seek_point()
 {
    const SEEK_POINT: u64 = 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read_next()],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    let (release, at_the_end) = gated(server.read_next());
    script_fetches(&cluster, [at_the_end]);
    start_fetch(&mut reader, &cluster, 2).await;
    cluster.hold(STORE_CONSUMER_OFFSET_CODE);

    let (sought, reads_during_store) = tokio::join!(
        reader.seek(PARTITION, PollingStrategy::offset(SEEK_POINT)),
        async {
            sleep(RETRY_INTERVAL).await;
            release.notify_one();
            eventually("the store of the seek arrives", || {
                cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
            })
            .await;
            sleep(POLL_TIMEOUT).await;
            let reads = read_starts(&cluster).len();
            cluster.release(STORE_CONSUMER_OFFSET_CODE);
            reads
        }
    );
    start_fetch(&mut reader, &cluster, 3).await;

    assert_eq!(sought, Ok(()));
    assert_eq!(
        reads_during_store, 2,
        "no read of the partition starts while the seek stores"
    );
    assert_eq!(
        targets_of(&cluster.received(FETCH_MESSAGES_CODE)[2]),
        [(PARTITION, WirePollingStrategy::offset(SEEK_POINT))],
        "the first read after the store starts at the seek point"
    );
}

/// The seek fails before anything moves, so the batch of the read that it waited for stays.
#[tokio::test(start_paused = true)]
async fn given_a_read_in_flight_when_the_store_of_a_seek_is_refused_should_keep_its_batch_and_read_on_from_the_old_position()
 {
    const PAST_THE_END: u64 = PARTITION_END + 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    let (release, rest) = gated(server.read_next());
    script_fetches(&cluster, [rest]);
    start_fetch(&mut reader, &cluster, 2).await;
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [server.store(PAST_THE_END - 1)],
    );

    let (sought, ()) = tokio::join!(
        reader.seek(PARTITION, PollingStrategy::offset(PAST_THE_END)),
        async {
            sleep(RETRY_INTERVAL).await;
            release.notify_one();
        }
    );
    let kept = next_batch(&mut reader).await;
    start_fetch(&mut reader, &cluster, 3).await;

    assert_eq!(
        sought.unwrap_err().as_code(),
        IggyError::InvalidOffset(PARTITION_END).as_code()
    );
    assert_eq!(
        partition_and_offsets(&kept),
        (PARTITION, (SECOND_BATCH_START..PARTITION_END).collect()),
        "the batch of the read that the seek waited for stays"
    );
    assert_eq!(
        targets_of(&cluster.received(FETCH_MESSAGES_CODE)[2]),
        [(PARTITION, next_at(PARTITION_END))],
        "the reads go on from the old position"
    );
}

/// The server answers `Next` after its stored offset, so a partition with one never reads from
/// the start of the reader.
#[tokio::test(start_paused = true)]
async fn given_an_on_read_offset_start_and_a_stored_offset_when_reading_should_resume_after_the_stored_offset()
 {
    const STORED: u64 = 5;
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [lookup_answer(PARTITION, Some(STORED))],
    );
    script_initial_next(&cluster, &[(PARTITION, STORED + 1)]);
    script_fetches(&cluster, [served_answer(PARTITION, STORED + 1)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();

    let batch = next_batch(&mut reader).await;

    assert_eq!(partition_and_offsets(&batch), (PARTITION, vec![STORED + 1]));
    assert_eq!(
        read_starts(&cluster),
        [WirePollingStrategy::next(), next_at(STORED + 1)],
        "no read starts at offset 0"
    );
}

#[tokio::test(start_paused = true)]
async fn given_an_on_read_offset_start_without_a_stored_offset_when_reading_should_read_from_the_start()
 {
    const START: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    script_fetches(&cluster, [served_answer(PARTITION, START)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(START))
        .build()
        .await
        .unwrap();

    let batch = next_batch(&mut reader).await;

    assert_eq!(partition_and_offsets(&batch), (PARTITION, vec![START]));
    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION)], vec![]));
    assert_eq!(read_starts(&cluster), [WirePollingStrategy::offset(START)]);
}

/// Each new assignment looks the offset up again, because the reads of the member moved it on.
#[tokio::test(start_paused = true)]
async fn given_an_on_read_member_assigned_a_partition_with_a_stored_offset_when_reading_should_resume_after_it_at_each_assignment()
 {
    const STORED: u64 = 5;
    let (client, cluster) = connect_to_three_replicas().await;
    let metadata_primary = endpoint(METADATA_PRIMARY);
    cluster.script(
        &metadata_primary,
        GET_CONSUMER_GROUP_CODE,
        repeat_n(existing_group(), GROUP_REFRESHES),
    );
    cluster.script(
        &metadata_primary,
        SYNC_CONSUMER_GROUP_CODE,
        [
            assignment(&[PARTITION]),
            assignment_at(GROUP_GENERATION + 1, &[]),
        ]
        .into_iter()
        .chain(repeat_n(
            assignment_at(GROUP_GENERATION + 2, &[PARTITION]),
            GROUP_REFRESHES,
        )),
    );
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [STORED, STORED + 1].map(|stored| lookup_answer(PARTITION, Some(stored))),
    );
    script_initial_next(
        &cluster,
        &[(PARTITION, STORED + 1), (PARTITION, STORED + 2)],
    );
    script_fetches(
        &cluster,
        [
            served_answer(PARTITION, STORED + 1),
            served_answer(PARTITION, STORED + 2),
        ],
    );
    let mut reader = committing(member(client))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();

    let first = next_batch(&mut reader).await;
    sleep(ASSIGNMENT_REFRESH_INTERVAL * 2).await;
    let reassigned = next_batch(&mut reader).await;

    assert_eq!(partition_and_offsets(&first), (PARTITION, vec![STORED + 1]));
    assert_eq!(
        partition_and_offsets(&reassigned),
        (PARTITION, vec![STORED + 2])
    );
    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION); 2], vec![]));
    assert_eq!(
        read_starts(&cluster),
        [
            WirePollingStrategy::next(),
            WirePollingStrategy::next(),
            next_at(STORED + 1),
            next_at(STORED + 2),
        ],
        "no read starts at offset 0"
    );
}

/// A read at the timestamp would commit no further than the stored offset ahead, so the reads
/// after it would jump to that offset.
#[tokio::test(start_paused = true)]
async fn given_an_on_read_stored_offset_ahead_when_seeking_to_a_timestamp_should_read_from_its_offset_to_the_end()
 {
    const TIMESTAMP_US: u64 = 1_000;
    const AT_TIMESTAMP: u64 = 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH), server.read_next()],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    next_batch(&mut reader).await;
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [polled_answer(PARTITION, &[AT_TIMESTAMP])],
    );

    let sought = reader
        .seek(
            PARTITION,
            PollingStrategy::timestamp(IggyTimestamp::from(TIMESTAMP_US)),
        )
        .await;
    server.apply_stores(&cluster);
    script_fetches(&cluster, [server.read_next()]);
    let resumed = next_batch(&mut reader).await;

    assert_eq!(sought, Ok(()));
    assert_eq!(stored_offsets(&cluster), [AT_TIMESTAMP - 1]);
    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, (AT_TIMESTAMP..PARTITION_END).collect())
    );
}

/// A lookup commits nothing, so a lookup whose reply is lost only runs again.
#[tokio::test(start_paused = true)]
async fn given_a_lookup_whose_reply_is_lost_when_reading_should_look_up_again_and_read_from_the_start()
 {
    const START: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );
    script_fetches(&cluster, [served_answer(PARTITION, START)]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(START))
        .build()
        .await
        .unwrap();

    let mut errors = Vec::new();
    let batch = timeout(TEST_BUDGET, async {
        loop {
            match reader.next().await {
                Ok(batch) => return batch,
                Err(error) => errors.push(error),
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(partition_and_offsets(&batch), (PARTITION, vec![START]));
    assert_eq!(offset_resets(&cluster), (vec![Some(PARTITION); 2], vec![]));
    assert!(
        errors.iter().all(|error| !matches!(
            error.error,
            IggyError::FetchProgressUncertain(_) | IggyError::PartitionLaneBlocked(..)
        )),
        "a lost lookup is no lost read: {errors:?}"
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

/// A partition that the server keeps refusing to the member is no leave. The member reports one
/// error at the limit and reads on, so a later read that the server serves arrives without a
/// seek.
#[tokio::test(start_paused = true)]
async fn given_a_partition_not_owned_past_the_limit_when_the_server_serves_it_again_should_deliver_it_without_a_seek()
 {
    const OFFSET: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    script_group(&cluster, &[PARTITION]);
    let not_owned = IggyError::ConsumerGroupPartitionNotOwned(u128::default(), PARTITION);
    let refusals = usize::try_from(MAX_LASTING_REFUSALS).unwrap();
    script_fetches(
        &cluster,
        repeat_n(sections_answer(&[refused(PARTITION, &not_owned)]), refusals).chain(once(
            sections_answer(&[served(PARTITION, &[OFFSET], OFFSET + 1)]),
        )),
    );
    let mut reader = member(client).build().await.unwrap();

    let reported = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();
    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();

    assert_eq!(
        reported.map(|_| ()),
        Err(ReadError::new(Some(PARTITION), not_owned))
    );
    assert_eq!(partition_and_offsets(&batch), (PARTITION, vec![OFFSET]));
}

#[tokio::test(start_paused = true)]
async fn given_a_partition_without_history_when_read_should_stop_it_until_a_seek() {
    const OFFSET: u64 = 3;
    const SEEK_OFFSET: u64 = 11;
    let (client, cluster) = connect_to_three_replicas().await;
    // The first reply gives the partition a position in its history, which the second loses.
    let (mut section, records) = served(PARTITION, &[OFFSET], OFFSET + 1);
    section.context = CONTEXT;
    script_fetches(
        &cluster,
        [
            sections_answer(&[(section, records)]),
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

    timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();
    let batch = timeout(POLL_TIMEOUT, reader.next()).await.unwrap().unwrap();
    let stopped = timeout(POLL_TIMEOUT, reader.next()).await.unwrap();
    eventually("the next fetch starts", || {
        cluster.count(FETCH_MESSAGES_CODE) == 3
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
        partitions_of(&fetches[2]),
        [OTHER_PARTITION],
        "a stopped partition reads nothing"
    );
    assert_eq!(
        targets_of(&fetches[3]),
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
/// A committing read refused for its permission can have stored its offset before the refusal,
/// in the current fetch state or an older one, so the close rewinds before its start.
#[tokio::test(start_paused = true)]
async fn given_a_committing_read_refused_for_its_permission_when_closing_should_rewind_before_its_start()
 {
    for older_epoch in [false, true] {
        let (client, cluster) = connect_to_three_replicas().await;
        if older_epoch {
            cluster.hold(FETCH_MESSAGES_CODE);
        }
        cluster.script(
            &endpoint(PRIMARY),
            FETCH_MESSAGES_CODE,
            [Answer::Refuse(IggyError::Unauthorized)],
        );
        let mut reader = committing(standalone(client, [PARTITION]))
            .start(PollingStrategy::offset(SECOND_BATCH_START))
            .build()
            .await
            .unwrap();
        start_fetch(&mut reader, &cluster, 1).await;
        if older_epoch {
            reader.shared.epoch.fetch_add(1, ORDERING);
            cluster.release(FETCH_MESSAGES_CODE);
        }
        // The server committed the read before it refused it.
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, Some(SECOND_BATCH_START))],
        );

        reader.close().await.unwrap();

        assert_rewound_before(
            &cluster,
            SECOND_BATCH_START,
            2,
            &format!("older epoch: {older_epoch}"),
        );
    }
}

/// A seek dropped once its store is on the wire still holds the reads of its partition until
/// the store ends, and then moves, so the close goes after it.
#[tokio::test(start_paused = true)]
async fn given_a_seek_dropped_while_its_store_is_on_the_wire_when_the_store_ends_should_move_the_reader_and_let_the_close_follow()
 {
    const SEEK_POINT: u64 = 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    cluster.hold(STORE_CONSUMER_OFFSET_CODE);
    tokio::select! {
        sought = reader.seek(PARTITION, PollingStrategy::offset(SEEK_POINT)) => {
            panic!("the store of the seek is held, yet the seek ended with {sought:?}");
        }
        () = eventually("the store of the seek arrives", || {
            cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
        }) => {}
    }
    script_fetches(&cluster, [server.read(&[SEEK_POINT])]);
    let reads_before = read_starts(&cluster).len();

    // The cancelled call leaves demand behind, which the hold keeps from reading.
    let waited = timeout(POLL_TIMEOUT, reader.next()).await;
    assert!(
        waited.is_err(),
        "no batch arrives while the store is on the wire"
    );
    assert_eq!(
        read_starts(&cluster).len(),
        reads_before,
        "no read of the partition starts while the store is on the wire"
    );
    cluster.release(STORE_CONSUMER_OFFSET_CODE);
    let resumed = next_batch(&mut reader).await;
    let closed = reader.close().await;

    assert_eq!(
        partition_and_offsets(&resumed),
        (PARTITION, vec![SEEK_POINT])
    );
    assert_eq!(
        targets_of(&cluster.received(FETCH_MESSAGES_CODE)[1]),
        [(PARTITION, WirePollingStrategy::offset(SEEK_POINT))],
        "the first read after the store starts at the seek point"
    );
    assert_eq!(closed, Ok(()));
    assert_eq!(stored_offsets(&cluster), [SEEK_POINT - 1]);
}

/// A seek dropped once its store is on the wire moves in the background and replaces the queued
/// batch of its partition. The close then keeps the store of the seek, and does not rewind
/// before that batch, which would skip the messages between the seek point and the batch.
#[tokio::test(start_paused = true)]
async fn given_a_queued_batch_when_a_dropped_seek_moves_in_the_background_should_close_without_rewinding_before_it()
 {
    const QUEUED_START: u64 = 10;
    const SEEK_POINT: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    let (mut header, records) = served(
        PARTITION,
        &[QUEUED_START, QUEUED_START + 1],
        QUEUED_START + 2,
    );
    // A server stamps the history of the partition on each section.
    header.context = CONTEXT;
    let (queued, queued_answer) = gated(sections_answer(&[(header, records)]));
    script_fetches(&cluster, [queued_answer]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(QUEUED_START))
        .build()
        .await
        .unwrap();
    // The application asks once and drops the call, so the batch stays queued.
    start_fetch(&mut reader, &cluster, 1).await;
    queued.notify_one();
    cluster.hold(STORE_CONSUMER_OFFSET_CODE);
    tokio::select! {
        sought = reader.seek(PARTITION, PollingStrategy::offset(SEEK_POINT)) => {
            panic!("the store of the seek is held, yet the seek ended with {sought:?}");
        }
        () = eventually("the store of the seek arrives", || {
            cluster.count(STORE_CONSUMER_OFFSET_CODE) == 1
        }) => {}
    }

    cluster.release(STORE_CONSUMER_OFFSET_CODE);
    let closed = reader.close().await;

    assert_eq!(closed, Ok(()));
    assert_eq!(
        stored_offsets(&cluster),
        [SEEK_POINT - 1],
        "the close keeps the store of the seek"
    );
}

/// A seek store with an unknown outcome can have moved the stored offset. The seek fails, the
/// next read reads by offset and skips no message, and a later seek and the close still write.
#[tokio::test(start_paused = true)]
async fn given_a_seek_store_with_an_unknown_outcome_when_reading_on_should_skip_no_message_and_keep_later_writes_working()
 {
    const SEEK_POINT: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );

    let sought = reader
        .seek(PARTITION, PollingStrategy::offset(SEEK_POINT))
        .await;
    // The store can have landed, so the reader reads by offset from where it was.
    script_fetches(
        &cluster,
        [sections_answer(&[served(
            PARTITION,
            &[SECOND_BATCH_START, SEEK_POINT],
            SEEK_POINT + 1,
        )])],
    );
    let recovered = next_batch(&mut reader).await;
    let later = reader
        .seek(PARTITION, PollingStrategy::offset(SECOND_BATCH_START))
        .await;
    let closed = reader.close().await;

    assert!(
        matches!(sought, Err(IggyError::OperationUncertain(_))),
        "{sought:?}"
    );
    assert_eq!(
        partition_and_offsets(&recovered),
        (PARTITION, vec![SECOND_BATCH_START, SEEK_POINT]),
        "the next read skips no message"
    );
    assert_eq!(later, Ok(()));
    assert_eq!(closed, Ok(()));
}

/// A seek before the first read follows the stored offset, so it looks that offset up before its
/// store. The store has an unknown outcome and lands. The reader reads on after the offset from
/// before the seek, and the first `Next` read that finds the moved offset reads the skipped
/// messages again.
#[tokio::test(start_paused = true)]
async fn given_a_seek_before_the_first_read_whose_store_has_an_unknown_outcome_when_reading_should_skip_no_message()
 {
    const STORED: u64 = 0;
    const SEEK_POINT: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [lookup_answer(PARTITION, Some(STORED))],
    );
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );

    let sought = reader
        .seek(PARTITION, PollingStrategy::offset(SEEK_POINT))
        .await;
    // The store landed. The first read serves one message, and the next `Next` read starts
    // after the stored offset.
    script_fetches(
        &cluster,
        [
            served_answer(PARTITION, STORED + 1),
            served_answer(PARTITION, SEEK_POINT),
        ],
    );
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [polled_answer(PARTITION, &[STORED + 2, SEEK_POINT])],
    );
    // A first read from another position fails the wait with its reason, before the batch
    // times out.
    let ((), first) = tokio::join!(
        eventually(
            "the first read starts after the offset stored before the seek",
            || {
                cluster
                    .received(FETCH_MESSAGES_CODE)
                    .first()
                    .is_some_and(|fetch| {
                        targets_of(fetch) == [(PARTITION, WirePollingStrategy::offset(STORED + 1))]
                    })
            }
        ),
        next_batch(&mut reader)
    );
    let second = next_batch(&mut reader).await;

    assert!(
        matches!(sought, Err(IggyError::OperationUncertain(_))),
        "{sought:?}"
    );
    assert_eq!(
        [
            partition_and_offsets(&first),
            partition_and_offsets(&second)
        ],
        [
            (PARTITION, vec![STORED + 1]),
            (PARTITION, vec![STORED + 2, SEEK_POINT])
        ],
        "the reads skip no message"
    );
}

/// A `Last` start whose lookup found no stored offset takes a seek whose store has an unknown
/// outcome. The close before any read deletes what the store may have left, so a restart
/// applies the start again.
#[tokio::test(start_paused = true)]
async fn given_a_last_start_without_a_stored_offset_when_a_seek_store_has_an_unknown_outcome_should_restore_the_empty_offset_at_the_close()
 {
    const SEEK_POINT: u64 = 3;
    let (client, cluster) = connect_to_three_replicas().await;
    // The check of the start waits, so the position stays `Last`.
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        repeat_n(Answer::Silence, SILENT_READS),
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::last())
        .build()
        .await
        .unwrap();
    eventually("the lookup of the start", || {
        cluster.count(GET_CONSUMER_OFFSET_CODE) == 1
    })
    .await;
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );

    let sought = reader
        .seek(PARTITION, PollingStrategy::offset(SEEK_POINT))
        .await;
    // The store landed.
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [lookup_answer(PARTITION, Some(SEEK_POINT - 1))],
    );
    let closed = reader.close().await;

    assert!(
        matches!(sought, Err(IggyError::OperationUncertain(_))),
        "{sought:?}"
    );
    assert_eq!(closed, Ok(()));
    assert_eq!(
        offset_resets(&cluster),
        (vec![Some(PARTITION); 3], vec![Some(PARTITION)]),
        "the start, the seek and the close look the offset up, and the close deletes it"
    );
    assert_eq!(stored_offsets(&cluster), [SEEK_POINT - 1]);
}

/// An offset start whose lookup found no stored offset takes a seek back, and its store has an
/// unknown outcome. A close before any batch of the partition arrived deletes what the store may
/// have left, so a restart applies the start again and does not read before it. A queued batch
/// still decides the rewind: the close stores the offset before it and deletes nothing.
#[tokio::test(start_paused = true)]
async fn given_an_offset_start_without_a_stored_offset_when_a_backward_seek_store_has_an_unknown_outcome_should_restore_the_empty_offset_at_the_close()
 {
    const START: u64 = 3;
    const SEEK_POINT: u64 = 1;
    for queued in [false, true] {
        let (client, cluster) = connect_to_three_replicas().await;
        let mut reader = committing(standalone(client, [PARTITION]))
            .start(PollingStrategy::offset(START))
            .build()
            .await
            .unwrap();
        eventually("the lookup of the start", || {
            cluster.count(GET_CONSUMER_OFFSET_CODE) == 1
        })
        .await;
        cluster.script(
            &endpoint(PRIMARY),
            STORE_CONSUMER_OFFSET_CODE,
            [Answer::Silence],
        );

        let sought = reader
            .seek(PARTITION, PollingStrategy::offset(SEEK_POINT))
            .await;
        if queued {
            // The partition reads on from the start, and the application takes nothing.
            script_fetches(&cluster, [served_answer(PARTITION, START)]);
            start_fetch(&mut reader, &cluster, 1).await;
        }
        // The store landed.
        cluster.script(
            &endpoint(PRIMARY),
            GET_CONSUMER_OFFSET_CODE,
            [lookup_answer(PARTITION, Some(SEEK_POINT - 1))],
        );
        let closed = reader.close().await;

        assert!(
            matches!(sought, Err(IggyError::OperationUncertain(_))),
            "queued: {queued}: {sought:?}"
        );
        assert_eq!(closed, Ok(()), "queued: {queued}");
        if queued {
            assert_eq!(
                offset_resets(&cluster),
                (vec![Some(PARTITION); 2], Vec::new()),
                "the start and the seek look the offset up, and the close deletes nothing"
            );
            assert_eq!(stored_offsets(&cluster), [SEEK_POINT - 1, START - 1]);
        } else {
            assert_eq!(
                offset_resets(&cluster),
                (vec![Some(PARTITION); 3], vec![Some(PARTITION)]),
                "the start, the seek and the close look the offset up, and the close deletes it"
            );
            assert_eq!(stored_offsets(&cluster), [SEEK_POINT - 1]);
        }
    }
}

/// A seek to `First` before the first read deletes the stored offset, and the delete has an
/// unknown outcome. The close before any delivery stores that offset again, also when it would
/// find none, so a restart resumes after it and not at offset 0.
#[tokio::test(start_paused = true)]
async fn given_a_stored_offset_when_a_seek_to_first_deletes_it_with_an_unknown_outcome_should_store_it_again_at_the_close()
 {
    const STORED: u64 = 2;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut reader = committing(standalone(client, [PARTITION]))
        .build()
        .await
        .unwrap();
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [lookup_answer(PARTITION, Some(STORED))],
    );
    cluster.script(
        &endpoint(PRIMARY),
        DELETE_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );

    // The delete lands, so a later lookup finds no offset.
    let sought = reader.seek(PARTITION, PollingStrategy::first()).await;
    let closed = reader.close().await;

    assert!(
        matches!(sought, Err(IggyError::OperationUncertain(_))),
        "{sought:?}"
    );
    assert_eq!(closed, Ok(()));
    assert_eq!(stored_offsets(&cluster), [STORED]);
}

/// A backward seek after a delivery stores before its point with an unknown outcome. The close
/// stores the last delivered message again, so a restart does not deliver the read messages
/// again.
#[tokio::test(start_paused = true)]
async fn given_delivered_messages_when_a_backward_seek_store_has_an_unknown_outcome_should_store_the_last_delivered_offset_at_the_close()
 {
    const SEEK_POINT: u64 = 1;
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence],
    );

    let sought = reader
        .seek(PARTITION, PollingStrategy::offset(SEEK_POINT))
        .await;
    let closed = reader.close().await;

    assert!(
        matches!(sought, Err(IggyError::OperationUncertain(_))),
        "{sought:?}"
    );
    assert_eq!(closed, Ok(()));
    assert_eq!(
        stored_offsets(&cluster),
        [SEEK_POINT - 1, FIRST_BATCH[FIRST_BATCH.len() - 1]]
    );
}

/// A rewind store of the close with an unknown outcome fails the close, which abandons it on the
/// client, so a later write of the partition on the same client goes out.
#[tokio::test(start_paused = true)]
async fn given_a_close_rewind_with_an_unknown_outcome_when_closing_should_fail_and_release_the_partition()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&[SECOND_BATCH_START, SECOND_BATCH_START + 1])],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(SECOND_BATCH_START))
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    cluster.script(
        &endpoint(PRIMARY),
        STORE_CONSUMER_OFFSET_CODE,
        [Answer::Silence, Answer::Commit(COMMIT_OP)],
    );
    let client = reader.config.client.clone();

    let closed = reader.close().await;
    let later = client
        .read()
        .await
        .store_consumer_offset(
            &Consumer::new(identifier(CONSUMER_ID)),
            &identifier(STREAM_ID),
            &identifier(TOPIC_ID),
            Some(PARTITION),
            SECOND_BATCH_START - 1,
        )
        .await;

    assert!(
        matches!(closed, Err(IggyError::OperationUncertain(_))),
        "{closed:?}"
    );
    assert_eq!(later, Ok(()), "the close abandoned its write on the client");
    assert_eq!(stored_offsets(&cluster), [SECOND_BATCH_START - 1; 2]);
}

/// A refused delete leaves the offset on the server, so the reader keeps its record of it, and
/// the close finds the last delivered message stored.
#[tokio::test(start_paused = true)]
async fn given_a_refused_delete_when_seeking_to_offset_0_should_keep_the_commit_record_for_the_close()
 {
    let (client, cluster) = connect_to_three_replicas().await;
    let mut server = ServerOffset::default();
    cluster.script(
        &endpoint(PRIMARY),
        FETCH_MESSAGES_CODE,
        [server.read(&FIRST_BATCH)],
    );
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(PollingStrategy::offset(0))
        .build()
        .await
        .unwrap();
    next_batch(&mut reader).await;
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [server.lookup()],
    );
    cluster.script(
        &endpoint(PRIMARY),
        DELETE_CONSUMER_OFFSET_CODE,
        [Answer::CommitRejected(
            COMMIT_OP,
            IggyError::HistoryUnavailable,
        )],
    );

    let sought = reader.seek(PARTITION, PollingStrategy::first()).await;
    let closed = reader.close().await;

    assert_eq!(sought, Err(IggyError::HistoryUnavailable));
    assert_eq!(closed, Ok(()));
    assert!(
        stored_offsets(&cluster).is_empty(),
        "the close stores the delivered offset again only after a lost record"
    );
}

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
    assignment_at(GROUP_GENERATION, partitions)
}

/// The server bumps the generation of a group whenever the partitions of a member change.
fn assignment_at(generation: u64, partitions: &[u32]) -> Answer {
    Answer::Body(sync_body(generation, partitions))
}

/// [`assignment_at`] as a replica read it at the metadata commit `commit`.
fn assignment_at_commit(commit: u64, generation: u64, partitions: &[u32]) -> Answer {
    Answer::CommitBody(commit, sync_body(generation, partitions))
}

fn sync_body(generation: u64, partitions: &[u32]) -> Vec<u8> {
    SyncConsumerGroupResponse {
        generation,
        partitions: partitions.to_vec(),
    }
    .to_bytes()
    .to_vec()
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

fn script_initial_next(cluster: &FakeCluster, starts: &[(u32, u64)]) {
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        starts
            .iter()
            .map(|&(partition, offset)| polled_answer(partition, &[offset])),
    );
}

fn sections_answer(sections: &[Section]) -> Answer {
    Answer::Body(fetch_body(sections).to_vec())
}

/// The offset that the server stores for the consumer of [`PARTITION`]. The commit of a read
/// moves it only forward, a delete removes it, and a delete of a missing offset fails, as does
/// a store past the last message. The replicas answer by script, so a test takes each answer
/// from this model before its request goes out, and applies a delete only once it arrived.
#[derive(Default)]
struct ServerOffset {
    stored: Option<u64>,
}

impl ServerOffset {
    /// The reply of a committing read of `offsets`.
    fn read(&mut self, offsets: &[u64]) -> Answer {
        let next_offset = offsets.last().map_or(0, |last| last + 1);
        self.commit(next_offset);
        sections_answer(&[served(PARTITION, offsets, next_offset)])
    }

    /// The reply of a committing `Next` read, which starts after the stored offset and serves
    /// the rest of the partition.
    fn read_next(&mut self) -> Answer {
        let from = self.stored.map_or(0, |stored| stored + 1);
        let offsets: Vec<u64> = (from..PARTITION_END).collect();
        let next_offset = from.max(PARTITION_END);
        self.commit(next_offset);
        sections_answer(&[served(PARTITION, &offsets, next_offset)])
    }

    fn commit(&mut self, next_offset: u64) {
        if let Some(last) = next_offset.checked_sub(1) {
            self.stored = Some(self.stored.map_or(last, |stored| stored.max(last)));
        }
    }

    fn lookup(&self) -> Answer {
        lookup_answer(PARTITION, self.stored)
    }

    fn store(&self, offset: u64) -> Answer {
        if offset < PARTITION_END {
            Answer::Commit(COMMIT_OP)
        } else {
            Answer::CommitRejected(COMMIT_OP, IggyError::InvalidOffset(offset))
        }
    }

    fn delete(&self) -> Answer {
        match self.stored {
            Some(_) => Answer::Commit(COMMIT_OP),
            None => Answer::CommitRejected(
                COMMIT_OP,
                IggyError::ConsumerOffsetNotFound(CONSUMER_ID as usize),
            ),
        }
    }

    fn apply_deletes(&mut self, cluster: &FakeCluster) {
        if self.stored.is_some() && cluster.count(DELETE_CONSUMER_OFFSET_CODE) > 0 {
            self.stored = None;
        }
    }

    /// An explicit store sets the offset, also below the stored one.
    fn apply_stores(&mut self, cluster: &FakeCluster) {
        if let Some(&stored) = stored_offsets(cluster).last() {
            self.stored = Some(stored);
        }
    }
}

/// The start of each deferred poll that `cluster` received, then of each fetch target.
fn read_starts(cluster: &FakeCluster) -> Vec<WirePollingStrategy> {
    let polls = cluster
        .received(POLL_MESSAGES_DEFERRED_CODE)
        .into_iter()
        .map(|poll| {
            DeferredPollMessagesRequest::decode_from(&poll.body)
                .unwrap()
                .poll
                .strategy
        });
    let fetches = cluster
        .received(FETCH_MESSAGES_CODE)
        .into_iter()
        .flat_map(|fetch| targets_of(&fetch).into_iter().map(|(_, strategy)| strategy));
    polls.chain(fetches).collect()
}

/// The `Next` continuation that expects `offset`.
fn next_at(offset: u64) -> WirePollingStrategy {
    WirePollingStrategy {
        value: offset,
        ..WirePollingStrategy::next()
    }
}

/// Reads [`PARTITION`] from `start` with a check that resolves it to `resolved`, loses the reply
/// of the first committing read and closes. The lookup of the close finds the offset that the
/// lost read committed.
async fn close_after_a_lost_first_read(start: PollingStrategy, resolved: u64) -> Arc<FakeCluster> {
    let (client, cluster) = connect_to_three_replicas().await;
    // A server stamps the history of the partition on each reply, so the check names a message
    // of a known history.
    cluster.script(
        &endpoint(PRIMARY),
        POLL_MESSAGES_DEFERRED_CODE,
        [polled_answer_in(PARTITION, &[resolved], CONTEXT)],
    );
    script_fetches(&cluster, [Answer::Silence]);
    let mut reader = committing(standalone(client, [PARTITION]))
        .start(start)
        .build()
        .await
        .unwrap();
    start_fetch(&mut reader, &cluster, 1).await;
    cluster.script(
        &endpoint(PRIMARY),
        GET_CONSUMER_OFFSET_CODE,
        [lookup_answer(PARTITION, Some(resolved))],
    );
    reader.close().await.unwrap();
    cluster
}

/// After `lookups` lookups, the close stored the offset before `resolved`, or deleted it at 0.
fn assert_rewound_before(cluster: &FakeCluster, resolved: u64, lookups: usize, case: &str) {
    let (looked_up, deleted) = offset_resets(cluster);
    assert_eq!(looked_up, vec![Some(PARTITION); lookups], "{case}");
    match resolved.checked_sub(1) {
        Some(before) => {
            assert_eq!(stored_offsets(cluster), [before], "{case}");
            assert!(deleted.is_empty(), "{case}");
        }
        None => {
            assert!(stored_offsets(cluster).is_empty(), "{case}");
            assert_eq!(deleted, [Some(PARTITION)], "{case}");
        }
    }
}

/// The partitions of the offset lookups and of the offset deletes that `cluster` received.
fn offset_resets(cluster: &FakeCluster) -> (Vec<Option<u32>>, Vec<Option<u32>>) {
    let lookups = cluster
        .received(GET_CONSUMER_OFFSET_CODE)
        .iter()
        .map(|lookup| {
            GetConsumerOffsetRequest::decode_from(&lookup.body)
                .unwrap()
                .partition_id
        })
        .collect();
    let deletes = cluster
        .received(DELETE_CONSUMER_OFFSET_CODE)
        .iter()
        .map(|delete| {
            DeleteConsumerOffsetRequest::decode_from(&delete.body)
                .unwrap()
                .partition_id
        })
        .collect();
    (lookups, deletes)
}

/// `answer`, which the replica sends only once the returned notify fires.
fn gated(answer: Answer) -> (Arc<Notify>, Answer) {
    let Answer::Body(body) = answer else {
        panic!("only a reply with a body waits for a release");
    };
    let release = Arc::new(Notify::new());
    let gated = Answer::GatedBody {
        release: Arc::clone(&release),
        completed: Arc::default(),
        body,
    };
    (release, gated)
}

/// A fetch reply that serves one message of `partition` at `offset`.
fn served_answer(partition: u32, offset: u64) -> Answer {
    sections_answer(&[served(partition, &[offset], offset + 1)])
}

/// A poll reply with one message of `partition` at each of `offsets`.
fn polled_answer(partition: u32, offsets: &[u64]) -> Answer {
    polled_answer_in(partition, offsets, PartitionContext::default())
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
