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

//! A committing `Next` read stores at most once, and only while the stored
//! offset is still the one it was admitted against.
//!
//! One client reads one partition with `POLL_MESSAGES_DEFERRED` plus
//! `auto_commit`, through the dispatch shell, so the origin and the owner run
//! the production code. The cases compose what the shard and session unit
//! tests prove one piece at a time: a reply lost on the wire after the owner
//! stored, a read that commits after a later read already moved the offset, a
//! cancellation that wins or loses against the store, a rewind racing a parked
//! read, and a request that waited in the connection drain past its timeout.

use bytes::Bytes;
use consensus::PartitionsHandle;
use iggy_binary_protocol::responses::messages::PollMessagesResponse;
use iggy_binary_protocol::{
    AckLevel, Command, KIND_CONSUMER, ReplyHeader, RoutedRequestHeader, WirePollingStrategy,
};
use iggy_common::IggyError;
use partitions::{Partition, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::CONSENSUS_TICK_INTERVAL;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{Packet, PacketSimulatorOptions, ProcessId};

const CLIENT: u128 = 1;
/// A second session that stores for the first client's consumer.
const OTHER_CLIENT: u128 = 2;
const PRIMARY: u8 = 0;
const REPLICAS: u8 = 3;
const SEED: u64 = 0xD4C0_1117;
const PARTITION: usize = 0;
/// Steps a reply or a store is given before the case fails.
const REPLY_STEPS: usize = 400;
/// Steps the dropped reply is given to reach its link and die there: well past the
/// mean one way delay, which is a few ticks.
const LOST_WINDOW_STEPS: usize = 60;
/// A parked read waits this long, longer than any case runs.
const PARKED_WAIT_US: u64 = 5_000_000;
const PARKED_TIMEOUT_US: u64 = 7_000_000;
/// A read that must not wait still carries a timeout the owner can enforce.
const QUICK_TIMEOUT_US: u64 = 2_000_000;
/// The queued read of the drain case gives up this long after its frame arrived.
const QUEUED_TIMEOUT_US: u64 = 1_500_000;

/// (a) The lost read committed first: the owner stored, the reply died on the
/// wire, and the client's next `Next` read starts after the stored offset, so
/// the gap it never saw shows as a first offset above its last delivered plus
/// one. The stored offset moved exactly once for the lost read.
#[test]
fn given_a_committing_read_whose_reply_is_lost_when_the_next_read_runs_should_start_after_the_single_store()
 {
    let (mut sim, client, _) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1", "m2", "m3"], &mut replies);
    assert_eq!(stored_offset(&sim), None);

    drop_replies(&mut sim, true);
    let lost = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUICK_TIMEOUT_US)),
    );
    assert!(
        (0..REPLY_STEPS).any(|_| {
            replies.extend(sim.step());
            stored_offset(&sim) == Some(1)
        }),
        "the lost read must store the offset of its last message"
    );
    settle(&mut sim, &mut replies);
    drop_replies(&mut sim, false);
    assert_eq!(stored_offset(&sim), Some(1), "one store for the lost read");

    let next = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUICK_TIMEOUT_US)),
    );
    let reply = await_reply(&mut sim, (CLIENT, next), &mut replies);
    assert_eq!(reply.header().status, 0, "the next read is served");
    assert_eq!(
        offsets_of(&reply),
        vec![2, 3],
        "the next read starts after the lost read's store, so the gap 0..1 is visible"
    );
    assert_eq!(stored_offset(&sim), Some(3));
    assert!(
        replies.iter().all(|reply| reply.header().request != lost),
        "the lost reply never reached the client"
    );
}

/// (a) The next read committed first: the lost read was still parked at the
/// owner when the client's next read stored, so its late commit finds the
/// stored offset moved since its admission and is refused with 58.
#[test]
fn given_a_parked_committing_read_when_a_later_read_stores_first_should_refuse_its_late_commit() {
    let (mut sim, client, other) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1"], &mut replies);

    let parked = submit(
        &mut sim,
        &client,
        next_read(&client, (4, 4), (PARKED_WAIT_US, PARKED_TIMEOUT_US)),
    );
    settle(&mut sim, &mut replies);
    assert!(
        replies.is_empty(),
        "two messages under min_count 4 park the read"
    );
    assert_eq!(stored_offset(&sim), None);

    let next = submit(
        &mut sim,
        &other,
        other.deferred_poll_messages_for_consumer(
            namespace(),
            WirePollingStrategy::next(),
            (2, 1),
            true,
            (0, QUICK_TIMEOUT_US),
            u32::try_from(CLIENT).expect("client id fits u32"),
        ),
    );
    let reply = await_reply(&mut sim, (OTHER_CLIENT, next), &mut replies);
    assert_eq!(reply.header().status, 0);
    assert_eq!(offsets_of(&reply), vec![0, 1]);
    assert_eq!(stored_offset(&sim), Some(1), "the later read stored first");

    produce(&mut sim, &client, &["m2", "m3"], &mut replies);
    let late = await_reply(&mut sim, (CLIENT, parked), &mut replies);
    assert_eq!(
        late.header().status,
        IggyError::TransientNotAccepted.as_code(),
        "the parked read's commit is fenced by the moved offset"
    );
    assert_eq!(
        stored_offset(&sim),
        Some(1),
        "the fenced read stored nothing"
    );
}

/// (b) The cancel wins: a parked committing read is cancelled before any read
/// passed its gate, so it answers exactly once with 58 and the produce that
/// would have filled it stores nothing.
#[test]
fn given_a_parked_committing_read_when_cancelled_before_its_store_should_answer_58_and_store_nothing()
 {
    let (mut sim, client, _) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1"], &mut replies);

    let parked = submit(
        &mut sim,
        &client,
        next_read(&client, (4, 4), (PARKED_WAIT_US, PARKED_TIMEOUT_US)),
    );
    settle(&mut sim, &mut replies);
    assert!(replies.is_empty(), "the read parks");

    submit(
        &mut sim,
        &client,
        client.cancel_deferred_request(namespace(), parked),
    );
    let cancelled = await_reply(&mut sim, (CLIENT, parked), &mut replies);
    assert_eq!(
        cancelled.header().status,
        IggyError::TransientNotAccepted.as_code(),
        "a cancelled committing read answers once with 58"
    );
    assert_eq!(stored_offset(&sim), None);

    produce(&mut sim, &client, &["m2", "m3"], &mut replies);
    settle(&mut sim, &mut replies);
    assert!(
        replies.iter().all(|reply| reply.header().request != parked),
        "the cancelled read answers only once"
    );
    assert_eq!(
        stored_offset(&sim),
        None,
        "the cancelled read stores nothing"
    );
}

/// (c) The cancel loses: the owner stored before the cancellation arrived, so
/// the read answers as usual, the store stands and no 58 follows.
#[test]
fn given_a_committing_read_that_stored_when_its_cancel_arrives_late_should_keep_the_store_and_answer_once()
 {
    let (mut sim, client, _) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1"], &mut replies);

    let parked = submit(
        &mut sim,
        &client,
        next_read(&client, (4, 4), (PARKED_WAIT_US, PARKED_TIMEOUT_US)),
    );
    settle(&mut sim, &mut replies);
    assert!(replies.is_empty(), "the read parks");

    produce(&mut sim, &client, &["m2", "m3"], &mut replies);
    assert!(
        (0..REPLY_STEPS).any(|_| {
            replies.extend(sim.step());
            stored_offset(&sim) == Some(3)
        }),
        "the produce fills the read and the owner stores"
    );
    submit(
        &mut sim,
        &client,
        client.cancel_deferred_request(namespace(), parked),
    );
    let reply = await_reply(&mut sim, (CLIENT, parked), &mut replies);
    assert_eq!(
        reply.header().status,
        0,
        "the cancel lost, the read answers as usual"
    );
    assert_eq!(offsets_of(&reply), vec![0, 1, 2, 3]);
    settle(&mut sim, &mut replies);
    assert!(
        replies.iter().all(|reply| reply.header().request != parked),
        "no second reply follows the lost cancel"
    );
    assert_eq!(stored_offset(&sim), Some(3), "the store stands");
}

/// (d) A close rewind against a late commit: the client delivered 0..1, lost
/// the reply of the read that stored 3, and parked a `Next` read at 4. The
/// rewind to the last delivered message comes from another session, because
/// the drain of the client's own connection orders a store of a consumer
/// behind that consumer's in-flight committing read. The produce that fills
/// the parked read must not let it commit past the rewind.
#[test]
fn given_a_parked_committing_read_when_another_session_rewinds_its_offset_should_refuse_the_late_commit()
 {
    let (mut sim, client, other) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1", "m2", "m3"], &mut replies);

    let delivered = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUICK_TIMEOUT_US)),
    );
    let reply = await_reply(&mut sim, (CLIENT, delivered), &mut replies);
    assert_eq!(offsets_of(&reply), vec![0, 1]);

    drop_replies(&mut sim, true);
    let lost = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUICK_TIMEOUT_US)),
    );
    assert!(
        (0..REPLY_STEPS).any(|_| {
            replies.extend(sim.step());
            stored_offset(&sim) == Some(3)
        }),
        "the lost read stores 3"
    );
    settle(&mut sim, &mut replies);
    drop_replies(&mut sim, false);

    let parked = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 2), (PARKED_WAIT_US, PARKED_TIMEOUT_US)),
    );
    settle(&mut sim, &mut replies);
    assert!(
        replies.iter().all(|reply| reply.header().request != parked),
        "nothing past 3 yet, the read parks at 4"
    );

    let rewind = other.store_consumer_offset(
        namespace(),
        KIND_CONSUMER,
        u32::try_from(CLIENT).expect("client id fits u32"),
        1,
        AckLevel::Quorum,
    );
    let rewind = submit(&mut sim, &other, rewind);
    let rewound = await_reply(&mut sim, (OTHER_CLIENT, rewind), &mut replies);
    assert_eq!(rewound.header().status, 0, "the rewind commits");
    assert_eq!(stored_offset(&sim), Some(1));

    produce(&mut sim, &client, &["m4", "m5"], &mut replies);
    let late = await_reply(&mut sim, (CLIENT, parked), &mut replies);
    assert_eq!(
        late.header().status,
        IggyError::TransientNotAccepted.as_code(),
        "the read admitted at 4 is fenced by the rewind to 1"
    );
    assert_eq!(stored_offset(&sim), Some(1), "the rewound offset stands");
    assert!(
        replies.iter().all(|reply| reply.header().request != lost),
        "the lost reply never reached the client"
    );
}

/// (e) A committing read waits in the connection drain behind an earlier
/// committing read of the same consumer. The earlier read outlives the next
/// request's deadline, so admission denies the queued request before it reads.
#[test]
fn given_a_committing_read_queued_past_its_timeout_when_the_drain_serves_it_should_deny_it_unread()
{
    let (mut sim, client, _) = cluster();
    let mut replies = Vec::new();
    produce(&mut sim, &client, &["m0", "m1", "m2", "m3"], &mut replies);
    let first = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUICK_TIMEOUT_US)),
    );
    let reply = await_reply(&mut sim, (CLIENT, first), &mut replies);
    assert_eq!(offsets_of(&reply), vec![0, 1]);

    let parked = submit(
        &mut sim,
        &client,
        next_read(&client, (4, 4), (PARKED_WAIT_US, PARKED_TIMEOUT_US)),
    );
    settle(&mut sim, &mut replies);
    assert!(replies.is_empty(), "the first read parks");
    let queued = submit(
        &mut sim,
        &client,
        next_read(&client, (2, 1), (0, QUEUED_TIMEOUT_US)),
    );
    for _ in 0..=steps_of(QUEUED_TIMEOUT_US) {
        replies.extend(sim.step());
    }
    assert!(
        replies.iter().all(|reply| reply.header().request != queued),
        "the queued read waits behind the first read's progress lane"
    );
    assert_eq!(
        stored_offset(&sim),
        Some(1),
        "nothing new was read while queued"
    );

    let parked_reply = await_reply(&mut sim, (CLIENT, parked), &mut replies);
    assert_eq!(parked_reply.header().status, 0);
    assert_eq!(offsets_of(&parked_reply), vec![2, 3]);

    let denied = await_reply(&mut sim, (CLIENT, queued), &mut replies);
    assert_eq!(
        denied.header().status,
        IggyError::TransientNotAccepted.as_code(),
        "a read served after its client gave up is denied unread"
    );
    assert_eq!(
        stored_offset(&sim),
        Some(3),
        "the denied read stored nothing"
    );
}

/// Three replicas of one shard each with the dispatch shell on, one seeded
/// partition, two clients logged in against root on the primary.
fn cluster() -> (Simulator, SimClient, SimClient) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let network = PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: 2,
        seed: SEED,
        ..PacketSimulatorOptions::default()
    };
    let mut sim = Simulator::with_shards_shell(
        usize::from(REPLICAS),
        1,
        [CLIENT, OTHER_CLIENT].into_iter(),
        network,
    );
    sim.init_partition(namespace());
    sim.seed_stream_topic_partition(namespace());
    let client = SimClient::new(CLIENT);
    let other = SimClient::new(OTHER_CLIENT);
    sim.shell_login(&client);
    sim.shell_login(&other);
    sim.discover_partition_contexts(&client);
    sim.discover_partition_contexts(&other);
    (sim, client, other)
}

fn produce(
    sim: &mut Simulator,
    client: &SimClient,
    payloads: &[&str],
    replies: &mut Vec<Message<ReplyHeader>>,
) {
    let payloads: Vec<Bytes> = payloads
        .iter()
        .map(|payload| Bytes::from((*payload).to_owned()))
        .collect();
    let request = submit(sim, client, client.send_messages(namespace(), &payloads));
    let reply = await_reply(sim, (client.client_id(), request), replies);
    assert_eq!(reply.header().status, 0, "the produce commits");
}

/// A committing `Next` read of the client's own consumer.
fn next_read(
    client: &SimClient,
    counts: (u32, u32),
    timers: (u64, u64),
) -> Message<RoutedRequestHeader> {
    client.deferred_poll_messages(
        namespace(),
        WirePollingStrategy::next(),
        counts,
        true,
        timers,
    )
}

/// Sends `request` to the primary and returns its wire request id.
fn submit(sim: &mut Simulator, client: &SimClient, request: Message<RoutedRequestHeader>) -> u64 {
    let request_id = request.header().request;
    sim.submit_request(client.client_id(), PRIMARY, request.into_generic());
    request_id
}

/// The reply to `request` of `client`, from those already kept or from the
/// steps to come. Every other reply is kept.
fn await_reply(
    sim: &mut Simulator,
    (client, request): (u128, u64),
    replies: &mut Vec<Message<ReplyHeader>>,
) -> Message<ReplyHeader> {
    let answers = |reply: &Message<ReplyHeader>| {
        reply.header().client == client && reply.header().request == request
    };
    if let Some(index) = replies.iter().position(answers) {
        return replies.remove(index);
    }
    for _ in 0..REPLY_STEPS {
        for reply in sim.step() {
            if answers(&reply) {
                return reply;
            }
            replies.push(reply);
        }
    }
    panic!("request {request} of client {client} got no reply within {REPLY_STEPS} steps");
}

/// Steps long enough for anything in flight to land, keeping the replies.
fn settle(sim: &mut Simulator, replies: &mut Vec<Message<ReplyHeader>>) {
    for _ in 0..LOST_WINDOW_STEPS {
        replies.extend(sim.step());
    }
}

/// Loses every reply on the way from the primary to the client, or stops losing them.
fn drop_replies(sim: &mut Simulator, dropping: bool) {
    *sim.network
        .link_drop_packet_fn(ProcessId::Replica(PRIMARY), ProcessId::Client(CLIENT)) =
        dropping.then_some(is_reply as fn(&Packet) -> bool);
}

fn is_reply(packet: &Packet) -> bool {
    packet.message.header().command == Command::Reply
}

/// The offset the primary stores for the client's consumer on the partition.
fn stored_offset(sim: &Simulator) -> Option<u64> {
    let consumer = PollingConsumer::Consumer(
        usize::try_from(CLIENT).expect("client id fits usize"),
        PARTITION,
    );
    sim.replicas[usize::from(PRIMARY)]
        .partition_shard(namespace())
        .plane
        .partitions()
        .get_by_ns(&namespace())
        .expect("the primary hosts the partition")
        .get_consumer_offset(consumer)
}

fn offsets_of(reply: &Message<ReplyHeader>) -> Vec<u64> {
    let response = PollMessagesResponse::decode(reply.body()).expect("a poll reply decodes");
    response
        .messages
        .map(|message| message.expect("a polled message decodes").offset)
        .collect()
}

/// Steps that cover `micros` of virtual time, one consensus tick per step.
#[allow(clippy::cast_possible_truncation)]
const fn steps_of(micros: u64) -> usize {
    micros.div_ceil(CONSENSUS_TICK_INTERVAL.as_micros() as u64) as usize
}

fn namespace() -> IggyNamespace {
    IggyNamespace::new(0, 0, PARTITION)
}
