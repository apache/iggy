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

//! The offset stores of consumers on shadow partitions, whose replies wait
//! for a round to store their positions, and the reads that wait for the
//! peers' positions.

use super::object_deletes::layout;
use super::segment_uploads::namespace;
use super::*;
use crate::geo_commits::HeldReader;
use crate::geo_positions::GeoPositionSettings;
use consensus::build_reply_message_with;
use iggy_common::{GeoMode, IggyError};
use iggy_object_store::{InlineTierExecutor, MemoryStore, ObjectPrefix};
use metadata::stm::consumer_group::ConsumerGroup;
use metadata::stm::shadow::{ShadowRetention, ShadowStream, ShadowTopic};
use partitions::{ExecutorTopicObjects, PollingConsumer, TopicObjects};

const ORIGIN: &str = "dc-a";
const GROUP: u32 = 5;
const DELETED_AT: u64 = 1_000_000;

/// The peer's partition incarnation that the shadow partition follows.
const PEER_PREFIX: ObjectPrefix = ObjectPrefix {
    stream_id: 7,
    topic_id: 0,
    partition_id: 0,
    created_revision: 41,
};

/// A shard whose partition [`namespace`] is the primary of a shadow
/// partition of [`ORIGIN`], on a node that holds the offset stores of
/// groups until their positions are stored.
fn shadow_owner() -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (mut owner, _sender) = test_owner(&bus, None);
    owner.plane.inner_mut().0 = shadow_metadata(None);
    {
        let partitions = owner.plane.partitions();
        let partition = partitions.get_mut_by_ns(&namespace()).unwrap();
        partition.register_builtin_state_machines().unwrap();
        partition.follow_peer_log(ORIGIN, PEER_PREFIX).unwrap();
    }
    owner.set_geo_positions(Some(GeoPositionSettings {
        origin: "dc-b".to_owned(),
        peers: vec![ORIGIN.to_owned()],
        publish_interval: Duration::from_secs(1),
        offset_upload_interval: Some(Duration::from_millis(50)),
        resume_timeout: Duration::from_secs(10),
    }));
    owner.set_topic_objects(Some(Rc::new(ExecutorTopicObjects::new(
        InlineTierExecutor::new(MemoryStore::new()),
        layout(),
    )) as Rc<dyn TopicObjects>));
    owner
}

/// The committed metadata of the shadow topic, which the origin deleted
/// at `deleted_at` when it holds, with the group [`GROUP`].
fn shadow_metadata(
    deleted_at: Option<u64>,
) -> IggyMetadata<
    VsrConsensus<Rc<IggyMessageBus>>,
    PrepareJournal,
    (),
    IoTestMetadata,
    HeldSuperblock,
> {
    let mut stream = Stream::default();
    stream.shadow = Some(ShadowStream {
        origin: ORIGIN.to_owned(),
        cluster_uid: 7,
        stream_id: PEER_PREFIX.stream_id,
    });
    let mut topic = Topic {
        shadow: Some(ShadowTopic {
            created_revision: PEER_PREFIX.created_revision,
            deleted_at,
            retention: ShadowRetention::default(),
            geo_mode: GeoMode::Geo,
        }),
        ..Default::default()
    };
    for partition_id in 0..TEST_PARTITIONS {
        topic.partitions.push(Partition::new(
            partition_id,
            IggyNamespace::new(0, 0, partition_id).inner(),
            IggyTimestamp::default(),
            1,
            0,
        ));
    }
    topic.consumer_groups.insert(
        u64::from(GROUP),
        Box::new(ConsumerGroup::new(u64::from(GROUP), Arc::from("billing"))),
    );
    stream.topics.insert(topic);
    let mut inner = StreamsInner::default();
    inner.items.insert(Arc::new(stream));
    let metadata = IoTestMetadata::new((Users::default(), (inner.into(), ())));
    IggyMetadata::new(None, None, None, None, metadata, None)
}

/// A `StoreConsumerOffset` that the group [`GROUP`] sends itself, with a
/// quorum ack.
fn group_store() -> Message<RoutedRequestHeader> {
    store(
        ConsumerKind::ConsumerGroup,
        WireIdentifier::Numeric(GROUP),
        AckLevel::Quorum,
    )
}

/// A `StoreConsumerOffset` of the consumer `id` of `kind`, as it reaches
/// the shard.
fn store(kind: ConsumerKind, id: WireIdentifier, ack: AckLevel) -> Message<RoutedRequestHeader> {
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id,
        },
        stream_id: WireIdentifier::Numeric(0),
        topic_id: WireIdentifier::Numeric(0),
        partition_id: Some(0),
        offset: 3,
        ack,
    }
    .to_bytes();
    let header_size = size_of::<RoutedRequestHeader>();
    let size = header_size + body.len();
    let mut request = Message::<RoutedRequestHeader>::new(size);
    request.as_mut_slice()[header_size..].copy_from_slice(&body);
    request.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::StoreConsumerOffset;
        header.client = 1;
        header.session = 1;
        header.request = 1;
        header.group = namespace().inner();
        header.size = u32::try_from(size).unwrap();
    })
}

#[test]
fn given_a_detached_shadow_topic_when_a_group_stores_its_offset_should_not_hold_the_reply() {
    let mut owner = shadow_owner();
    assert!(
        owner.geo_commit_hold(&group_store()).is_some(),
        "a round stores the positions of a shadow topic its origin holds"
    );

    owner.plane.inner_mut().0 = shadow_metadata(Some(DELETED_AT));

    assert!(owner.geo_commit_hold(&group_store()).is_none());
}

#[test]
fn given_a_held_store_when_its_shadow_topic_detaches_should_answer_it_as_committed() {
    let mut owner = shadow_owner();
    let hold = owner.geo_commit_hold(&group_store()).unwrap();
    let committed = build_reply_message_with(
        &PrepareHeader {
            operation: Operation::StoreConsumerOffset,
            ..Default::default()
        },
        0,
        |_| {},
    );
    let (reply, replies) = channel(1);
    hold.keep(committed, reply);
    let Ok(ShardFrame::Lifecycle(LifecycleFrame::GeoCommitHeld(commit))) = owner.inbox.try_recv()
    else {
        panic!("the shard's pump takes the held reply");
    };
    owner.on_geo_commit_held(*commit);
    owner.expire_geo_commits();
    assert!(
        replies.try_recv().is_err(),
        "the reply waits for a round to store its position"
    );

    owner.plane.inner_mut().0 = shadow_metadata(Some(DELETED_AT));
    owner.expire_geo_commits();

    let reply = replies.try_recv().unwrap().unwrap();
    assert_eq!(
        reply
            .try_into_typed::<ReplyHeader>()
            .unwrap()
            .header()
            .status,
        0
    );
}

#[test]
fn given_an_external_group_or_a_plain_consumer_when_it_stores_its_offset_should_hold_the_reply_too()
{
    let owner = shadow_owner();
    let reporter = WireIdentifier::named("reporter").unwrap();

    for (kind, id, held) in [
        (
            ConsumerKind::ExternalGroup,
            WireIdentifier::Numeric(GROUP),
            GROUP,
        ),
        (ConsumerKind::Consumer, WireIdentifier::Numeric(7), 7),
        (
            ConsumerKind::Consumer,
            reporter,
            iggy_common::calculate_32(b"reporter"),
        ),
    ] {
        let hold = owner
            .geo_commit_hold(&store(kind, id.clone(), AckLevel::Quorum))
            .unwrap_or_else(|| panic!("a store of {kind:?} {id:?} waits for its position"));
        assert_eq!(hold.reader(), HeldReader { kind, id: held });
        assert!(
            owner
                .geo_commit_hold(&store(kind, id, AckLevel::NoAck))
                .is_none(),
            "only a quorum ack waits"
        );
    }
}

#[test]
fn given_a_held_store_of_a_plain_consumer_when_no_group_has_its_id_should_keep_waiting_for_a_round()
{
    let owner = shadow_owner();
    let hold = owner
        .geo_commit_hold(&store(
            ConsumerKind::Consumer,
            WireIdentifier::Numeric(77),
            AckLevel::Quorum,
        ))
        .unwrap();
    let committed = build_reply_message_with(
        &PrepareHeader {
            operation: Operation::StoreConsumerOffset,
            ..Default::default()
        },
        0,
        |_| {},
    );
    let (reply, replies) = channel(1);
    hold.keep(committed, reply);
    let Ok(ShardFrame::Lifecycle(LifecycleFrame::GeoCommitHeld(commit))) = owner.inbox.try_recv()
    else {
        panic!("the shard's pump takes the held reply");
    };
    owner.on_geo_commit_held(*commit);

    owner.expire_geo_commits();

    assert!(
        replies.try_recv().is_err(),
        "a plain consumer is never gone, so its reply waits for its position"
    );
}

#[test]
fn given_a_shadow_partition_while_the_node_waits_for_its_peers_when_a_plain_consumer_or_the_gateway_reads_should_answer_58()
 {
    let owner = shadow_owner();
    assert!(owner.geo_positions_resume_waiting(namespace()));

    assert_eq!(
        owner.geo_position_refusal(namespace(), PollingConsumer::Consumer(7, 0)),
        Some(IggyError::TransientNotAccepted)
    );
    assert_eq!(
        owner.geo_reader_refusal(namespace(), ConsumerKind::ExternalGroup, GROUP),
        Some(IggyError::TransientNotAccepted)
    );
    assert_eq!(
        owner.geo_missing_reader_refusal(namespace()),
        Some(IggyError::TransientNotAccepted)
    );
}

#[test]
fn given_a_shadow_partition_once_the_wait_ended_when_a_plain_consumer_or_the_gateway_reads_should_answer()
 {
    let owner = shadow_owner();
    owner.set_geo_positions(Some(GeoPositionSettings {
        origin: "dc-b".to_owned(),
        peers: vec![ORIGIN.to_owned()],
        publish_interval: Duration::from_secs(1),
        offset_upload_interval: Some(Duration::from_millis(50)),
        resume_timeout: Duration::ZERO,
    }));

    assert_eq!(
        owner.geo_position_refusal(namespace(), PollingConsumer::Consumer(7, 0)),
        None
    );
    assert_eq!(
        owner.geo_reader_refusal(namespace(), ConsumerKind::ExternalGroup, GROUP),
        None
    );
    assert_eq!(owner.geo_missing_reader_refusal(namespace()), None);
}
