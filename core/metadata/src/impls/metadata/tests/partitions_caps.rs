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

use super::*;

#[test]
fn given_no_partitions_cap_when_admitting_create_topic_should_admit() {
    let mut metadata = metadata_plane();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));

    assert!(
        metadata
            .admit_partitions(&create_topic_request(1, TOPIC_MANAGER))
            .is_ok(),
        "a zero partitions_max must admit a create on a node with partitions"
    );
}

/// The gated apply answers a create from a user without the grant, for a
/// stream that does not exist, or of a topic name in use, with
/// `Unauthorized`, `NotFound` or `TopicNameAlreadyExists`. The cap must not
/// answer first, or that user could probe the cap state, and a client that
/// creates a topic only when it is missing would get the wrong error.
#[test]
fn given_create_topic_past_partitions_cap_when_apply_would_refuse_should_leave_it_to_apply() {
    const UNGRANTED_CLIENT: u128 = 1;
    const MANAGER_CLIENT: u128 = 2;
    let mut metadata = metadata_plane();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    metadata.set_partitions_max(1);

    metadata
        .client_table
        .borrow_mut()
        .commit_register(
            UNGRANTED_CLIENT,
            UNGRANTED,
            [0x5a; 32],
            register_reply(UNGRANTED_CLIENT, 1),
        )
        .unwrap();
    assert!(
        metadata
            .admit_partitions(&create_topic_request(UNGRANTED_CLIENT, UNGRANTED))
            .is_ok(),
        "a user without create_topic must get Unauthorized from apply"
    );

    metadata
        .client_table
        .borrow_mut()
        .commit_register(
            MANAGER_CLIENT,
            TOPIC_MANAGER,
            [0x5a; 32],
            register_reply(MANAGER_CLIENT, 1),
        )
        .unwrap();
    let missing_stream = create_named_topic_request(
        MANAGER_CLIENT,
        TOPIC_MANAGER,
        WireIdentifier::numeric(9),
        "t",
    );
    assert!(
        metadata.admit_partitions(&missing_stream).is_ok(),
        "a create for a missing stream must get NotFound from apply"
    );

    let taken_name = create_named_topic_request(
        MANAGER_CLIENT,
        TOPIC_MANAGER,
        WireIdentifier::numeric(0),
        "existing",
    );
    assert!(
        metadata.admit_partitions(&taken_name).is_ok(),
        "a create of a topic name in use must get TopicNameAlreadyExists from apply"
    );

    assert!(
        matches!(
            metadata.admit_partitions(&create_topic_request(MANAGER_CLIENT, TOPIC_MANAGER)),
            Err(IggyError::PartitionsLimitReached)
        ),
        "a create that apply would carry out is denied at the cap"
    );
}

/// The cap is graded before the apply grades the name, so a create at the
/// cap reports the cap even when its name is taken, as the legacy server
/// did.
#[test]
fn given_at_limit_duplicate_name_when_admitting_should_report_limit_not_duplicate() {
    let metadata = metadata_capped_over(users_holding_token());

    let error = metadata
        .admit_personal_access_token(&pat_request(
            1,
            1,
            Operation::CreatePersonalAccessToken,
            &token_create(TOKEN_NAME).to_bytes(),
        ))
        .expect_err("a create at the cap must be denied even for a taken name");

    assert!(
        matches!(
            error,
            IggyError::PersonalAccessTokensLimitReached(TOKEN_OWNER, TOKEN_CAP)
        ),
        "expected the cap error before the name grading, got {error:?}"
    );
}

/// A delete is how a user at the cap frees a slot, so capping it too
/// would lock the account at the cap.
#[test]
fn given_user_at_token_cap_when_deleting_should_admit() {
    let metadata = metadata_capped_over(users_holding_token());

    assert!(
        metadata
            .admit_personal_access_token(&pat_request(
                1,
                1,
                Operation::DeletePersonalAccessToken,
                &token_delete().to_bytes(),
            ))
            .is_ok()
    );
}

#[test]
fn given_deletion_freed_a_slot_when_admitting_should_admit_the_next_create() {
    let mut users = users_holding_token();
    let deleted = StateHandler::apply(&token_delete(), &mut users, IggyTimestamp::now());
    assert_eq!(deleted.code, 0, "the fixture token must be deleted");
    let metadata = metadata_capped_over(users);

    let admitted = metadata.admit_personal_access_token(&pat_request(
        1,
        1,
        Operation::CreatePersonalAccessToken,
        &token_create("replacement").to_bytes(),
    ));

    assert!(
        admitted.is_ok(),
        "a freed slot must admit the next create, got {admitted:?}"
    );
}

/// The cap counts the creates in flight, not only the committed tokens, so
/// creates pipelined at the boundary never take a user past it. A create
/// in flight holds its slot until it leaves the pipeline: the apply
/// refuses it, it commits as a token, or a view change drops it. Only the
/// committed tokens prove that a create cannot fit, so until then a create
/// waits instead of getting the cap's error.
#[test]
fn given_token_creates_in_flight_when_admitting_at_cap_should_count_them_until_they_leave_the_pipeline()
 {
    const TOKENS_MAX: u32 = 3;
    let metadata = primary_with_clients(
        NoopBus,
        users_holding_token().into(),
        StreamsInner::new(),
        TOKEN_OWNER,
        FIRST_CLIENT..=FIRST_CLIENT + 4,
    );
    metadata.set_max_tokens_per_user(TOKENS_MAX);
    let consensus = metadata.consensus.as_ref().unwrap();
    let create = |client: u128, name: &str| {
        metadata.prepare_request(pat_request(
            client,
            1,
            Operation::CreatePersonalAccessToken,
            &token_create(name).to_bytes(),
        ))
    };
    let assert_deferred = |client: u128, name: &str| {
        let refused = create(client, name);
        assert!(
            matches!(refused, Err(IggyError::TransientNotAccepted)),
            "{name} must wait for the creates in flight, got {:?}",
            refused.err()
        );
    };
    // Committed the way `on_ack` commits the pipeline head.
    let commit_head = |prepare: Message<PrepareHeader>| {
        let op = prepare.header().op;
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.pop().unwrap();
        });
        let reply = metadata.apply_prepare(prepare);
        consensus.advance_commit_max(op);
        consensus.advance_commit_min(op);
        iggy_binary_protocol::result_code(reply.body())
    };

    // The committed token leaves two slots. The first create reuses its
    // name, so the apply will refuse it.
    let mut in_flight = Vec::new();
    for (client, name) in (FIRST_CLIENT..).zip([TOKEN_NAME, "second", "third", "fourth"]) {
        if let Ok(prepare) = create(client, name) {
            consensus.pipeline_message(PlaneKind::Metadata, &prepare);
            in_flight.push(prepare);
        }
    }
    assert_eq!(in_flight.len(), 2, "two slots admit two creates in flight");
    assert_deferred(FIRST_CLIENT + 2, "third");

    assert_ne!(
        commit_head(in_flight.remove(0)),
        Some(0),
        "the apply must refuse the taken name"
    );
    let third = create(FIRST_CLIENT + 2, "third").expect("the refused create freed its slot");
    consensus.pipeline_message(PlaneKind::Metadata, &third);
    assert_deferred(FIRST_CLIENT + 3, "fourth");

    assert_eq!(commit_head(in_flight.remove(0)), Some(0));
    assert_deferred(FIRST_CLIENT + 3, "fourth");

    drop_uncommitted_suffix(consensus);
    let fourth = create(FIRST_CLIENT + 3, "fourth").expect("a dropped create must free its slot");
    consensus.pipeline_message(PlaneKind::Metadata, &fourth);
    assert_eq!(commit_head(fourth), Some(0));
    let refused = create(FIRST_CLIENT + 4, "fifth");
    assert!(
        matches!(
            refused,
            Err(IggyError::PersonalAccessTokensLimitReached(
                TOKEN_OWNER,
                TOKENS_MAX
            ))
        ),
        "a create past the cap on committed tokens keeps the cap error, got {:?}",
        refused.err()
    );
}

/// `partitions_max` counts the partitions of the creates in flight the
/// same way, and frees them when a view change drops those creates.
#[test]
fn given_topic_creates_in_flight_when_admitting_at_cap_should_count_them_until_they_leave_the_pipeline()
 {
    const PARTITIONS_MAX: u32 = 4;
    let metadata = primary_with_clients(
        NoopBus,
        users_with_topic_manager(),
        stream_with_one_partition(),
        TOPIC_MANAGER,
        FIRST_CLIENT..=FIRST_CLIENT + 4,
    );
    metadata.set_partitions_max(PARTITIONS_MAX);
    let consensus = metadata.consensus.as_ref().unwrap();
    let create = |client: u128, name: &str| {
        metadata.prepare_request(create_named_topic_request(
            client,
            TOPIC_MANAGER,
            WireIdentifier::numeric(0),
            name,
        ))
    };

    // The committed partition leaves room for three one-partition topics.
    let mut in_flight = 0;
    for (client, name) in (FIRST_CLIENT..).zip(["a", "b", "c", "d", "e"]) {
        match create(client, name) {
            Ok(prepare) => {
                consensus.pipeline_message(PlaneKind::Metadata, &prepare);
                in_flight += 1;
            }
            Err(error) => assert!(
                matches!(error, IggyError::TransientNotAccepted),
                "{name} must wait for the creates in flight, got {error:?}"
            ),
        }
    }
    assert_eq!(in_flight, 3, "three free partitions admit three creates");

    drop_uncommitted_suffix(consensus);
    assert!(
        create(FIRST_CLIENT + 3, "d").is_ok(),
        "a dropped create must free its partitions"
    );
}

/// A create in flight that its apply refuses holds a reservation until it
/// commits. A valid create that fits once that reservation is freed must
/// not get the terminal cap error before then.
#[test]
fn given_refused_create_in_flight_when_a_valid_create_fits_committed_partitions_should_defer_it() {
    const PARTITIONS_MAX: u32 = 3;
    let metadata = primary_with_clients(
        NoopBus,
        users_with_topic_manager(),
        stream_with_one_partition(),
        TOPIC_MANAGER,
        FIRST_CLIENT..=FIRST_CLIENT + 2,
    );
    metadata.set_partitions_max(PARTITIONS_MAX);
    let consensus = metadata.consensus.as_ref().unwrap();
    let create = |client: u128, name: &str, partitions_count: u32| {
        metadata.prepare_request(create_topic_request_with_count(
            client,
            TOPIC_MANAGER,
            WireIdentifier::numeric(0),
            name,
            partitions_count,
        ))
    };

    let taken_name = create(FIRST_CLIENT, "existing", 2).unwrap();
    consensus.pipeline_message(PlaneKind::Metadata, &taken_name);
    assert!(
        matches!(
            create(FIRST_CLIENT + 1, "fresh", 1),
            Err(IggyError::TransientNotAccepted)
        ),
        "a create that fits the committed partitions must wait"
    );
    assert!(
        matches!(
            create(FIRST_CLIENT + 2, "large", PARTITIONS_MAX),
            Err(IggyError::PartitionsLimitReached)
        ),
        "a create past the cap on committed partitions keeps the cap error"
    );

    let op = taken_name.header().op;
    consensus.with_pipeline_mut(|pipeline| {
        pipeline.pop().unwrap();
    });
    let reply = metadata.apply_prepare(taken_name);
    assert_ne!(iggy_binary_protocol::result_code(reply.body()), Some(0));
    consensus.advance_commit_max(op);
    consensus.advance_commit_min(op);
    assert!(
        create(FIRST_CLIENT + 1, "fresh", 1).is_ok(),
        "the refused create must free its reservation"
    );
}

/// The gate that lets a create past the cap reach its own apply error reads
/// the committed state, and an op in flight can change its verdict before
/// the create applies: here it creates the create's stream. Such a create
/// must not take the node past the cap.
#[test]
fn given_create_behind_its_uncommitted_stream_when_past_partitions_cap_should_not_pass_the_cap() {
    const PARTITIONS_MAX: u32 = 2;
    const STREAM: &str = "pending";
    const ROOT_CLIENT: u128 = 1;
    let metadata = primary_with_clients(
        NoopBus,
        users_with_topic_manager(),
        stream_with_one_partition(),
        TOPIC_MANAGER,
        [],
    );
    metadata.set_partitions_max(PARTITIONS_MAX);
    let consensus = metadata.consensus.as_ref().unwrap();
    let create = || {
        metadata.prepare_request(create_topic_request_with_count(
            ROOT_CLIENT + 1,
            0,
            WireIdentifier::named(STREAM).unwrap(),
            "topic",
            PARTITIONS_MAX,
        ))
    };
    let mut in_flight = vec![
        metadata
            .prepare_request(create_stream_request(ROOT_CLIENT, 1, STREAM))
            .unwrap(),
    ];
    consensus.pipeline_message(PlaneKind::Metadata, &in_flight[0]);

    let behind = create();
    if let Ok(prepare) = &behind {
        consensus.pipeline_message(PlaneKind::Metadata, prepare);
        in_flight.push(prepare.clone());
    }
    for prepare in in_flight {
        let op = prepare.header().op;
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.pop().unwrap();
        });
        metadata.apply_prepare(prepare);
        consensus.advance_commit_max(op);
        consensus.advance_commit_min(op);
    }
    let partitions = metadata.mux_stm.streams().partition_count();
    assert!(
        partitions <= PARTITIONS_MAX as usize,
        "{partitions} partitions committed past the cap of {PARTITIONS_MAX}"
    );
    assert!(
        matches!(behind, Err(IggyError::TransientNotAccepted)),
        "a create whose gate an op in flight can change must wait, got {:?}",
        behind.err()
    );
    assert!(
        matches!(create(), Err(IggyError::PartitionsLimitReached)),
        "once its stream committed, the create must get the cap's error"
    );
}

/// A cap refusal answers a well-formed request: the wire ingress and the
/// promotion of a queued create reply with the cap's code and keep the
/// session.
#[compio::test]
async fn given_create_past_token_cap_when_wire_or_queue_submits_should_reply_52_without_eviction() {
    const WIRE_CLIENT: u128 = FIRST_CLIENT;
    const QUEUED_CLIENT: u128 = FIRST_CLIENT + 1;
    let metadata = primary_with_clients(
        ClientSendSpyBus::default(),
        users_holding_token().into(),
        StreamsInner::new(),
        TOKEN_OWNER,
        WIRE_CLIENT..=QUEUED_CLIENT,
    );
    metadata.set_max_tokens_per_user(TOKEN_CAP);
    let consensus = metadata.consensus.as_ref().unwrap();
    let create = |client: u128| {
        pat_request(
            client,
            1,
            Operation::CreatePersonalAccessToken,
            &token_create("next").to_bytes(),
        )
    };
    let cap_code = IggyError::PersonalAccessTokensLimitReached(TOKEN_OWNER, TOKEN_CAP).as_code();

    metadata.on_request(create(WIRE_CLIENT)).await;
    {
        let replies = consensus.message_bus().client_replies.borrow();
        assert_eq!(replies.len(), 1, "the wire ingress must answer the refusal");
        let frame = replies[0].first();
        let header =
            bytemuck::checked::from_bytes::<ReplyHeader>(&frame[..size_of::<ReplyHeader>()]);
        assert_eq!(
            header.command,
            Command::Reply,
            "a cap refusal must not evict"
        );
        assert_eq!(
            iggy_binary_protocol::result_code(&frame[size_of::<ReplyHeader>()..]),
            Some(cap_code)
        );
    }

    let (queued, mut receiver) = consensus::RequestEntry::with_subscriber(create(QUEUED_CLIENT));
    assert!(consensus.push_queued_request(queued).is_ok());
    metadata.drain_request_queue_into_prepares().await;
    let std::task::Poll::Ready(Ok(reply)) = futures::poll!(&mut receiver) else {
        panic!("the promotion must answer a queued create past the cap");
    };
    assert_eq!(
        iggy_binary_protocol::result_code(reply.body()),
        Some(cap_code)
    );
    assert_eq!(
        consensus.pipeline_len(),
        0,
        "a create past the cap must not be prepared"
    );
}

#[test]
fn given_zero_partitions_create_past_partitions_cap_when_validating_should_admit() {
    assert!(validate_partitions_limit(10, 0, 12).is_ok());
}

#[test]
fn given_create_reaching_partitions_cap_when_validating_should_admit() {
    assert!(validate_partitions_limit(10, 4, 6).is_ok());
}

#[test]
fn given_create_past_partitions_cap_when_validating_should_deny() {
    assert!(matches!(
        validate_partitions_limit(10, 5, 6),
        Err(IggyError::PartitionsLimitReached)
    ));
    assert!(matches!(
        validate_partitions_limit(10, 1, 10),
        Err(IggyError::PartitionsLimitReached)
    ));
}
