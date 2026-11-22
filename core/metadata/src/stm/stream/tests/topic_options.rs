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
fn given_topic_with_retention_when_restoring_snapshot_should_rebuild_resolved_view() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let message_expiry = IggyExpiry::from(5_000_000u64);
    let max_topic_size = MaxTopicSize::from(10_000_000_000u64);
    let segment_size = IggyByteSize::from(64 * 1024 * 1024);
    let create = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("topic").unwrap(),
            options: TopicCreateOptions {
                message_expiry: Some(message_expiry),
                ..TopicCreateOptions::default()
            }
            .to_explicit_wire(|key| key == topic_option_keys::MESSAGE_EXPIRY)
            .unwrap(),
        },
        derived_options: TopicCreateOptions {
            max_topic_size: Some(max_topic_size),
            segment_size: Some(segment_size),
            ..TopicCreateOptions::default()
        }
        .to_wire()
        .unwrap(),
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 1,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&create, &mut inner, IggyTimestamp::from(1)).code,
        0
    );
    let streams: Streams = inner.into();
    let mut snapshot = MetadataSnapshot::new(1);
    snapshot.streams = Some(streams.to_snapshot());
    let decoded = MetadataSnapshot::decode(&snapshot.encode().unwrap()).unwrap();

    let restored = Streams::from_snapshot(decoded.streams.unwrap()).unwrap();

    assert_eq!(
        restored.topic_retention_config(0, 0).unwrap(),
        (message_expiry, max_topic_size, 1, Some(segment_size))
    );
    assert_eq!(
        restored.topic_segment_size(&WireIdentifier::numeric(0), &WireIdentifier::numeric(0)),
        Some(segment_size)
    );
}

#[test]
fn given_stored_critical_marker_when_patch_declares_more_keys_should_store_the_bounded_union() {
    let marker_key = HeaderKey::from(OptionKey::CriticalKeys);
    let marker = |text: &str| OptionValue::explicit(HeaderValue::try_from(text).unwrap());
    let update = |names: &str| {
        let mut explicit = ResourceOptions::new();
        explicit.insert(marker_key.clone(), marker(names));
        UpdateStreamResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            name: None,
            explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
            derived_options: WireOptions::empty(),
        }
    };
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    inner.items[0]
        .options
        .insert(marker_key.clone(), marker("encryption_key_wrapped"));

    let reply = StateHandler::apply(&update("a_future_key"), &mut inner, IggyTimestamp::now());
    assert_eq!(reply.code, 0);
    let merged = marker("a_future_key,encryption_key_wrapped");
    assert_eq!(inner.items[0].options.get(&marker_key), Some(&merged));

    let reply = StateHandler::apply(&update(&"z".repeat(230)), &mut inner, IggyTimestamp::now());
    assert_eq!(
        reply.code,
        u32::from(UpdateStreamResult::OptionsBlockTooLarge)
    );
    assert_eq!(inner.items[0].options.get(&marker_key), Some(&merged));
}

#[test]
fn given_internal_or_unservable_topic_options_when_creating_should_not_echo_them() {
    let marker_key = HeaderKey::from(OptionKey::CriticalKeys);
    let wrapped_key = HeaderKey::from(OptionKey::EncryptionKeyWrapped);
    let future_key = HeaderKey::try_from("a_future_key").unwrap();
    let marker = |text: &str| OptionValue::explicit(HeaderValue::try_from(text).unwrap());
    let set = WrapSet::from(envelope(
        &key_context(OLD_CONFIG_KEY, &[]),
        1,
        StreamKeyOrigin::Server,
    ));
    let mut servable = ResourceOptions::new();
    servable.insert(marker_key.clone(), marker("encryption_key_wrapped"));
    servable.insert(
        wrapped_key.clone(),
        OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
    );
    let mut unservable = ResourceOptions::new();
    unservable.insert(marker_key.clone(), marker("a_future_key"));
    unservable.insert(
        future_key.clone(),
        OptionValue::explicit(HeaderValue::from(7u64)),
    );

    for stored in [servable, unservable] {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, "topic"),
            derived_options: resource_options_to_wire(&stored, OptionsProvenance::All).unwrap(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
                initial_view: 0,
                members: Vec::new(),
            }],
        };

        let apply = StateHandler::apply(&create, &mut inner, IggyTimestamp::now());

        assert_eq!(apply.code, 0);
        assert!(inner.items[0].topics[0].options.contains_key(&marker_key));
        let (reply, _) = GetTopicResponse::decode(&apply.body).unwrap();
        let echoed: Vec<HeaderKey> = resource_options_from_wire(&reply.topic.options, true)
            .unwrap()
            .into_keys()
            .chain(
                resource_options_from_wire(&reply.topic.derived_options, false)
                    .unwrap()
                    .into_keys(),
            )
            .collect();
        for hidden in [&marker_key, &wrapped_key, &future_key] {
            assert!(!echoed.contains(hidden), "{echoed:?}");
        }
    }
}

#[test]
fn given_unknown_critical_key_when_applying_create_stream_should_store_it_without_echoing_options()
{
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::CriticalKeys),
        OptionValue::explicit(HeaderValue::try_from("a_future_key").unwrap()),
    );
    options.insert(
        HeaderKey::try_from("a_future_key").unwrap(),
        OptionValue::explicit(HeaderValue::from(7u64)),
    );
    let request = CreateStreamRequest {
        name: WireName::new("future").unwrap(),
        options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
    };
    let mut inner = StreamsInner::new();

    let reply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());

    assert_eq!(reply.code, 0);
    assert_eq!(inner.items[0].options, options);
    let (response, _) = GetStreamResponse::decode(&reply.body).unwrap();
    assert_eq!(
        (response.stream.options, response.stream.derived_options),
        (WireOptions::empty(), WireOptions::empty())
    );
}

#[test]
fn given_reused_stream_id_when_creating_should_have_distinct_created_op() {
    let mut inner = StreamsInner::new();
    let timestamp = IggyTimestamp::from(1);
    let create = CreateStreamRequest {
        name: WireName::new("orders").unwrap(),
        options: WireOptions::empty(),
    };
    inner.apply_context.metadata_op = 5;
    assert_eq!(StateHandler::apply(&create, &mut inner, timestamp).code, 0);
    let first = inner.items[0].created_op;

    inner.apply_context.metadata_op = 6;
    let delete = DeleteStreamRequest {
        stream_id: WireIdentifier::numeric(0),
    };
    assert_eq!(
        crate::stm::lifecycle::apply_with_lifecycle_completion(&delete, &mut inner, timestamp).code,
        0
    );
    assert!(inner.items.get(0).is_none());

    inner.apply_context.metadata_op = 7;
    assert_eq!(StateHandler::apply(&create, &mut inner, timestamp).code, 0);
    let recreated = &inner.items[0];
    assert_eq!(recreated.created_at, timestamp);
    assert_eq!((first, recreated.created_op), (5, 7));
}

#[test]
fn given_rename_only_patch_when_applying_should_preserve_options_and_provenance() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let message_expiry = IggyExpiry::from(5_000_000u64);
    let max_topic_size = MaxTopicSize::from(10_000_000_000u64);
    let create = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("topic").unwrap(),
            options: TopicCreateOptions {
                message_expiry: Some(message_expiry),
                ..TopicCreateOptions::default()
            }
            .to_explicit_wire(|key| key == topic_option_keys::MESSAGE_EXPIRY)
            .unwrap(),
        },
        derived_options: TopicCreateOptions {
            max_topic_size: Some(max_topic_size),
            ..TopicCreateOptions::default()
        }
        .to_wire()
        .unwrap(),
        partitions: vec![CreatedPartitionAssignment {
            partition_id: 0,
            consensus_group_id: 1,
            initial_view: 0,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&create, &mut inner, IggyTimestamp::from(1)).code,
        0
    );
    let original_options = inner.items[0].topics[0].options.clone();

    let update = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: Some(WireName::new("renamed").unwrap()),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };
    assert_eq!(
        StateHandler::apply(&update, &mut inner, IggyTimestamp::from(2)).code,
        0
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "renamed");
    assert_eq!(topic.resolved.message_expiry, message_expiry);
    assert_eq!(topic.resolved.max_topic_size, max_topic_size);
    assert_eq!(topic.options, original_options);
}

#[test]
fn given_topic_patch_without_name_when_applying_should_preserve_name_and_other_keys() {
    let mut inner = streams_with_resize_policy(Some(PartitionResizePolicy::Mutable));
    let before = inner.items[0].topics[0].options.clone();
    let expiry = IggyExpiry::from(9_000_000u64);
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::MessageExpiry),
        OptionValue::explicit(expiry.encode().unwrap()),
    );
    let update = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: None,
        explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
        derived_options: WireOptions::empty(),
    };
    assert_eq!(
        StateHandler::apply(&update, &mut inner, IggyTimestamp::now()).code,
        0
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "topic");
    assert_eq!(topic.resolved.message_expiry, expiry);
    let mut expected = before;
    expected.extend(explicit);
    assert_eq!(topic.options, expected);
}

#[test]
fn given_no_durability_when_applying_a_topic_create_should_store_the_derived_value() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let geo = without_durability(&geo_topic());
    let normal = without_durability(&TopicCreateOptions::default());

    assert_eq!(
        apply_create_topic_block(&mut inner, 0, "geo", &geo, 1).code,
        0
    );
    assert_eq!(
        apply_create_topic_block(&mut inner, 0, "normal", &normal, 2).code,
        0
    );

    assert_eq!(
        stored_durability(&inner, 0),
        (
            Durability::Persisted,
            durability_entry(Durability::Persisted, false)
        )
    );
    assert_eq!(
        stored_durability(&inner, 1),
        (
            Durability::Replicated,
            durability_entry(Durability::Replicated, false)
        )
    );
    let offsets = HeaderKey::from(OptionKey::ConsumerOffsetDurability);
    for topic in [0, 1] {
        assert_eq!(
            inner.items[0].topics[topic].options.get(&offsets),
            Some(&durability_entry(Durability::Replicated, false)),
            "only message durability depends on the topic type"
        );
    }
}

/// Apply stores what the primary committed and recomputes nothing, so a
/// geo topic created before its mode fixed the durability keeps the one
/// it was created with through a replay.
#[test]
fn given_a_committed_geo_create_naming_a_durability_when_replaying_should_store_it_as_committed() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    for (topic_id, consensus_group_id, durability) in [
        (0, 1, Durability::Replicated),
        (1, 2, Durability::Persisted),
    ] {
        let explicit = TopicCreateOptions {
            durability,
            ..geo_topic()
        };
        let name = format!("geo-{durability}");
        assert_eq!(
            apply_create_topic(&mut inner, 0, &name, &explicit, consensus_group_id).code,
            0
        );
        assert_eq!(
            stored_durability(&inner, topic_id),
            (durability, durability_entry(durability, true))
        );
    }
}

#[test]
fn given_a_geo_topic_without_durability_when_updating_it_should_keep_persisted() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    assert_eq!(
        apply_create_topic_block(&mut inner, 0, "geo", &without_durability(&geo_topic()), 1).code,
        0
    );
    let explicit = |key: OptionKey, value: HeaderValue| {
        ResourceOptions::from([(HeaderKey::from(key), OptionValue::explicit(value))])
    };
    let apply = |inner: &mut StreamsInner, options: &ResourceOptions| {
        let update = UpdateTopicResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            name: None,
            explicit_options: resource_options_to_wire(options, OptionsProvenance::All).unwrap(),
            derived_options: WireOptions::empty(),
        };
        StateHandler::apply(&update, inner, IggyTimestamp::now()).code
    };
    let not_updatable = IggyError::OptionNotUpdatable(String::new()).as_code();
    let expiry = explicit(
        OptionKey::MessageExpiry,
        IggyExpiry::from(9_000_000u64).encode().unwrap(),
    );
    let replicated = explicit(
        OptionKey::Durability,
        Durability::Replicated.encode().unwrap(),
    );
    // A geo topic's durations are the only options an update can change.
    let grace = explicit(
        OptionKey::TombstoneGrace,
        HeaderValue::from(3_600_000_000u64),
    );

    assert_eq!(apply(&mut inner, &expiry), not_updatable);
    assert_eq!(apply(&mut inner, &replicated), not_updatable);
    assert_eq!(apply(&mut inner, &grace), 0);
    assert!(
        inner.items[0].topics[0]
            .resolved
            .geo_durations
            .tombstone_grace
            .is_some()
    );
    assert_eq!(
        stored_durability(&inner, 0),
        (
            Durability::Persisted,
            durability_entry(Durability::Persisted, false)
        )
    );
}

#[test]
fn given_an_encrypted_stream_when_applying_a_geo_topic_create_should_refuse_it_as_unavailable() {
    let context = key_context(OLD_CONFIG_KEY, &[]);
    let mut inner = StreamsInner::new();
    let set = WrapSet::from(envelope(&context, 1, StreamKeyOrigin::Server));
    create_encrypted_stream(&mut inner, "sealed", &set, 5);
    create_stream(&mut inner, "plain");
    let revision = inner.revision;

    assert_eq!(
        apply_create_topic(&mut inner, 0, "geo", &geo_topic(), 1).code,
        IggyError::FeatureUnavailable.as_code()
    );
    assert!(inner.items[0].topics.is_empty());
    assert_eq!(inner.revision, revision);

    let normal = TopicCreateOptions::default();
    assert_eq!(
        apply_create_topic(&mut inner, 0, "normal", &normal, 2).code,
        0
    );
    assert_eq!(
        apply_create_topic(&mut inner, 1, "geo", &geo_topic(), 3).code,
        0
    );
    let topic = &inner.items[1].topics[0];
    assert_eq!(topic.resolved.topic_type, TopicType::Geo);
    assert_eq!(
        topic.resolved.partition_resize_policy,
        PartitionResizePolicy::Fixed
    );
    assert!(topic.resolved.runtime.places_batches);
}

#[test]
fn given_a_geo_topic_when_applying_a_patch_or_a_resize_should_refuse_it_and_keep_the_topic() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    assert_eq!(
        apply_create_topic(&mut inner, 0, "geo", &geo_topic(), 1).code,
        0
    );
    let normal = TopicCreateOptions::default();
    assert_eq!(
        apply_create_topic(&mut inner, 0, "normal", &normal, 2).code,
        0
    );
    let before = inner.items[0].topics[0].options.clone();
    let revision = inner.revision;
    let mut expiry = ResourceOptions::new();
    expiry.insert(
        HeaderKey::from(OptionKey::MessageExpiry),
        OptionValue::explicit(IggyExpiry::from(9_000_000u64).encode().unwrap()),
    );
    let patch = |topic_id: u32, name: Option<&str>, explicit: &ResourceOptions| {
        UpdateTopicResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(topic_id),
            name: name.map(|name| WireName::new(name).unwrap()),
            explicit_options: resource_options_to_wire(explicit, OptionsProvenance::All).unwrap(),
            derived_options: WireOptions::empty(),
        }
    };

    for refused in [
        patch(0, None, &expiry),
        patch(0, Some("renamed"), &ResourceOptions::new()),
    ] {
        assert_eq!(
            StateHandler::apply(&refused, &mut inner, IggyTimestamp::now()).code,
            IggyError::OptionNotUpdatable(String::new()).as_code()
        );
    }
    let resize = CreatePartitionsWithAssignmentsRequest {
        request: WireCreatePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 1,
        },
        partitions: vec![CreatedPartitionAssignment {
            initial_view: 0,
            partition_id: 0,
            consensus_group_id: 3,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        crate::stm::lifecycle::apply_with_lifecycle_completion(
            &resize,
            &mut inner,
            IggyTimestamp::now()
        )
        .code,
        IggyError::PartitionResizeDisabled.as_code()
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "geo");
    assert_eq!(topic.options, before);
    assert_eq!(topic.partitions.len(), 1);
    assert_eq!(inner.revision, revision);

    assert_eq!(
        StateHandler::apply(&patch(1, None, &expiry), &mut inner, IggyTimestamp::now()).code,
        0,
        "a normal topic takes the same patch"
    );
}

#[test]
fn given_a_geo_topic_when_applying_a_patch_of_its_durations_alone_should_take_it() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    assert_eq!(
        apply_create_topic(&mut inner, 0, "geo", &geo_topic(), 1).code,
        0
    );
    let normal = TopicCreateOptions::default();
    assert_eq!(
        apply_create_topic(&mut inner, 0, "normal", &normal, 2).code,
        0
    );
    let options = |entries: &[(OptionKey, u64)], explicit: bool| -> WireOptions {
        let options: ResourceOptions = entries
            .iter()
            .map(|(key, value)| {
                let value = HeaderValue::from(*value);
                let option = if explicit {
                    OptionValue::explicit(value)
                } else {
                    OptionValue::derived(value)
                };
                (HeaderKey::from(*key), option)
            })
            .collect();
        resource_options_to_wire(&options, OptionsProvenance::All).unwrap()
    };
    let patch =
        |topic_id: u32, explicit: WireOptions, derived: WireOptions| UpdateTopicResolvedRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(topic_id),
            name: None,
            explicit_options: explicit,
            derived_options: derived,
        };
    let durations = [
        (OptionKey::TombstoneGrace, 3_600_000_000),
        (OptionKey::FreshUploadInterval, 0),
    ];
    let apply = |inner: &mut StreamsInner, request: &UpdateTopicResolvedRequest| {
        StateHandler::apply(request, inner, IggyTimestamp::now()).code
    };

    let taken = patch(0, options(&durations, true), WireOptions::empty());
    assert_eq!(apply(&mut inner, &taken), 0);
    let resolved = inner.items[0].topics[0].resolved.geo_durations;
    assert_eq!(
        resolved.tombstone_grace,
        Some(iggy_common::IggyDuration::from(3_600_000_000u64))
    );
    assert_eq!(
        resolved.fresh_upload_interval,
        Some(iggy_common::IggyDuration::from(0u64))
    );

    let not_updatable = IggyError::OptionNotUpdatable(String::new()).as_code();
    let beside_another_key = patch(
        0,
        options(
            &[
                (OptionKey::TombstoneGrace, 60_000_000),
                (OptionKey::MessageExpiry, 9_000_000),
            ],
            true,
        ),
        WireOptions::empty(),
    );
    let with_a_reset = patch(
        0,
        options(&durations, true),
        options(&[(OptionKey::MessageExpiry, u64::MAX)], false),
    );
    for refused in [beside_another_key, with_a_reset] {
        assert_eq!(apply(&mut inner, &refused), not_updatable);
    }
    assert_eq!(inner.items[0].topics[0].resolved.geo_durations, resolved);
    assert_eq!(
        apply(
            &mut inner,
            &patch(1, options(&durations, true), WireOptions::empty())
        ),
        IggyError::InvalidOptionValue(String::new()).as_code(),
        "a normal topic has no durations"
    );
}

#[test]
fn given_explicit_topic_key_when_resetting_should_store_primary_default_as_derived() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let create = CreateTopicWithAssignmentsRequest {
        request: WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            partitions_count: 1,
            name: WireName::new("topic").unwrap(),
            options: TopicCreateOptions {
                max_topic_size: Some(MaxTopicSize::from(10_000_000_000u64)),
                ..TopicCreateOptions::default()
            }
            .to_explicit_wire(|key| key == topic_option_keys::MAX_TOPIC_SIZE)
            .unwrap(),
        },
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            partition_id: 0,
            consensus_group_id: 1,
            initial_view: 0,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&create, &mut inner, IggyTimestamp::from(1)).code,
        0
    );
    let default = OptionKey::MaxTopicSize
        .descriptor()
        .default_value()
        .unwrap()
        .unwrap();
    let mut derived = ResourceOptions::new();
    derived.insert(
        HeaderKey::from(OptionKey::MaxTopicSize),
        OptionValue::derived(default.clone()),
    );
    let update = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: None,
        explicit_options: WireOptions::empty(),
        derived_options: resource_options_to_wire(&derived, OptionsProvenance::All).unwrap(),
    };
    assert_eq!(
        StateHandler::apply(&update, &mut inner, IggyTimestamp::from(2)).code,
        0
    );
    let topic = &inner.items[0].topics[0];
    let stored = &topic.options[&HeaderKey::from(OptionKey::MaxTopicSize)];
    assert_eq!(stored, &OptionValue::derived(default));
    assert_eq!(
        topic.resolved.max_topic_size,
        MaxTopicSize::from(iggy_common::DEFAULT_MAX_TOPIC_SIZE)
    );
}

#[test]
fn given_conflicting_rename_with_patch_when_applying_should_leave_state_unchanged() {
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    for name in ["first", "second"] {
        let create = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, name),
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
                initial_view: 0,
                members: Vec::new(),
            }],
        };
        assert_eq!(
            StateHandler::apply(&create, &mut inner, IggyTimestamp::now()).code,
            0
        );
    }
    let before = inner.items[0].topics[0].options.clone();
    let index_before = inner.items[0].topic_index.clone();
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::MessageExpiry),
        OptionValue::explicit(IggyExpiry::from(1_000_000u64).encode().unwrap()),
    );
    let update = UpdateTopicResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        name: Some(WireName::new("second").unwrap()),
        explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
        derived_options: WireOptions::empty(),
    };
    assert_eq!(
        StateHandler::apply(&update, &mut inner, IggyTimestamp::now()).code,
        u32::from(UpdateTopicResult::NameAlreadyExists)
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "first");
    assert_eq!(topic.options, before);
    assert_eq!(inner.items[0].topic_index, index_before);

    let rename_stream = UpdateStreamResolvedRequest {
        stream_id: WireIdentifier::numeric(0),
        name: Some(WireName::new("renamed-stream").unwrap()),
        explicit_options: WireOptions::empty(),
        derived_options: WireOptions::empty(),
    };
    assert_eq!(
        StateHandler::apply(&rename_stream, &mut inner, IggyTimestamp::now()).code,
        0
    );
    assert_eq!(inner.items[0].name.as_ref(), "renamed-stream");
    assert_eq!(
        inner.resolve_stream_id(&WireIdentifier::named("renamed-stream").unwrap()),
        Some(0)
    );
    assert_eq!(
        inner.resolve_stream_id(&WireIdentifier::named("stream").unwrap()),
        None
    );
}

#[test]
fn given_cap_below_topic_segment_size_when_applying_update_should_refuse_it_unchanged() {
    let segment_size = 64 * 1024 * 1024;
    let mut inner = StreamsInner::new();
    create_stream(&mut inner, "stream");
    let mut request = make_topic_request(0, 1, "topic");
    request.options = TopicCreateOptions {
        segment_size: Some(IggyByteSize::from(segment_size)),
        ..TopicCreateOptions::default()
    }
    .to_explicit_wire(|key| key == topic_option_keys::SEGMENT_SIZE)
    .unwrap();
    let create = CreateTopicWithAssignmentsRequest {
        request,
        derived_options: WireOptions::empty(),
        partitions: vec![CreatedPartitionAssignment {
            partition_id: 0,
            consensus_group_id: 1,
            initial_view: 0,
            members: Vec::new(),
        }],
    };
    assert_eq!(
        StateHandler::apply(&create, &mut inner, IggyTimestamp::from(1)).code,
        0
    );
    let before = inner.items[0].topics[0].options.clone();

    let below = topic_cap_update(segment_size - 1, Some("renamed"));
    assert_eq!(
        StateHandler::apply(&below, &mut inner, IggyTimestamp::from(2)).code,
        u32::from(UpdateTopicResult::InvalidTopicSize)
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "topic");
    assert_eq!(topic.options, before);

    let at_floor = topic_cap_update(segment_size, Some("renamed"));
    assert_eq!(
        StateHandler::apply(&at_floor, &mut inner, IggyTimestamp::from(3)).code,
        0
    );
    let topic = &inner.items[0].topics[0];
    assert_eq!(topic.name.as_ref(), "renamed");
    assert_eq!(
        topic.resolved.max_topic_size,
        MaxTopicSize::from(segment_size)
    );
}

#[test]
fn given_topic_without_segment_size_when_applying_cap_below_default_should_refuse_it() {
    let mut inner = streams_with_resize_policy(None);
    let before = inner.items[0].topics[0].options.clone();
    let update = topic_cap_update(DEFAULT_SEGMENT_SIZE - 1, None);
    assert_eq!(
        StateHandler::apply(&update, &mut inner, IggyTimestamp::now()).code,
        u32::from(UpdateTopicResult::InvalidTopicSize)
    );
    assert_eq!(inner.items[0].topics[0].options, before);
}

#[test]
fn create_stream_options_survive_snapshot_roundtrip() {
    use crate::stm::snapshot::FillSnapshot;
    use iggy_binary_protocol::primitives::user_headers::encode_user_headers;

    let mut headers = bytes::BytesMut::new();
    encode_user_headers(&[(2, b"future_key", 2, b"future_value")], &mut headers);
    let request = CreateStreamRequest {
        name: WireName::new("stream-with-options").unwrap(),
        options: WireOptions::from_bytes(headers.freeze()).unwrap(),
    };

    let streams = Streams::default();
    streams
        .inner
        .try_apply(StreamsCommand::CreateStream(
            request,
            IggyTimestamp::from(1),
            ApplyContext::default(),
        ))
        .expect("create stream applies");

    let mut snapshot = MetadataSnapshot::new(1);
    streams.fill_snapshot(&mut snapshot).unwrap();
    let encoded = snapshot.encode().unwrap();
    let decoded = MetadataSnapshot::decode(&encoded).unwrap();
    let restored: Streams = crate::stm::snapshot::RestoreSnapshot::restore_snapshot(&decoded)
        .expect("streams section restores");

    let restored_options = restored.read(|inner| {
        let (_, stream) = inner.items.iter().next().expect("stream restored");
        stream.options.clone()
    });
    assert_eq!(restored_options.len(), 1);
    let (key, option) = restored_options.iter().next().unwrap();
    assert_eq!(key.as_bytes(), b"future_key");
    assert_eq!(option.value.as_bytes(), b"future_value");
    assert!(option.explicit);
}

#[test]
fn given_resize_policy_when_resizing_after_snapshot_restore_should_preserve_the_contract() {
    for policy in [
        None,
        Some(PartitionResizePolicy::Mutable),
        Some(PartitionResizePolicy::Fixed),
    ] {
        let inner = streams_with_resize_policy(policy);
        let streams: Streams = inner.into();
        let mut snapshot = MetadataSnapshot::new(1);
        snapshot.streams = Some(streams.to_snapshot());
        let decoded = MetadataSnapshot::decode(&snapshot.encode().unwrap()).unwrap();
        let mut restored =
            StreamsInner::inner_from_snapshot(decoded.streams.unwrap(), Arc::default());
        let revision = restored.revision;
        let create = CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 3,
                members: Vec::new(),
            }],
        };
        let delete = DeletePartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitions_count: 1,
        };
        let fixed = policy == Some(PartitionResizePolicy::Fixed);
        let expected = if fixed {
            iggy_common::IggyError::PartitionResizeDisabled.as_code()
        } else {
            0
        };
        assert_eq!(
            crate::stm::lifecycle::apply_with_lifecycle_completion(
                &create,
                &mut restored,
                IggyTimestamp::now()
            )
            .code,
            expected,
            "{policy:?}"
        );
        assert_eq!(
            restored.items[0].topics[0].partitions.len(),
            if fixed { 2 } else { 3 }
        );
        assert_eq!(
            crate::stm::lifecycle::apply_with_lifecycle_completion(
                &delete,
                &mut restored,
                IggyTimestamp::now()
            )
            .code,
            expected,
            "{policy:?}"
        );
        assert_eq!(restored.items[0].topics[0].partitions.len(), 2);
        assert_eq!(restored.revision, revision + if fixed { 0 } else { 3 });
        if fixed {
            let streams: Streams = restored.into();
            let mut after = MetadataSnapshot::new(1);
            after.streams = Some(streams.to_snapshot());
            assert_eq!(
                after.encode().unwrap(),
                snapshot.encode().unwrap(),
                "rejected resize must preserve all metadata"
            );
        }
    }
}

/// Regression guard for the [`StreamsSnapshot`] serialized-form invariant: a
/// populated snapshot must re-encode byte-identically after a decode, or the
/// checkpoint checksum cross-check (`recovery::verify_checkpoint_pairing`) would
/// diverge and refuse boot on a healthy node. Populates the three map-derived
/// `Vec`s (`items`, `topics`, `consumer_groups`) with two entries each, since one
/// entry cannot reorder and only >= 2 makes a regression observable.
#[test]
fn populated_streams_snapshot_reencode_is_byte_stable() {
    let mut inner = StreamsInner::new();
    for name in ["alpha", "beta"] {
        create_stream(&mut inner, name);
    }
    // Streams are assigned ids 0, 1 in creation order; two topics per stream,
    // two consumer groups per topic.
    for stream_id in 0..2u32 {
        for topic_name in ["logs", "events"] {
            let create_topic = CreateTopicWithAssignmentsRequest {
                request: make_topic_request(stream_id, 2, topic_name),
                derived_options: WireOptions::empty(),
                partitions: vec![
                    CreatedPartitionAssignment {
                        initial_view: 0,
                        partition_id: 0,
                        consensus_group_id: 1,
                        members: Vec::new(),
                    },
                    CreatedPartitionAssignment {
                        initial_view: 0,
                        partition_id: 1,
                        consensus_group_id: 2,
                        members: Vec::new(),
                    },
                ],
            };
            let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        }
    }
    for stream_id in 0..2u32 {
        for topic_id in 0..2u32 {
            for group_name in ["cg-a", "cg-b"] {
                let request = CreateConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(stream_id),
                    topic_id: WireIdentifier::numeric(topic_id),
                    name: WireName::new(group_name).unwrap(),
                };
                let _ = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
            }
        }
    }
    let streams: Streams = inner.into();

    let mut snapshot = MetadataSnapshot::new(7);
    snapshot.streams = Some(streams.to_snapshot());

    // The tree really is populated, else a byte-stable empty snapshot would pass
    // vacuously.
    let streams_snapshot = snapshot.streams.as_ref().unwrap();
    assert_eq!(streams_snapshot.items.len(), 2, "two streams");
    let (_, first_stream) = &streams_snapshot.items[0];
    assert_eq!(
        first_stream.topics.len(),
        2,
        "two topics in the first stream"
    );
    let (_, first_topic) = &first_stream.topics[0];
    assert_eq!(
        first_topic.consumer_groups.len(),
        2,
        "two consumer groups in the first topic"
    );

    let encoded = snapshot.encode().unwrap();
    let reencoded = MetadataSnapshot::decode(&encoded)
        .unwrap()
        .encode()
        .unwrap();
    assert_eq!(
        encoded, reencoded,
        "a populated snapshot must re-encode byte-identically after a decode; an \
         unordered collection would reorder and break the checkpoint checksum \
         cross-check, refusing boot on a healthy node"
    );
}
