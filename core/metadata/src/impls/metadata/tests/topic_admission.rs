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
fn prepare_request_stamps_create_topic_from_client_table_not_wire() {
    // `CreateTopic` is the projection that reaches `build_prepare_message`
    // through the post-stamp `header` copy, so the built prepare must carry
    // the ClientTable identity, not the (bogus) wire value. This pins the
    // stamp-then-re-read ordering: a hoist would ship the untrusted wire
    // value.
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    const WIRE_USER: u32 = 999;
    let plane = metadata_plane();
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    let prepare = plane
        .prepare_request(create_topic_request(CLIENT, WIRE_USER))
        .expect("CreateTopic is client-allowed");
    assert_eq!(
        prepare.header().operation,
        Operation::CreateTopicWithAssignments,
        "CreateTopic projects to the enriched form"
    );
    assert_eq!(
        prepare.header().user_id,
        ACTING_USER,
        "prepare must carry the ClientTable identity, not the wire value"
    );
}

#[test]
fn prepare_request_stamps_create_topic_message_expiry_default() {
    // A `CreateTopic` without an explicit `message_expiry` option must be
    // resolved at primary admission to the build default, riding the
    // derived block, so the replicated prepare -- and thus every
    // replica's commit -- holds a concrete expiry.
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    let plane = metadata_plane();
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();

    // `create_topic_request` builds the body with no options at all.
    let prepare = plane
        .prepare_request(create_topic_request(CLIENT, ACTING_USER))
        .expect("CreateTopic is client-allowed");
    let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
    let persisted = PersistedCreateTopicRequest::decode_from(body)
        .expect("create topic with assignments prepare must decode");
    assert!(
        persisted.request.options.is_empty(),
        "a client that sent no options gets an empty explicit block"
    );
    let derived = iggy_common::TopicCreateOptions::parse(&persisted.derived_options)
        .expect("derived block parses against the catalog");
    assert_eq!(
        derived.message_expiry,
        Some(iggy_common::IggyExpiry::from(
            iggy_common::DEFAULT_MESSAGE_EXPIRY
        )),
        "ServerDefault expiry must be resolved into the derived block at admission"
    );
    assert_eq!(
        persisted.partitions.len(),
        1,
        "absent partitions_count defaults to one partition"
    );
}

#[test]
fn given_an_explicit_critical_key_when_preparing_create_topic_should_record_the_marker() {
    let persisted = prepare_topic_with(&iggy_common::TopicCreateOptions {
        segment_max_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
        ..Default::default()
    })
    .expect("an hour-long age roll is admitted");

    let explicit = resource_options_from_wire(&persisted.request.options, true)
        .expect("explicit block decodes");
    assert_eq!(
        explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
            .value
            .as_bytes(),
        b"segment_max_age"
    );
    let stored = TopicResolvedOptions::from(&explicit);
    assert_eq!(
        stored.runtime.effective_segment_max_age(),
        Some(HOUR_MICROS)
    );

    let unmarked = prepare_topic_with(&iggy_common::TopicCreateOptions::default())
        .expect("a keyless topic is admitted");
    assert!(
        !resource_options_from_wire(&unmarked.request.options, true)
            .expect("explicit block decodes")
            .contains_key(&HeaderKey::from(OptionKey::CriticalKeys)),
        "a topic setting no critical key carries no marker"
    );
}

#[test]
fn given_an_age_roll_with_preallocated_segments_when_preparing_should_refuse_it() {
    let refused = prepare_topic_with(&iggy_common::TopicCreateOptions {
        segment_max_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
        preallocate_segments: Some(true),
        ..Default::default()
    });
    assert_eq!(
        refused.err(),
        Some(IggyError::InvalidOptionValue("segment_max_age".to_owned()))
    );

    assert!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            segment_max_age: Some(iggy_common::IggyExpiry::NeverExpire),
            preallocate_segments: Some(true),
            ..Default::default()
        })
        .is_ok(),
        "preallocation with segments rolled by size alone stays admitted"
    );
}

#[test]
fn given_an_age_roll_below_ten_minutes_when_preparing_should_refuse_it() {
    let nine_minutes = iggy_common::MIN_SEGMENT_MAX_AGE - 60_000_000;
    assert_eq!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            segment_max_age: Some(iggy_common::IggyExpiry::from(nine_minutes)),
            ..Default::default()
        })
        .err(),
        Some(IggyError::InvalidOptionValue("segment_max_age".to_owned()))
    );
    assert!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            segment_max_age: Some(iggy_common::IggyExpiry::from(
                iggy_common::MIN_SEGMENT_MAX_AGE
            )),
            ..Default::default()
        })
        .is_ok()
    );
}

#[test]
fn given_tiered_storage_when_preparing_create_topic_should_admit_on_only_in_a_ready_cluster() {
    let tiered = |enabled: Option<bool>| iggy_common::TopicCreateOptions {
        tiered_storage: enabled,
        ..Default::default()
    };
    for cluster_ready in [false, true] {
        let prepare = |enabled: Option<bool>| {
            let plane = metadata_plane();
            plane.observe_tiered_storage_readiness(cluster_ready);
            prepare_topic_on(&plane, &tiered(enabled))
        };
        match prepare(Some(true)) {
            Ok(persisted) => {
                assert!(cluster_ready, "an unready cluster admitted a tiered topic");
                let explicit = resource_options_from_wire(&persisted.request.options, true)
                    .expect("explicit block decodes");
                assert!(TopicResolvedOptions::from(&explicit).tiered_storage);
            }
            Err(error) => {
                assert!(!cluster_ready, "a ready cluster refused a tiered topic");
                assert_eq!(error, IggyError::FeatureUnavailable);
            }
        }
        assert!(prepare(Some(false)).is_ok(), "ready {cluster_ready}");
        assert!(prepare(None).is_ok(), "ready {cluster_ready}");
    }
    assert_eq!(
        prepare_topic_with(&tiered(Some(true))).err(),
        Some(IggyError::FeatureUnavailable),
        "a primary that was told nothing refuses"
    );

    let persisted = prepare_topic_with(&tiered(Some(false)))
        .expect("a topic that states it is not tiered is admitted");
    let explicit = resource_options_from_wire(&persisted.request.options, true)
        .expect("explicit block decodes");
    assert_eq!(
        explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
            .value
            .as_bytes(),
        b"tiered_storage"
    );
    assert!(!TopicResolvedOptions::from(&explicit).tiered_storage);
}

#[test]
fn given_a_topic_type_when_preparing_create_topic_should_admit_geo_only_in_a_ready_cluster() {
    let typed = |topic_type: Option<TopicType>, geo_mode: Option<GeoMode>| {
        iggy_common::TopicCreateOptions {
            topic_type,
            geo_mode,
            ..Default::default()
        }
    };
    for cluster_ready in [false, true] {
        let plane = metadata_plane();
        plane.observe_tiered_storage_readiness(cluster_ready);
        for geo_mode in [None, Some(GeoMode::Geo)] {
            let admitted = prepare_topic_on(&plane, &typed(Some(TopicType::Geo), geo_mode));
            if cluster_ready {
                assert!(admitted.is_ok(), "mode {geo_mode:?}: {admitted:?}");
            } else {
                assert_eq!(
                    admitted.err(),
                    Some(IggyError::FeatureUnavailable),
                    "mode {geo_mode:?}"
                );
            }
        }
        assert_eq!(
            prepare_topic_on(
                &plane,
                &typed(Some(TopicType::Geo), Some(GeoMode::GeoTiered))
            )
            .err(),
            Some(IggyError::FeatureUnavailable),
            "geo_tiered, ready {cluster_ready}"
        );
        for (refused, key) in [
            (
                iggy_common::TopicCreateOptions {
                    tiered_storage: Some(true),
                    ..typed(Some(TopicType::Geo), None)
                },
                "tiered_storage",
            ),
            (
                iggy_common::TopicCreateOptions {
                    partition_resize_policy: Some(PartitionResizePolicy::Mutable),
                    ..typed(Some(TopicType::Geo), None)
                },
                "partition_resize_policy",
            ),
        ] {
            assert_eq!(
                prepare_topic_on(&plane, &refused).err(),
                Some(IggyError::InvalidOptionValue(key.to_owned())),
                "ready {cluster_ready}"
            );
        }
    }
    for topic_type in [None, Some(TopicType::Normal)] {
        for geo_mode in [GeoMode::Geo, GeoMode::GeoTiered] {
            assert_eq!(
                prepare_topic_with(&typed(topic_type, Some(geo_mode))).err(),
                Some(IggyError::InvalidOptionValue("geo_mode".to_owned())),
                "{topic_type:?} with {geo_mode}"
            );
        }
    }

    let persisted = prepare_topic_with(&typed(None, None)).expect("a default topic is admitted");
    let explicit = resource_options_from_wire(&persisted.request.options, true)
        .expect("explicit block decodes");
    let derived = resource_options_from_wire(&persisted.derived_options, false)
        .expect("derived block decodes");
    assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::TopicType)));
    assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::CriticalKeys)));
    assert_eq!(
        derived[&HeaderKey::from(OptionKey::TopicType)]
            .value
            .as_bytes(),
        b"normal"
    );
    assert!(!derived.contains_key(&HeaderKey::from(OptionKey::GeoMode)));
    let mut stored = derived;
    stored.extend(explicit);
    let resolved = TopicResolvedOptions::from(&stored);
    assert_eq!(resolved.topic_type, TopicType::Normal);
    assert_eq!(resolved.geo_mode, None);

    let stated = prepare_topic_with(&typed(Some(TopicType::Normal), None))
        .expect("a topic that states it is normal is admitted");
    assert_eq!(
        resource_options_from_wire(&stated.request.options, true).expect("explicit block decodes")
            [&HeaderKey::from(OptionKey::CriticalKeys)]
            .value
            .as_bytes(),
        b"topic_type"
    );
}

#[test]
fn given_a_ready_cluster_when_preparing_a_geo_topic_should_store_its_mode_fixed_partitions_and_both_critical_keys()
 {
    let plane = metadata_plane();
    plane.observe_tiered_storage_readiness(true);
    let persisted = prepare_topic_on(
        &plane,
        &iggy_common::TopicCreateOptions {
            topic_type: Some(TopicType::Geo),
            ..Default::default()
        },
    )
    .expect("a ready cluster admits a geo topic");
    let explicit = resource_options_from_wire(&persisted.request.options, true)
        .expect("explicit block decodes");
    let derived = resource_options_from_wire(&persisted.derived_options, false)
        .expect("derived block decodes");
    assert_eq!(
        explicit[&HeaderKey::from(OptionKey::CriticalKeys)]
            .value
            .as_bytes(),
        b"geo_mode,topic_type"
    );
    assert!(!explicit.contains_key(&HeaderKey::from(OptionKey::GeoMode)));
    assert_eq!(
        derived[&HeaderKey::from(OptionKey::GeoMode)]
            .value
            .as_bytes(),
        b"geo"
    );
    assert_eq!(
        derived[&HeaderKey::from(OptionKey::PartitionResizePolicy)]
            .value
            .as_bytes(),
        b"fixed"
    );
    let mut stored = derived;
    stored.extend(explicit);
    assert_eq!(check_critical_options(&stored), Ok(()));
    let resolved = TopicResolvedOptions::from(&stored);
    assert_eq!(resolved.geo_mode, Some(GeoMode::Geo));
    assert_eq!(
        resolved.partition_resize_policy,
        PartitionResizePolicy::Fixed
    );
    assert!(resolved.runtime.places_batches);
}

#[test]
fn given_a_geo_topic_naming_a_durability_when_preparing_should_ignore_it_and_derive_persisted() {
    let durability = HeaderKey::from(OptionKey::Durability);
    for named in [Durability::Replicated, Durability::Persisted] {
        let plane = metadata_plane();
        plane.observe_tiered_storage_readiness(true);
        let persisted = prepare_topic_on(
            &plane,
            &iggy_common::TopicCreateOptions {
                topic_type: Some(TopicType::Geo),
                durability: named,
                ..Default::default()
            },
        )
        .expect("a ready cluster admits a geo topic");
        let explicit = resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes");
        let derived = resource_options_from_wire(&persisted.derived_options, false)
            .expect("derived block decodes");
        assert!(!explicit.contains_key(&durability), "{named} is ignored");
        assert_eq!(
            derived.get(&durability),
            Some(&OptionValue::derived(
                Durability::Persisted.encode().unwrap()
            )),
            "{named}"
        );
    }

    let normal = prepare_topic_with(&iggy_common::TopicCreateOptions::default())
        .expect("a normal topic is admitted");
    let explicit =
        resource_options_from_wire(&normal.request.options, true).expect("explicit block decodes");
    assert_eq!(
        explicit.get(&durability),
        Some(&OptionValue::explicit(
            Durability::Replicated.encode().unwrap()
        )),
        "a normal topic keeps the durability it names"
    );
}

#[test]
fn given_local_retention_above_the_topic_retention_when_preparing_create_topic_should_refuse_it() {
    const GIB: u64 = 1 << 30;
    assert_eq!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            message_expiry: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
            local_retention_age: Some(iggy_common::IggyExpiry::from(2 * HOUR_MICROS)),
            ..Default::default()
        })
        .err(),
        Some(IggyError::InvalidOptionValue(
            "local_retention_age".to_owned()
        ))
    );
    assert_eq!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            max_topic_size: Some(iggy_common::MaxTopicSize::from(2 * GIB)),
            local_retention_size: Some(iggy_common::MaxTopicSize::from(4 * GIB)),
            ..Default::default()
        })
        .err(),
        Some(IggyError::InvalidOptionValue(
            "local_retention_size".to_owned()
        ))
    );

    let persisted = prepare_topic_with(&iggy_common::TopicCreateOptions {
        message_expiry: Some(iggy_common::IggyExpiry::from(2 * HOUR_MICROS)),
        local_retention_age: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
        max_topic_size: Some(iggy_common::MaxTopicSize::from(4 * GIB)),
        local_retention_size: Some(iggy_common::MaxTopicSize::from(2 * GIB)),
        ..Default::default()
    })
    .expect("a local retention within the topic retention is admitted");
    let stored = TopicResolvedOptions::from(
        &resource_options_from_wire(&persisted.request.options, true)
            .expect("explicit block decodes"),
    );
    assert_eq!(
        stored.local_retention_age,
        iggy_common::IggyExpiry::from(HOUR_MICROS)
    );
    assert_eq!(
        stored.local_retention_size,
        iggy_common::MaxTopicSize::from(2 * GIB)
    );

    // The one-day default is above this expiry and yields to it.
    assert!(
        prepare_topic_with(&iggy_common::TopicCreateOptions {
            message_expiry: Some(iggy_common::IggyExpiry::from(HOUR_MICROS)),
            ..Default::default()
        })
        .is_ok()
    );
}

#[test]
fn given_tiered_storage_when_resolving_a_topic_update_should_admit_on_only_in_a_ready_cluster() {
    let update = |set: &[(OptionKey, &str)], cluster_ready: bool| {
        let patch = TopicUpdateOptions {
            raw: set
                .iter()
                .map(|(key, value)| (key.to_string(), (*value).to_owned()))
                .collect(),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        resolve_update_resource(
            &UpdateResourceRequest {
                target: ResourceTarget::Topic {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                },
                reset: patch.reset,
                options: patch.set,
            },
            cluster_ready,
        )
        .map(|(operation, _)| operation)
    };
    let enable = [(OptionKey::TieredStorage, "true")];

    assert_eq!(update(&enable, false), Err(IggyError::FeatureUnavailable));
    assert_eq!(update(&enable, true), Ok(Operation::UpdateTopicResolved));
    for cluster_ready in [false, true] {
        assert_eq!(
            update(&[(OptionKey::TieredStorage, "false")], cluster_ready),
            Err(IggyError::OptionNotUpdatable("tiered_storage".to_owned())),
            "ready {cluster_ready}"
        );
        assert_eq!(
            update(&[(OptionKey::MessageExpiry, "1h")], cluster_ready),
            Ok(Operation::UpdateTopicResolved),
            "ready {cluster_ready}"
        );
    }
}

#[test]
fn given_a_tiered_storage_patch_when_the_primary_prepares_it_should_follow_the_cluster_verdict() {
    const CLIENT: u128 = 1;
    let enable = || {
        let patch = TopicUpdateOptions {
            raw: BTreeMap::from([(OptionKey::TieredStorage.to_string(), "true".to_owned())]),
            ..TopicUpdateOptions::default()
        }
        .to_patch()
        .unwrap();
        update_resource_request(
            CLIENT,
            &UpdateResourceRequest {
                target: ResourceTarget::Topic {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                },
                reset: patch.reset,
                options: patch.set,
            }
            .to_bytes(),
        )
    };
    let plane = metadata_plane();
    plane
        .client_table
        .borrow_mut()
        .commit_register(CLIENT, 7, [0x5a; 32], register_reply(CLIENT, 10))
        .expect("register commits");

    let refused = plane.prepare_request(enable()).err();
    assert_eq!(refused, Some(IggyError::FeatureUnavailable));
    assert!(
        refused.as_ref().is_some_and(is_admission_refusal),
        "the client gets the refusal as a reply and keeps its session"
    );
    plane.observe_tiered_storage_readiness(true);
    assert_eq!(
        plane
            .prepare_request(enable())
            .map(|prepare| prepare.header().operation),
        Ok(Operation::UpdateTopicResolved)
    );
    plane.observe_tiered_storage_readiness(false);
    assert_eq!(
        plane.prepare_request(enable()).err(),
        Some(IggyError::FeatureUnavailable)
    );
}

#[test]
fn given_a_cluster_verdict_when_the_metadata_view_changes_should_stop_admitting() {
    let mut metadata = placement_metadata();
    assert!(!metadata.tiered_storage_cluster_ready());
    metadata.observe_tiered_storage_readiness(true);
    assert!(metadata.tiered_storage_cluster_ready());

    metadata.consensus.as_mut().unwrap().set_view(1);
    assert!(
        !metadata.tiered_storage_cluster_ready(),
        "a verdict from the previous view admits nothing"
    );
}

#[test]
fn given_keyless_create_topic_when_preparing_should_derive_every_catalog_default() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    let plane = metadata_plane();
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .expect("register commits");

    let prepare = plane
        .prepare_request(create_topic_request(CLIENT, ACTING_USER))
        .expect("CreateTopic is client-allowed");
    let body = &prepare.as_slice()[size_of::<PrepareHeader>()..prepare.header().size as usize];
    let persisted = PersistedCreateTopicRequest::decode_from(body)
        .expect("create topic with assignments prepare must decode");
    let derived = resource_options_from_wire(&persisted.derived_options, false)
        .expect("derived block decodes");
    let mut expected = ResourceOptions::new();
    for descriptor in descriptors_for(OptionsScope::Topic)
        .filter(|descriptor| descriptor.storage == OptionStorage::Map)
    {
        if let Some(default) = descriptor.default_value().expect("default encodes") {
            expected.insert(
                HeaderKey::from(descriptor.key),
                OptionValue::derived(default),
            );
        }
    }
    assert!(!expected.is_empty(), "the topic catalog has defaulted rows");
    // The factor default is lowered to this single-node cluster.
    expected.insert(
        HeaderKey::from(OptionKey::ReplicationFactor),
        OptionValue::derived(HeaderValue::from(1u32)),
    );
    assert_eq!(derived, expected);
    assert!(
        persisted
            .partitions
            .iter()
            .all(|partition| partition.members == [0]),
        "every partition lists the one node"
    );
}

#[test]
fn given_plaintext_stream_key_when_preparing_create_stream_should_refuse_before_replication() {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    let plane = metadata_plane();
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .expect("register commits");
    let mut options = ResourceOptions::new();
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKey),
        OptionValue::explicit(
            HeaderValue::try_from([7u8; 32].as_slice()).expect("raw key encodes"),
        ),
    );
    let options =
        resource_options_to_wire(&options, OptionsProvenance::All).expect("options encode");

    let Err(refusal) =
        plane.prepare_request(create_stream_request_with(CLIENT, 1, "secrets", options))
    else {
        panic!("a plaintext stream key must not be sealed into a prepare");
    };

    assert!(
        matches!(
            &refusal,
            IggyError::InvalidOptionValue(key) if *key == OptionKey::EncryptionKey.to_string()
        ),
        "expected the plaintext key refusal, got {refusal:?}"
    );
}

#[test]
fn given_wrap_set_no_primary_key_opens_when_preparing_create_stream_should_refuse_before_replication()
 {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    let primary_keys = Arc::new(
        ServerKeyContext::from_config("/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=", &[], true)
            .expect("valid key"),
    );
    let stranger_keys =
        ServerKeyContext::from_config("AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=", &[], true)
            .expect("valid key");
    let plane = metadata_plane().with_stream_key_context(Arc::clone(&primary_keys));
    plane
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .expect("register commits");
    let wrapped_under = |keys: &ServerKeyContext| {
        let set = keys
            .wrap_set(
                &StreamEncryptionKey::try_from([7u8; 32].as_slice()).expect("32 bytes"),
                StreamKeyOrigin::Server,
            )
            .expect("wraps");
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).expect("fits")),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).expect("marker"),
            ),
        );
        resource_options_to_wire(&options, OptionsProvenance::All).expect("options encode")
    };

    let Err(refusal) = plane.prepare_request(create_stream_request_with(
        CLIENT,
        1,
        "stranger",
        wrapped_under(&stranger_keys),
    )) else {
        panic!("a wrap set the primary cannot open must not be sealed into a prepare");
    };

    assert_eq!(
        refusal.as_code(),
        IggyError::StreamEncryptionUnavailable.as_code()
    );
    assert!(
        plane
            .prepare_request(create_stream_request_with(
                CLIENT,
                2,
                "own",
                wrapped_under(&primary_keys),
            ))
            .is_ok()
    );
}

/// A view change or a boot re-pipeline rebuilds a pending expired-token
/// delete without its in-process sender. Committing it must not send a
/// reply to the reserved client id, which no connection owns.
#[compio::test]
async fn given_repipelined_server_operations_when_committed_should_send_no_client_reply() {
    const USER: u32 = 7;
    const TOKEN: &str = "expired";

    let dir = tempfile::tempdir().unwrap();
    let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
        .await
        .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        ClientSendSpyBus::default(),
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
        Some(consensus),
        Some(journal),
        None,
        None,
        TestMux::default(),
        None,
    );
    let consensus = md.consensus.as_ref().unwrap();

    let body = DeletePersonalAccessTokenRequest {
        user_id: USER,
        name: WireName::new(TOKEN).unwrap(),
        only_if_expired: true,
    }
    .to_bytes();
    let header = RoutedRequestHeader {
        client: RESERVED_CLIENT_ID,
        group: server_common::sharding::METADATA_GROUP,
        ..RoutedRequestHeader::default()
    };
    let prepare = consensus::seal_prepare_capacity(
        build_prepare_message(
            consensus,
            &header,
            Operation::DeletePersonalAccessToken,
            &body,
        ),
        consensus.retry_capacity(&md.client_table.borrow()),
    );
    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
    md.on_replicate(prepare).await;
    let mut loopback = Vec::new();
    consensus.drain_loopback_into(&mut loopback);
    let ack = loopback
        .pop()
        .expect("one self-ack per prepare")
        .try_into_typed::<PrepareOkHeader>()
        .expect("loopback holds self PrepareOks");
    md.on_ack(ack).await;

    assert_eq!(consensus.commit_min(), 1, "the delete must commit");
    assert!(
        consensus.message_bus().client_sends.borrow().is_empty(),
        "no connection owns the reserved client id, so nothing may be sent to it"
    );

    let body = CompleteLifecycleRequest::new(
        1,
        1,
        vec![LifecycleCompletion {
            topic_id: 1,
            partition_id: 0,
            partition_op: 1,
        }],
    )
    .unwrap()
    .to_bytes();
    let header = RoutedRequestHeader {
        client: message_bus::server_internal_client_id(server_common::sharding::METADATA_GROUP),
        ..header
    };
    let prepare = consensus::seal_prepare_capacity(
        build_prepare_message(consensus, &header, Operation::CompleteLifecycle, &body),
        consensus.retry_capacity(&md.client_table.borrow()),
    );
    consensus.pipeline_message(PlaneKind::Metadata, &prepare);
    md.on_replicate(prepare).await;
    consensus.drain_loopback_into(&mut loopback);
    let ack = loopback
        .pop()
        .unwrap()
        .try_into_typed::<PrepareOkHeader>()
        .unwrap();
    md.on_ack(ack).await;
    assert_eq!(consensus.commit_min(), 2, "the completion must commit");
    assert!(
        consensus.message_bus().client_sends.borrow().is_empty(),
        "a completion without its in-process sender must never send a wire reply"
    );
}

/// A replayed `CreatePersonalAccessToken` must be refused, not served from
/// the dedup cache: the committed secret is unrecoverable (never
/// replicated) and the rewrite has already minted a fresh one whose hash
/// never reached consensus, so replaying would hand back a credential that
/// authenticates against nothing. The refusal keeps the committed header,
/// so the client can still prove that the original create committed. A
/// cached REJECTION still replays -- it carries no secret.
#[compio::test]
async fn replayed_pat_create_is_refused_but_other_replays_pass_through() {
    const CLIENT: u128 = 1;
    const USER: u32 = 7;
    const PAT_OP: u64 = 2;
    const PAT_REQUEST_CHECKSUM: u128 = 0x2a;

    let dir = tempfile::tempdir().unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
        );
    md.client_table
        .borrow_mut()
        .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
        .unwrap();

    // Cache a committed SUCCESS for request 1 under the PAT operation,
    // exactly as the commit path would (empty apply body + result section).
    let pat_success = build_reply_message(
        &PrepareHeader {
            client: CLIENT,
            request: 1,
            request_checksum: PAT_REQUEST_CHECKSUM,
            operation: Operation::CreatePersonalAccessToken,
            op: PAT_OP,
            user_id: USER,
            ..PrepareHeader::default()
        },
        &bytes::Bytes::copy_from_slice(&0u32.to_le_bytes()),
    );
    let committed = *pat_success.header();
    md.client_table
        .borrow_mut()
        .commit_reply(CLIENT, USER, pat_success);

    // Replaying request 1 as a PAT create must NOT return the cached
    // success; it must refuse with the name-taken code.
    let refusal = md
        .submit_request_in_process(pat_request(
            CLIENT,
            1,
            Operation::CreatePersonalAccessToken,
            &[],
        ))
        .await
        .expect("refusal is a reply, not a submit error")
        .try_into_typed::<ReplyHeader>()
        .unwrap();
    assert_eq!(
        iggy_binary_protocol::result_code(refusal.body()),
        Some(IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code()),
        "a replayed PAT create must be refused, never answered from cache"
    );
    let header = refusal.header();
    assert!(
        header.has_committed_result(),
        "the refusal must prove that the original create committed"
    );
    assert_eq!((header.op, header.commit), (PAT_OP, PAT_OP));
    let expected = ReplyHeader {
        size: header.size,
        ..committed
    };
    assert_eq!(
        bytemuck::bytes_of(header),
        bytemuck::bytes_of(&expected),
        "the refusal must keep the committed client, request, request checksum and operation"
    );

    // A cached REJECTION for the same operation carries no secret, so it
    // replays untouched.
    let rejected_code = IggyError::InvalidPersonalAccessTokenExpiry.as_code();
    let pat_rejection = committed_reply(
        CLIENT,
        2,
        Operation::CreatePersonalAccessToken,
        rejected_code,
    );
    md.client_table
        .borrow_mut()
        .commit_reply(CLIENT, USER, pat_rejection);
    let reply = md
        .submit_request_in_process(pat_request(
            CLIENT,
            2,
            Operation::CreatePersonalAccessToken,
            &[],
        ))
        .await
        .expect("cached rejection replays");
    let body = &reply.as_slice()[size_of::<ReplyHeader>()..];
    assert_eq!(
        iggy_binary_protocol::result_code(body),
        Some(rejected_code),
        "a cached PAT rejection is safe to replay verbatim"
    );
}

/// A same-id replay of a `CreateTopic` that already committed must get its
/// cached reply at the partitions cap. Admitting it again would count the
/// committed topic's own partitions a second time and deny an op that
/// succeeded.
#[compio::test]
async fn given_committed_create_topic_at_partitions_cap_when_replayed_should_serve_cached_reply() {
    const CLIENT: u128 = 1;
    const USER: u32 = TOPIC_MANAGER;
    let mut metadata = metadata_plane();
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    metadata.set_partitions_max(1);
    metadata
        .client_table
        .borrow_mut()
        .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
        .unwrap();
    // The commit that created the topic above cached its success.
    metadata.client_table.borrow_mut().commit_reply(
        CLIENT,
        USER,
        committed_reply(CLIENT, 1, Operation::CreateTopicWithAssignments, 0),
    );

    let replay = metadata
        .submit_request_in_process(create_topic_request(CLIENT, USER))
        .await
        .expect("a replay is a reply, not a submit error");
    assert_eq!(
        iggy_binary_protocol::result_code(&replay.as_slice()[size_of::<ReplyHeader>()..]),
        Some(0),
        "the replay must get the cached success, not a cap denial"
    );

    let mut fresh = create_topic_request(CLIENT, USER);
    bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(
        &mut fresh.as_mut_slice()[..size_of::<RoutedRequestHeader>()],
    )
    .request = 2;
    let denied = metadata
        .submit_request_in_process(fresh)
        .await
        .expect("a denial is a reply, not a submit error");
    assert_eq!(
        iggy_binary_protocol::result_code(&denied.as_slice()[size_of::<ReplyHeader>()..]),
        Some(IggyError::PartitionsLimitReached.as_code()),
        "a new create past the cap must be denied"
    );
}

/// A normal backup names the primary of its view as the place to retry,
/// and a backup in transfer cannot. Neither admits the mutation.
#[compio::test]
async fn given_backup_when_mutation_is_submitted_should_refuse_by_role() {
    const CLIENT: u128 = 1;
    const USER: u32 = TOPIC_MANAGER;
    const BACKUP_REPLICA: u8 = 1;
    const REPLICA_COUNT: u8 = 3;
    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        let consensus = VsrConsensus::new(
            1,
            BACKUP_REPLICA,
            REPLICA_COUNT,
            server_common::sharding::METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.init();
        if transferring {
            consensus.begin_state_transfer_await();
        }
        let metadata: IggyMetadata<_, PrepareJournal, (), TestMux> = IggyMetadata::new(
            Some(consensus),
            None,
            None,
            None,
            TestMux::new((
                users_with_topic_manager(),
                (stream_with_one_partition().into(), ()),
            )),
            None,
        );
        metadata
            .client_table
            .borrow_mut()
            .commit_register(CLIENT, USER, [0x5a; 32], register_reply(CLIENT, 1))
            .unwrap();

        let reply = metadata
            .submit_request_in_process(create_topic_request(CLIENT, USER))
            .await
            .expect("a refusal is a reply, not a submit error");
        assert_eq!(
            iggy_binary_protocol::result_code(&reply.as_slice()[size_of::<ReplyHeader>()..]),
            Some(expected.as_code()),
            "transferring {transferring}"
        );
        assert_eq!(
            metadata.consensus.as_ref().unwrap().pipeline_len(),
            0,
            "a backup must not prepare the mutation"
        );
    }
}
