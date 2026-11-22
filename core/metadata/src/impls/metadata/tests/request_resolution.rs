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
fn given_create_user_when_building_prepare_should_not_store_body_hash_as_request_identifier() {
    let body = CreateUserRequest {
        username: WireName::new("alice").expect("valid username"),
        password: "guessable-password".to_owned(),
        status: UserStatus::Active.as_code(),
        permissions: None,
        options: WireOptions::empty(),
    }
    .to_bytes();
    let client =
        RequestHeader::for_request(CREATE_USER_CODE, 7, 1, 3, &body).expect("request header");
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let routed = RoutedRequestHeader {
        request_checksum: client.request_checksum,
        ..RoutedRequestHeader::default()
    };
    let prepare = build_prepare_message(&consensus, &routed, Operation::CreateUser, &body);
    assert_ne!(prepare.header().request_checksum, 0);
    assert_eq!(prepare.header().request_checksum, client.request_checksum);
    assert_ne!(
        prepare.header().request_checksum,
        u128::from(iggy_common::calculate_checksum(&body))
    );
}

#[test]
fn given_topic_patch_when_resolving_should_extract_name_and_resolve_reset_defaults() {
    let patch = TopicUpdateOptions {
        name: Some("renamed".to_owned()),
        message_expiry: Some(IggyExpiry::from(9_000_000u64)),
        reset: BTreeSet::from([OptionKey::MaxTopicSize]),
        ..TopicUpdateOptions::default()
    }
    .to_patch()
    .unwrap();
    let request = UpdateResourceRequest {
        target: ResourceTarget::Topic {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::named("orders").unwrap(),
        },
        reset: patch.reset,
        options: patch.set,
    };

    let (operation, body) = resolve_update_resource(&request, false).unwrap();

    assert_eq!(operation, Operation::UpdateTopicResolved);
    let resolved = UpdateTopicResolvedRequest::decode_from(&body).unwrap();
    let mut explicit = ResourceOptions::new();
    explicit.insert(
        HeaderKey::from(OptionKey::MessageExpiry),
        OptionValue::explicit(IggyExpiry::from(9_000_000u64).encode().unwrap()),
    );
    let mut derived = ResourceOptions::new();
    derived.insert(
        HeaderKey::from(OptionKey::MaxTopicSize),
        OptionValue::derived(
            OptionKey::MaxTopicSize
                .descriptor()
                .default_value()
                .unwrap()
                .unwrap(),
        ),
    );
    assert_eq!(
        resolved,
        UpdateTopicResolvedRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::named("orders").unwrap(),
            name: Some(WireName::new("renamed").unwrap()),
            explicit_options: resource_options_to_wire(&explicit, OptionsProvenance::All).unwrap(),
            derived_options: resource_options_to_wire(&derived, OptionsProvenance::All).unwrap(),
        }
    );
}

#[test]
fn given_default_sentinel_when_resolving_patch_should_leave_the_key_unchanged() {
    let patch = TopicUpdateOptions {
        name: Some("renamed".to_owned()),
        raw: BTreeMap::from([(
            OptionKey::MessageExpiry.to_string(),
            "server_default".to_owned(),
        )]),
        ..TopicUpdateOptions::default()
    }
    .to_patch()
    .unwrap();
    let request = UpdateResourceRequest {
        target: ResourceTarget::Topic {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
        },
        reset: patch.reset,
        options: patch.set,
    };

    let (_, body) = resolve_update_resource(&request, false).unwrap();

    let resolved = UpdateTopicResolvedRequest::decode_from(&body).unwrap();
    assert_eq!(resolved.name, Some(WireName::new("renamed").unwrap()));
    assert!(resolved.explicit_options.is_empty());
    assert!(resolved.derived_options.is_empty());
}

#[test]
fn given_create_only_or_foreign_key_when_resolving_patch_should_refuse_before_replication() {
    let topic = ResourceTarget::Topic {
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(2),
    };
    let stream = ResourceTarget::Stream {
        stream_id: WireIdentifier::numeric(1),
    };
    let raw = |key: OptionKey, value: &str| TopicUpdateOptions {
        raw: BTreeMap::from([(key.to_string(), value.to_owned())]),
        ..TopicUpdateOptions::default()
    };
    let cases = [
        (
            topic.clone(),
            raw(OptionKey::PartitionResizePolicy, "fixed"),
            IggyError::OptionNotUpdatable(OptionKey::PartitionResizePolicy.to_string()),
        ),
        (
            topic,
            raw(OptionKey::EncryptionKeySource, "stream"),
            IggyError::UnsupportedOptionKey(OptionKey::EncryptionKeySource.to_string()),
        ),
        (
            stream,
            raw(OptionKey::MessageExpiry, "1000"),
            IggyError::UnsupportedOptionKey(OptionKey::MessageExpiry.to_string()),
        ),
    ];
    for (target, options, expected) in cases {
        let patch = options.to_patch().unwrap();
        let request = UpdateResourceRequest {
            target,
            reset: patch.reset,
            options: patch.set,
        };
        assert_eq!(
            resolve_update_resource(&request, false)
                .unwrap_err()
                .as_code(),
            expected.as_code()
        );
    }
}

#[test]
fn given_user_patch_when_resolving_should_carry_username_and_status_columns() {
    let patch = UserUpdateOptions {
        username: Some("alicia".to_owned()),
        status: Some(UserStatus::Inactive),
        ..UserUpdateOptions::default()
    }
    .to_patch()
    .unwrap();
    let request = UpdateResourceRequest {
        target: ResourceTarget::User {
            user_id: WireIdentifier::numeric(3),
        },
        reset: patch.reset,
        options: patch.set,
    };

    let (operation, body) = resolve_update_resource(&request, false).unwrap();

    assert_eq!(operation, Operation::UpdateUserResolved);
    assert_eq!(
        UpdateUserResolvedRequest::decode_from(&body).unwrap(),
        UpdateUserResolvedRequest {
            user_id: WireIdentifier::numeric(3),
            username: Some(WireName::new("alicia").unwrap()),
            status: Some(UserStatus::Inactive.as_code()),
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        }
    );
}
