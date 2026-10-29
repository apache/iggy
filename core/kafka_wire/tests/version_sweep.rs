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

//! Every validator, at every version it guards, against the bytes `kafka_protocol` itself
//! encodes: a well-formed request with one element in every array is accepted and decodes, every
//! strict prefix of it is refused, and the versions just outside the guarded range are refused.
//!
//! This is the crate's proof that each walk stays aligned with the decoder's field order. A walk
//! that reads one field too few or too many fails the prefix check, because a cut inside that
//! field goes unnoticed or a complete body reads as truncated.

use bytes::{Bytes, BytesMut};
use iggy_kafka_wire::Result;
use iggy_kafka_wire::api::{
    API_KEY_ALTER_CONFIGS, API_KEY_API_VERSIONS, API_KEY_CREATE_PARTITIONS, API_KEY_CREATE_TOPICS,
    API_KEY_DELETE_GROUPS, API_KEY_DELETE_TOPICS, API_KEY_DESCRIBE_ACLS, API_KEY_DESCRIBE_CLUSTER,
    API_KEY_DESCRIBE_CONFIGS, API_KEY_DESCRIBE_GROUPS, API_KEY_FETCH, API_KEY_FIND_COORDINATOR,
    API_KEY_HEARTBEAT, API_KEY_INCREMENTAL_ALTER_CONFIGS, API_KEY_INIT_PRODUCER_ID,
    API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_DELETE, API_KEY_OFFSET_FETCH,
    API_KEY_PRODUCE, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, API_KEY_SYNC_GROUP,
};
use iggy_kafka_wire::bounds_guard::{
    guarded_range, validate_alter_configs_shape, validate_api_versions_shape,
    validate_create_partitions_shape, validate_create_topics_shape, validate_delete_groups_shape,
    validate_delete_topics_shape, validate_describe_acls_shape, validate_describe_cluster_shape,
    validate_describe_configs_shape, validate_describe_groups_shape, validate_fetch_shape,
    validate_find_coordinator_shape, validate_heartbeat_shape,
    validate_incremental_alter_configs_shape, validate_init_producer_id_shape,
    validate_join_group_shape, validate_leave_group_shape, validate_list_groups_shape,
    validate_list_offsets_shape, validate_metadata_shape, validate_offset_commit_shape,
    validate_offset_delete_shape, validate_offset_fetch_shape, validate_produce_shape,
    validate_sasl_authenticate_shape, validate_sasl_handshake_shape, validate_sync_group_shape,
};
use iggy_kafka_wire::codec::decode_exhaustive;
use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_topics_request::{
    CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource as IncrementalAlterConfigsResource,
    AlterableConfig as IncrementalAlterableConfig,
};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_delete_request::{
    OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    AlterConfigsRequest, ApiVersionsRequest, BrokerId, CreatePartitionsRequest,
    CreateTopicsRequest, DeleteGroupsRequest, DeleteTopicsRequest, DescribeAclsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest, FetchRequest,
    FindCoordinatorRequest, GroupId, HeartbeatRequest, IncrementalAlterConfigsRequest,
    InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
    ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetDeleteRequest,
    OffsetFetchRequest, ProduceRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
    SyncGroupRequest, TopicName, TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

const MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;

const fn text(value: &'static str) -> StrBytes {
    StrBytes::from_static_str(value)
}

const fn topic() -> TopicName {
    TopicName(text("t"))
}

const fn group() -> GroupId {
    GroupId(text("g"))
}

fn encode<R: Encodable>(request: &R, version: i16) -> Bytes {
    let mut buf = BytesMut::new();
    request
        .encode(&mut buf, version)
        .unwrap_or_else(|error| panic!("encode at v{version}: {error}"));
    buf.freeze()
}

fn sweep<R: Encodable + Decodable>(
    api_key: i16,
    build: impl Fn(i16) -> R,
    validate: impl Fn(i16, &Bytes) -> Result<()>,
) {
    let range =
        guarded_range(api_key).unwrap_or_else(|| panic!("api key {api_key} has no guarded range"));
    for version in range.min_version..=range.max_version {
        let body = encode(&build(version), version);
        validate(version, &body).unwrap_or_else(|error| {
            panic!("api {api_key} v{version}: a well-formed body was refused: {error}")
        });
        if let Err(error) = decode_exhaustive::<R>(version, body.clone()) {
            panic!("api {api_key} v{version}: the decoder disagrees with the guard: {error}");
        }
        for cut in 0..body.len() {
            assert!(
                validate(version, &body.slice(..cut)).is_err(),
                "api {api_key} v{version}: a prefix of {cut} of {} bytes was accepted",
                body.len()
            );
        }
    }
    let body = encode(&build(range.max_version), range.max_version);
    assert!(
        validate(range.max_version + 1, &body).is_err(),
        "api {api_key}: v{} is above the guarded range and must be refused",
        range.max_version + 1
    );
    let body = encode(&build(range.min_version), range.min_version);
    assert!(
        validate(range.min_version - 1, &body).is_err(),
        "api {api_key}: v{} is below the guarded range and must be refused",
        range.min_version - 1
    );
}

#[test]
fn produce() {
    sweep(
        API_KEY_PRODUCE,
        |_| {
            ProduceRequest::default()
                .with_transactional_id(Some(TransactionalId(text("txn"))))
                .with_acks(1)
                .with_timeout_ms(1000)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(topic())
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(0)
                                .with_records(Some(Bytes::from_static(b"records"))),
                        ]),
                ])
        },
        |version, body| validate_produce_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn fetch() {
    sweep(
        API_KEY_FETCH,
        |version| {
            let forgotten = if version >= 7 {
                vec![
                    ForgottenTopic::default()
                        .with_topic(TopicName(text("forgotten")))
                        .with_partitions(vec![0]),
                ]
            } else {
                Vec::new()
            };
            let rack = if version >= 11 {
                text("rack")
            } else {
                StrBytes::default()
            };
            FetchRequest::default()
                .with_cluster_id((version >= 12).then(|| text("cluster")))
                .with_replica_id(BrokerId(-1))
                .with_max_wait_ms(100)
                .with_min_bytes(1)
                .with_max_bytes(1 << 20)
                .with_topics(vec![
                    FetchTopic::default()
                        .with_topic(topic())
                        .with_partitions(vec![
                            FetchPartition::default()
                                .with_partition(0)
                                .with_fetch_offset(7)
                                .with_partition_max_bytes(1 << 20),
                        ]),
                ])
                .with_forgotten_topics_data(forgotten)
                .with_rack_id(rack)
        },
        |version, body| validate_fetch_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn list_offsets() {
    sweep(
        API_KEY_LIST_OFFSETS,
        |_| {
            ListOffsetsRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_topics(vec![
                    ListOffsetsTopic::default()
                        .with_name(topic())
                        .with_partitions(vec![
                            ListOffsetsPartition::default()
                                .with_partition_index(0)
                                .with_timestamp(-1),
                        ]),
                ])
        },
        |version, body| validate_list_offsets_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn metadata() {
    sweep(
        API_KEY_METADATA,
        |_| {
            MetadataRequest::default().with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic())),
            ]))
        },
        |version, body| validate_metadata_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn offset_commit() {
    sweep(
        API_KEY_OFFSET_COMMIT,
        |version| {
            OffsetCommitRequest::default()
                .with_group_id(group())
                .with_generation_id_or_member_epoch(1)
                .with_member_id(text("member"))
                .with_group_instance_id((version >= 7).then(|| text("instance")))
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(topic())
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(5)
                                .with_committed_metadata(Some(text("meta"))),
                        ]),
                ])
        },
        |version, body| validate_offset_commit_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn offset_fetch() {
    sweep(
        API_KEY_OFFSET_FETCH,
        |version| {
            if version <= 7 {
                OffsetFetchRequest::default()
                    .with_group_id(group())
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopic::default()
                            .with_name(topic())
                            .with_partition_indexes(vec![0]),
                    ]))
            } else {
                OffsetFetchRequest::default().with_groups(vec![
                    OffsetFetchRequestGroup::default()
                        .with_group_id(group())
                        .with_member_id((version >= 9).then(|| text("member")))
                        .with_topics(Some(vec![
                            OffsetFetchRequestTopics::default()
                                .with_name(topic())
                                .with_partition_indexes(vec![0]),
                        ])),
                ])
            }
        },
        |version, body| validate_offset_fetch_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn find_coordinator() {
    sweep(
        API_KEY_FIND_COORDINATOR,
        |version| {
            if version <= 3 {
                FindCoordinatorRequest::default().with_key(text("g"))
            } else {
                FindCoordinatorRequest::default().with_coordinator_keys(vec![text("g")])
            }
        },
        |version, body| validate_find_coordinator_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn join_group() {
    sweep(
        API_KEY_JOIN_GROUP,
        |version| {
            JoinGroupRequest::default()
                .with_group_id(group())
                .with_session_timeout_ms(10_000)
                .with_member_id(text("member"))
                .with_group_instance_id((version >= 5).then(|| text("instance")))
                .with_protocol_type(text("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(text("range"))
                        .with_metadata(Bytes::from_static(b"meta")),
                ])
                .with_reason((version >= 8).then(|| text("reason")))
        },
        |version, body| validate_join_group_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn heartbeat() {
    sweep(
        API_KEY_HEARTBEAT,
        |version| {
            HeartbeatRequest::default()
                .with_group_id(group())
                .with_generation_id(1)
                .with_member_id(text("member"))
                .with_group_instance_id((version >= 3).then(|| text("instance")))
        },
        validate_heartbeat_shape,
    );
}

#[test]
fn leave_group() {
    sweep(
        API_KEY_LEAVE_GROUP,
        |version| {
            if version <= 2 {
                LeaveGroupRequest::default()
                    .with_group_id(group())
                    .with_member_id(text("member"))
            } else {
                LeaveGroupRequest::default()
                    .with_group_id(group())
                    .with_members(vec![
                        MemberIdentity::default()
                            .with_member_id(text("member"))
                            .with_group_instance_id(Some(text("instance")))
                            .with_reason((version >= 5).then(|| text("reason"))),
                    ])
            }
        },
        |version, body| validate_leave_group_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn sync_group() {
    sweep(
        API_KEY_SYNC_GROUP,
        |version| {
            SyncGroupRequest::default()
                .with_group_id(group())
                .with_generation_id(1)
                .with_member_id(text("member"))
                .with_group_instance_id((version >= 3).then(|| text("instance")))
                .with_protocol_type((version >= 5).then(|| text("consumer")))
                .with_protocol_name((version >= 5).then(|| text("range")))
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(text("member"))
                        .with_assignment(Bytes::from_static(b"assignment")),
                ])
        },
        |version, body| validate_sync_group_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn describe_groups() {
    sweep(
        API_KEY_DESCRIBE_GROUPS,
        |_| DescribeGroupsRequest::default().with_groups(vec![group()]),
        |version, body| validate_describe_groups_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn list_groups() {
    sweep(
        API_KEY_LIST_GROUPS,
        |version| {
            let states = if version >= 4 {
                vec![text("Stable")]
            } else {
                Vec::new()
            };
            let types = if version >= 5 {
                vec![text("classic")]
            } else {
                Vec::new()
            };
            ListGroupsRequest::default()
                .with_states_filter(states)
                .with_types_filter(types)
        },
        |version, body| validate_list_groups_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn sasl_handshake() {
    sweep(
        API_KEY_SASL_HANDSHAKE,
        |_| SaslHandshakeRequest::default().with_mechanism(text("PLAIN")),
        validate_sasl_handshake_shape,
    );
}

#[test]
fn api_versions() {
    sweep(
        API_KEY_API_VERSIONS,
        |version| {
            if version >= 3 {
                ApiVersionsRequest::default()
                    .with_client_software_name(text("client"))
                    .with_client_software_version(text("1"))
            } else {
                ApiVersionsRequest::default()
            }
        },
        validate_api_versions_shape,
    );
}

#[test]
fn create_topics() {
    sweep(
        API_KEY_CREATE_TOPICS,
        |_| {
            CreateTopicsRequest::default()
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(topic())
                        .with_num_partitions(1)
                        .with_replication_factor(1)
                        .with_assignments(vec![
                            CreatableReplicaAssignment::default()
                                .with_partition_index(0)
                                .with_broker_ids(vec![BrokerId(1)]),
                        ])
                        .with_configs(vec![
                            CreatableTopicConfig::default()
                                .with_name(text("retention.ms"))
                                .with_value(Some(text("1"))),
                        ]),
                ])
                .with_timeout_ms(1000)
        },
        |version, body| validate_create_topics_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn delete_topics() {
    sweep(
        API_KEY_DELETE_TOPICS,
        |_| {
            DeleteTopicsRequest::default()
                .with_topic_names(vec![topic()])
                .with_timeout_ms(1000)
        },
        |version, body| validate_delete_topics_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn init_producer_id() {
    sweep(
        API_KEY_INIT_PRODUCER_ID,
        |_| {
            InitProducerIdRequest::default()
                .with_transactional_id(Some(TransactionalId(text("txn"))))
                .with_transaction_timeout_ms(1000)
        },
        validate_init_producer_id_shape,
    );
}

#[test]
fn describe_acls() {
    sweep(
        API_KEY_DESCRIBE_ACLS,
        |_| {
            DescribeAclsRequest::default()
                .with_resource_type_filter(2)
                .with_resource_name_filter(Some(text("t")))
                .with_pattern_type_filter(3)
                .with_principal_filter(Some(text("User:alice")))
                .with_host_filter(Some(text("*")))
                .with_operation(3)
                .with_permission_type(3)
        },
        validate_describe_acls_shape,
    );
}

#[test]
fn describe_configs() {
    sweep(
        API_KEY_DESCRIBE_CONFIGS,
        |_| {
            DescribeConfigsRequest::default().with_resources(vec![
                DescribeConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(text("t"))
                    .with_configuration_keys(Some(vec![text("retention.ms")])),
            ])
        },
        |version, body| validate_describe_configs_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn alter_configs() {
    sweep(
        API_KEY_ALTER_CONFIGS,
        |_| {
            AlterConfigsRequest::default().with_resources(vec![
                AlterConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(text("t"))
                    .with_configs(vec![
                        AlterableConfig::default()
                            .with_name(text("retention.ms"))
                            .with_value(Some(text("1"))),
                    ]),
            ])
        },
        |version, body| validate_alter_configs_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn sasl_authenticate() {
    sweep(
        API_KEY_SASL_AUTHENTICATE,
        |_| SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(b"\0user\0pass")),
        validate_sasl_authenticate_shape,
    );
}

#[test]
fn create_partitions() {
    sweep(
        API_KEY_CREATE_PARTITIONS,
        |_| {
            CreatePartitionsRequest::default()
                .with_topics(vec![
                    CreatePartitionsTopic::default()
                        .with_name(topic())
                        .with_count(2)
                        .with_assignments(Some(vec![
                            CreatePartitionsAssignment::default()
                                .with_broker_ids(vec![BrokerId(1)]),
                        ])),
                ])
                .with_timeout_ms(1000)
        },
        |version, body| validate_create_partitions_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn delete_groups() {
    sweep(
        API_KEY_DELETE_GROUPS,
        |_| DeleteGroupsRequest::default().with_groups_names(vec![group()]),
        |version, body| validate_delete_groups_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn incremental_alter_configs() {
    sweep(
        API_KEY_INCREMENTAL_ALTER_CONFIGS,
        |_| {
            IncrementalAlterConfigsRequest::default().with_resources(vec![
                IncrementalAlterConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(text("t"))
                    .with_configs(vec![
                        IncrementalAlterableConfig::default()
                            .with_name(text("retention.ms"))
                            .with_config_operation(0)
                            .with_value(Some(text("1"))),
                    ]),
            ])
        },
        |version, body| validate_incremental_alter_configs_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn offset_delete() {
    sweep(
        API_KEY_OFFSET_DELETE,
        |_| {
            OffsetDeleteRequest::default()
                .with_group_id(group())
                .with_topics(vec![
                    OffsetDeleteRequestTopic::default()
                        .with_name(topic())
                        .with_partitions(vec![
                            OffsetDeleteRequestPartition::default().with_partition_index(0),
                        ]),
                ])
        },
        |version, body| validate_offset_delete_shape(version, body, MAX_FRAME_SIZE),
    );
}

#[test]
fn describe_cluster() {
    sweep(
        API_KEY_DESCRIBE_CLUSTER,
        |_| DescribeClusterRequest::default().with_include_cluster_authorized_operations(true),
        validate_describe_cluster_shape,
    );
}
