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

//! Dumps byte-exact wire fixtures from the Rust protocol crates.
//!
//! The Swift SDK re-implements the binary protocol from scratch, so every
//! encoder and decoder it ships is checked against bytes produced by the
//! reference implementation rather than against itself. Run from
//! `foreign/swift`:
//!
//! ```text
//! cargo run --manifest-path Tools/golden-vectors/Cargo.toml -- Tests/IggyTests/Fixtures/golden.json
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader, calculate_batch_checksum};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::*;
use iggy_binary_protocol::consensus::{Command, EvictionHeader, EvictionReason, HEADER_SIZE, Operation, ReplyHeader, RequestHeader};
use iggy_binary_protocol::primitives::options::WireOptions;
use iggy_binary_protocol::primitives::permissions::{WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions};
use iggy_binary_protocol::primitives::user_headers::encode_user_headers;
use iggy_binary_protocol::requests::consumer_groups::*;
use iggy_binary_protocol::requests::consumer_offsets::*;
use iggy_binary_protocol::requests::messages::{FlushUnsavedBufferRequest, PollMessagesRequest, RawMessage, SendMessagesEncoder};
use iggy_binary_protocol::requests::partitions::{CreatePartitionsRequest, DeletePartitionsRequest};
use iggy_binary_protocol::requests::personal_access_tokens::*;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::*;
use iggy_binary_protocol::requests::system::*;
use iggy_binary_protocol::requests::topics::{CreateTopicRequest, DeleteTopicRequest, GetTopicRequest, GetTopicsRequest, PurgeTopicRequest, UpdateTopicRequest};
use iggy_binary_protocol::requests::users::*;
use iggy_binary_protocol::responses::clients::{ClientDetailsResponse, ClientResponse, ConsumerGroupInfoResponse, GetClientsResponse};
use iggy_binary_protocol::responses::consumer_groups::{ConsumerGroupDetailsResponse, ConsumerGroupMemberResponse, ConsumerGroupResponse, GetConsumerGroupsResponse, SyncConsumerGroupResponse};
use iggy_binary_protocol::responses::consumer_offsets::ConsumerOffsetResponse;
use iggy_binary_protocol::responses::messages::{PollMessagesResponseHeader, SendMessagesConfirmationResponse, SendMessagesResponse};
use iggy_binary_protocol::responses::personal_access_tokens::{GetPersonalAccessTokensResponse, PersonalAccessTokenResponse, RawPersonalAccessTokenResponse};
use iggy_binary_protocol::responses::streams::{GetStreamResponse, GetStreamsResponse, StreamResponse, TopicHeader};
use iggy_binary_protocol::responses::system::get_stats::{CacheMetricEntry, StatsResponse};
use iggy_binary_protocol::responses::system::{ClusterMetadataResponse, ClusterNodeResponse, DescribeOptionsResponse, OptionDescriptor};
use iggy_binary_protocol::responses::topics::{GetTopicResponse, GetTopicsResponse, PartitionResponse};
use iggy_binary_protocol::responses::users::{GetUsersResponse, IdentityResponse, LoginRegisterResponse, UserDetailsResponse, UserResponse};
use iggy_binary_protocol::{AckLevel, ClientVersionInfo, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN, WireConsumer, WireIdentifier, WireName, WirePartitioning, WirePollingStrategy};
use iggy_common::{CompressionAlgorithm, IggyByteSize, IggyDuration, IggyError, IggyExpiry, MaxTopicSize, TopicCreateOptions, TopicUpdateOptions};
use secrecy::SecretString;
use serde::Serialize;
use std::collections::BTreeMap;
use twox_hash::{XxHash3_64, XxHash32};

#[derive(Serialize)]
struct Golden {
    protocol_version: u32,
    protocol_version_min: u32,
    xxh3_64: Vec<HashVector>,
    xxh32: Vec<HashVector>,
    errors: Vec<ErrorEntry>,
    vectors: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct HashVector {
    len: usize,
    hash: String,
}

#[derive(Serialize)]
struct ErrorEntry {
    code: u32,
    name: String,
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(7)).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn numeric(id: u32) -> WireIdentifier {
    WireIdentifier::numeric(id)
}

fn named(name: &str) -> WireIdentifier {
    WireIdentifier::named(name).expect("valid name")
}

fn name(value: &str) -> WireName {
    WireName::new(value).expect("valid name")
}

fn options(entries: &[(u8, &[u8], u8, &[u8])]) -> WireOptions {
    let mut buf = BytesMut::new();
    encode_user_headers(entries, &mut buf);
    WireOptions::from_bytes(buf.freeze()).expect("valid options")
}

fn sample_options() -> WireOptions {
    options(&[(2, b"enforce_fsync", 3, &[1]), (2, b"segment_size", 12, &1_073_741_824u64.to_le_bytes())])
}

fn version_info() -> ClientVersionInfo {
    ClientVersionInfo {
        protocol_version: IGGY_PROTOCOL_VERSION,
        sdk_name: name("swift-sdk"),
        sdk_version: name("0.1.0"),
    }
}

fn sample_permissions() -> WirePermissions {
    WirePermissions {
        global: WireGlobalPermissions {
            manage_servers: true,
            read_servers: true,
            manage_users: false,
            read_users: true,
            manage_streams: false,
            read_streams: true,
            manage_topics: false,
            read_topics: true,
            poll_messages: true,
            send_messages: false,
        },
        streams: vec![
            WireStreamPermissions {
                stream_id: 1,
                manage_stream: true,
                read_stream: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
                topics: vec![
                    WireTopicPermissions { topic_id: 10, manage_topic: true, read_topic: true, poll_messages: false, send_messages: true },
                    WireTopicPermissions { topic_id: 20, manage_topic: false, read_topic: true, poll_messages: true, send_messages: true },
                ],
            },
            WireStreamPermissions {
                stream_id: 2,
                manage_stream: false,
                read_stream: true,
                manage_topics: true,
                read_topics: false,
                poll_messages: false,
                send_messages: true,
                topics: vec![],
            },
        ],
    }
}

fn frame(id: u128, offset_delta: u32, timestamp_delta: u32, payload: &[u8], user_headers: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0u8; BATCH_MESSAGE_HEADER_SIZE];
    bytes[8..24].copy_from_slice(&id.to_le_bytes());
    bytes[24..28].copy_from_slice(&offset_delta.to_le_bytes());
    bytes[28..32].copy_from_slice(&timestamp_delta.to_le_bytes());
    bytes[32..36].copy_from_slice(&(user_headers.len() as u32).to_le_bytes());
    bytes[36..40].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(user_headers);
    let checksum = XxHash3_64::oneshot(&bytes[8..]);
    bytes[0..8].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn batch_record(partition_id: u64, base_offset: u64, base_timestamp: u64, origin_timestamp: u64, frames: &[Vec<u8>]) -> Vec<u8> {
    let blob: Vec<u8> = frames.concat();
    let mut header = BatchHeader::new(partition_id, origin_timestamp, (BATCH_HEADER_SIZE + blob.len()) as u64, frames.len() as u32);
    header.base_offset = base_offset;
    header.base_timestamp = base_timestamp;
    header.batch_checksum = calculate_batch_checksum(&header, &blob);
    let mut bytes = vec![0u8; BATCH_HEADER_SIZE];
    header.encode_into(&mut bytes);
    bytes.extend_from_slice(&blob);
    bytes
}

fn topic_header(id: u32, name_value: &str) -> TopicHeader {
    TopicHeader {
        id,
        created_at: 1_710_000_000_000_000,
        partitions_count: 3,
        message_expiry: 604_800_000_000,
        compression_algorithm: 1,
        max_topic_size: 1_073_741_824,
        size_bytes: 4096,
        messages_count: 100,
        name: name(name_value),
        options: sample_options(),
        derived_options: options(&[(2, b"max_topic_size", 12, &u64::MAX.to_le_bytes())]),
    }
}

fn stream_response(id: u32, topics_count: u32) -> StreamResponse {
    StreamResponse {
        id,
        created_at: 1_710_000_000_000_000,
        topics_count,
        size_bytes: 2048,
        messages_count: 200,
        name: name("my-stream"),
        options: WireOptions::empty(),
    }
}

fn add<T: WireEncode>(vectors: &mut BTreeMap<String, String>, key: &str, value: &T) {
    vectors.insert(key.to_owned(), hex(&value.to_bytes()));
}

fn request_header(code: u32, client: u128, request: u64, session: u64, payload: &[u8]) -> RequestHeader {
    let operation = match code {
        LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE => Operation::Register,
        LOGOUT_USER_CODE => Operation::Logout,
        _ => Operation::from_command_code(code).unwrap_or(Operation::NonReplicated),
    };
    let request_checksum = if operation.is_partition() || operation == Operation::NonReplicated {
        0
    } else {
        u128::from(XxHash3_64::oneshot(payload))
    };
    let mut reserved = [0u8; 60];
    if operation == Operation::NonReplicated {
        reserved[0..4].copy_from_slice(&code.to_le_bytes());
    }
    RequestHeader {
        command: Command::Request,
        operation,
        size: (HEADER_SIZE + payload.len()) as u32,
        client,
        request,
        session,
        request_checksum,
        timestamp: 0,
        reserved,
        ..Default::default()
    }
}

fn main() {
    let output = std::env::args().nth(1).expect("output path argument");
    let mut vectors = BTreeMap::new();

    let mut lengths: Vec<usize> = (0..=300).collect();
    lengths.extend([511, 512, 513, 1000, 1023, 1024, 1025, 1279, 1280, 1281, 2047, 2048, 2049, 4096, 10_000, 65_536, 100_003]);
    let xxh3_64 = lengths.iter().map(|&len| HashVector { len, hash: format!("{:016x}", XxHash3_64::oneshot(&pattern(len))) }).collect();
    let xxh32 = lengths.iter().map(|&len| HashVector { len, hash: format!("{:08x}", XxHash32::oneshot(0, &pattern(len))) }).collect();

    let errors = (1u32..=20_000)
        .filter_map(|code| {
            let error = IggyError::from_code(code);
            (error.as_code() == code).then(|| ErrorEntry { code, name: error.as_string().to_owned() })
        })
        .collect();

    // Primitives.
    add(&mut vectors, "identifier.numeric.1", &numeric(1));
    add(&mut vectors, "identifier.named.my-stream", &named("my-stream"));
    add(&mut vectors, "consumer.numeric.42", &WireConsumer::consumer(numeric(42)));
    add(&mut vectors, "consumer_group.named.my-group", &WireConsumer::consumer_group(named("my-group")));
    add(&mut vectors, "partitioning.balanced", &WirePartitioning::Balanced);
    add(&mut vectors, "partitioning.partition_id.7", &WirePartitioning::PartitionId(7));
    add(&mut vectors, "partitioning.messages_key.user-123", &WirePartitioning::MessagesKey(b"user-123".to_vec()));
    add(&mut vectors, "polling.offset.100", &WirePollingStrategy::offset(100));
    add(&mut vectors, "polling.timestamp.1700000000000", &WirePollingStrategy::timestamp(1_700_000_000_000));
    add(&mut vectors, "polling.first", &WirePollingStrategy::first());
    add(&mut vectors, "polling.last", &WirePollingStrategy::last());
    add(&mut vectors, "polling.next", &WirePollingStrategy::next());
    add(&mut vectors, "options.golden", &sample_options());
    add(&mut vectors, "permissions.sample", &sample_permissions());
    add(&mut vectors, "version_info", &version_info());

    // Requests.
    add(&mut vectors, "request.create_stream", &CreateStreamRequest { name: name("test-stream"), options: WireOptions::empty() });
    add(&mut vectors, "request.create_stream.options", &CreateStreamRequest { name: name("test-stream"), options: sample_options() });
    add(&mut vectors, "request.delete_stream", &DeleteStreamRequest { stream_id: numeric(5) });
    add(&mut vectors, "request.get_stream", &GetStreamRequest { stream_id: named("my-stream") });
    add(&mut vectors, "request.get_streams", &GetStreamsRequest);
    add(&mut vectors, "request.purge_stream", &PurgeStreamRequest { stream_id: numeric(1) });
    add(&mut vectors, "request.update_stream", &UpdateStreamRequest { stream_id: named("old-name"), name: name("new-name"), options: WireOptions::empty() });
    add(&mut vectors, "request.create_topic", &CreateTopicRequest { stream_id: numeric(1), partitions_count: 3, name: name("orders"), options: sample_options() });
    add(&mut vectors, "request.delete_topic", &DeleteTopicRequest { stream_id: numeric(1), topic_id: numeric(5) });
    add(&mut vectors, "request.get_topic", &GetTopicRequest { stream_id: named("my-stream"), topic_id: named("my-topic") });
    add(&mut vectors, "request.get_topics", &GetTopicsRequest { stream_id: numeric(42) });
    add(&mut vectors, "request.purge_topic", &PurgeTopicRequest { stream_id: numeric(1), topic_id: numeric(3) });
    add(&mut vectors, "request.update_topic", &UpdateTopicRequest { stream_id: numeric(1), topic_id: numeric(2), name: name("updated-topic"), options: options(&[(2, b"message_expiry", 12, &604_800_000_000u64.to_le_bytes())]) });
    add(&mut vectors, "request.create_partitions", &CreatePartitionsRequest { stream_id: numeric(1), topic_id: numeric(2), partitions_count: 5 });
    add(&mut vectors, "request.delete_partitions", &DeletePartitionsRequest { stream_id: named("stream"), topic_id: named("topic"), partitions_count: 2 });
    add(&mut vectors, "request.delete_segments", &DeleteSegmentsRequest { stream_id: numeric(1), topic_id: numeric(2), partition_id: 3, segments_count: 10 });
    add(&mut vectors, "request.create_consumer_group", &CreateConsumerGroupRequest { stream_id: numeric(1), topic_id: numeric(2), name: name("grp") });
    add(&mut vectors, "request.delete_consumer_group", &DeleteConsumerGroupRequest { stream_id: numeric(1), topic_id: numeric(2), group_id: numeric(3) });
    add(&mut vectors, "request.get_consumer_group", &GetConsumerGroupRequest { stream_id: named("stream-1"), topic_id: named("topic-1"), group_id: named("group-1") });
    add(&mut vectors, "request.get_consumer_groups", &GetConsumerGroupsRequest { stream_id: numeric(1), topic_id: numeric(2) });
    add(&mut vectors, "request.join_consumer_group", &JoinConsumerGroupRequest { stream_id: numeric(1), topic_id: numeric(2), group_id: numeric(3) });
    add(&mut vectors, "request.leave_consumer_group", &LeaveConsumerGroupRequest { stream_id: numeric(1), topic_id: numeric(2), group_id: named("g") });
    add(&mut vectors, "request.sync_consumer_group", &SyncConsumerGroupRequest { stream_id: named("stream-1"), topic_id: named("topic-1"), group_id: named("group-1") });
    add(&mut vectors, "request.store_consumer_offset", &StoreConsumerOffsetRequest { consumer: WireConsumer::consumer(numeric(1)), stream_id: numeric(10), topic_id: numeric(20), partition_id: Some(5), offset: 12_345, ack: AckLevel::Quorum });
    add(&mut vectors, "request.store_consumer_offset.no_partition", &StoreConsumerOffsetRequest { consumer: WireConsumer::consumer_group(numeric(3)), stream_id: numeric(1), topic_id: numeric(1), partition_id: None, offset: u64::MAX, ack: AckLevel::NoAck });
    add(&mut vectors, "request.get_consumer_offset", &GetConsumerOffsetRequest { consumer: WireConsumer::consumer(named("my-consumer")), stream_id: named("stream-1"), topic_id: named("topic-1"), partition_id: Some(0) });
    add(&mut vectors, "request.delete_consumer_offset", &DeleteConsumerOffsetRequest { consumer: WireConsumer::consumer(numeric(1)), stream_id: numeric(10), topic_id: numeric(20), partition_id: Some(5), ack: AckLevel::Quorum });
    add(&mut vectors, "request.poll_messages", &PollMessagesRequest { consumer: WireConsumer::consumer(numeric(1)), stream_id: numeric(10), topic_id: numeric(20), partition_id: Some(5), strategy: WirePollingStrategy::offset(100), count: 50, auto_commit: true });
    add(&mut vectors, "request.poll_messages.group", &PollMessagesRequest { consumer: WireConsumer::consumer_group(numeric(3)), stream_id: numeric(1), topic_id: numeric(1), partition_id: None, strategy: WirePollingStrategy::first(), count: 10, auto_commit: false });
    add(&mut vectors, "request.flush_unsaved_buffer", &FlushUnsavedBufferRequest { stream_id: numeric(1), topic_id: numeric(2), partition_id: 3, fsync: true });
    add(&mut vectors, "request.create_user", &CreateUserRequest { username: name("admin"), password: "p@ssw0rd".to_owned(), status: 1, permissions: Some(sample_permissions()), options: WireOptions::empty() });
    add(&mut vectors, "request.create_user.no_permissions", &CreateUserRequest { username: name("user"), password: "secret123".to_owned(), status: 2, permissions: None, options: WireOptions::empty() });
    add(&mut vectors, "request.delete_user", &DeleteUserRequest { user_id: named("old-user") });
    add(&mut vectors, "request.get_user", &GetUserRequest { user_id: numeric(42) });
    add(&mut vectors, "request.get_users", &GetUsersRequest);
    add(&mut vectors, "request.update_user", &UpdateUserRequest { user_id: numeric(1), username: Some(name("new-name")), status: Some(2), options: WireOptions::empty() });
    add(&mut vectors, "request.update_user.none", &UpdateUserRequest { user_id: numeric(5), username: None, status: None, options: WireOptions::empty() });
    add(&mut vectors, "request.update_permissions", &UpdatePermissionsRequest { user_id: numeric(1), permissions: Some(sample_permissions()) });
    add(&mut vectors, "request.update_permissions.none", &UpdatePermissionsRequest { user_id: named("admin"), permissions: None });
    add(&mut vectors, "request.change_password", &ChangePasswordRequest { user_id: numeric(1), current_password: "old-pass".to_owned(), new_password: "new-pass-123".to_owned() });
    add(&mut vectors, "request.login_register", &LoginRegisterRequest { version_info: version_info(), username: name("iggy"), password: SecretString::from("iggy"), client_context: None });
    add(&mut vectors, "request.login_register_with_pat", &LoginRegisterWithPatRequest { version_info: version_info(), token: SecretString::from("pat-abc123def456"), client_context: None });
    add(&mut vectors, "request.logout_user", &LogoutUserRequest);
    add(&mut vectors, "request.create_personal_access_token", &CreatePersonalAccessTokenRequest { name: name("my-token"), expiry: 3600 });
    add(&mut vectors, "request.delete_personal_access_token", &DeletePersonalAccessTokenRequest { name: name("my-token") });
    add(&mut vectors, "request.get_personal_access_tokens", &GetPersonalAccessTokensRequest);
    add(&mut vectors, "request.ping", &PingRequest);
    add(&mut vectors, "request.get_stats", &GetStatsRequest);
    add(&mut vectors, "request.get_me", &GetMeRequest);
    add(&mut vectors, "request.get_client", &GetClientRequest { client_id: 42 });
    add(&mut vectors, "request.get_clients", &GetClientsRequest);
    add(&mut vectors, "request.get_cluster_metadata", &GetClusterMetadataRequest);
    add(&mut vectors, "request.get_snapshot", &GetSnapshotRequest { compression: 2, snapshot_types: vec![1, 5] });
    add(&mut vectors, "request.describe_options", &DescribeOptionsRequest { scope: 1 });

    // Send messages: the full body a producer writes.
    {
        let stream_id = numeric(1);
        let topic_id = numeric(2);
        let partitioning = WirePartitioning::PartitionId(0);
        let mut headers = BytesMut::new();
        // Key order matters for the bytes: the reference SDK keeps headers in a
        // `BTreeMap`, so entries are written sorted by key.
        encode_user_headers(&[(2, b"attempt", 11, &3u32.to_le_bytes()), (2, b"trace-id", 2, b"abc")], &mut headers);
        let headers = headers.freeze();
        let messages = [
            RawMessage { id: 1, origin_timestamp: 1_000, headers: None, payload: b"first" },
            RawMessage { id: 2, origin_timestamp: 1_500, headers: Some(&headers), payload: b"second" },
            RawMessage { id: 0xDEAD_BEEF_0000_0000_0000_0000_0000_0003, origin_timestamp: 1_000, headers: None, payload: b"" },
        ];
        let mut buf = BytesMut::new();
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages).expect("encodes");
        vectors.insert("request.send_messages".to_owned(), hex(&buf));
        vectors.insert("user_headers.sample".to_owned(), hex(&headers));
    }

    // Consensus headers.
    let create_stream_payload = CreateStreamRequest { name: name("stream"), options: WireOptions::empty() }.to_bytes();
    vectors.insert("payload.create_stream".to_owned(), hex(&create_stream_payload));
    let client_id: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
    vectors.insert("vsr.request.register".to_owned(), hex(bytemuck::bytes_of(&request_header(LOGIN_REGISTER_CODE, client_id, 0, 0, &[1, 2, 3]))));
    vectors.insert("vsr.request.ping".to_owned(), hex(bytemuck::bytes_of(&request_header(PING_CODE, client_id, 1, 0, &[]))));
    vectors.insert("vsr.request.ping.bound".to_owned(), hex(bytemuck::bytes_of(&request_header(PING_CODE, client_id, 3, 99, &[]))));
    vectors.insert("vsr.request.create_stream".to_owned(), hex(bytemuck::bytes_of(&request_header(CREATE_STREAM_CODE, client_id, 1, 99, &create_stream_payload))));
    vectors.insert("vsr.request.send_messages".to_owned(), hex(bytemuck::bytes_of(&request_header(SEND_MESSAGES_CODE, client_id, 2, 99, &[9, 9, 9, 9]))));
    vectors.insert("vsr.request.logout".to_owned(), hex(bytemuck::bytes_of(&request_header(LOGOUT_USER_CODE, client_id, 4, 99, &[]))));
    vectors.insert("vsr.request.unknown_code".to_owned(), hex(bytemuck::bytes_of(&request_header(60_000, client_id, 5, 99, &[]))));

    let reply = ReplyHeader { command: Command::Reply, operation: Operation::CreateStream, size: (HEADER_SIZE + 12) as u32, request: 7, status: 0, client: client_id, ..Default::default() };
    vectors.insert("vsr.reply.ok".to_owned(), hex(bytemuck::bytes_of(&reply)));
    let denied = ReplyHeader { command: Command::Reply, operation: Operation::NonReplicated, size: HEADER_SIZE as u32, request: 8, status: IggyError::Unauthorized.as_code(), ..Default::default() };
    vectors.insert("vsr.reply.denied".to_owned(), hex(bytemuck::bytes_of(&denied)));
    let eviction = EvictionHeader::new(0, 0, 0, client_id, EvictionReason::StaleClient);
    vectors.insert("vsr.eviction.stale_client".to_owned(), hex(bytemuck::bytes_of(&eviction)));
    let incompatible = EvictionHeader::incompatible_protocol(0, 0, 0, client_id, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN);
    vectors.insert("vsr.eviction.incompatible_protocol".to_owned(), hex(bytemuck::bytes_of(&incompatible)));

    // Responses.
    add(&mut vectors, "response.stream", &stream_response(1, 2));
    add(&mut vectors, "response.get_stream", &GetStreamResponse { stream: stream_response(1, 2), topics: vec![topic_header(1, "topic-a"), topic_header(2, "topic-b")] });
    add(&mut vectors, "response.get_streams", &GetStreamsResponse { streams: vec![stream_response(1, 2), stream_response(2, 0)] });
    add(&mut vectors, "response.topic_header", &topic_header(5, "events"));
    let partitions = vec![
        PartitionResponse { id: 0, created_at: 1_710_000_000_000_000, segments_count: 2, current_offset: 99, size_bytes: 1024, messages_count: 100 },
        PartitionResponse { id: 1, created_at: 1_710_000_000_000_001, segments_count: 1, current_offset: 0, size_bytes: 0, messages_count: 0 },
        PartitionResponse { id: 2, created_at: 1_710_000_000_000_002, segments_count: 3, current_offset: 7, size_bytes: 512, messages_count: 8 },
    ];
    add(&mut vectors, "response.get_topic", &GetTopicResponse { topic: topic_header(1, "my-topic"), partitions });
    add(&mut vectors, "response.get_topics", &GetTopicsResponse { topics: vec![topic_header(1, "events"), topic_header(2, "logs")] });
    add(&mut vectors, "response.get_topics.empty", &GetTopicsResponse { topics: vec![] });
    add(&mut vectors, "response.consumer_group", &ConsumerGroupResponse { id: 1, partitions_count: 4, members_count: 2, name: name("my-group") });
    add(&mut vectors, "response.consumer_group_details", &ConsumerGroupDetailsResponse { group: ConsumerGroupResponse { id: 1, partitions_count: 6, members_count: 2, name: name("my-group") }, members: vec![ConsumerGroupMemberResponse { id: 1, partitions_count: 3, partitions: vec![0, 1, 2] }, ConsumerGroupMemberResponse { id: 2, partitions_count: 3, partitions: vec![3, 4, 5] }] });
    add(&mut vectors, "response.get_consumer_groups", &GetConsumerGroupsResponse { groups: vec![ConsumerGroupResponse { id: 1, partitions_count: 4, members_count: 2, name: name("group-a") }, ConsumerGroupResponse { id: 2, partitions_count: 4, members_count: 0, name: name("group-b") }] });
    add(&mut vectors, "response.sync_consumer_group", &SyncConsumerGroupResponse { generation: 7, partitions: vec![0, 2, 4] });
    add(&mut vectors, "response.consumer_offset", &ConsumerOffsetResponse { partition_id: 1, current_offset: 1000, stored_offset: 500 });
    let client = ClientResponse { client_id: 1, user_id: 10, transport: 1, address: "127.0.0.1:8080".to_owned(), consumer_groups_count: 2 };
    add(&mut vectors, "response.client", &client);
    add(&mut vectors, "response.client_details", &ClientDetailsResponse { client: client.clone(), consumer_groups: vec![ConsumerGroupInfoResponse { stream_id: 1, topic_id: 2, group_id: 3 }, ConsumerGroupInfoResponse { stream_id: 4, topic_id: 5, group_id: 6 }] });
    add(&mut vectors, "response.client_details.no_user", &ClientDetailsResponse { client: ClientResponse { client_id: 2, user_id: u32::MAX, transport: 1, address: "10.0.0.1:6000".to_owned(), consumer_groups_count: 0 }, consumer_groups: vec![] });
    add(&mut vectors, "response.get_clients", &GetClientsResponse { clients: vec![client, ClientResponse { client_id: 2, user_id: 20, transport: 2, address: "10.0.0.1:6000".to_owned(), consumer_groups_count: 3 }] });
    let user = UserResponse { id: 1, created_at: 1_710_000_000_000_000, status: 1, username: name("admin"), options: WireOptions::empty() };
    add(&mut vectors, "response.user", &user);
    add(&mut vectors, "response.user_details", &UserDetailsResponse { user: user.clone(), permissions: Some(sample_permissions()) });
    add(&mut vectors, "response.user_details.no_permissions", &UserDetailsResponse { user: user.clone(), permissions: None });
    add(&mut vectors, "response.get_users", &GetUsersResponse { users: vec![user, UserResponse { id: 2, created_at: 200, status: 2, username: name("alice"), options: WireOptions::empty() }] });
    add(&mut vectors, "response.login_register", &LoginRegisterResponse { user_id: 42, session: 100, server_protocol_version: IGGY_PROTOCOL_VERSION, server_version: name("0.11.0") });
    add(&mut vectors, "response.identity", &IdentityResponse { user_id: 42 });
    add(&mut vectors, "response.raw_personal_access_token", &RawPersonalAccessTokenResponse { token: name("raw-secret-token-value") });
    add(&mut vectors, "response.get_personal_access_tokens", &GetPersonalAccessTokensResponse { tokens: vec![PersonalAccessTokenResponse { name: name("token-a"), expiry_at: 1_710_000_000_000_000 }, PersonalAccessTokenResponse { name: name("token-b"), expiry_at: 0 }] });
    add(&mut vectors, "response.stats", &StatsResponse {
        process_id: 1234,
        cpu_usage: 25.5,
        total_cpu_usage: 50.0,
        memory_usage: 1_073_741_824,
        total_memory: 8_589_934_592,
        available_memory: 4_294_967_296,
        run_time: 3600,
        start_time: 1_710_000_000_000_000,
        read_bytes: 1_000_000,
        written_bytes: 500_000,
        messages_size_bytes: 2_000_000,
        streams_count: 3,
        topics_count: 10,
        partitions_count: 30,
        segments_count: 90,
        messages_count: 50_000,
        clients_count: 5,
        consumer_groups_count: 2,
        hostname: "node-1".to_owned(),
        os_name: "Linux".to_owned(),
        os_version: "6.1".to_owned(),
        kernel_version: "6.1.0".to_owned(),
        iggy_server_version: "0.11.0".to_owned(),
        iggy_server_semver: Some(11_264),
        cache_metrics: vec![CacheMetricEntry { stream_id: 1, topic_id: 1, partition_id: 0, hits: 1000, misses: 50, hit_ratio: 0.952_380_95 }],
        threads_count: 16,
        free_disk_space: 107_374_182_400,
        total_disk_space: 512_110_190_592,
    });
    add(&mut vectors, "response.cluster_metadata", &ClusterMetadataResponse {
        name: "prod-cluster".to_owned(),
        nodes: vec![
            ClusterNodeResponse { name: "node-1".to_owned(), ip: "10.0.0.1".to_owned(), tcp_port: 8090, quic_port: 8080, http_port: 3000, websocket_port: 8092, role: 0, status: 0 },
            ClusterNodeResponse { name: "node-2".to_owned(), ip: "fd00::2".to_owned(), tcp_port: 8091, quic_port: 0, http_port: 3001, websocket_port: 0, role: 1, status: 3 },
        ],
    });
    add(&mut vectors, "response.describe_options", &DescribeOptionsResponse {
        entries: vec![
            OptionDescriptor { key: name("segment_size"), kind: 12, default_value: Bytes::copy_from_slice(&1_073_741_824u64.to_le_bytes()), description: "Segment size".to_owned() },
            OptionDescriptor { key: name("enforce_fsync"), kind: 3, default_value: Bytes::copy_from_slice(&[0]), description: String::new() },
        ],
    });
    add(&mut vectors, "response.send_messages", &SendMessagesResponse { confirmations: vec![SendMessagesConfirmationResponse { stream_id: 1, topic_id: 2, partition_id: 3, base_offset: 4 }, SendMessagesConfirmationResponse { stream_id: 1, topic_id: 2, partition_id: 0, base_offset: 42 }] });
    add(&mut vectors, "response.send_messages.empty", &SendMessagesResponse { confirmations: vec![] });

    // Poll response: header + two batch records.
    {
        let first = batch_record(7, 100, 5_000, 1_000, &[frame(11, 0, 0, b"a", &[]), frame(12, 1, 10, b"b", &vectors_user_headers())]);
        let second = batch_record(7, 102, 6_000, 2_000, &[frame(13, 0, 0, b"c", &[])]);
        let mut body = BytesMut::new();
        PollMessagesResponseHeader { partition_id: 7, current_offset: 102, messages_count: 3 }.encode(&mut body);
        body.put_slice(&first);
        body.put_slice(&second);
        vectors.insert("response.poll_messages".to_owned(), hex(&body));
        let mut empty = BytesMut::new();
        PollMessagesResponseHeader { partition_id: 0, current_offset: 0, messages_count: 0 }.encode(&mut empty);
        vectors.insert("response.poll_messages.empty".to_owned(), hex(&empty));
    }

    // Domain option blocks.
    let create_options = TopicCreateOptions {
        partitions_count: Some(3),
        compression_algorithm: Some(CompressionAlgorithm::Gzip),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::new_from_secs(604_800))),
        max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(1_073_741_824))),
        segment_size: Some(IggyByteSize::from(1_048_576)),
        enforce_fsync: Some(true),
        messages_required_to_save: Some(7),
        size_of_messages_required_to_save: Some(IggyByteSize::from(4096)),
        preallocate_segments: Some(false),
        raw: BTreeMap::new(),
    };
    add(&mut vectors, "options.topic_create.full", &create_options.to_wire().expect("encodes"));
    let update_options = TopicUpdateOptions { compression_algorithm: Some(CompressionAlgorithm::None), message_expiry: Some(IggyExpiry::NeverExpire), max_topic_size: Some(MaxTopicSize::Unlimited), raw: BTreeMap::new() };
    add(&mut vectors, "options.topic_update.full", &update_options.to_wire().expect("encodes"));
    let mut raw = BTreeMap::new();
    raw.insert("enforce_fsync".to_owned(), "true".to_owned());
    let raw_options = TopicCreateOptions { raw, ..Default::default() };
    add(&mut vectors, "options.topic_create.raw", &raw_options.to_wire().expect("encodes"));

    let golden = Golden { protocol_version: IGGY_PROTOCOL_VERSION, protocol_version_min: IGGY_PROTOCOL_VERSION_MIN, xxh3_64, xxh32, errors, vectors };
    let json = serde_json::to_string_pretty(&golden).expect("serializes");
    std::fs::write(&output, json).expect("writes output");
    println!("wrote {output}");
}

fn vectors_user_headers() -> Vec<u8> {
    let mut headers = BytesMut::new();
    encode_user_headers(&[(2, b"k", 2, b"v")], &mut headers);
    headers.to_vec()
}
