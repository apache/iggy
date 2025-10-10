use crate::streaming::{
    partitions::partition2,
    personal_access_tokens::personal_access_token::PersonalAccessToken,
    streams::stream2,
    topics::{
        consumer_group2::{self},
        topic2,
    },
};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, Permissions, TransportProtocol,
    UserStatus,
};
use std::net::SocketAddr;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ShardEvent {
    FlushUnsavedBuffer {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        fsync: bool,
    },
    CreatedStream2 {
        id: usize,
        stream: stream2::Stream,
    },
    DeletedStream2 {
        id: usize,
        stream_id: Identifier,
    },
    UpdatedStream2 {
        stream_id: Identifier,
        name: String,
    },
    PurgedStream2 {
        stream_id: Identifier,
    },
    CreatedPartitions2 {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions: Vec<partition2::Partition>,
    },
    DeletedPartitions2 {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
        partition_ids: Vec<usize>,
    },
    CreatedTopic2 {
        stream_id: Identifier,
        topic: topic2::Topic,
    },
    CreatedConsumerGroup2 {
        stream_id: Identifier,
        topic_id: Identifier,
        cg: consumer_group2::ConsumerGroup,
    },
    DeletedConsumerGroup2 {
        id: usize,
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    },
    UpdatedTopic2 {
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    },
    PurgedTopic {
        stream_id: Identifier,
        topic_id: Identifier,
    },
    DeletedTopic2 {
        id: usize,
        stream_id: Identifier,
        topic_id: Identifier,
    },
    CreatedUser {
        user_id: u32,
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    },
    UpdatedPermissions {
        user_id: Identifier,
        permissions: Option<Permissions>,
    },
    DeletedUser {
        user_id: Identifier,
    },
    LoginUser {
        client_id: u32,
        username: String,
        password: String,
    },
    LogoutUser {
        client_id: u32,
    },
    UpdatedUser {
        user_id: Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    },
    ChangedPassword {
        user_id: Identifier,
        current_password: String,
        new_password: String,
    },
    CreatedPersonalAccessToken {
        personal_access_token: PersonalAccessToken,
    },
    DeletedPersonalAccessToken {
        user_id: u32,
        name: String,
    },
    LoginWithPersonalAccessToken {
        client_id: u32,
        token: String,
    },
    DeletedSegments {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        segments_count: u32,
    },
    NewSession {
        address: SocketAddr,
        transport: TransportProtocol,
    },
    JoinedConsumerGroup {
        client_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    },
    LeftConsumerGroup {
        client_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    },
    AddressBound {
        protocol: TransportProtocol,
        address: SocketAddr,
    },
}
