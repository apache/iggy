use crate::{
    shard::namespace::IggyNamespace,
    streaming::{
        clients::client_manager::Transport,
        partitions::partition2,
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        polling_consumer::PollingConsumer,
        streams::stream2,
        topics::{
            consumer_group2::{self},
            topic2,
        },
    },
};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, Permissions, UserStatus,
};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum ShardEvent {
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
        partition_ids: Vec<u32>,
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
    StoredOffset {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        consumer: PollingConsumer,
        offset: u64,
    },
    DeletedOffset {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        consumer: PollingConsumer,
    },
    CreatedUser {
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
        token: String,
    },
    NewSession {
        address: SocketAddr,
        transport: Transport,
    },
    TcpBound {
        address: SocketAddr,
    },
}
