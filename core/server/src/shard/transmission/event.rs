use crate::streaming::{
    partitions::partition,
    personal_access_tokens::personal_access_token::PersonalAccessToken,
    streams::stream,
    topics::{
        consumer_group::{self},
        topic,
    },
};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, Permissions, TransportProtocol,
    UserStatus,
};
use std::net::SocketAddr;
use strum::Display;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Display)]
#[strum(serialize_all = "PascalCase")]
pub enum ShardEvent {
    FlushUnsavedBuffer {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        fsync: bool,
    },
    CreatedStream {
        id: usize,
        stream: stream::Stream,
    },
    DeletedStream {
        id: usize,
        stream_id: Identifier,
    },
    UpdatedStream {
        stream_id: Identifier,
        name: String,
    },
    PurgedStream {
        stream_id: Identifier,
    },
    CreatedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions: Vec<partition::Partition>,
    },
    DeletedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
        partition_ids: Vec<usize>,
    },
    CreatedTopic {
        stream_id: Identifier,
        topic: topic::Topic,
    },
    CreatedConsumerGroup {
        stream_id: Identifier,
        topic_id: Identifier,
        cg: consumer_group::ConsumerGroup,
    },
    DeletedConsumerGroup {
        id: usize,
        stream_id: Identifier,
        topic_id: Identifier,
        group_id: Identifier,
    },
    UpdatedTopic {
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
    DeletedTopic {
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
    AddressBound {
        protocol: TransportProtocol,
        address: SocketAddr,
    },
}
