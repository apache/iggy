use std::{net::SocketAddr, sync::Arc};

use arcshift::ArcShift;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, Permissions, UserStatus,
};
use slab::Slab;

use crate::{
    shard::namespace::IggyNamespace,
    streaming::{
        clients::client_manager::Transport,
        personal_access_tokens::personal_access_token::PersonalAccessToken,
        polling_consumer::PollingConsumer,
        stats::stats::{StreamStats, TopicStats},
        topics::consumer_group2::Member,
    },
};

#[derive(Debug)]
pub enum ShardEvent {
    CreatedShardTableRecords {
        stream_id: u32,
        topic_id: u32,
        partition_ids: Vec<u32>,
    },
    DeletedShardTableRecords {
        namespaces: Vec<IggyNamespace>,
    },
    CreatedStream2 {
        id: usize,
        name: String,
        stats: Arc<StreamStats>,
    },
    CreatedStream {
        stream_id: Option<u32>,
        name: String,
    },
    DeletedStream {
        stream_id: Identifier,
    },
    UpdatedStream {
        stream_id: Identifier,
        name: String,
    },
    PurgedStream {
        stream_id: Identifier,
    },
    CreatedPartitions2 {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
    },
    CreatedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
    },
    DeletedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_ids: Vec<u32>,
    },
    CreatedTopic {
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    },
    CreatedTopic2 {
        id: usize,
        stream_id: Identifier,
        name: String,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
        stats: Arc<TopicStats>,
    },
    CreatedConsumerGroup {
        stream_id: Identifier,
        topic_id: Identifier,
        consumer_group_id: Option<u32>,
        name: String,
    },
    CreatedConsumerGroup2 {
        cg_id: usize,
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        members: ArcShift<Slab<Member>>,
    },
    DeletedConsumerGroup {
        stream_id: Identifier,
        topic_id: Identifier,
        consumer_group_id: Identifier,
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
        stream_id: Identifier,
        topic_id: Identifier,
    },
    DeletedTopic2 {
        id: usize,
        stream_id: Identifier,
        topic_id: Identifier,
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
