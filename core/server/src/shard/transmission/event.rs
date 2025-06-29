use std::net::SocketAddr;

use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};

use crate::streaming::clients::client_manager::Transport;

#[derive(Debug)]
pub enum ShardEvent {
    CreatedStream {
        stream_id: Option<u32>,
        name: String,
    },
    //DeletedStream(Identifier),
    //UpdatedStream(Identifier, String),
    //PurgedStream(Identifier),
    //CreatedPartitions(Identifier, Identifier, u32),
    //DeletedPartitions(Identifier, Identifier, u32),
    CreatedTopic {
        stream_id: Identifier,
        topic_id: Option<u32>,
        name: String,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    },
    //CreatedConsumerGroup(Identifier, Identifier, Option<u32>, String),
    //DeletedConsumerGroup(Identifier, Identifier, Identifier),
    /*
    UpdatedTopic(
        Identifier,
        Identifier,
        String,
        IggyExpiry,
        CompressionAlgorithm,
        MaxTopicSize,
        Option<u8>,
    ),
    */
    //PurgedTopic(Identifier, Identifier),
    //DeletedTopic(Identifier, Identifier),
    //CreatedUser(String, String, UserStatus, Option<Permissions>),
    //DeletedUser(Identifier),
    LoginUser {
        client_id: u32,
        username: String,
        password: String,
    },
    //LogoutUser,
    //UpdatedUser(Identifier, Option<String>, Option<UserStatus>),
    //ChangedPassword(Identifier, String, String),
    //CreatedPersonalAccessToken(String, IggyExpiry),
    //DeletedPersonalAccessToken(String),
    //LoginWithPersonalAccessToken(String),
    //StoredConsumerOffset(Identifier, Identifier, PollingConsumer, u64),
    NewSession {
        address: SocketAddr,
        transport: Transport,
    },
}
