use std::net::SocketAddr;

use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};

use crate::streaming::clients::client_manager::Transport;

pub enum ShardEvent {
    CreatedStream {
        stream_id: Option<u32>,
        topic_id: Identifier
    },
    //DeletedStream(Identifier),
    //UpdatedStream(Identifier, String),
    //PurgedStream(Identifier),
    //CreatedPartitions(Identifier, Identifier, u32),
    //DeletedPartitions(Identifier, Identifier, u32),
    CreatedTopic(
        Identifier,
        Option<u32>,
        String,
        u32,
        IggyExpiry,
        CompressionAlgorithm,
        MaxTopicSize,
        Option<u8>,
    ),
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
        user_id: u32,
        socket_addr: SocketAddr,
        transport: Transport,
    },
}