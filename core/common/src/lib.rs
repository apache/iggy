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

mod alloc;
mod certificates;
mod commands;
mod configs;
mod error;
mod sender;
mod traits;
mod types;
mod utils;

pub use error::{
    client_error::ClientError,
    iggy_error::{IggyError, IggyErrorDiscriminants},
};
// Locking is feature gated, thus only mod level re-export.
pub mod locking;
pub use alloc::{
    buffer::PooledBuffer,
    memory_pool::{MEMORY_POOL, MemoryPool, MemoryPoolConfigOther, memory_pool},
};

pub use certificates::generate_self_signed_certificate;
pub use commands::{
    consumer_groups::*,
    consumer_offsets::*,
    messages::*,
    partitions::*,
    personal_access_tokens::*,
    segments::*,
    streams::*,
    system::{get_cluster_metadata::*, *},
    topics::*,
    users::*,
};
pub use configs::*;
pub use sender::{
    QuicSender, Sender, SenderKind, TcpSender, TcpTlsSender, WebSocketSender, WebSocketTlsSender,
};
pub use traits::{
    bytes_serializable::BytesSerializable, partitioner::Partitioner, sizeable::Sizeable,
    validatable::Validatable,
};
pub use types::{
    args::*,
    client::client_info::*,
    client_state::ClientState,
    cluster::*,
    command::*,
    compression::compression_algorithm::*,
    configuration::{
        auth_config::{
            auto_login::*, connection_string::*, connection_string_options::*, credentials::*,
        },
        http_config::{
            http_client_config::*, http_client_config_builder::*, http_connection_string_options::*,
        },
        quic_config::{
            quic_client_config::*, quic_client_config_builder::*,
            quic_client_reconnection_config::*, quic_connection_string_options::*,
        },
        tcp_config::{
            tcp_client_config::*, tcp_client_config_builder::*, tcp_client_reconnection_config::*,
            tcp_connection_string_options::*,
        },
        transport::TransportProtocol,
        websocket_config::{
            websocket_client_config::*, websocket_client_config_builder::*,
            websocket_client_reconnection_config::*, websocket_connection_string_options::*,
        },
    },
    consensus::*,
    consumer::{consumer_group::*, consumer_kind::*, consumer_offset_info::*},
    diagnostic::diagnostic_event::DiagnosticEvent,
    identifier::*,
    message::*,
    partition::*,
    permissions::{permissions_global::*, personal_access_token::*},
    snapshot::*,
    stats::*,
    stream::*,
    topic::*,
    user::{user_identity_info::*, user_info::*, user_status::*},
};
pub use utils::{
    byte_size::IggyByteSize,
    checksum::*,
    crypto::*,
    duration::{IggyDuration, SEC_IN_MICRO},
    expiry::IggyExpiry,
    personal_access_token_expiry::PersonalAccessTokenExpiry,
    text,
    timestamp::*,
    topic_size::MaxTopicSize,
};
