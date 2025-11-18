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

mod certificates;
mod commands;
mod configs;
mod error;
mod traits;
mod types;
mod utils;

pub use error::client_error::ClientError;
pub use error::iggy_error::{IggyError, IggyErrorDiscriminants};
// Locking is feature gated, thus only mod level re-export.
pub mod locking;
pub use certificates::generate_self_signed_certificate;
pub use commands::consumer_groups::*;
pub use commands::consumer_offsets::*;
pub use commands::messages::*;
pub use commands::partitions::*;
pub use commands::personal_access_tokens::*;
pub use commands::segments::*;
pub use commands::streams::*;
pub use commands::system::get_cluster_metadata::*;
pub use commands::system::*;
pub use commands::topics::*;
pub use commands::users::*;
pub use configs::*;
pub use traits::bytes_serializable::BytesSerializable;
pub use traits::partitioner::Partitioner;
pub use traits::sizeable::Sizeable;
pub use traits::validatable::Validatable;
pub use types::args::*;
pub use types::client::client_info::*;
pub use types::client_state::ClientState;
pub use types::cluster::*;
pub use types::command::*;
pub use types::compression::compression_algorithm::*;
pub use types::configuration::auth_config::auto_login::*;
pub use types::configuration::auth_config::connection_string::*;
pub use types::configuration::auth_config::connection_string_options::*;
pub use types::configuration::auth_config::credentials::*;
pub use types::configuration::http_config::http_client_config::*;
pub use types::configuration::http_config::http_client_config_builder::*;
pub use types::configuration::http_config::http_connection_string_options::*;
pub use types::configuration::quic_config::quic_client_config::*;
pub use types::configuration::quic_config::quic_client_config_builder::*;
pub use types::configuration::quic_config::quic_client_reconnection_config::*;
pub use types::configuration::quic_config::quic_connection_string_options::*;
pub use types::configuration::tcp_config::tcp_client_config::*;
pub use types::configuration::tcp_config::tcp_client_config_builder::*;
pub use types::configuration::tcp_config::tcp_client_reconnection_config::*;
pub use types::configuration::tcp_config::tcp_connection_string_options::*;
pub use types::configuration::transport::TransportProtocol;
pub use types::configuration::websocket_config::websocket_client_config::*;
pub use types::configuration::websocket_config::websocket_client_config_builder::*;
pub use types::configuration::websocket_config::websocket_client_reconnection_config::*;
pub use types::configuration::websocket_config::websocket_connection_string_options::*;
pub use types::consumer::consumer_group::*;
pub use types::consumer::consumer_kind::*;
pub use types::consumer::consumer_offset_info::*;
pub use types::diagnostic::diagnostic_event::DiagnosticEvent;
pub use types::identifier::*;
pub use types::message::*;
pub use types::partition::*;
pub use types::permissions::permissions_global::*;
pub use types::permissions::personal_access_token::*;
pub use types::snapshot::*;
pub use types::stats::*;
pub use types::stream::*;
pub use types::topic::*;
pub use types::user::user_identity_info::*;
pub use types::user::user_info::*;
pub use types::user::user_status::*;
pub use utils::byte_size::IggyByteSize;
pub use utils::checksum::*;
pub use utils::crypto::*;
pub use utils::duration::{IggyDuration, SEC_IN_MICRO};
pub use utils::expiry::IggyExpiry;
pub use utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
pub use utils::text;
pub use utils::timestamp::*;
pub use utils::topic_size::MaxTopicSize;
