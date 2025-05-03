/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::consumer::Consumer;
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::permissions::Permissions;
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::snapshot::Snapshot;
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::models::user_status::UserStatus;
use crate::prelude::*;
use crate::snapshot::{SnapshotCompression, SystemSnapshotType};
use crate::tcp::config::{TcpClientConfig, TcpClientReconnectionConfig};
use crate::utils::duration::IggyDuration;
use crate::utils::expiry::IggyExpiry;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use crate::utils::topic_size::MaxTopicSize;
use async_broadcast::Receiver;
use async_trait::async_trait;
use std::fmt::Debug;
use std::str::FromStr;

const CONNECTION_STRING_PREFIX: &str = "iggy://";

#[derive(Debug)]
pub(crate) struct ConnectionString {
    server_address: String,
    auto_login: AutoLogin,
    options: ConnectionStringOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AutoLogin {
    Disabled,
    Enabled(Credentials),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Credentials {
    UsernamePassword(String, String),
    PersonalAccessToken(String),
}

/// The client trait which is the main interface to the Iggy server.
/// It consists of multiple modules, each of which is responsible for a specific set of commands.
/// Except the ping, login and get me, all the other methods require authentication.
#[async_trait]
pub trait Client:
    SystemClient
    + UserClient
    + PersonalAccessTokenClient
    + StreamClient
    + TopicClient
    + PartitionClient
    + SegmentClient
    + MessageClient
    + ConsumerOffsetClient
    + ConsumerGroupClient
    + Sync
    + Send
    + Debug
{
    /// Connect to the server. Depending on the selected transport and provided configuration it might also perform authentication, retry logic etc.
    /// If the client is already connected, it will do nothing.
    async fn connect(&self) -> Result<(), IggyError>;

    /// Disconnect from the server. If the client is not connected, it will do nothing.
    async fn disconnect(&self) -> Result<(), IggyError>;

    // Shutdown the client and release all the resources.
    async fn shutdown(&self) -> Result<(), IggyError>;

    /// Subscribe to diagnostic events.
    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent>;
}

/// This trait defines the methods to interact with the system module.
#[async_trait]
pub trait SystemClient {
    /// Get the stats of the system such as PID, memory usage, streams count etc.
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_stats(&self) -> Result<Stats, IggyError>;
    /// Get the info about the currently connected client (not to be confused with the user).
    ///
    /// Authentication is required.
    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError>;
    /// Get the info about a specific client by unique ID (not to be confused with the user).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError>;
    /// Get the info about all the currently connected clients (not to be confused with the users).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError>;
    /// Ping the server to check if it's alive.
    async fn ping(&self) -> Result<(), IggyError>;
    async fn heartbeat_interval(&self) -> IggyDuration;
    /// Capture and package the current system state as a snapshot.
    ///
    /// Authentication is required.
    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError>;
}

/// This trait defines the methods to interact with the user module.
#[async_trait]
pub trait UserClient {
    /// Get the info about a specific user by unique ID or username.
    ///
    /// Authentication is required, and the permission to read the users, unless the provided user ID is the same as the authenticated user.
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError>;
    /// Get the info about all the users.
    ///
    /// Authentication is required, and the permission to read the users.
    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError>;
    /// Create a new user.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, IggyError>;
    /// Delete a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError>;
    /// Update a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError>;
    /// Update the permissions of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError>;
    /// Change the password of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users, unless the provided user ID is the same as the authenticated user.
    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError>;
    /// Login a user by username and password.
    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError>;
    /// Logout the currently authenticated user.
    async fn logout_user(&self) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the personal access token module.
#[async_trait]
pub trait PersonalAccessTokenClient {
    /// Get the info about all the personal access tokens of the currently authenticated user.
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError>;
    /// Create a new personal access token for the currently authenticated user.
    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError>;
    /// Delete a personal access token of the currently authenticated user by unique token name.
    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError>;
    /// Login the user with the provided personal access token.
    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError>;
}

/// This trait defines the methods to interact with the stream module.
#[async_trait]
pub trait StreamClient {
    /// Get the info about a specific stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError>;
    /// Get the info about all the streams.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError>;
    /// Create a new stream.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, IggyError>;
    /// Update a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError>;
    /// Delete a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
    /// Purge a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the topic module.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait TopicClient {
    /// Get the info about a specific topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError>;
    /// Get the info about all the topics.
    ///
    /// Authentication is required, and the permission to read the topics.
    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError>;
    /// Create a new topic.
    ///
    /// Authentication is required, and the permission to manage the topics.
    #[allow(clippy::too_many_arguments)]
    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError>;
    /// Update a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError>;
    /// Delete a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Purge a topic by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the topics.
    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the partition module.
#[async_trait]
pub trait PartitionClient {
    /// Create new N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 3 partitions, if you create 2 partitions, the topic will have 5 partitions (from 1 to 5).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError>;
    /// Delete last N partitions for a topic by unique ID or name.
    ///
    /// For example, given a topic with 5 partitions, if you delete 2 partitions, the topic will have 3 partitions left (from 1 to 3).
    ///
    /// Authentication is required, and the permission to manage the partitions.
    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the partition module.
#[async_trait]
pub trait SegmentClient {
    /// Delete last N segments for a partition by unique ID or name.
    ///
    /// For example, given a partition with 5 segments, if you delete 2 segments, the topic will have 3 segments left (from 1 to 3).
    ///
    /// Authentication is required, and the permission to manage the segments.
    async fn delete_segments(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the messaging module.
#[async_trait]
pub trait MessageClient {
    /// Poll given amount of messages using the specified consumer and strategy from the specified stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    #[allow(clippy::too_many_arguments)]
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError>;

    /// Send messages using specified partitioning strategy to the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to send the messages.
    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError>;

    /// Force flush of the `unsaved_messages` buffer to disk, optionally fsyncing the data.
    #[allow(clippy::too_many_arguments)]
    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the consumer offset module.
#[async_trait]
pub trait ConsumerOffsetClient {
    /// Store the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError>;
    /// Get the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError>;
    /// Delete the consumer offset for a specific consumer or consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to poll the messages.
    async fn delete_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(), IggyError>;
}

/// This trait defines the methods to interact with the consumer group module.
#[async_trait]
pub trait ConsumerGroupClient {
    /// Get the info about a specific consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError>;
    /// Get the info about all the consumer groups for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError>;
    /// Create a new consumer group for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, IggyError>;
    /// Delete a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to manage the streams or topics.
    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Join a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
    /// Leave a consumer group by unique ID or name for the given stream and topic by unique IDs or names.
    ///
    /// Authentication is required, and the permission to read the streams or topics.
    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError>;
}

impl FromStr for ConnectionString {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ConnectionString::new(s)
    }
}

impl ConnectionString {
    pub fn new(connection_string: &str) -> Result<Self, IggyError> {
        if connection_string.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if !connection_string.starts_with(CONNECTION_STRING_PREFIX) {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string = connection_string.replace(CONNECTION_STRING_PREFIX, "");
        let parts = connection_string.split('@').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let credentials = parts[0].split(':').collect::<Vec<&str>>();
        if credentials.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let username = credentials[0];
        let password = credentials[1];
        if username.is_empty() || password.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_and_options = parts[1].split('?').collect::<Vec<&str>>();
        if server_and_options.len() > 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_address = server_and_options[0];
        if server_address.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if !server_address.contains(':') {
            return Err(IggyError::InvalidConnectionString);
        }

        let port = server_address.split(':').collect::<Vec<&str>>()[1];
        if port.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if port.parse::<u16>().is_err() {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string_options;
        if let Some(options) = server_and_options.get(1) {
            connection_string_options = ConnectionString::parse_options(options)?;
        } else {
            connection_string_options = ConnectionStringOptions::default();
        }

        Ok(ConnectionString {
            server_address: server_address.to_owned(),
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_owned(),
                password.to_owned(),
            )),
            options: connection_string_options,
        })
    }

    fn parse_options(options: &str) -> Result<ConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut tls_enabled = false;
        let mut tls_domain = "localhost".to_string();
        let mut tls_ca_file = None;
        let mut reconnection_retries = "unlimited".to_owned();
        let mut reconnection_interval = "1s".to_owned();
        let mut reestablish_after = "5s".to_owned();
        let mut heartbeat_interval = "5s".to_owned();
        let mut nodelay = false;

        for option in options {
            let option_parts = option.split('=').collect::<Vec<&str>>();
            if option_parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }
            match option_parts[0] {
                "tls" => {
                    tls_enabled = option_parts[1] == "true";
                }
                "tls_domain" => {
                    tls_domain = option_parts[1].to_string();
                }
                "tls_ca_file" => {
                    tls_ca_file = Some(option_parts[1].to_string());
                }
                "reconnection_retries" => {
                    reconnection_retries = option_parts[1].to_string();
                }
                "reconnection_interval" => {
                    reconnection_interval = option_parts[1].to_string();
                }
                "reestablish_after" => {
                    reestablish_after = option_parts[1].to_string();
                }
                "heartbeat_interval" => {
                    heartbeat_interval = option_parts[1].to_string();
                }
                "nodelay" => {
                    nodelay = option_parts[1] == "true";
                }
                _ => {
                    return Err(IggyError::InvalidConnectionString);
                }
            }
        }
        Ok(ConnectionStringOptions {
            tls_enabled,
            tls_domain,
            tls_ca_file,
            heartbeat_interval: IggyDuration::from_str(heartbeat_interval.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
            reconnection: TcpClientReconnectionConfig {
                enabled: true,
                max_retries: match reconnection_retries.as_str() {
                    "unlimited" => None,
                    _ => Some(
                        reconnection_retries
                            .parse()
                            .map_err(|_| IggyError::InvalidNumberValue)?,
                    ),
                },
                interval: IggyDuration::from_str(reconnection_interval.as_str())
                    .map_err(|_| IggyError::InvalidConnectionString)?,
                reestablish_after: IggyDuration::from_str(reestablish_after.as_str())
                    .map_err(|_| IggyError::InvalidConnectionString)?,
            },
            nodelay,
        })
    }
}

#[derive(Debug)]
struct ConnectionStringOptions {
    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    reconnection: TcpClientReconnectionConfig,
    heartbeat_interval: IggyDuration,
    nodelay: bool,
}

impl Default for ConnectionStringOptions {
    fn default() -> Self {
        ConnectionStringOptions {
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            reconnection: Default::default(),
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            nodelay: false,
        }
    }
}

impl From<ConnectionString> for TcpClientConfig {
    fn from(connection_string: ConnectionString) -> Self {
        TcpClientConfig {
            server_address: connection_string.server_address,
            auto_login: connection_string.auto_login,
            tls_enabled: connection_string.options.tls_enabled,
            tls_domain: connection_string.options.tls_domain,
            tls_ca_file: connection_string.options.tls_ca_file,
            reconnection: connection_string.options.reconnection,
            heartbeat_interval: connection_string.options.heartbeat_interval,
            nodelay: connection_string.options.nodelay,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_without_username_should_fail() {
        let server_address = "localhost:1234";
        let value = format!("{CONNECTION_STRING_PREFIX}:secret@{server_address}");
        let connection_string: Result<ConnectionString, IggyError> = ConnectionString::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn connection_string_without_password_should_fail() {
        let server_address = "localhost:1234";
        let value = format!("{CONNECTION_STRING_PREFIX}user1@{server_address}");
        let connection_string = ConnectionString::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn connection_string_without_server_address_should_fail() {
        let value = format!("{CONNECTION_STRING_PREFIX}user:secret");
        let connection_string = ConnectionString::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn connection_string_without_port_should_fail() {
        let value = format!("{CONNECTION_STRING_PREFIX}user:secret@localhost");
        let connection_string = ConnectionString::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn connection_string_without_options_should_be_parsed_correctly() {
        let username = "user1";
        let password = "secret";
        let server_address = "localhost:1234";
        let value = format!("{CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}");
        let connection_string = ConnectionString::new(&value);
        assert!(connection_string.is_ok());
        let connection_string = connection_string.unwrap();
        assert_eq!(connection_string.server_address, server_address);
        assert_eq!(
            connection_string.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );
        assert!(!connection_string.options.tls_enabled);
        assert!(connection_string.options.tls_domain.is_empty());
        assert!(connection_string.options.tls_ca_file.is_none());
        assert!(connection_string.options.reconnection.enabled);
        assert!(connection_string.options.reconnection.max_retries.is_none());
        assert_eq!(
            connection_string.options.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert!(!connection_string.options.nodelay);
    }

    #[test]
    fn connection_string_with_options_should_be_parsed_correctly() {
        let username = "user1";
        let password = "secret";
        let server_address = "localhost:1234";
        let tls = true;
        let tls_domain = "test.com";
        let tls_ca_file = "ca.pem";
        let reconnection_retries = 5;
        let reconnection_interval = "5s";
        let reestablish_after = "10s";
        let heartbeat_interval = "3s";
        let nodelay = true;
        let value = format!("{CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}?tls={tls}&tls_domain={tls_domain}&tls_ca_file={tls_ca_file}&reconnection_retries={reconnection_retries}&reconnection_interval={reconnection_interval}&reestablish_after={reestablish_after}&heartbeat_interval={heartbeat_interval}&nodelay={nodelay}");
        let connection_string = ConnectionString::new(&value);
        assert!(connection_string.is_ok());
        let connection_string = connection_string.unwrap();
        assert_eq!(connection_string.server_address, server_address);
        assert_eq!(
            connection_string.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );
        assert!(connection_string.options.tls_enabled);
        assert_eq!(connection_string.options.tls_enabled, tls);
        assert_eq!(connection_string.options.tls_domain, tls_domain);
        assert_eq!(
            connection_string.options.tls_ca_file,
            Some(tls_ca_file.to_owned())
        );
        assert!(connection_string.options.reconnection.enabled);
        assert_eq!(
            connection_string.options.reconnection.max_retries,
            Some(reconnection_retries)
        );
        assert_eq!(
            connection_string.options.reconnection.interval,
            IggyDuration::from_str(reconnection_interval).unwrap()
        );
        assert_eq!(
            connection_string.options.reconnection.reestablish_after,
            IggyDuration::from_str(reestablish_after).unwrap()
        );
        assert_eq!(
            connection_string.options.heartbeat_interval,
            IggyDuration::from_str(heartbeat_interval).unwrap()
        );
        assert_eq!(connection_string.options.nodelay, nodelay);
    }
}
