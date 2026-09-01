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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::client_wrappers::connection_info::ConnectionInfo;
use crate::clients::client_builder::IggyClientBuilder;
use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::EncryptorKind;
use crate::prelude::IggyConsumerBuilder;
use crate::prelude::IggyError;
use crate::prelude::IggyProducerBuilder;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use crate::websocket::websocket_client::WebSocketClient;
use async_broadcast::Receiver;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGIN_USER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGOUT_USER_CODE,
};
use iggy_common::Consumer;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{BinaryTransport, Client, HttpMethod, SystemClient};
use iggy_common::{ConnectionStringUtils, DiagnosticEvent, Partitioner, TransportProtocol};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::log::warn;
use tracing::{debug, error, info};

/// Auth/session codes rejected by the raw binary path. Must go through the
/// typed `login_user` / `logout_user` methods to keep session state correct.
const SESSION_CONTROL_CODES: [u32; 5] = [
    LOGIN_USER_CODE,
    LOGOUT_USER_CODE,
    LOGIN_REGISTER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
    LOGIN_REGISTER_WITH_PAT_CODE,
];

/// A high-level, transport-agnostic client for an Iggy server.
///
/// `IggyClient` wraps a transport-specific low-level **client** ([`ClientWrapper`]) and
/// **provides access to the full server API**.
/// Iggy comes with four options for client-server communication: TCP, QUIC, WebSocket and HTTP.
/// The `IggyClient` is configured with one of these transport modes, hence abstracting
/// transport specific implementations away.
///
/// The [`ClientWrapper`] lives behind an [`IggyRwLock`] so that the connection
/// can be shared safely. You create a single client and use it from many tasks
/// at once (e.g. producers, consumers). Every operation takes a shared read
/// guard.
///
/// A [`Partitioner`] and a client-side [`EncryptorKind`] are optional, and both
/// default to disabled. The [`Partitioner`] computes on the client-side the target
/// partition for messages published without an explicit partition. Hence, you can
/// configure routing to partitions within a topic yourself dependent on the stream,
/// topic, and/ or message contents.
///
/// The [`EncryptorKind`] encrypts each message payload before it leaves the client and decrypts it on
/// the way back, keeping payloads opaque to the server. Attach either through
/// [`create()`].
///
/// # What you can do
///
/// Configure a connection with an Iggy server and interact with it.
/// The `IggyClient` provides various methods to setup the connection using connection strings,
/// builder patterns or an already existing [`ClientWrapper`].
/// You can spawn [`IggyConsumer`]s and [`IggyProducer`]s that share that connection.
///
/// The full server API is split into domain-specific traits.
/// `IggyClient` implements [`Client`], the supertrait, which pulls every domain-specific trait.
/// Bring the one you need into scope to call its methods.
/// `use iggy::prelude::*` brings all of them in at once.
///
/// - [`SystemClient`]: ping, server statistics, snapshots, and connected-client info.
/// - [`UserClient`]: create, inspect, update, and delete users and their permissions.
/// - [`PersonalAccessTokenClient`]: create, list, and delete personal access tokens, log in with one.
/// - [`StreamClient`]: create, get, update, delete, and purge streams.
/// - [`TopicClient`]: create, get, update, delete, and purge topics within a stream.
/// - [`PartitionClient`]: add and remove partitions on a topic.
/// - [`SegmentClient`]: delete closed segments from a partition.
/// - [`ConsumerGroupClient`]: create, get, delete, and join or leave consumer groups.
/// - [`ConsumerOffsetClient`]: store, read, and delete consumer offsets.
/// - [`MessageClient`]: send and poll messages, and flush the unsaved buffer.
///
/// Additionally, you can bypass invoking methods from these traits and directly talk to the server with [`send_binary_request`] and [`send_http_request`] for http.
/// Both trade typed API's safety for low-level control. You need to know the server codes and the wire format.
///
/// # Usage
///
/// The typical lifecycle of an `IggyClient` is construct, [`connect()`], use, and finally shutdown.
///
/// 1. Construct a client from a connection string ([`from_connection_string()`]),
///    from the [`builder()`], or by wrapping an existing transport client with
///    [`new()`] / [`create()`].
/// 2. Call [`connect()`] to establish the transport-level connection. If the transport was
///    configured with auto-login, this also authenticates. Otherwise call
///    [`login_user()`] afterwards. For HTTP always call [`login_user()`] instead of [`connect()`].
/// 3. Spawn [`IggyConsumer`]s and [`IggyProducer`]s to write to, and consume messages
///    from, the server.
/// 4. To shut everything down, call [`IggyConsumer::shutdown()`] on each consumer to store their
///    final offset and leave consumer groups. Then, call [`IggyProducer::shutdown()`] on each
///    [`IggyProducer`] so that _background_ producers flush the latest state. Finally,
///    call [`shutdown()`] on the [`IggyClient`] which closes the connection.
///    Use [`disconnect()`] rather than [`shutdown()`] to close the connection but keep the client usable, as a
///    client that has been shut down cannot reconnect. Note, if `auto-login` is configured, the client
///    will reconnect automatically and undo the disconnect.
///
/// # Examples
///
/// Build a client from a connection string, connect, publish through a
/// background batching producer, consume with a standalone consumer, and shut down cleanly.
///
/// ```no_run
/// use iggy::prelude::*;
/// use futures_util::StreamExt;
/// use std::str::FromStr;
///
/// # async fn run() -> Result<(), IggyError> {
/// // Auto-logs in from the credentials in the string and retries forever on disconnect.
/// let client = IggyClient::builder_from_connection_string(
///     "iggy+tcp://user:secret@localhost:8090\
///      ?reconnection_retries=unlimited&reconnection_interval=1s&heartbeat_interval=5s&nodelay=true",
/// )?
/// .build()?;
/// client.connect().await?;
///
/// // A background producer batches in the background, retries failed sends,
/// // and creates a topic.
/// let producer = client
///     .producer("stream_name", "topic_name")?
///     .background(
///         BackgroundConfig::builder()
///             .batch_length(1000)
///             .linger_time(IggyDuration::ONE_SECOND)
///             .build(),
///     )
///     .partitioning(Partitioning::balanced())
///     .send_retries(Some(3), Some(NonZeroIggyDuration::ONE_SECOND))
///     .create_topic_if_not_exists(
///         3,
///         IggyExpiry::ServerDefault,
///         MaxTopicSize::ServerDefault,
///     )
///     .build();
/// producer.init().await?;
/// producer
///     .send(vec![IggyMessage::from_str("our-first-message")?])
///     .await?;
///
/// // A consumer pinned to one partition, committing its offset
/// // automatically when messages are polled (not consumed!).
/// let mut consumer = client
///     .consumer("consumer_name", "stream_name", "topic_name", 1)?
///     .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
///     .polling_strategy(PollingStrategy::next())
///     .poll_interval(IggyDuration::ONE_SECOND)
///     .batch_length(1000)
///     .build();
/// consumer.init().await?;
///
/// while let Some(message) = consumer.next().await {
///     let message = message?;
///     // Handle `message.message.payload` here however required
///     break;
/// }
///
/// // Finish commit before stopping (comp. method docs).
/// consumer.shutdown().await?;
/// // Finish flush before stopping (comp. method docs).
/// producer.shutdown().await;
///
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// [`IggyConsumer`]: crate::prelude::IggyConsumer
/// [`IggyProducer`]: crate::prelude::IggyProducer
/// [`IggyConsumer::shutdown()`]: crate::prelude::IggyConsumer::shutdown
/// [`IggyProducer::shutdown()`]: crate::prelude::IggyProducer::shutdown
/// [`new()`]: IggyClient::new
/// [`create()`]: IggyClient::create
/// [`send_binary_request`]: IggyClient::send_binary_request
/// [`send_http_request`]: IggyClient::send_http_request
/// [`shutdown()`]: IggyClient::shutdown
/// [`login_user()`]: crate::prelude::UserClient::login_user
/// [`connect()`]: IggyClient::connect
/// [`disconnect()`]: IggyClient::disconnect
/// [`producer()`]: IggyClient::producer
/// [`consumer()`]: IggyClient::consumer
/// [`builder()`]: IggyClient::builder
/// [`from_connection_string()`]: IggyClient::from_connection_string
/// [`consumer_group()`]: IggyClient::consumer_group
/// [`Client`]: crate::prelude::Client
/// [`SystemClient`]: crate::prelude::SystemClient
/// [`UserClient`]: crate::prelude::UserClient
/// [`PersonalAccessTokenClient`]: crate::prelude::PersonalAccessTokenClient
/// [`StreamClient`]: crate::prelude::StreamClient
/// [`TopicClient`]: crate::prelude::TopicClient
/// [`PartitionClient`]: crate::prelude::PartitionClient
/// [`SegmentClient`]: crate::prelude::SegmentClient
/// [`MessageClient`]: crate::prelude::MessageClient
/// [`ConsumerOffsetClient`]: crate::prelude::ConsumerOffsetClient
/// [`ConsumerGroupClient`]: crate::prelude::ConsumerGroupClient
/// [`ClusterClient`]: crate::prelude::ClusterClient
#[derive(Debug)]
#[allow(dead_code)]
pub struct IggyClient {
    pub(crate) client: IggyRwLock<ClientWrapper>,
    partitioner: Option<Arc<dyn Partitioner>>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(ClientWrapper::Tcp(TcpClient::default()))
    }
}

impl IggyClient {
    /// Returns an empty [`IggyClientBuilder`].
    ///
    /// The returned builder is not ready to be [`IggyClientBuilder::build()`].
    /// It sill needs to configure a mode of transport.
    pub fn builder() -> IggyClientBuilder {
        IggyClientBuilder::new()
    }

    /// Creates an [`IggyClientBuilder`] with the transport preconfigured from a
    /// connection string.
    ///
    /// The transport is selected from the scheme:
    /// - `iggy://` defaults to TCP.
    /// - `iggy+tcp://` for TCP.
    /// - `iggy+quic://` for QUIC.
    /// - `iggy+http://` for HTTP.
    /// - `iggy+ws://` for WebSocket.
    ///
    /// Authentication at the server is mandatory.
    /// - user + password: `<user>:<password>@<host>:<port>`
    /// - personal access token: `<personal_access_token>@host:port`
    ///
    /// Optional `?key=value&key=value` queries carry transport specific
    /// configuration. The query is parsed per transport, so the accepted keys differ
    /// by scheme. An unknown key is rejected.
    /// If no queries are provided optional configurations are automatically set to their default values.
    ///
    /// # Examples
    ///
    /// Each example lists configuration options per transport mode and shows
    /// one concrete example.
    ///
    /// If the value of an option is
    /// - a duration pass a `humantime` string such as `5s`, `500ms`, or `1h 1m 1s`.
    ///   These are cast into an [`IggyDuration`]/[`NonZeroIggyDuration`].
    ///   For [`IggyDuration`], `unlimited`, `none`, `disabled`, and `0` parse to zero.
    /// - a retry count use either the literal `unlimited` or a number such as `5`.
    /// - a bool use the literal `true` or `false`.
    /// - bytes and millisecond options provide a number such as `1024`.
    ///
    /// ## TCP
    ///
    /// The same options apply for `iggy://` and `iggy+tcp://`.
    ///
    /// - `tls`: bool. Enable/disable TLS. Default: `false`.
    /// - `tls_domain`: string. Server name to validate the certificate against. Default: unset.
    /// - `tls_ca_file`: filesystem path. Extra CA certificate to trust. Default: unset.
    /// - `reconnection_retries`: "unlimited" or u32. Number of attempts to connect. Default: `unlimited`.
    /// - `reconnection_interval`: [`NonZeroIggyDuration`]. Wait between reconnection attempts. Default: `1s`.
    /// - `reestablish_after`: [`IggyDuration`]. Grace period before reconnecting. Default: `5s`.
    /// - `heartbeat_interval`: [`NonZeroIggyDuration`]. Client heartbeat period. Default: `5s`.
    /// - `nodelay`: `bool`. Disable Nagle's algorithm (`TCP_NODELAY`). Default: `false`.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::builder_from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090\
    ///      ?tls=true&tls_domain=localhost&tls_ca_file=/etc/iggy/ca.pem\
    ///      &reconnection_retries=unlimited&reconnection_interval=1s&reestablish_after=5s\
    ///      &heartbeat_interval=5s&nodelay=true",
    /// )?
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## QUIC
    ///
    /// - `validate_certificate`: bool. Verify the server certificate. Default: `false`.
    /// - `heartbeat_interval`: [`NonZeroIggyDuration`]. Client heartbeat period. Default: `5s`.
    /// - `reconnection_max_retries`: "unlimited" or u32. Number of attempts to connect. Default: `unlimited`.
    /// - `reconnection_interval`: [`NonZeroIggyDuration`]. Wait between reconnection attempts. Default: `1s`.
    /// - `reconnection_reestablish_after`: [`IggyDuration`]. Grace period before reconnecting. Default: `5s`.
    /// - `response_buffer_size`: u64. Number of bytes in the response receive buffer. Default: `10000000`.
    /// - `max_concurrent_bidi_streams`: u64. Number of concurrent bidirectional streams. Default: `10000`.
    /// - `datagram_send_buffer_size`: u64. Number of bytes in the datagram send buffer. Default: `100000`.
    /// - `initial_mtu`: u16. Initial MTU estimate (in bytes). Default: `1200`.
    /// - `send_window`: u64. Number of bytes bytes of the flow-control send window. Default: `100000`.
    /// - `receive_window`: u64. Number of bytes of the flow-control receive window. Default: `100000`.
    /// - `keep_alive_interval`: u64. QUIC keep-alive period (in milliseconds). Default: `5000`.
    /// - `max_idle_timeout`: u64. Close after this much idle time (in middleseconds). Default: `10000`.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::builder_from_connection_string(
    ///     "iggy+quic://user:secret@localhost:8080\
    ///      ?validate_certificate=true&heartbeat_interval=5s\
    ///      &reconnection_max_retries=unlimited&reconnection_interval=1s&reconnection_reestablish_after=5s\
    ///      &response_buffer_size=10000000&max_concurrent_bidi_streams=10000\
    ///      &datagram_send_buffer_size=100000&initial_mtu=1200\
    ///      &send_window=100000&receive_window=100000\
    ///      &keep_alive_interval=5000&max_idle_timeout=10000",
    /// )?
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## HTTP
    ///
    /// A REST transport. Iggy uses the `reqwest` crate to manage the HTTP client.
    /// Hence, transport specific configuration is abstracted away.
    ///
    /// Configurable options are:
    /// - `heartbeat_interval`: [`IggyDuration`]. Client heartbeat period. Default: `5s`.
    /// - `retries`: u32. Number of retries when sending a request. Default: `3`.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::builder_from_connection_string(
    ///     "iggy+http://localhost:3000?heartbeat_interval=5s&retries=3",
    /// )?
    /// .build()?;
    /// client.login_user("user", "password").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## WebSocket
    ///
    /// - `heartbeat_interval`: [`NonZeroIggyDuration`]. Client heartbeat period. Default: `5s`.
    /// - `reconnection_retries`: "unlimited" or u32. Number of attempts to connect. Default: `unlimited`.
    /// - `reconnection_interval`: [`NonZeroIggyDuration`]. Wait between reconnection attempts. Default: `1s`.
    /// - `reestablish_after`: [`IggyDuration`]. Grace period before reconnecting. Default: `5s`.
    /// - `read_buffer_size`: usize. Size of the read buffer in bytes. Default: `131072`.
    /// - `write_buffer_size`: usize. Size of the write buffer in bytes. Default: `131072`.
    /// - `max_write_buffer_size`: usize. Maximum size of the write buffer in bytes. Default: `usize::MAX`.
    /// - `max_message_size`: usize. Maximum accepted message size in bytes. Default: `67108864`.
    /// - `max_frame_size`: usize. Maximum accepted frame size in bytes. Default: `16777216`.
    /// - `accept_unmasked_frames`: bool. Accept/ decline unmasked frames. Default: `false`.
    /// - `tls`: `bool`. Enable/disbale TLS. Default: `false`.
    /// - `tls_domain`: string. Server name to validate the certificate against. Default: unset.
    /// - `tls_ca_file`: filesystem path. Extra CA certificate to trust. Default: unset.
    /// - `tls_validate_certificate`: bool. Whether to verify the server certificate. Default: `false`.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::builder_from_connection_string(
    ///     "iggy+ws://user:secret@localhost:8092\
    ///      ?heartbeat_interval=5s&reconnection_retries=unlimited&reconnection_interval=1s&reestablish_after=5s\
    ///      &read_buffer_size=131072&write_buffer_size=131072\
    ///      &max_message_size=67108864&max_frame_size=16777216&accept_unmasked_frames=false\
    ///      &tls=true&tls_domain=localhost&tls_ca_file=/etc/iggy/ca.pem&tls_validate_certificate=true",
    /// )?
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidConnectionString`] if the connection string is malformed.
    ///
    /// [`IggyDuration`]: crate::prelude::IggyDuration
    /// [`NonZeroIggyDuration`]: crate::prelude::NonZeroIggyDuration
    pub fn builder_from_connection_string(
        connection_string: &str,
    ) -> Result<IggyClientBuilder, IggyError> {
        IggyClientBuilder::from_connection_string(connection_string)
    }

    /// Creates a new `IggyClient` from an already-constructed transport client.
    ///
    /// Use this when you built the transport client yourself
    /// and want the full high-level `IggyClient` surface on top of it. To start
    /// from a connection string instead, prefer
    /// [`from_connection_string`](IggyClient::from_connection_string) or
    /// [`builder_from_connection_string`](IggyClient::builder_from_connection_string).
    /// To also attach a [`Partitioner`] or client-side [`EncryptorKind`], use
    /// [`create`].
    ///
    /// # Examples
    ///
    /// Build a [`TcpClient`] from a config, wrap it in a [`ClientWrapper`], and
    /// hand it to `IggyClient` for the full server API.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # fn run() -> Result<(), IggyError> {
    /// let config = TcpClientConfigBuilder::new()
    ///     .with_server_address("127.0.0.1:8090".to_owned())
    ///     .build()?;
    /// let tcp_client = TcpClient::create(Arc::new(config))?;
    ///
    /// let client = IggyClient::new(ClientWrapper::Tcp(tcp_client));
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`create`]: IggyClient::create
    /// [`Partitioner`]: crate::prelude::Partitioner
    /// [`EncryptorKind`]: crate::prelude::EncryptorKind
    /// [`TcpClient`]: crate::prelude::TcpClient
    /// [`ClientWrapper`]: crate::prelude::ClientWrapper
    pub fn new(client: ClientWrapper) -> Self {
        let client = IggyRwLock::new(client);
        IggyClient {
            client,
            partitioner: None,
            encryptor: None,
            heartbeat_handle: Mutex::new(None),
        }
    }

    /// Creates a new `IggyClient` directly from a connection string.
    ///
    /// This is a shortcut for [`builder_from_connection_string`] followed by
    /// [`build`](IggyClientBuilder::build) when no partitioner or encryptor is
    /// needed.
    ///
    /// Refer to [`builder_from_connection_string`] for concise examples on how
    /// to use a connection string.
    /// To also attach a [`Partitioner`] or client-side [`EncryptorKind`], use
    /// [`create`].
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidConnectionString`] if the connection string is
    /// malformed.
    ///
    /// [`builder_from_connection_string`]: IggyClient::builder_from_connection_string
    /// [`create`]: IggyClient::create
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => Ok(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Quic => Ok(IggyClient::new(ClientWrapper::Quic(
                QuicClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Http => Ok(IggyClient::new(ClientWrapper::Http(
                HttpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::WebSocket => Ok(IggyClient::new(ClientWrapper::WebSocket(
                WebSocketClient::from_connection_string(connection_string)?,
            ))),
        }
    }

    /// Creates a new `IggyClient` from a transport client, with an optional
    /// [`Partitioner`] and client-side [`EncryptorKind`].
    ///
    /// The partitioner picks the target partition for messages published without
    /// an explicit partition assigned to them. Note, that setting a [`Partitioner`] overrides a producer's
    /// [`Partitioning`](crate::prelude::Partitioning) the partition id is
    /// computed client-side and the partitioning strategy is forced to [`PartitioningKind::PartitionId`] using the computed ID.
    /// The encryptor encrypts payloads before they leave the client. Pass [`None`] for either to
    /// disable it, just as [`new`](IggyClient::new) does for both.
    ///
    /// # Examples
    ///
    /// Wrap a [`TcpClient`] together with a custom [`Partitioner`] and an
    /// AES-256-GCM payload [`EncryptorKind`].
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    /// use std::sync::Arc;
    ///
    /// // Routes every message to partition 1.
    /// #[derive(Debug)]
    /// struct FixedPartitioner;
    ///
    /// impl Partitioner for FixedPartitioner {
    ///     fn calculate_partition_id(
    ///         &self,
    ///         _stream_id: &Identifier,
    ///         _topic_id: &Identifier,
    ///         _messages: &[IggyMessage],
    ///     ) -> Result<u32, IggyError> {
    ///         Ok(1)
    ///     }
    /// }
    ///
    /// # fn run() -> Result<(), IggyError> {
    /// let tcp_client =
    ///     TcpClient::from_connection_string("iggy+tcp://user:secret@localhost:8090")?;
    ///
    /// let partitioner: Arc<dyn Partitioner> = Arc::new(FixedPartitioner);
    /// let encryptor = Arc::new(EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[0u8; 32])?));
    ///
    /// let client = IggyClient::create(
    ///     ClientWrapper::Tcp(tcp_client),
    ///     Some(partitioner),
    ///     Some(encryptor),
    /// );
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Partitioner`]: crate::prelude::Partitioner
    /// [`PartitioningKind::PartitionId`]: iggy_common::PartitioningKind::PartitionId
    /// [`EncryptorKind`]: crate::prelude::EncryptorKind
    /// [`TcpClient`]: crate::prelude::TcpClient
    /// [`ClientWrapper`]: crate::prelude::ClientWrapper
    pub fn create(
        client: ClientWrapper,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = IggyRwLock::new(client);
        IggyClient {
            client,
            partitioner,
            encryptor,
            heartbeat_handle: Mutex::new(None),
        }
    }

    /// Returns a handle to the underlying transport client.
    ///
    /// The returned [`ClientWrapper`] is behind an [`IggyRwLock`].
    /// Thus, the returned type shares ownership with this `IggyClient`, meaning
    /// changes made through either are visible to both.
    ///
    /// # Examples
    ///
    /// Take the shared handle, acquire a read guard, and reach the underlying
    /// transport directly, here to ping the server over the raw connection.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    /// use crate::iggy::prelude::locking::IggyRwLockFn;
    ///
    /// # async fn run(client: IggyClient) -> Result<(), IggyError> {
    /// let handle = client.client();
    /// handle.read().await.ping().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn client(&self) -> IggyRwLock<ClientWrapper> {
        self.client.clone()
    }

    /// Returns an [`IggyConsumerBuilder`] to build a standalone consumer.
    ///
    /// Copies the client and the encryptor registered with the [`IggyClient`]
    /// into a [`IggyConsumerBuilder`] and returns it.
    /// Sets the consumer to [`ConsumerKind::Consumer`], i.e. a single consumer,
    /// with the provided name.
    /// Registers a consumer for `stream`, `topic` and `partition`.
    ///
    /// To get builder for a load-balanced consumer group, use
    /// [`consumer_group`](IggyClient::consumer_group) instead.
    ///
    /// Refer to the [`IggyConsumer`] type and the [`IggyConsumerBuilder`]
    /// for details on how a consumer can be configured.
    ///
    /// # Examples
    ///
    /// Connect a client, build a consumer pinned to partition 1, and configure
    /// it to auto-commit when messages are polled.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090",
    /// )?;
    /// client.connect().await?;
    ///
    /// let consumer = client
    ///     .consumer("consumer_name", "stream_name", "topic_name", 1)? // returns IggyConsumerBuilder from IggyClient
    ///     .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
    ///     .polling_strategy(PollingStrategy::next())
    ///     .poll_interval(IggyDuration::ONE_SECOND)
    ///     .batch_length(1000)
    ///     .build(); // returns IggyConsumer from IggyConsumerBuilder
    /// # let _ = consumer;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidIdentifier`] if `name`, `stream`, or `topic`
    /// is not a valid identifier.
    ///
    /// [`ConsumerKind::Consumer`]: crate::prelude::ConsumerKind::Consumer
    /// [`IggyConsumer`]: crate::prelude::IggyConsumer
    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns an [`IggyConsumerBuilder`] for a member of a consumer group.
    ///
    /// Copies the client and the encryptor registered with the [`IggyClient`]
    /// into a [`IggyConsumerBuilder`] and returns it.
    /// Sets the consumer to [`ConsumerKind::ConsumerGroup`], i.e. a member of a
    /// load-balanced group. The provided name identifies the group.
    /// Registers the member for every partition of `topic` in `stream`. The
    /// group then balances those partitions across its members, so each message is
    /// delivered to exactly one member. When consumers leave or join a group,
    /// rebalancing (re-assigning consumers to partitions) might deliver messages again
    /// in cases where they were polled, but the read did not commit yet.
    /// Since the new consumer starts reading from the last commit, messages are delivered
    /// at-least-once.
    ///
    /// For a consumer pinned to a single partition, use
    /// [`consumer`](IggyClient::consumer) instead.
    ///
    /// Refer to the [`IggyConsumer`] type and the [`IggyConsumerBuilder`]
    /// for details on how a consumer can be configured.
    ///
    /// # Examples
    ///
    /// Connect a client, build a member of a consumer group, and configure it to
    /// auto-commit when polled.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090",
    /// )?;
    /// client.connect().await?;
    ///
    /// let consumer = client
    ///     .consumer_group("group_name", "stream_name", "topic_name")? // returns IggyConsumerBuilder from IggyClient
    ///     .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
    ///     .polling_strategy(PollingStrategy::next())
    ///     .poll_interval(IggyDuration::ONE_SECOND)
    ///     .batch_length(1000)
    ///     .build(); // returns IggyConsumer from IggyConsumerBuilder
    /// # let _ = consumer;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidIdentifier`] if `name`, `stream`, or `topic`
    /// is not a valid identifier.
    ///
    /// [`ConsumerKind::ConsumerGroup`]: crate::prelude::ConsumerKind::ConsumerGroup
    /// [`IggyConsumer`]: crate::prelude::IggyConsumer
    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns an [`IggyProducerBuilder`].
    ///
    /// Copies the client and the encryptor registered with the [`IggyClient`]
    /// into a [`IggyProducerBuilder`] and returns it.
    ///
    /// Binds a producer to the provided stream and topic.
    /// Refer to the [`IggyProducer`] type and the [`IggyProducerBuilder`]
    /// for details on how a producer can be configured.
    ///
    /// # Examples
    ///
    /// Connect a client, build a producer that creates the topic if it is
    /// missing and retries failed sends, then publish a message.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    /// use std::str::FromStr;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090",
    /// )?;
    /// client.connect().await?;
    ///
    /// let producer = client
    ///     .producer("stream_name", "topic_name")? // returns IggyProducerBuilder from IggyClient
    ///     .partitioning(Partitioning::balanced())
    ///     .send_retries(Some(3), Some(NonZeroIggyDuration::ONE_SECOND))
    ///     .create_topic_if_not_exists(
    ///         3,
    ///         IggyExpiry::ServerDefault,
    ///         MaxTopicSize::ServerDefault,
    ///     )
    ///     .build(); // returns IggyProducer from IggyProducerBuilder
    /// producer.init().await?;
    /// producer
    ///     .send(vec![IggyMessage::from_str("our-first-message")?])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidIdentifier`] if `stream` or `topic` is not a
    /// valid identifier.
    ///
    /// [`IggyProducer`]: crate::prelude::IggyProducer
    pub fn producer(&self, stream: &str, topic: &str) -> Result<IggyProducerBuilder, IggyError> {
        Ok(IggyProducerBuilder::new(
            self.client.clone(),
            stream.try_into()?,
            stream.to_owned(),
            topic.try_into()?,
            topic.to_owned(),
            self.encryptor.clone(),
            self.partitioner.clone(),
        ))
    }

    /// Returns the current [`ConnectionInfo`].
    ///
    /// The transport protocol and the server address the client is connected to.
    ///
    /// # Examples
    ///
    /// Connect a client and print the transport protocol and server address it
    /// is connected to.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090",
    /// )?;
    /// client.connect().await?;
    ///
    /// let info = client.get_connection_info().await;
    /// println!("connected to {} over {}", info.server_address, info.protocol);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_connection_info(&self) -> ConnectionInfo {
        self.client.read().await.get_connection_info().await
    }

    /// Sends a raw binary command (`code` plus serialized `payload`) and returns
    /// the raw response payload.
    ///
    /// Use this method for commands the typed API does not cover, for
    /// example a command you added to a forked server.
    ///
    /// `code` selects the command. Available codes are defined in
    /// [`iggy_binary_protocol::codes`]
    ///
    /// `payload` is the command body already serialized in the Iggy wire format,
    /// and the returned [`Bytes`] is the raw response body in that same format,
    /// which you need to decode yourself. The wire frame that carries both (length, code,
    /// status) is documented at the [`iggy_binary_protocol`]. You pass
    /// and receive only the payload, the transport configured with the client frames it.
    ///
    /// Only the binary transports (TCP, QUIC, WebSocket) have a raw binary path.
    /// The HTTP counterpart is
    /// [`send_http_request`](IggyClient::send_http_request).
    ///
    /// # Examples
    ///
    /// Ping the server over the raw binary path. `PING_CODE` takes an empty
    /// payload and the server replies with an empty payload.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    /// use iggy_binary_protocol::codes::PING_CODE;
    /// use bytes::Bytes;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+tcp://user:secret@localhost:8090",
    /// )?;
    /// client.connect().await?;
    ///
    /// let response = client.send_binary_request(PING_CODE, Bytes::new()).await?;
    /// assert!(response.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// [`IggyError::InvalidCommand`] if `code` is one of the session-control
    /// codes (login, logout, and register). Use the typed `login_user` /
    /// `logout_user` methods so the SDK's session state stays correct.
    /// [`IggyError::FeatureUnavailable`] on the HTTP transport, which has no
    /// binary path.
    ///
    /// [`iggy_binary_protocol`]: iggy_binary_protocol
    /// [`iggy_binary_protocol::codes`]: iggy_binary_protocol::codes
    pub async fn send_binary_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if SESSION_CONTROL_CODES.contains(&code) {
            return Err(IggyError::InvalidCommand);
        }
        match &*self.client.read().await {
            ClientWrapper::Tcp(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Quic(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::WebSocket(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => Err(IggyError::FeatureUnavailable),
        }
    }

    /// Invokes a HTTP endpoint and returns the raw response body.
    ///
    /// This is the HTTP counterpart to
    /// [`send_binary_request`](IggyClient::send_binary_request).
    ///
    /// `method` is the HTTP verb and `path` is joined onto the
    /// client's configured API URL, e.g. `/streams`.
    /// `body` (if needed) is sent as-is as the request body. The client
    /// attaches its bearer token. The returned [`Bytes`] is the raw response body
    /// for a response, which you decode yourself.
    ///
    /// # Examples
    ///
    /// Fetch the server's stats over the raw HTTP path with a `GET` and no body.
    ///
    /// ```no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn run() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string(
    ///     "iggy+http://user:secret@localhost:3000",
    /// )?;
    /// client.login_user("user", "password").await?;
    ///
    /// let response = client
    ///     .send_http_request(HttpMethod::Get, "/stats", None)
    ///     .await?;
    /// // `response` is the raw JSON body, decode it however required.
    /// # let _ = response;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`IggyError::FeatureUnavailable`] on the TCP, QUIC, and WebSocket
    /// transports, which have no HTTP path.
    pub async fn send_http_request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Bytes>,
    ) -> Result<Bytes, IggyError> {
        match &*self.client.read().await {
            ClientWrapper::Http(client) => client.send_http_request(method, path, body).await,
            ClientWrapper::Tcp(_)
            | ClientWrapper::Quic(_)
            | ClientWrapper::WebSocket(_)
            | ClientWrapper::Iggy(_) => Err(IggyError::FeatureUnavailable),
        }
    }
}

impl Drop for IggyClient {
    fn drop(&mut self) {
        let heartbeat_handle = self
            .heartbeat_handle
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&self) -> Result<(), IggyError> {
        let heartbeat_interval;
        {
            let client = self.client.read().await;
            client.connect().await?;
            heartbeat_interval = client.heartbeat_interval().await;
        }

        let mut heartbeat_handle = self
            .heartbeat_handle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if heartbeat_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(());
        }

        drop(heartbeat_handle.take());
        let client = self.client.clone();
        *heartbeat_handle = Some(spawn(async move {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(error) = client.read().await.ping().await {
                    error!("There was an error when sending a heartbeat. {error}");
                    if error == IggyError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                    // Picks up a widened assignment (e.g. partition-count
                    // change) without waiting for an ownership-fence rejection.
                    client
                        .read()
                        .await
                        .refresh_consumer_group_assignments()
                        .await;
                }
                sleep(heartbeat_interval.get_duration()).await
            }
        }));
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().await.disconnect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.client.read().await.shutdown().await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.client.read().await.subscribe_events().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = IggyClient::from_connection_string(value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_reject_http_request_on_binary_transport() {
        let client = IggyClient::default();
        let result = client
            .send_http_request(HttpMethod::Get, "/ping", None)
            .await;
        assert!(matches!(result, Err(IggyError::FeatureUnavailable)));
    }

    #[tokio::test]
    async fn should_reject_binary_request_on_http_transport() {
        let client =
            IggyClient::from_connection_string("iggy+http://user:secret@127.0.0.1:1234").unwrap();
        let result = client.send_binary_request(0, Bytes::new()).await;
        assert!(matches!(result, Err(IggyError::FeatureUnavailable)));
    }

    #[tokio::test]
    async fn should_reject_session_control_codes_on_binary_request() {
        let client = IggyClient::default();
        for code in SESSION_CONTROL_CODES {
            let result = client.send_binary_request(code, Bytes::new()).await;
            assert!(
                matches!(result, Err(IggyError::InvalidCommand)),
                "code {code} must be rejected before reaching the transport"
            );
        }
    }
}
