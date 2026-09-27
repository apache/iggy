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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClient, IggyClientBuilder, IggyError,
};
use tokio::sync::Semaphore;
use tracing::info;

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

mod fetch;
mod offsets;
mod produce;
mod topics;

/// Passes attempted, after the first, before [`IggyBridge::connect`] gives up and returns `Err`.
///
/// Not the SDK's own default (`TcpClientReconnectionConfig::default()` is `max_retries: None` -
/// unlimited, one dial per second, forever). A Kafka client already retries at the wire-protocol
/// level once a handler maps a bridge failure to a retriable error code; the bridge blocking a
/// request task inside an unbounded internal reconnect loop would just add a second, invisible
/// retry layer underneath that one instead of surfacing the failure so the mapped code can be
/// sent.
///
/// This bounds the *count*, not the *wall-clock time*, of that inner retry loop - see
/// [`REQUEST_TIMEOUT`] for the latter.
const RECONNECTION_RETRIES: u32 = 3;

/// Wall-clock ceiling [`with_request_timeout`] applies uniformly: to the initial `client.connect()`
/// in [`IggyBridge::connect`] (including every attempt [`RECONNECTION_RETRIES`] makes internally),
/// and to every call made after it succeeds (`get_stream`, `create_stream`, `get_topic`,
/// `create_topic`, `shutdown`).
///
/// 15s covers a slow but live server's handshake and login.
///
/// A reconnect dial runs in the caller's future, so this timeout cancels a hung dial too.
///
/// Sends use the Produce deadline instead. See `send_records`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Wraps a single Iggy client call in [`REQUEST_TIMEOUT`].
///
/// On expiry, maps to [`BridgeError::Timeout`], not `IggyError::CannotEstablishConnection`: the
/// SDK's write/read run in a detached task this timeout cannot abort, so the call may still land.
async fn with_request_timeout<T>(
    op: impl Future<Output = Result<T, IggyError>>,
) -> Result<T, BridgeError> {
    tokio::time::timeout(REQUEST_TIMEOUT, op)
        .await
        .map_err(|_elapsed| BridgeError::Timeout)?
        .map_err(BridgeError::Iggy)
}

/// Owns one connected `IggyClient` and resolves Kafka topics against it.
///
/// One lockstep client serves every Kafka connection, so Iggy calls run one at a time. A pool is
/// a TODO in `docs/SCOPE.md`.
pub struct IggyBridge {
    client: Arc<IggyClient>,
    config: IggyBridgeConfig,
    /// One Produce send inside the SDK at a time. See `send_records`.
    send_slot: Arc<Semaphore>,
}

/// The Iggy stream and topic one Kafka topic maps to. Resolve once per topic, use many times.
pub struct TopicTarget {
    stream_id: Identifier,
    topic_id: Identifier,
}

impl IggyBridge {
    /// Connects to Iggy using `config` and authenticates.
    ///
    /// Builds the client through the SDK's fluent TCP builder rather than hand-assembling an
    /// `iggy://user:pass@host` connection string: that string format splits on `@` then `:`, so a
    /// password containing either character (`p@ss:word`) would be misparsed into a garbled
    /// address instead of failing with a diagnosable config error. The fluent builder passes
    /// `username`/`password` as already-separated fields, sidestepping the ambiguity entirely.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `config.address` is empty. Returns
    /// [`BridgeError::Timeout`] if connecting takes longer than `REQUEST_TIMEOUT` (an
    /// unreachable-and-silently-dropping address, not just a refused one, is covered - see that
    /// constant's doc). Returns [`BridgeError::Iggy`] if the address is malformed, the TCP
    /// connection fails, or authentication is rejected - this is the boundary
    /// [`BridgeError::to_kafka_error_code`] exists for: a handler calling this must map the error
    /// to a wire response, never panic or unwrap, since an unreachable Iggy backend is an
    /// expected runtime condition, not a bug.
    pub async fn connect(config: IggyBridgeConfig) -> Result<Self, BridgeError> {
        if config.address.trim().is_empty() {
            return Err(BridgeError::InvalidConfig(
                "Iggy address must not be empty".to_string(),
            ));
        }

        let credentials =
            Credentials::UsernamePassword(config.username.clone(), config.password.clone());
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(config.address.clone())
            .with_auto_sign_in(AutoLogin::Enabled(credentials))
            .with_reconnection_max_retries(Some(RECONNECTION_RETRIES))
            .build()
            .map_err(BridgeError::Iggy)?;
        with_request_timeout(client.connect()).await?;
        info!("Iggy bridge connected to {}", config.address);

        Ok(Self {
            client: Arc::new(client),
            config,
            send_slot: Arc::new(Semaphore::new(1)),
        })
    }

    /// Checks `kafka_topic` against Kafka's name rules and resolves its Iggy stream and topic.
    ///
    /// Creates nothing. `Metadata` owns topic creation.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's own rules.
    /// [`BridgeError::Iggy`] if a mapped name is not a valid Iggy identifier.
    pub fn topic_target(&self, kafka_topic: &str) -> Result<TopicTarget, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        Ok(TopicTarget {
            stream_id: Identifier::named(stream_name).map_err(BridgeError::Iggy)?,
            topic_id: Identifier::named(topic_name).map_err(BridgeError::Iggy)?,
        })
    }

    /// Tears down the underlying Iggy client, including its background heartbeat task.
    ///
    /// Not `IggyClient::disconnect`: that only tears down the transport
    /// (`TcpClient::disconnect_transport`) and never touches `heartbeat_handle` - only
    /// `IggyClient`'s own `Drop` aborts that task (`client.rs`). A `disconnect`ed-but-not-dropped
    /// bridge would keep heartbeating on a schedule, hit `NotConnected` (itself in the SDK's
    /// retriable set), and reconnect plus re-authenticate using the credentials `connect`
    /// configured, so the "closed" client silently comes back. `shutdown` sets
    /// `ClientState::Shutdown`, which the heartbeat loop's next `ping` observes as
    /// `IggyError::ClientShutdown` and self-terminates on, and which `sign_in_credentials` never
    /// dials past.
    ///
    /// Takes `self` by value: `shutdown` is terminal (no reconnect is coming back from it), so
    /// nothing legitimate is left to call on this bridge afterward. A bridge shared via `Arc`
    /// needs `Arc::try_unwrap` first.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Timeout`] if it takes longer than `REQUEST_TIMEOUT`. Returns
    /// [`BridgeError::Iggy`] if the underlying client reports a shutdown failure (e.g. the socket
    /// was already in a state that rejects a clean shutdown).
    pub async fn close(self) -> Result<(), BridgeError> {
        with_request_timeout(self.client.shutdown()).await
    }
}
