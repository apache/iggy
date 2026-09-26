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
use std::time::Duration;

use iggy::prelude::{AutoLogin, Client, Credentials, IggyClient, IggyClientBuilder, IggyError};
use tracing::info;

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

mod fetch;
mod offsets;
mod produce;
mod topics;

pub use topics::{KafkaTopicMetadata, TopicCreationOutcome};

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
/// One constant, not two separately-named ones with the same value: both call sites bound the
/// identical underlying hazard. `TcpClient::establish_bounded` only applies its own
/// `FAILOVER_DIAL_TIMEOUT` (2s) when at least two failover candidates are configured
/// (`tcp_client.rs`) - a bridge always configures exactly one address, so that guard never engages
/// at either site, and the plain `TcpStream::connect` underneath has no deadline of its own.
/// Against a firewall that drops SYN packets instead of refusing them, each dial attempt pays the
/// kernel's own SYN-retry timeout (minutes, not seconds) rather than the `reconnection_interval`
/// between attempts - a closed port (instant RST) never exercises this path, so the failure mode
/// only shows up in production. 15s comfortably covers a slow-but-alive server's handshake (well
/// above p99 login latency) while still failing well short of the pathological multi-minute case.
///
/// Known limitation, not closed by this constant: [`with_request_timeout`] cancels only the
/// *caller's* wait, not the SDK's own work. `TcpClient::send_raw_vsr_attempt` (`tcp_client.rs`)
/// writes, flushes and reads inside a detached `tokio::spawn` specifically so that dropping the
/// awaiting future - exactly what this timeout does on expiry - cannot abort it mid-flight (that
/// function's own "SAFETY: we run code holding the `stream` lock in a task so we can't be
/// cancelled while holding the lock"). A call that times out here can leave that detached task
/// still holding `IggyClient`'s single stream mutex for up to `RESPONSE_READ_TIMEOUT` (30s,
/// `tcp_client.rs`) longer, queuing every other bridge call behind it. Raising this constant does
/// not close the gap either: a mid-call reconnect (`send_raw_with_response`, `tcp_client.rs`)
/// replays on a fresh `RESPONSE_READ_TIMEOUT` budget, and the reconnect dial itself goes through
/// the same undead-lined `TcpStream::connect` this constant exists to bound - genuinely unbounded,
/// so no finite value here can guarantee catching it. That same unboundedness is why this timeout
/// cannot simply be dropped for post-connect calls either: [`IggyBridge`] holds one `IggyClient`
/// with no pooling, so an unbounded reconnect dial with nothing here to stop it would wedge every
/// later call on this bridge, not just the one that triggered it. No longer a hypothetical: since
/// `ListOffsets` (#3537) this bridge is called from a live Kafka handler, with no semaphore
/// bounding concurrent bridge calls. Closing the underlying gap needs either a cooperatively
/// cancellable SDK call or a deadline on the SDK's own reconnect dial, neither of which this
/// bridge can add from the outside; bounding concurrent bridge calls is a separate, addressable
/// fix that becomes more pressing as `CreateTopics` and `Metadata` (#3538/#3534) add more live
/// callers.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Wraps a single Iggy client call in [`REQUEST_TIMEOUT`]. See that constant's doc for why every
/// bridge method needs this, not just [`IggyBridge::connect`] - and for the cancellation gap this
/// wrapper does not close.
///
/// On expiry, maps to [`BridgeError::Timeout`], not `IggyError::CannotEstablishConnection`: the
/// SDK's write/read run in a detached task this timeout cannot abort (see [`REQUEST_TIMEOUT`]'s
/// doc), so by the time this fires the request may already be on the wire, or already applied
/// server-side - a genuinely unknown outcome, not a known connection failure. `BridgeError`'s own
/// mapping already treats an unknown outcome (`IggyError::TransientNotCommitted`) as distinct from
/// a known-safe-to-retry one for exactly this reason; a caller-side timeout is the same shape and
/// must not borrow the known-safe code.
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
/// Produce/Fetch handler wiring is a separate, later change (`#3535`/`#3536`) - this type is the
/// shared plumbing those handlers will call into, exercised standalone here via its own tests and
/// an integration test against a real `iggy-server`.
///
/// One `IggyClient`, shared across every Kafka connection this gateway serves - and the SDK's TCP
/// transport is lockstep, one request in flight at a time (`tcp_client.rs`: "the connection is
/// lockstep", its stream mutex held across write, flush, and read). Every concurrent Kafka
/// connection ends up serialized behind whichever single Iggy request is in flight; the Kafka
/// side's own connection limit does nothing to relieve this. No pooling exists yet - `close`
/// already takes `self` by value, which anticipates an eventual `Arc`-shared bridge, but nothing
/// wires that up today. Documented here and in the README rather than silently discovered under
/// load once `#3535`/`#3536` land.
pub struct IggyBridge {
    client: IggyClient,
    config: IggyBridgeConfig,
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

        Ok(Self { client, config })
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
    /// nothing legitimate is left to call on this bridge afterward. This does mean a bridge shared
    /// via `Arc` cannot call this directly (`Arc::try_unwrap` first) - not a concern before
    /// `#3535`/`#3536` wire an owning caller in.
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
