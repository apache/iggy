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

use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClient, IggyClientBuilder, IggyError,
    StreamClient, TopicClient, TopicCreateOptions,
};
use tracing::{debug, info};

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

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
/// later call on this bridge, not just the one that triggered it. Tolerable only because nothing
/// calls this bridge from a live Kafka handler yet - closing it needs either a cooperatively
/// cancellable SDK call or a deadline on the SDK's own reconnect dial, neither of which this
/// bridge can add from the outside. Must be resolved before #3535/#3536 share this client across
/// concurrent connections.
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

    /// Ensures the Iggy stream and topic backing `kafka_topic` exist, creating either or both if
    /// missing. Resolves `kafka_topic` through the configured [`TopicMapping`](crate::bridge::topic_map::TopicMapping).
    ///
    /// Idempotent when repeated with the *same* `partition_count`: a `get` before each `create`
    /// means calling this twice for the same topic is a no-op the second time, and a
    /// `NameAlreadyExists` race from a concurrent caller creating the same stream/topic between
    /// this call's `get` and `create` is treated as success, not an error - the desired end state
    /// (it exists) is what idempotency actually promises, not that this call was the one that
    /// created it. A *different* `partition_count` against an already-existing topic is not
    /// idempotent - see [`BridgeError::PartitionCountMismatch`].
    ///
    /// Ensures the stream before the topic, so a topic-creation failure (a different
    /// `partition_count`, a transient error) can leave a stream that now exists with no topic in
    /// it yet - a retry heals this (idempotent on the stream half too), and no rollback is
    /// attempted: `TopicMapping::resolve` sends every *unmapped* Kafka topic to the same
    /// `default_stream`, so deleting a stream on a topic-creation failure risks deleting another
    /// topic's data that happens to share it, and this call has no way to tell whether it was the
    /// one that created the stream in the first place.
    ///
    /// `partition_count` is `u32`, so it cannot carry Kafka's own `CreateTopics` sentinel
    /// (`num_partitions == -1`, KIP-464 "use the broker default" - `protocol/responses.rs`
    /// already accepts that sentinel at the wire-validation layer). Resolving *what* the broker
    /// default should be for this bridge is a decision for whichever of `#3535`/`#3536` first
    /// calls this with a real Kafka request in hand, not one to invent here ahead of that need.
    ///
    /// No caching: every call pays a `get_stream` and a `get_topic` (two round trips once both
    /// already exist), even for a topic this same bridge already confirmed a moment ago. A cache
    /// keyed on `kafka_topic` would remove that cost, but would also have to answer "how does a
    /// cache entry ever get invalidated" - the topic being deleted and recreated with a different
    /// partition count out from under a stale cache entry is exactly
    /// `ensure_topic_targets_the_streams_live_incarnation_after_a_delete_and_recreate`'s own
    /// scenario, and a naive cache breaks that guarantee to save two round trips. Whatever wires
    /// this into `#3535`/`#3536` should call it once per topic and remember that it did, rather
    /// than once per Produce/Fetch - the cost belongs at the call site's discretion, not hidden
    /// (and potentially made wrong) inside this method.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if a call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] for connectivity/auth failures. Returns
    /// [`BridgeError::PartitionCountMismatch`] if the topic already exists with a different
    /// partition count than `partition_count`.
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = self.ensure_stream(stream_name).await?;
        self.ensure_topic(&stream_id, topic_name, kafka_topic, partition_count)
            .await?;
        Ok(())
    }

    /// Ensures the stream named `stream_name` exists, creating it if missing.
    ///
    /// `Identifier::named` - never `Identifier::try_from`/`FromStr` - because the latter parses
    /// an all-digit string as a numeric Iggy ID rather than a name. A stream or topic named e.g.
    /// `"42"` would otherwise resolve against the wrong resource on every call after the first:
    /// the first `ensure_stream_and_topic("42", ...)` creates a stream *named* `"42"`, but a
    /// second call would look it up *by ID* `42` instead, almost certainly finding nothing and
    /// breaking the "idempotent on repeated calls" guarantee.
    ///
    /// Returns the same *named* `Identifier` it was given, not the numeric id the SDK hands back
    /// from `get`/`create` - streams are backed by a recycled slab (`core/metadata`'s
    /// `stm/stream.rs`: freed keys are reused by the next created stream), so a numeric id
    /// captured here could point at a *different* stream by the time `ensure_topic` uses it, if
    /// this stream is deleted and recreated in between. The name has no such window.
    async fn ensure_stream(&self, stream_name: &str) -> Result<Identifier, BridgeError> {
        let identifier = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        if with_request_timeout(self.client.get_stream(&identifier))
            .await?
            .is_some()
        {
            debug!("Iggy stream '{stream_name}' already exists");
            return Ok(identifier);
        }

        match with_request_timeout(self.client.create_stream(stream_name)).await {
            Ok(_created) => {
                info!("created Iggy stream '{stream_name}'");
                Ok(identifier)
            }
            Err(BridgeError::Iggy(IggyError::StreamNameAlreadyExists(_))) => {
                // Lost a create race - the name now exists regardless of who won it.
                Ok(identifier)
            }
            Err(err) => Err(err),
        }
    }

    /// Looks up (or creates) the topic named `topic_name` under `stream_id`.
    ///
    /// `Identifier::named`, not `Identifier::try_from` - the same numeric-name ambiguity
    /// [`Self::ensure_stream`]'s doc comment describes for stream names applies to topic names.
    ///
    /// `partition_count == 0` is accepted here as defense in depth, not the primary guard: the
    /// server allows it by design (`rewrite.rs`), and the `CreateTopics` stub already rejects it
    /// at the wire level (`protocol/responses.rs`) before any bridge call would be reachable.
    async fn ensure_topic(
        &self,
        stream_id: &Identifier,
        topic_name: &str,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let identifier = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        if let Some(existing) =
            with_request_timeout(self.client.get_topic(stream_id, &identifier)).await?
        {
            debug!("Iggy topic '{topic_name}' already exists");
            // ensure_topic's contract is "the topic has partition_count partitions afterward" -
            // a mismatch here means that's false. Returning Ok(()) anyway (even with a warn!)
            // would let two concurrent callers requesting different counts for the same topic
            // both believe they succeeded; growing partitions on the caller's behalf is also a
            // bigger decision (CreatePartitions has its own semantics) than this method should
            // make silently. Erring is the only response that keeps the postcondition honest.
            if existing.partitions_count != partition_count {
                return Err(BridgeError::PartitionCountMismatch {
                    // The Kafka-side name a caller actually asked about, not `topic_name` - see
                    // the identical note on `PartitionOutOfRange` in `high_watermark`.
                    topic: kafka_topic.to_string(),
                    existing: existing.partitions_count,
                    requested: partition_count,
                });
            }
            return Ok(());
        }

        // message_expiry left at TopicCreateOptions::default() (None -> ServerDefault) means
        // never-expire (segment_cleaner.rs treats ServerDefault the same as NeverExpire), not
        // Kafka's own 7-day default - deliberate for now (imposing a retention policy is a product
        // decision this bridge shouldn't make unasked), but a real surprise for anyone repointing
        // a Kafka app that assumes bounded retention. Flagged in the README; revisit once there's
        // a way to configure it (env var, topic-mapping field) rather than hardcoding a number.
        let options = TopicCreateOptions {
            partitions_count: Some(partition_count),
            ..TopicCreateOptions::default()
        };
        match with_request_timeout(self.client.create_topic(stream_id, topic_name, &options)).await
        {
            Ok(created) => {
                info!("created Iggy topic '{topic_name}' with {partition_count} partitions");
                // Cheap: TopicDetails is already in hand, no extra round trip. `partitions_count`
                // is a hard argument to create_topic (Some(partition_count), never None), so the
                // server has no "resolve at admission" substitution to fall back on here - but
                // checking anyway, the same way the other two branches check their own
                // postcondition, means a future server-side clamp/cap fails loudly here instead
                // of this method silently reporting success under a broken contract.
                if created.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: kafka_topic.to_string(),
                        existing: created.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(())
            }
            // Lost a create race - re-verify by name rather than trusting the race outcome alone.
            // The winner may have created it with a different partition count than this call
            // requested, so this needs the same mismatch check the existing-topic branch above
            // makes - skipping it here would let two concurrent ensure_topic(N) / ensure_topic(M)
            // calls for the same topic both return Ok(()).
            //
            // Untested: reaching this arm needs a real concurrent second caller mid-race, and
            // `IggyBridge` holds a concrete `IggyClient` with no seam for a fake that returns
            // `TopicNameAlreadyExists` on demand. A test that spins up two real concurrent callers
            // would hit it only sometimes - flaky, and passing wouldn't prove this arm ran. Left
            // as a known gap until something introduces a client seam.
            Err(BridgeError::Iggy(IggyError::TopicNameAlreadyExists(_, _))) => {
                let existing = with_request_timeout(self.client.get_topic(stream_id, &identifier))
                    .await?
                    .ok_or_else(|| {
                        BridgeError::Iggy(IggyError::TopicNameNotFound(
                            topic_name.to_string(),
                            stream_id.to_string(),
                        ))
                    })?;
                if existing.partitions_count != partition_count {
                    return Err(BridgeError::PartitionCountMismatch {
                        topic: kafka_topic.to_string(),
                        existing: existing.partitions_count,
                        requested: partition_count,
                    });
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Returns the high watermark (one past the highest *committed* offset - see
    /// `Partition::offset_frontier`'s own definition) for every partition in `partitions` of the
    /// Iggy topic `kafka_topic` maps to, in one round trip.
    ///
    /// Takes `kafka_topic`, not raw Iggy stream/topic names, and resolves it through the same
    /// [`TopicMapping`](crate::bridge::topic_map::TopicMapping) `ensure_stream_and_topic`
    /// uses - a caller (a future `ListOffsets` handler) only ever has the Kafka-side name, and a
    /// topic with a mapping override would silently query the wrong Iggy resource if this took
    /// Iggy-space names directly instead.
    ///
    /// One call to `get_topic`, not one per partition: a single Kafka `ListOffsets` request asks
    /// about many partitions of one topic at once (`ListOffsetsRequest.json`'s
    /// `Topics[] -> Partitions[]`), and `get_topic`'s decode already rebuilds and sorts the whole
    /// partition vector regardless of how many of them the caller wants
    /// (`wire_conversions.rs`) - a naive one-call-per-partition wrapper around a
    /// single-partition method would turn one Kafka request into N round trips and N times the
    /// decode work. Returns results in the same order as `partitions`.
    ///
    /// `i64`, not `u64`: the `ListOffsets` response field this exists to fill is `int64`
    /// (`ListOffsetsResponse.json`), so a future handler needs no cast at the wire boundary - the
    /// lossy conversion happens once, here (`Partition::current_offset` is `u64`; converted via
    /// `i64::try_from`, saturating to `i64::MAX` on the practically-unreachable overflow case
    /// rather than panicking or wrapping).
    ///
    /// `Partition::current_offset` is the offset of the *last written* message, not "next offset
    /// to produce" - confirmed against a live server (3 produced messages read back
    /// `current_offset == 2`). An empty partition has no last-written offset at all, so this
    /// needs a dedicated empty case rather than inferring it from `current_offset == 0` (also a
    /// fresh partition's default value, indistinguishable from "one message at offset 0").
    ///
    /// That empty case is `messages_count == 0 && current_offset == 0`, not `messages_count == 0`
    /// alone: retention cleanup decrements `messages_count`
    /// (`iggy_partition.rs::decrement_messages_count`) without rewinding `current_offset`, so a
    /// fully-purged but previously-produced-to partition would otherwise read as empty. This is
    /// still not exact: a partition whose *only* message, at offset 0, gets trimmed by the same
    /// retention pass reports `(messages_count, current_offset) == (0, 0)` too, so its watermark
    /// would read `1` then drop back to `0` - a real Kafka watermark never moves backward. Narrow
    /// (needs retention to fully empty a partition that only ever held one message) and not
    /// fixable client-side (`PartitionResponse` carries no frontier field to disambiguate it), but
    /// worth having on the record rather than only implicitly true of the check below.
    ///
    /// Not the same value as `Partition::mint_frontier` (the offset the *next* mint will take):
    /// after a crash-recovery reservation, the append point can sit above the committed frontier
    /// by the reservation's lease block, and neither value is on the wire to tell them apart. This
    /// is still correct for `ListOffsets` LATEST (defined in terms of the committed offset), but a
    /// future Produce handler must not use it to predict a base offset for a write in flight.
    ///
    /// Known remaining gap, not fixable client-side: `(0, 0)` is also what a stats-registry MISS
    /// reports (`responses.rs`'s `PartitionResponse` builder), indistinguishable on the wire from
    /// a genuinely empty partition. Narrow and self-healing - `partition_reconciler.rs`'s
    /// `settle_partition_stats` opens this only on the teardown-for-rebuild path, closing once the
    /// rebuild completes; deletes never open it. The real answer (`Partition::offset_frontier`)
    /// stays server-side and isn't in this response. The check matches the server's own
    /// `PartitionState::store_offset_range_error` condition, so it's not an invented heuristic - a
    /// future `ListOffsets` (`#3537`) built on this inherits the same blind spot.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if the call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] if the mapped stream doesn't exist (see
    /// the note above on which resource is actually missing). These are call-level failures -
    /// nothing about any individual partition could be resolved. A single out-of-range
    /// `partition` does not fail the whole call: it is reported per-partition (see the return
    /// type), so the partitions that did resolve are never discarded to report one that didn't.
    pub async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        let details = with_request_timeout(self.client.get_topic(&stream_id, &topic_id))
            .await?
            .ok_or_else(|| {
                // A missing *stream* also makes get_topic return Ok(None) (responses.rs), so this
                // reports TopicNameNotFound even when the stream is what's actually gone - both
                // map to the same Kafka wire code either way, so only the log text is affected.
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    topic_name.to_string(),
                    stream_name.to_string(),
                ))
            })?;

        Ok(partitions
            .iter()
            .map(|&partition| {
                // `TryFrom<GetTopicResponse> for TopicDetails` (wire_conversions.rs) sorts
                // `partitions` by `id` on every decode, so a binary search is correct here, not
                // just faster than a linear scan - for a 1000-partition topic, the difference is
                // O(log n) vs O(n) probes per requested partition.
                let watermark = details
                    .partitions
                    .binary_search_by_key(&partition, |p| p.id)
                    .map(|index| &details.partitions[index])
                    .map_err(|_| BridgeError::PartitionOutOfRange {
                        // The Kafka-side name a caller (a future ListOffsets handler) actually
                        // asked about, not `topic_name` - a mapping override would otherwise
                        // quote the wrong (Iggy-side) name back at a Kafka client that never
                        // heard of it.
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count: details.partitions_count,
                    })
                    .map(|partition_details| {
                        if partition_details.messages_count == 0
                            && partition_details.current_offset == 0
                        {
                            0
                        } else {
                            partition_details.current_offset.saturating_add(1)
                        }
                    })
                    .map(|watermark| i64::try_from(watermark).unwrap_or(i64::MAX));
                (partition, watermark)
            })
            .collect())
    }

    /// Convenience wrapper around [`Self::high_watermarks`] for a single partition. See that
    /// method's doc for the full semantics - callers wanting more than one partition of the same
    /// topic should call it directly instead of this in a loop, to get its one-round-trip batching.
    ///
    /// # Errors
    ///
    /// See [`Self::high_watermarks`].
    ///
    /// # Panics
    ///
    /// Never in practice: [`Self::high_watermarks`] returns exactly one result per requested
    /// partition on `Ok`, and this always requests exactly one.
    pub async fn high_watermark(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> Result<i64, BridgeError> {
        let (_, watermark) = self
            .high_watermarks(kafka_topic, &[partition])
            .await?
            .into_iter()
            .next()
            .expect(
                "high_watermarks returns exactly one result per requested partition, \
                 and exactly one was requested",
            );
        watermark
    }

    /// Looks up `kafka_topic` without creating it. Unlike [`Self::ensure_stream_and_topic`],
    /// Metadata is read-only discovery - a client asking "does this topic exist" must not have
    /// the asking itself create the topic.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidKafkaTopicName`] if `kafka_topic` fails Kafka's own
    /// topic-naming rules. Returns [`BridgeError::Timeout`] if a call takes longer than
    /// `REQUEST_TIMEOUT`. Returns [`BridgeError::Iggy`] for connectivity/auth failures.
    pub async fn get_kafka_topic(
        &self,
        kafka_topic: &str,
    ) -> Result<Option<KafkaTopicMetadata>, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        if with_request_timeout(self.client.get_stream(&stream_id))
            .await?
            .is_none()
        {
            return Ok(None);
        }
        let topic_id = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        let Some(topic) =
            with_request_timeout(self.client.get_topic(&stream_id, &topic_id)).await?
        else {
            return Ok(None);
        };
        Ok(Some(KafkaTopicMetadata {
            kafka_topic: kafka_topic.to_string(),
            partitions_count: topic.partitions_count,
        }))
    }

    /// Every Kafka-facing topic this bridge currently knows about, for a Metadata request with a
    /// null (all-topics) `topics` array.
    ///
    /// Two sources, not one `get_topics(default_stream)` call: every topic actually present in
    /// `default_stream` resolves back to a Kafka topic of the identical name (`TopicMapping::new`
    /// already rejects any override that could alias that path - see its own doc), so those are
    /// free to list in bulk. An override's *target* is a different stream (or the same stream
    /// under a different Iggy topic name), invisible to that one bulk call, so each is checked
    /// individually - and only included if the Iggy side has actually been created, since real
    /// Kafka Metadata lists existing topics, not configured-but-uncreated ones.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Timeout`] if a call takes longer than `REQUEST_TIMEOUT`. Returns
    /// [`BridgeError::Iggy`] for connectivity/auth failures.
    pub async fn list_kafka_topics(&self) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        let default_stream = self.config.topic_mapping.default_stream();
        let default_stream_id = Identifier::named(default_stream).map_err(BridgeError::Iggy)?;
        let mut topics = Vec::new();
        if with_request_timeout(self.client.get_stream(&default_stream_id))
            .await?
            .is_some()
        {
            let default_topics =
                with_request_timeout(self.client.get_topics(&default_stream_id)).await?;
            topics.extend(default_topics.into_iter().map(|topic| KafkaTopicMetadata {
                kafka_topic: topic.name,
                partitions_count: topic.partitions_count,
            }));
        }
        for kafka_topic in self.config.topic_mapping.overrides().keys() {
            if let Some(metadata) = self.get_kafka_topic(kafka_topic).await? {
                topics.push(metadata);
            }
        }
        Ok(topics)
    }
}

/// Kafka-facing topic name plus partition count, as `handle_metadata` needs to report it.
///
/// A `Topic`/`TopicDetails` (`core/common`) carries far more than Metadata's wire shape uses, and
/// neither carries the *Kafka-side* name a mapping override may have renamed away from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaTopicMetadata {
    pub kafka_topic: String,
    pub partitions_count: u32,
}
