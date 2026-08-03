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

//! Shard-0 connection coordinator.
//!
//! Shard 0 is the sole binder of the replica listener and client listener,
//! and the sole outbound dialer for higher-id peer replicas. On every
//! accept / successful dial the coordinator:
//!
//! 1. picks the next target shard via round-robin,
//! 2. duplicates the TCP fd,
//! 3. sends a connection-setup `LifecycleFrame`
//!    (`ReplicaInboundSetup` / `ReplicaOutboundSetup` /
//!    `Client{,Ws}ConnectionSetup`) to the target shard's inbox,
//! 4. drops its own `TcpStream` so only the target shard's wrapped fd
//!    keeps the socket alive.
//!
//! Replica delegation is blind: no byte is read or written on shard 0,
//! the owning shard runs the handshake and reports the outcome back via
//! `Replica{Inbound,Outbound}HandshakeDone`.
//!
//! The shard-shared [`message_bus::ReplicaOwnerTable`] is the
//! authoritative `replica_id -> owning_shard` view; the owning shard
//! stamps and CAS-clears its slot synchronously through
//! `mark_replica_owned` / `clear_replica_owned`, so the coordinator does
//! not need a separate mapping cache or broadcast subsystem.
//!
//! Client ids encode the owning shard in their top 16 bits, so clients do
//! not need a mapping table; any shard can route a client reply from the
//! id alone.
//!
//! On `try_send` failure into the inter-shard channel (inbox full) the
//! coordinator closes the duplicated fd and returns an error. VSR's
//! retransmission plus the connector's periodic reconnect sweep cover the
//! dropped connection.

use crate::config::CoordinatorConfig;
use crate::metrics::{frame_drop_reason, frame_drop_variant};
use crate::{LifecycleFrame, ShardCtorError, ShardFrame, TaggedSender, validate_sender_ordering};
use compio::net::TcpStream;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::{SendError, fd_transfer};
use std::cell::Cell;
use std::rc::Rc;
use tracing::warn;

/// Bit position of the target-shard tag inside a minted client id: the top 16
/// bits carry the shard, the bottom 112 the mint sequence.
const CLIENT_ID_SHARD_SHIFT: u32 = 112;

/// Sequence half of a minted client id. The tag above it is the inter-shard
/// reply routing key (`message_bus::client_id_owning_shard`), so a sequence
/// that bled past this mask would silently re-route a connection's replies to
/// another shard for the process lifetime.
const CLIENT_SEQUENCE_MASK: u128 = (1 << CLIENT_ID_SHARD_SHIFT) - 1;

/// Coordinator owned by shard 0 only.
///
/// Wrapped in `Rc` by the bootstrap and shared with the replica listener,
/// the connector, and the client listener so each of those paths can
/// delegate immediately.
pub struct ShardZeroCoordinator {
    /// Inter-shard channel senders, indexed by shard id.
    ///
    /// Each [`TaggedSender`] carries the id of the shard whose paired
    /// receiver drains it; the ctor asserts `senders[i].shard_id() == i`
    /// so a permuted Vec is caught at boot rather than misrouting frames.
    senders: Rc<Vec<TaggedSender>>,
    /// Per-shard observability handle. Cloned at construction; bumps the
    /// `frame_drops_total{variant=fd_transfer}` counter when a `try_send`
    /// rejection occurs in the lifecycle path.
    metrics: crate::metrics::ShardMetrics,
    total_shards: u16,
    cfg: CoordinatorConfig,
    replica_rr: Cell<u16>,
    client_rr: Cell<u16>,
    client_seq: Cell<u128>,
    /// View the mint counter was last seeded for. `None` until the first seed.
    /// Tracked so [`Self::seed_client_sequence`] can be called on the minting
    /// path and fold the table exactly once per view instead of per mint.
    seeded_view: Cell<Option<u32>>,
}

impl ShardZeroCoordinator {
    /// # Errors
    ///
    /// Returns [`ShardCtorError::CoordinatorSendersMismatch`] if
    /// `senders.len() != total_shards` or `total_shards == 0`, and
    /// [`ShardCtorError::SenderOrderingInvalid`] if any
    /// `senders[i].shard_id() != i`. Permuted senders are a bootstrap
    /// programming error; `TaggedSender` lifts the ordering invariant
    /// from a doc comment to a ctor check.
    pub fn new(
        senders: Rc<Vec<TaggedSender>>,
        total_shards: u16,
        cfg: CoordinatorConfig,
        metrics: crate::metrics::ShardMetrics,
    ) -> Result<Self, ShardCtorError> {
        if total_shards == 0 || senders.len() != total_shards as usize {
            return Err(ShardCtorError::CoordinatorSendersMismatch {
                senders: senders.len(),
                total_shards,
            });
        }
        validate_sender_ordering(&senders)?;
        Ok(Self {
            senders,
            metrics,
            total_shards,
            cfg,
            replica_rr: Cell::new(0),
            client_rr: Cell::new(0),
            client_seq: Cell::new(1),
            seeded_view: Cell::new(None),
        })
    }

    /// Pick the next target shard for a replica connection.
    ///
    /// When `total_shards > 1` and `cfg.skip_shard_zero_for_replicas` is
    /// true (the default), the selection wraps over `[1, total_shards)`.
    fn next_replica_target(&self) -> u16 {
        rr_pick(
            &self.replica_rr,
            self.total_shards,
            self.cfg.skip_shard_zero_for_replicas,
        )
    }

    /// Pick the next target shard for a client connection.
    ///
    /// When `total_shards > 1` and `cfg.skip_shard_zero_for_clients` is
    /// true, the selection wraps over `[1, total_shards)`. Default false.
    fn next_client_target(&self) -> u16 {
        rr_pick(
            &self.client_rr,
            self.total_shards,
            self.cfg.skip_shard_zero_for_clients,
        )
    }

    /// Mint a client id encoding `target_shard` in the top 16 bits and a
    /// monotonic per-coordinator counter in the bottom 112 bits.
    ///
    /// The sequence is masked into its half of the id. Counting there cannot
    /// reach 2^112, but the counter is also SEEDED from recovered client-table
    /// ids ([`Self::seed_client_sequence`]), and those include values a client
    /// supplied on the wire -- so without the mask a chosen id near the top of
    /// the range would, after a restart, push the next mint's carry into the
    /// shard tag and misroute that connection's replies. Zero is skipped
    /// because the wire header rejects `client == 0`.
    fn mint_client_id(&self, target_shard: u16) -> u128 {
        let mut seq = self.client_seq.get() & CLIENT_SEQUENCE_MASK;
        if seq == 0 {
            seq = 1;
        }
        self.client_seq
            .set(seq.wrapping_add(1) & CLIENT_SEQUENCE_MASK);
        (u128::from(target_shard) << CLIENT_ID_SHARD_SHIFT) | seq
    }

    /// Reseed the mint counter above every sequence in `recovered_ids`, once
    /// per `view`.
    ///
    /// The counter is per process and starts at 1, while the client table it
    /// must not collide with is REPLICATED -- so it holds ids minted by other
    /// nodes' counters, and by this node's previous boot. Left alone, a mint
    /// lands on an id that is already taken: a different user's login is then
    /// refused outright (the register ownership gate), and the same user's
    /// rebinds and inherits a watermark it never wrote. Seeding past the
    /// table's high-water mark makes that unreachable instead of handled.
    ///
    /// Two moments need it, which is why `view` is the key rather than "call
    /// this at boot": startup, where the table was rebuilt from the previous
    /// boot's WAL, and PROMOTION, where a node starts minting against ids its
    /// predecessor committed from an unrelated counter. A view change is the
    /// observable edge for the second, and re-seeding for a view already
    /// covered is skipped, so this is cheap enough to call from the minting
    /// path.
    ///
    /// Never lowers the counter, so a redundant call cannot hand back an id
    /// this process already minted.
    ///
    /// Only ids whose tag names a shard of this node are folded in. The table
    /// also holds ids a client chose for itself (the TCP login path takes
    /// `client` straight off the wire), and those carry arbitrary tags; folding
    /// them would let one login steer this node's counter. A chosen id with a
    /// *plausible* tag can still move it, which is why the mint masks -- the
    /// worst case is then a counter that wraps low and collides with live
    /// entries, which the register ownership gate refuses rather than
    /// mis-serves.
    pub fn seed_client_sequence(&self, view: u32, recovered_ids: impl Iterator<Item = u128>) {
        if self.seeded_view.get() == Some(view) {
            return;
        }
        self.seeded_view.set(Some(view));
        let highest = recovered_ids
            .filter(|id| (id >> CLIENT_ID_SHARD_SHIFT) < u128::from(self.total_shards))
            .map(|id| id & CLIENT_SEQUENCE_MASK)
            .max();
        let Some(highest) = highest else {
            return;
        };
        let next = highest.saturating_add(1) & CLIENT_SEQUENCE_MASK;
        if next > self.client_seq.get() {
            self.client_seq.set(next);
        }
    }

    /// Mint a client id for a connection that terminates locally on shard 0
    /// (QUIC, TCP-TLS) instead of being round-robin delegated.
    ///
    /// Shares the coordinator's `client_seq` counter with the delegated
    /// TCP/WS path. Both a shard-0-local connection and a delegated
    /// connection that round-robined to shard 0 encode `target_shard = 0`
    /// in the top 16 bits; drawing from separate counters would let them
    /// mint the same id and collide in shard 0's single connection
    /// registry. One counter keeps every shard-0 id distinct.
    #[must_use]
    pub fn mint_shard_zero_client_id(&self) -> u128 {
        self.mint_client_id(0)
    }

    /// Ship a blind-delegated inbound replica connection to the next
    /// round-robin target shard. No byte has been read: the owning shard
    /// runs the acceptor handshake and answers shard 0 with
    /// [`LifecycleFrame::ReplicaInboundHandshakeDone`] echoing `slot`
    /// (the in-flight cap slot acquired by the accept callback).
    ///
    /// Returns `Ok(target_shard)` on a successful `try_send`. The owning
    /// shard records itself in the shard-shared
    /// [`message_bus::ReplicaOwnerTable`] once its installer accepts the
    /// handshaked connection, so no extra broadcast or mapping cache is
    /// needed here. On inter-shard channel failure closes the duplicated
    /// fd and returns `Err(SendError::RoutingFailed)`; the caller must
    /// release `slot`.
    ///
    /// # Errors
    ///
    /// Returns an error when `dup(2)` fails or when the target shard's
    /// inbox refuses the setup frame (full or disconnected).
    pub fn delegate_replica_inbound(&self, stream: TcpStream, slot: u64) -> Result<u16, SendError> {
        self.ship_replica_fd(stream, "delegate_replica_inbound", |fd| {
            LifecycleFrame::ReplicaInboundSetup { fd, slot }
        })
    }

    /// Ship a dialed outbound replica connection to the next round-robin
    /// target shard. The owning shard runs the dialer handshake toward
    /// `replica_id` and answers shard 0 with
    /// [`LifecycleFrame::ReplicaOutboundHandshakeDone`]; on success the
    /// caller marks the peer dial-pending so the reconnect sweep skips
    /// it until the handshake outcome arrives.
    ///
    /// # Errors
    ///
    /// Returns an error when `dup(2)` fails or when the target shard's
    /// inbox refuses the setup frame (full or disconnected).
    pub fn delegate_replica_outbound(
        &self,
        stream: TcpStream,
        replica_id: u8,
    ) -> Result<u16, SendError> {
        self.ship_replica_fd(stream, "delegate_replica_outbound", |fd| {
            LifecycleFrame::ReplicaOutboundSetup { fd, replica_id }
        })
    }

    /// Shared dup-and-ship leg of the two replica delegation paths: pick
    /// the round-robin target, dup the fd, `try_send` the built setup
    /// frame, drop the original stream so the target's dup keeps the
    /// socket alive.
    fn ship_replica_fd(
        &self,
        stream: TcpStream,
        label: &'static str,
        build_frame: impl FnOnce(fd_transfer::DupedFd) -> LifecycleFrame,
    ) -> Result<u16, SendError> {
        let target = self.next_replica_target();
        let fd = fd_transfer::dup_fd(&stream).map_err(SendError::DupFailed)?;

        let setup = build_frame(fd);
        if let Err(e) = self.senders[target as usize].try_send(ShardFrame::lifecycle(setup)) {
            // The frame (and the `DupedFd` inside) is returned in `e` and
            // dropped at end-of-block, which closes the dup. No explicit
            // `close_fd` needed.
            self.metrics
                .record_frame_drop(frame_drop_variant::FD_TRANSFER, classify_try_send_err(&e));
            warn!(target, "{label} try_send failed: {e:?}");
            return Err(SendError::RoutingFailed(target));
        }

        // Shard 0 drops the original stream; the target shard's dup keeps
        // the socket open.
        drop(stream);

        Ok(target)
    }

    /// Ship a client TCP connection to the next round-robin target shard.
    ///
    /// On success returns the minted client id. On failure closes the
    /// duplicated fd and returns an error.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::DupFailed`] if `stream.peer_addr()` lookup
    /// fails or `dup(2)` fails. Returns [`SendError::RoutingFailed`]
    /// when the target shard's inbox refuses the setup frame (full or
    /// disconnected).
    pub fn delegate_client(&self, stream: TcpStream) -> Result<u128, SendError> {
        let target = self.next_client_target();
        let client_id = self.mint_client_id(target);
        let peer_addr = stream.peer_addr().map_err(SendError::DupFailed)?;

        let fd = fd_transfer::dup_fd(&stream).map_err(SendError::DupFailed)?;
        let meta = ClientConnMeta::new(client_id, peer_addr, ClientTransportKind::Tcp);
        let setup = LifecycleFrame::ClientConnectionSetup { fd, meta };
        if let Err(e) = self.senders[target as usize].try_send(ShardFrame::lifecycle(setup)) {
            // The returned frame owns the `DupedFd` and closes it on drop.
            self.metrics
                .record_frame_drop(frame_drop_variant::FD_TRANSFER, classify_try_send_err(&e));
            warn!(client_id, target, "delegate_client try_send failed: {e:?}");
            return Err(SendError::RoutingFailed(target));
        }

        drop(stream);
        Ok(client_id)
    }

    /// Ship a WebSocket client's pre-upgrade TCP connection to the next
    /// round-robin target shard.
    ///
    /// Identical wire path to [`Self::delegate_client`] but ships
    /// [`LifecycleFrame::ClientWsConnectionSetup`] so the receiving
    /// shard runs `compio_ws::accept_async_with_config` before
    /// installing the connection. The fd at ship-time is plain TCP;
    /// the WS state machine only materialises post-upgrade on the
    /// owning shard.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::DupFailed`] if `stream.peer_addr()` lookup
    /// fails or `dup(2)` fails. Returns [`SendError::RoutingFailed`]
    /// when the target shard's inbox refuses the setup frame.
    pub fn delegate_ws_client(&self, stream: TcpStream) -> Result<u128, SendError> {
        let target = self.next_client_target();
        let client_id = self.mint_client_id(target);
        let peer_addr = stream.peer_addr().map_err(SendError::DupFailed)?;

        let fd = fd_transfer::dup_fd(&stream).map_err(SendError::DupFailed)?;
        let meta = ClientConnMeta::new(client_id, peer_addr, ClientTransportKind::Ws);
        let setup = LifecycleFrame::ClientWsConnectionSetup { fd, meta };
        if let Err(e) = self.senders[target as usize].try_send(ShardFrame::lifecycle(setup)) {
            self.metrics
                .record_frame_drop(frame_drop_variant::FD_TRANSFER, classify_try_send_err(&e));
            warn!(
                client_id,
                target, "delegate_ws_client try_send failed: {e:?}"
            );
            return Err(SendError::RoutingFailed(target));
        }

        drop(stream);
        Ok(client_id)
    }

    #[must_use]
    pub const fn total_shards(&self) -> u16 {
        self.total_shards
    }
}

/// Map `crossfire::TrySendError` to the `frame_drop_reason` label set so
/// every drop site picks the same string.
pub(crate) const fn classify_try_send_err<T>(err: &crossfire::TrySendError<T>) -> &'static str {
    match err {
        crossfire::TrySendError::Full(_) => frame_drop_reason::FULL,
        crossfire::TrySendError::Disconnected(_) => frame_drop_reason::DISCONNECTED,
    }
}

/// Advance `counter` and return the next target shard.
///
/// When `skip_zero` is true and `total_shards > 1`, wraps over
/// `[1, total_shards)`; otherwise wraps over `[0, total_shards)`. With
/// `total_shards == 1` the flag is ignored and the function always
/// returns 0.
fn rr_pick(counter: &Cell<u16>, total_shards: u16, skip_zero: bool) -> u16 {
    let use_skip = skip_zero && total_shards > 1;
    let offset: u16 = u16::from(use_skip);
    let width = total_shards.saturating_sub(offset).max(1);
    let cur = counter.get();
    counter.set(cur.wrapping_add(1));
    offset + (cur % width)
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::net::{TcpListener, TcpStream};

    fn build_senders(total: u16) -> Rc<Vec<TaggedSender>> {
        let mut senders = Vec::with_capacity(total as usize);
        for shard_id in 0..total {
            let (tx, _rx) = crate::shard_channel(shard_id, 16);
            senders.push(tx);
        }
        Rc::new(senders)
    }

    fn build_senders_with_rx(
        total: u16,
    ) -> (Rc<Vec<TaggedSender>>, Vec<crate::Receiver<ShardFrame>>) {
        let mut senders = Vec::with_capacity(total as usize);
        let mut receivers = Vec::with_capacity(total as usize);
        for shard_id in 0..total {
            let (tx, rx) = crate::shard_channel(shard_id, 16);
            senders.push(tx);
            receivers.push(rx);
        }
        (Rc::new(senders), receivers)
    }

    #[test]
    fn ctor_rejects_permuted_sender_vec() {
        // Build senders in correct order, then swap two entries so the
        // indexed position no longer matches the tagged shard id.
        let (tx0, _rx0) = crate::shard_channel(0, 16);
        let (tx1, _rx1) = crate::shard_channel(1, 16);
        let (tx2, _rx2) = crate::shard_channel(2, 16);
        let (tx3, _rx3) = crate::shard_channel(3, 16);
        let permuted = Rc::new(vec![tx0, tx2, tx1, tx3]);
        let result = ShardZeroCoordinator::new(
            permuted,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        );
        assert!(
            matches!(
                result.as_ref().err(),
                Some(ShardCtorError::SenderOrderingInvalid { .. }),
            ),
            "expected SenderOrderingInvalid, got {:?}",
            result.as_ref().err(),
        );
        drop(result);
    }

    #[test]
    fn replica_rr_default_skips_shard_zero() {
        // Default config: replicas skip shard 0, clients include it.
        let senders = build_senders(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // Replicas wrap over [1, 4).
        assert_eq!(coord.next_replica_target(), 1);
        assert_eq!(coord.next_replica_target(), 2);
        assert_eq!(coord.next_replica_target(), 3);
        assert_eq!(coord.next_replica_target(), 1);
        assert_eq!(coord.next_replica_target(), 2);

        // Clients span [0, 4).
        assert_eq!(coord.next_client_target(), 0);
        assert_eq!(coord.next_client_target(), 1);
        assert_eq!(coord.next_client_target(), 2);
        assert_eq!(coord.next_client_target(), 3);
        assert_eq!(coord.next_client_target(), 0);
    }

    #[test]
    fn rr_includes_shard_zero_when_skip_flags_off() {
        let senders = build_senders(4);
        let cfg = CoordinatorConfig {
            skip_shard_zero_for_replicas: false,
            skip_shard_zero_for_clients: false,
        };
        let coord =
            ShardZeroCoordinator::new(senders, 4, cfg, crate::metrics::ShardMetrics::for_shard())
                .expect("coord ctor ok");

        assert_eq!(coord.next_replica_target(), 0);
        assert_eq!(coord.next_replica_target(), 1);
        assert_eq!(coord.next_replica_target(), 2);
        assert_eq!(coord.next_replica_target(), 3);
        assert_eq!(coord.next_replica_target(), 0);

        assert_eq!(coord.next_client_target(), 0);
        assert_eq!(coord.next_client_target(), 1);
    }

    #[test]
    fn rr_skips_shard_zero_when_both_flags_on() {
        let senders = build_senders(4);
        let cfg = CoordinatorConfig {
            skip_shard_zero_for_replicas: true,
            skip_shard_zero_for_clients: true,
        };
        let coord =
            ShardZeroCoordinator::new(senders, 4, cfg, crate::metrics::ShardMetrics::for_shard())
                .expect("coord ctor ok");

        for _ in 0..8 {
            let r = coord.next_replica_target();
            let c = coord.next_client_target();
            assert!((1..4).contains(&r), "replica target {r} must skip shard 0");
            assert!((1..4).contains(&c), "client target {c} must skip shard 0");
        }
    }

    #[test]
    fn rr_single_shard_returns_zero_regardless_of_flags() {
        let senders = build_senders(1);
        let cfg = CoordinatorConfig {
            skip_shard_zero_for_replicas: true,
            skip_shard_zero_for_clients: true,
        };
        let coord =
            ShardZeroCoordinator::new(senders, 1, cfg, crate::metrics::ShardMetrics::for_shard())
                .expect("coord ctor ok");
        for _ in 0..4 {
            assert_eq!(coord.next_replica_target(), 0);
            assert_eq!(coord.next_client_target(), 0);
        }
    }

    #[test]
    fn mint_client_id_encodes_target_shard() {
        let senders = build_senders(8);
        let coord = ShardZeroCoordinator::new(
            senders,
            8,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        let id = coord.mint_client_id(5);
        assert_eq!((id >> 112) as u16, 5);
        assert_eq!(id & ((1u128 << 112) - 1), 1, "first seq is 1");
    }

    // A restarted node recovers table entries keyed by the previous boot's
    // ids while the minter restarts at 1. Reseeding past the recovered
    // high-water mark is what keeps a fresh login off an existing entry.
    #[test]
    fn seed_client_sequence_mints_above_recovered_ids() {
        let senders = build_senders(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // Recovered ids carry their own shard tags; only the sequence matters.
        let recovered = [
            (1u128 << CLIENT_ID_SHARD_SHIFT) | 0x07,
            (3u128 << CLIENT_ID_SHARD_SHIFT) | 0x2a,
            9,
        ];
        // total_shards is 4, so tags 1 and 3 are this node's and fold in.
        coord.seed_client_sequence(0, recovered.into_iter());

        let id = coord.mint_client_id(2);
        assert_eq!(
            id & ((1u128 << CLIENT_ID_SHARD_SHIFT) - 1),
            0x2b,
            "must mint above the highest recovered sequence"
        );
        assert_eq!((id >> CLIENT_ID_SHARD_SHIFT) as u16, 2, "tag preserved");
    }

    // Never lower the counter: a later reseed (or an empty table) must not
    // hand back ids this process already minted.
    #[test]
    fn seed_client_sequence_never_lowers_the_counter() {
        let senders = build_senders(2);
        let coord = ShardZeroCoordinator::new(
            senders,
            2,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        for _ in 0..5 {
            let _ = coord.mint_client_id(0);
        }
        coord.seed_client_sequence(0, std::iter::empty());
        coord.seed_client_sequence(1, [1u128, 2].into_iter());

        let id = coord.mint_client_id(0);
        assert_eq!(
            id & ((1u128 << CLIENT_ID_SHARD_SHIFT) - 1),
            6,
            "counter must keep advancing from where minting left off"
        );
    }

    // The sequence must never carry into the shard tag: that tag is the
    // inter-shard reply routing key, so a carry would send a live connection's
    // replies to a different shard for the process lifetime. Reachable only
    // because the counter is seeded from recovered ids, and the TCP login path
    // takes `client` straight off the wire -- so a client can choose one near
    // the ceiling and a restart folds it in.
    #[test]
    fn mint_never_carries_the_sequence_into_the_shard_tag() {
        let senders = build_senders(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // A chosen id with a plausible tag, sequence at the ceiling.
        coord.seed_client_sequence(0, std::iter::once(CLIENT_SEQUENCE_MASK));
        for _ in 0..4 {
            let id = coord.mint_shard_zero_client_id();
            assert_eq!(
                id >> CLIENT_ID_SHARD_SHIFT,
                0,
                "shard-0 mint must keep tag 0, got id {id:#x}"
            );
            assert_ne!(id, 0, "the wire header rejects client == 0");
        }
    }

    // Ids a client chose for itself carry arbitrary tags. Folding those into
    // this node's counter would let one login steer it.
    #[test]
    fn seed_ignores_ids_tagged_outside_this_node() {
        let senders = build_senders(2);
        let coord = ShardZeroCoordinator::new(
            senders,
            2,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // Tag 9 is not a shard of a 2-shard node: caller-supplied, ignored.
        coord.seed_client_sequence(
            0,
            std::iter::once((9u128 << CLIENT_ID_SHARD_SHIFT) | 0x0f_ff_ff),
        );
        assert_eq!(
            coord.mint_shard_zero_client_id() & CLIENT_SEQUENCE_MASK,
            1,
            "an id tagged outside this node must not move the counter"
        );
    }

    // Promotion is the second moment the counter can be wrong: the new primary
    // mints against ids its predecessor committed from an unrelated counter.
    // A view change is the observable edge, and re-seeding within one view is
    // skipped so this is cheap enough to sit on the minting path.
    #[test]
    fn seed_refolds_the_table_once_per_view() {
        let senders = build_senders(2);
        let coord = ShardZeroCoordinator::new(
            senders,
            2,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        coord.seed_client_sequence(4, std::iter::once(0x10));
        assert_eq!(
            coord.mint_shard_zero_client_id() & CLIENT_SEQUENCE_MASK,
            0x11
        );

        // Same view: the table is not refolded, so a later entry is ignored.
        coord.seed_client_sequence(4, std::iter::once(0x80));
        assert_eq!(
            coord.mint_shard_zero_client_id() & CLIENT_SEQUENCE_MASK,
            0x12
        );

        // Promotion: new view, so the predecessor's high-water mark is folded.
        coord.seed_client_sequence(5, std::iter::once(0x80));
        assert_eq!(
            coord.mint_shard_zero_client_id() & CLIENT_SEQUENCE_MASK,
            0x81,
            "a promoted primary must mint above what its predecessor committed"
        );
    }

    #[test]
    fn shard_zero_local_and_delegated_ids_never_collide() {
        let senders = build_senders(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // Interleave shard-0-local mints (QUIC / TCP-TLS path) with
        // delegated mints that round-robin to shard 0. Both encode target
        // shard 0 in the top 16 bits; the shared counter keeps every id
        // distinct so they cannot overwrite each other in shard 0's
        // connection registry.
        let mut ids = std::collections::HashSet::new();
        for _ in 0..8 {
            assert!(ids.insert(coord.mint_shard_zero_client_id()));
            assert!(ids.insert(coord.mint_client_id(0)));
        }
        assert_eq!(
            ids.len(),
            16,
            "every minted shard-0 client id must be unique"
        );
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn delegate_replica_inbound_ships_setup_to_target_shard() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        // Loopback TCP pair so the delegation has a real fd to dup.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer_addr) = accept.await.unwrap();

        let target = coord
            .delegate_replica_inbound(client, 42)
            .expect("delegate ok");
        assert_eq!(
            target, 1,
            "first replica target skips shard 0 under default config",
        );

        // Target shard should observe ReplicaInboundSetup echoing the slot.
        let setup_frame = receivers[target as usize].recv().await.unwrap();
        match setup_frame {
            ShardFrame::Lifecycle(LifecycleFrame::ReplicaInboundSetup { fd, slot }) => {
                assert_eq!(slot, 42);
                drop(fd);
            }
            _ => panic!("expected ReplicaInboundSetup on target shard"),
        }

        // Non-target shards must not receive any frame: delegation
        // does not broadcast a mapping update, the shard-shared
        // owner table covers cross-shard routing.
        for (idx, rx) in receivers.iter().enumerate() {
            if idx == target as usize {
                continue;
            }
            assert!(
                rx.try_recv().is_err(),
                "shard {idx} unexpectedly received a frame after delegate_replica_inbound",
            );
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn delegate_replica_outbound_ships_setup_with_peer_id() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer_addr) = accept.await.unwrap();

        let target = coord
            .delegate_replica_outbound(client, 7)
            .expect("delegate ok");

        let setup_frame = receivers[target as usize].recv().await.unwrap();
        match setup_frame {
            ShardFrame::Lifecycle(LifecycleFrame::ReplicaOutboundSetup { fd, replica_id }) => {
                assert_eq!(replica_id, 7);
                drop(fd);
            }
            _ => panic!("expected ReplicaOutboundSetup on target shard"),
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn delegate_client_ships_setup_with_meta_transport_tcp() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer_addr) = accept.await.unwrap();

        let client_id = coord.delegate_client(client).expect("delegate ok");
        let target = (client_id >> 112) as u16;
        assert!(
            (0..4).contains(&target),
            "client target out of range; got {target}",
        );

        let setup_frame = receivers[target as usize].recv().await.unwrap();
        match setup_frame {
            ShardFrame::Lifecycle(LifecycleFrame::ClientConnectionSetup { fd, meta }) => {
                assert_eq!(meta.client_id, client_id);
                assert!(matches!(meta.transport, ClientTransportKind::Tcp));
                drop(fd);
            }
            _ => panic!("expected ClientConnectionSetup variant"),
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn delegate_ws_client_ships_setup_with_meta_transport_ws() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::new(
            senders,
            4,
            CoordinatorConfig::default(),
            crate::metrics::ShardMetrics::for_shard(),
        )
        .expect("coord ctor ok");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer_addr) = accept.await.unwrap();

        let client_id = coord.delegate_ws_client(client).expect("delegate ok");
        let target = (client_id >> 112) as u16;
        assert!(
            (0..4).contains(&target),
            "client target out of range; got {target}",
        );

        let setup_frame = receivers[target as usize].recv().await.unwrap();
        match setup_frame {
            ShardFrame::Lifecycle(LifecycleFrame::ClientWsConnectionSetup { fd, meta }) => {
                assert_eq!(meta.client_id, client_id);
                assert!(
                    matches!(meta.transport, ClientTransportKind::Ws),
                    "ws delegate must tag meta.transport = Ws, got {:?}",
                    meta.transport,
                );
                drop(fd);
            }
            _ => panic!("expected ClientWsConnectionSetup variant"),
        }
    }
}
