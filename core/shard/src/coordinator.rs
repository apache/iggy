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
//! 3. sends a `ShardFramePayload::{Replica,Client}ConnectionSetup` frame to
//!    the target shard's inbox,
//! 4. drops its own `TcpStream` so only the target shard's wrapped fd
//!    keeps the socket alive,
//! 5. broadcasts the resulting replica -> owning shard mapping so every
//!    bus' `send_to_replica` slow path can route to the owner.
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
use crate::{ShardFrame, ShardFramePayload, TaggedSender, assert_sender_ordering};
use compio::net::TcpStream;
use compio::runtime::JoinHandle;
use message_bus::SendError;
use message_bus::fd_transfer;
use message_bus::lifecycle::ShutdownToken;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, warn};

/// Coordinator owned by shard 0 only.
///
/// Wrapped in `Rc` by the bootstrap and shared with the replica listener,
/// the connector, and the client listener so each of those paths can
/// delegate immediately.
pub struct ShardZeroCoordinator<R: Send + 'static = ()> {
    /// Inter-shard channel senders, indexed by shard id.
    ///
    /// Each [`TaggedSender`] carries the id of the shard whose paired
    /// receiver drains it, and the ctor asserts `senders[i].shard_id() == i`.
    /// The previous plain-`Sender` form was a silent-misroute hazard: a Vec
    /// with correct length but permuted ordering had no way to be caught.
    senders: Rc<Vec<TaggedSender<R>>>,
    total_shards: u16,
    cfg: CoordinatorConfig,
    replica_rr: Cell<u16>,
    client_rr: Cell<u16>,
    client_seq: Cell<u128>,
    /// Authoritative `replica_id -> owning_shard` view as understood by the
    /// coordinator. Populated on successful `delegate_replica`, cleared by
    /// [`forget_mapping`](Self::forget_mapping) when a replica dies. The
    /// periodic refresh task re-broadcasts this snapshot so shards whose
    /// inbox was full when the original `ReplicaMappingUpdate` was sent
    /// recover their mapping on the next tick rather than staying silently
    /// stale until the replica reconnects.
    ///
    /// Flat `[Option<u16>; 256]` to mirror `ReplicaRegistry` and avoid a
    /// hash lookup on the refresh hot path. 512 bytes per coordinator.
    mappings: RefCell<[Option<u16>; 256]>,
}

impl<R: Send + 'static> ShardZeroCoordinator<R> {
    /// # Panics
    ///
    /// Panics if `senders.len() != total_shards`, if `total_shards < 1`, or
    /// if any `senders[i].shard_id() != i`. Permuted senders are a
    /// bootstrap programming error; `TaggedSender` lifts the ordering
    /// invariant from a doc comment to a ctor assertion.
    #[must_use]
    pub fn new(
        senders: Rc<Vec<TaggedSender<R>>>,
        total_shards: u16,
        cfg: CoordinatorConfig,
    ) -> Self {
        assert_eq!(
            senders.len(),
            total_shards as usize,
            "senders must have one entry per shard",
        );
        assert!(total_shards >= 1, "total_shards must be at least 1");
        assert_sender_ordering(&senders);
        Self {
            senders,
            total_shards,
            cfg,
            replica_rr: Cell::new(0),
            client_rr: Cell::new(0),
            client_seq: Cell::new(1),
            mappings: RefCell::new([None; 256]),
        }
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
    fn mint_client_id(&self, target_shard: u16) -> u128 {
        let seq = self.client_seq.get();
        self.client_seq.set(seq.wrapping_add(1));
        (u128::from(target_shard) << 112) | seq
    }

    /// Ship a replica TCP connection to the next round-robin target shard.
    ///
    /// On success broadcasts a `ReplicaMappingUpdate` to every shard and
    /// returns `Ok(target_shard)`. On inter-shard channel failure closes
    /// the duplicated fd and returns `Err(SendError::RoutingFailed)`.
    ///
    /// # Errors
    ///
    /// Returns an error when `dup(2)` fails or when the target shard's
    /// inbox refuses the setup frame (full or disconnected).
    pub fn delegate_replica(&self, stream: TcpStream, replica_id: u8) -> Result<u16, SendError> {
        let target = self.next_replica_target();
        let raw_fd = fd_transfer::dup_fd(&stream)
            .map_err(|e| SendError::DupFailed(e.raw_os_error().unwrap_or(-1)))?;

        let setup = ShardFramePayload::ReplicaConnectionSetup { raw_fd, replica_id };
        if let Err(e) = self.senders[target as usize].try_send(ShardFrame::lifecycle(setup)) {
            warn!(
                replica_id,
                target, "delegate_replica try_send failed: {e:?}"
            );
            fd_transfer::close_fd(raw_fd);
            return Err(SendError::RoutingFailed(target));
        }

        // Shard 0 drops the original stream; the target shard's dup keeps
        // the socket open.
        drop(stream);

        // Record the mapping on the coordinator's authoritative snapshot
        // BEFORE broadcasting: if `broadcast_mapping_update` drops a frame
        // on a full inbox, the periodic refresh task reads this snapshot
        // and re-broadcasts.
        self.mappings.borrow_mut()[usize::from(replica_id)] = Some(target);
        self.broadcast_mapping_update(replica_id, target);

        Ok(target)
    }

    fn broadcast_mapping_update(&self, replica_id: u8, owning_shard: u16) {
        // Broadcast to every shard (including shard 0 and the owner
        // itself). Drops on full inbox are tolerable: the owning shard
        // already holds the fd and the periodic refresh task reconciles
        // any missed mapping on its next tick.
        for sender in self.senders.iter() {
            let update = ShardFramePayload::ReplicaMappingUpdate {
                replica_id,
                owning_shard,
            };
            if let Err(e) = sender.try_send(ShardFrame::lifecycle(update)) {
                debug!(
                    shard_id = sender.shard_id(),
                    replica_id, "mapping update try_send failed: {e:?}"
                );
            }
        }
    }

    /// Ship a client TCP connection to the next round-robin target shard.
    ///
    /// On success returns the minted client id. On failure closes the
    /// duplicated fd and returns an error.
    ///
    /// # Errors
    ///
    /// Returns an error when `dup(2)` fails or the target shard's inbox
    /// refuses the setup frame.
    pub fn delegate_client(&self, stream: TcpStream) -> Result<u128, SendError> {
        let target = self.next_client_target();
        let client_id = self.mint_client_id(target);

        let raw_fd = fd_transfer::dup_fd(&stream)
            .map_err(|e| SendError::DupFailed(e.raw_os_error().unwrap_or(-1)))?;
        let setup = ShardFramePayload::ClientConnectionSetup { raw_fd, client_id };
        if let Err(e) = self.senders[target as usize].try_send(ShardFrame::lifecycle(setup)) {
            warn!(client_id, target, "delegate_client try_send failed: {e:?}");
            fd_transfer::close_fd(raw_fd);
            return Err(SendError::RoutingFailed(target));
        }

        drop(stream);
        Ok(client_id)
    }

    /// Broadcast a `ReplicaMappingClear` to every shard. Used by the
    /// `ConnectionLost` handler before the next `delegate_replica` runs.
    pub fn broadcast_mapping_clear(&self, replica_id: u8) {
        crate::broadcast_mapping_clear(&self.senders, replica_id);
    }

    /// Drop the coordinator's authoritative entry for `replica_id`, so
    /// the periodic refresh task does not keep re-broadcasting the
    /// mapping of a replica that has disconnected.
    ///
    /// Call from shard 0's `ConnectionLost` handler paired with
    /// [`broadcast_mapping_clear`](Self::broadcast_mapping_clear).
    pub fn forget_mapping(&self, replica_id: u8) {
        self.mappings.borrow_mut()[usize::from(replica_id)] = None;
    }

    /// Re-broadcast every mapping the coordinator is tracking.
    ///
    /// A `delegate_replica` broadcast is `try_send` best-effort: a shard
    /// whose inbox was full at the moment of the original broadcast
    /// permanently lost that mapping and `send_to_replica` on that shard
    /// would return `ReplicaNotConnected` until the next reconnect sweep
    /// reinstalled the peer. Periodic refresh covers that gap.
    pub fn broadcast_mapping_snapshot(&self) {
        // Collect into a small stack buffer to release the `RefCell`
        // borrow before broadcasting (try_send is sync but the borrow is
        // cheap to drop first for consistency with other mapping paths).
        let snapshot: Vec<(u8, u16)> = self
            .mappings
            .borrow()
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| {
                slot.map(|owner| {
                    #[allow(clippy::cast_possible_truncation)]
                    (idx as u8, owner)
                })
            })
            .collect();
        for (replica_id, owning_shard) in snapshot {
            self.broadcast_mapping_update(replica_id, owning_shard);
        }
    }

    /// Spawn a compio task that calls [`broadcast_mapping_snapshot`] every
    /// `period` until `token` fires. Bootstrap is expected to track the
    /// returned handle on the bus's background tasks so graceful shutdown
    /// awaits it.
    pub fn spawn_refresh_task(
        self: &Rc<Self>,
        token: ShutdownToken,
        period: Duration,
    ) -> JoinHandle<()> {
        let coord = Rc::clone(self);
        compio::runtime::spawn(async move {
            while token.sleep_or_shutdown(period).await {
                coord.broadcast_mapping_snapshot();
            }
            debug!("coordinator mapping refresh task exiting");
        })
    }

    #[must_use]
    pub const fn total_shards(&self) -> u16 {
        self.total_shards
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
            let (tx, _rx) = crate::shard_channel::<()>(shard_id, 16);
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
            let (tx, rx) = crate::shard_channel::<()>(shard_id, 16);
            senders.push(tx);
            receivers.push(rx);
        }
        (Rc::new(senders), receivers)
    }

    #[test]
    #[should_panic(expected = "inter-shard vec must be in canonical order")]
    fn ctor_rejects_permuted_sender_vec() {
        // Build senders in correct order, then swap two entries so the
        // indexed position no longer matches the tagged shard id.
        let (tx0, _rx0) = crate::shard_channel::<()>(0, 16);
        let (tx1, _rx1) = crate::shard_channel::<()>(1, 16);
        let (tx2, _rx2) = crate::shard_channel::<()>(2, 16);
        let (tx3, _rx3) = crate::shard_channel::<()>(3, 16);
        let permuted = Rc::new(vec![tx0, tx2, tx1, tx3]);
        let _coord = ShardZeroCoordinator::<()>::new(permuted, 4, CoordinatorConfig::default());
    }

    #[test]
    fn replica_rr_default_skips_shard_zero() {
        // Default config: replicas skip shard 0, clients include it.
        let senders = build_senders(4);
        let coord = ShardZeroCoordinator::<()>::new(senders, 4, CoordinatorConfig::default());

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
            ..CoordinatorConfig::default()
        };
        let coord = ShardZeroCoordinator::<()>::new(senders, 4, cfg);

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
            ..CoordinatorConfig::default()
        };
        let coord = ShardZeroCoordinator::<()>::new(senders, 4, cfg);

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
            ..CoordinatorConfig::default()
        };
        let coord = ShardZeroCoordinator::<()>::new(senders, 1, cfg);
        for _ in 0..4 {
            assert_eq!(coord.next_replica_target(), 0);
            assert_eq!(coord.next_client_target(), 0);
        }
    }

    #[test]
    fn mint_client_id_encodes_target_shard() {
        let senders = build_senders(8);
        let coord = ShardZeroCoordinator::<()>::new(senders, 8, CoordinatorConfig::default());

        let id = coord.mint_client_id(5);
        assert_eq!((id >> 112) as u16, 5);
        assert_eq!(id & ((1u128 << 112) - 1), 1, "first seq is 1");
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn delegate_replica_sends_setup_then_broadcasts_mapping() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::<()>::new(senders, 4, CoordinatorConfig::default());

        // Loopback TCP pair so delegate_replica has a real fd to dup.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = compio::runtime::spawn(async move { listener.accept().await.unwrap() });
        let client = TcpStream::connect(addr).await.unwrap();
        let (_server, _peer_addr) = accept.await.unwrap();

        let target = coord.delegate_replica(client, 7).expect("delegate ok");
        assert_eq!(
            target, 1,
            "first replica target skips shard 0 under default config",
        );

        // Target shard should observe ReplicaConnectionSetup.
        let setup_frame = receivers[target as usize].recv().await.unwrap();
        match setup_frame.payload {
            ShardFramePayload::ReplicaConnectionSetup { raw_fd, replica_id } => {
                assert_eq!(replica_id, 7);
                // Clean up the dup'd fd so the test does not leak.
                fd_transfer::close_fd(raw_fd);
            }
            _ => panic!("expected ReplicaConnectionSetup on target shard"),
        }

        // Every shard (including shard 0 and the target itself) should
        // observe the ReplicaMappingUpdate broadcast.
        for (idx, rx) in receivers.iter().enumerate() {
            let frame = rx.recv().await.unwrap();
            match frame.payload {
                ShardFramePayload::ReplicaMappingUpdate {
                    replica_id,
                    owning_shard,
                } => {
                    assert_eq!(replica_id, 7, "shard {idx} mapping update replica id");
                    assert_eq!(owning_shard, target, "shard {idx} mapping update owner");
                }
                _ => panic!("shard {idx} expected ReplicaMappingUpdate"),
            }
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn broadcast_mapping_clear_reaches_every_shard() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = ShardZeroCoordinator::<()>::new(senders, 4, CoordinatorConfig::default());

        coord.broadcast_mapping_clear(9);

        for (idx, rx) in receivers.iter().enumerate() {
            let frame = rx.recv().await.unwrap();
            match frame.payload {
                ShardFramePayload::ReplicaMappingClear { replica_id } => {
                    assert_eq!(replica_id, 9, "shard {idx} mapping clear replica id");
                }
                _ => panic!("shard {idx} expected ReplicaMappingClear"),
            }
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn snapshot_rebroadcasts_every_tracked_mapping() {
        let (senders, receivers) = build_senders_with_rx(4);
        let coord = Rc::new(ShardZeroCoordinator::<()>::new(
            senders,
            4,
            CoordinatorConfig::default(),
        ));

        // Seed the coordinator's tracked mappings directly (delegate_replica
        // needs a real TCP fd; the snapshot path is orthogonal to dup).
        coord.mappings.borrow_mut()[3] = Some(1);
        coord.mappings.borrow_mut()[7] = Some(2);

        coord.broadcast_mapping_snapshot();

        // Each shard must observe an update for each seeded mapping. The
        // order across replica ids is unspecified (HashMap->Vec iteration
        // on the collected snapshot), so collect and compare as a set.
        for (idx, rx) in receivers.iter().enumerate() {
            let mut observed = std::collections::BTreeSet::new();
            for _ in 0..2 {
                let frame = rx.recv().await.unwrap();
                match frame.payload {
                    ShardFramePayload::ReplicaMappingUpdate {
                        replica_id,
                        owning_shard,
                    } => {
                        observed.insert((replica_id, owning_shard));
                    }
                    _ => panic!("shard {idx} expected ReplicaMappingUpdate"),
                }
            }
            let expected: std::collections::BTreeSet<_> =
                [(3u8, 1u16), (7u8, 2u16)].into_iter().collect();
            assert_eq!(
                observed, expected,
                "shard {idx} did not receive the full snapshot"
            );
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn forget_mapping_prunes_entry_from_future_snapshots() {
        let (senders, _receivers) = build_senders_with_rx(2);
        let coord = ShardZeroCoordinator::<()>::new(senders, 2, CoordinatorConfig::default());

        coord.mappings.borrow_mut()[4] = Some(1);
        coord.mappings.borrow_mut()[5] = Some(1);
        coord.forget_mapping(4);

        assert!(coord.mappings.borrow()[4].is_none());
        assert_eq!(coord.mappings.borrow()[5], Some(1));
    }
}
