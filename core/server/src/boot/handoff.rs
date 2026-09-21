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

//! Cross-shard boot handoff: the metadata bundle broadcast and the
//! listener barrier.

use crate::server_error::ServerError;
use crate::shell::ServerMetadataBundle;
// `try_send` / `try_recv` resolve through these traits on `MAsyncTx` /
// `MAsyncRx`; the metadata-handoff loops below depend on the
// non-blocking variants for cancel-safe shutdown polling.
use crossfire::{AsyncRxTrait, AsyncTxTrait};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Shard-local end of the metadata bundle handoff.
///
/// Shard 0 owns the WAL writer and runs `recover()` to build the only
/// `WriteHandle`-bearing [`crate::shell::ServerMuxStateMachine`]. It then mints a
/// [`ServerMetadataBundle`] (a tuple of `Send + Sync`
/// `ReadHandleFactory`s) and pushes one clone per peer onto `bundle_tx`.
/// Every other shard receives the bundle and rebuilds a reader-mode
/// `MuxStateMachine` on its own runtime - no WAL access, no replay, no
/// `RecoverySync` two-phase fence. The old phase-2 WAL fence is gone
/// because peers no longer scan the WAL. They do still scan live shared
/// metadata to load their on-disk partitions, so a separate listener
/// fence is still required - see [`BootstrapBarrier`].
///
/// The channel is bounded to the peer count so shard 0's `send` never
/// blocks beyond a peer drain. A peer that dies before recv drops its
/// `bundle_rx`, so shard 0's `send` eventually sees a disconnected
/// channel; the cross-thread shutdown flag drives every waiter out of
/// its `recv` loop if shard 0 panics before broadcasting.
pub(in crate::boot) enum MetadataHandoff {
    Owner {
        bundle_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    },
    Waiter {
        bundle_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    },
}

/// Reverse handshake to [`MetadataHandoff`]: gates shard 0's client
/// listeners until every peer has loaded its on-disk partitions.
///
/// Peers build their owned-partition set from live shared metadata and
/// load each segment from disk in `build_shard_for_thread`. If shard 0
/// opened listeners the instant `broadcast_metadata_bundle` returned
/// (peers have only *received* the bundle, not *loaded* partitions), a
/// client could create a partition before a peer's load scan finished.
/// That freshly committed partition would surface in the peer's scan
/// with no segment dir on disk yet, and `load_partition`'s `walk_dir`
/// would fail with `CannotReadPartitions`, aborting the whole node. A
/// partition created after boot must take the runtime reconciler path
/// (which creates its dir), never the bootstrap load path.
///
/// Shard 0 (`Owner`) drains one signal per peer before binding
/// listeners; each peer (`Waiter`) sends one once its load completes.
/// The cross-thread shutdown flag drives both sides out of their poll
/// loop if any shard dies mid-boot.
pub(in crate::boot) enum BootstrapBarrier {
    Owner {
        ready_rx: crossfire::MAsyncRx<crossfire::mpmc::Array<u16>>,
    },
    Waiter {
        ready_tx: crossfire::MAsyncTx<crossfire::mpmc::Array<u16>>,
    },
}

/// Block until shard 0 broadcasts the metadata factory bundle, or the
/// cross-thread shutdown flag flips. Polled in a `poll_interval` loop
/// so a shard 0 that panics before it broadcasts cannot strand peer
/// shards: the shutdown path flips the flag, every waiter observes it
/// on the next tick, and the server tears down instead of hanging.
///
/// Uses `try_recv` + sleep rather than `timeout(recv())`. Crossfire 3.x
/// documents `recv()` as cancellation-safe (no leak/deadlock) but does
/// not guarantee atomicity for the dropped future's result; `try_recv`
/// keeps each tick fully synchronous and side-effect-free, so the
/// shutdown poll cadence cannot ambiguously consume a bundle.
pub(in crate::boot) async fn await_metadata_bundle(
    shard_id: u16,
    bundle_rx: &crossfire::MAsyncRx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<ServerMetadataBundle, ServerError> {
    loop {
        match bundle_rx.try_recv() {
            Ok(bundle) => return Ok(bundle),
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
                }
                compio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Push `peers` cloned bundles onto `bundle_tx`, polling each send in a
/// `poll_interval` loop so the cross-thread shutdown flag can interrupt
/// a stalled handoff. Symmetric to [`await_metadata_bundle`]: shutdown
/// observed mid-handshake aborts cleanly rather than stalling on a
/// `send` future that can no longer make progress.
///
/// Uses `try_send` + sleep rather than `timeout(send())`. Crossfire 3.x
/// documents `send()` as cancellation-safe in the leak/deadlock sense
/// but explicitly warns the true result is unknown when `SendFuture` is
/// dropped on cancellation. For a retry loop that re-clones on every
/// tick that would risk publishing the same bundle twice, stuffing the
/// bounded channel past `peers` and stranding a follow-up `send`.
/// `try_send` returns the bundle back inside `TrySendError::Full`, so
/// the loop reuses it instead of re-cloning when the channel is full.
pub(in crate::boot) async fn broadcast_metadata_bundle(
    shard_id: u16,
    bundle_tx: &crossfire::MAsyncTx<crossfire::mpmc::Array<ServerMetadataBundle>>,
    bundle: ServerMetadataBundle,
    peers: u16,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerError> {
    for _ in 0..peers {
        let mut pending = bundle.clone();
        loop {
            match bundle_tx.try_send(pending) {
                Ok(()) => break,
                Err(crossfire::TrySendError::Disconnected(_)) => {
                    // Every peer dropped its `bundle_rx` before recv. Shard
                    // 0 must not silently continue past handoff: it would
                    // bind listeners and commit consensus state for a
                    // cluster whose peers are gone. Propagate the abort so
                    // `shard_main` short-circuits before further side
                    // effects; `shutdown_flag` will flip via the normal
                    // teardown path.
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
                }
                Err(crossfire::TrySendError::Full(returned)) => {
                    if shutdown_flag.load(Ordering::Relaxed) {
                        return Err(ServerError::MetadataHandoffAborted { shard_id });
                    }
                    pending = returned;
                    compio::time::sleep(poll_interval).await;
                }
            }
        }
    }
    Ok(())
}

/// Peer side of [`BootstrapBarrier`]: tell shard 0 this shard finished
/// loading its on-disk partitions. Mirrors [`broadcast_metadata_bundle`]'s
/// `try_send`-or-shutdown poll loop so a sibling failure (which flips the
/// shutdown flag) drives this out instead of stranding it on a full
/// channel. The channel is sized to the peer count and each peer sends
/// exactly once, so `Full` is not expected; the branch only keeps the
/// loop interruptible.
pub(in crate::boot) async fn signal_bootstrap_complete(
    shard_id: u16,
    ready_tx: &crossfire::MAsyncTx<crossfire::mpmc::Array<u16>>,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerError> {
    let mut pending = shard_id;
    loop {
        match ready_tx.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(crossfire::TrySendError::Disconnected(_)) => {
                // Shard 0 dropped its `ready_rx` before draining (it
                // aborted before binding listeners). Propagate so this
                // shard short-circuits; the shutdown flag flips via the
                // normal teardown path.
                return Err(ServerError::MetadataHandoffAborted { shard_id });
            }
            Err(crossfire::TrySendError::Full(returned)) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::MetadataHandoffAborted { shard_id });
                }
                pending = returned;
                compio::time::sleep(poll_interval).await;
            }
        }
    }
}

/// Owner side of [`BootstrapBarrier`]: drain one ready signal per peer
/// before shard 0 binds listeners. Polls the shutdown flag so a peer that
/// dies mid-load (flipping the flag) aborts the wait instead of hanging on
/// a signal that will never arrive. A single shard (`peers == 0`) returns
/// immediately.
pub(in crate::boot) async fn await_bootstrap_complete(
    ready_rx: &crossfire::MAsyncRx<crossfire::mpmc::Array<u16>>,
    peers: usize,
    shutdown_flag: &Arc<AtomicBool>,
    poll_interval: Duration,
) -> Result<(), ServerError> {
    let mut remaining = peers;
    while remaining > 0 {
        match ready_rx.try_recv() {
            Ok(_shard_id) => remaining -= 1,
            Err(crossfire::TryRecvError::Disconnected) => {
                return Err(ServerError::ShardBootstrapBarrierAborted { remaining });
            }
            Err(crossfire::TryRecvError::Empty) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return Err(ServerError::ShardBootstrapBarrierAborted { remaining });
                }
                compio::time::sleep(poll_interval).await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shell::ServerMuxStateMachine;

    const TEST_POLL_INTERVAL: Duration = Duration::from_millis(50);

    #[compio::test]
    async fn broadcast_metadata_bundle_returns_immediately_with_no_peers() {
        // Single-shard deployment: shard 0 has no peers to fan out to,
        // so the handoff must complete without ever calling `send`.
        let (bundle_tx, _bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(0);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerMuxStateMachine::default();
        broadcast_metadata_bundle(
            0,
            &bundle_tx,
            mux.factory_bundle(),
            0,
            &flag,
            TEST_POLL_INTERVAL,
        )
        .await
        .expect("zero peers must not block shard 0");
    }

    #[compio::test]
    async fn metadata_bundle_round_trips_through_channel() {
        // End-to-end: shard 0 mints a bundle, a peer receives it on
        // another runtime, and `from_factory_bundle` constructs a
        // reader-mode mux that observes shard 0's writes via the same
        // LeftRight pair.
        let peers = 1u16;
        let (bundle_tx, bundle_rx) =
            crossfire::mpmc::bounded_async::<ServerMetadataBundle>(usize::from(peers));
        let flag = Arc::new(AtomicBool::new(false));

        let owner = ServerMuxStateMachine::default();
        let bundle = owner.factory_bundle();
        broadcast_metadata_bundle(0, &bundle_tx, bundle, peers, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("broadcast must succeed with one peer drained");

        let received = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer must receive the broadcast bundle");
        let _peer_mux = ServerMuxStateMachine::from_factory_bundle(received);
    }

    #[compio::test]
    async fn broadcast_metadata_bundle_aborts_when_peers_drop_rx() {
        // Shard 0 drives handoff but every peer's `bundle_rx` was dropped
        // before recv. Silently returning Ok would commit listener binds
        // and consensus init for a cluster whose peers are gone; the
        // broadcast must surface the disconnect so `shard_main` aborts.
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(0);
        drop(bundle_rx);
        let flag = Arc::new(AtomicBool::new(false));
        let mux = ServerMuxStateMachine::default();

        let err = broadcast_metadata_bundle(
            0,
            &bundle_tx,
            mux.factory_bundle(),
            3,
            &flag,
            TEST_POLL_INTERVAL,
        )
        .await
        .expect_err("dropped rx must surface as MetadataHandoffAborted");
        assert!(
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 0 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_when_owner_drops_without_sending() {
        let (bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(1);
        let flag = Arc::new(AtomicBool::new(false));

        // Shard 0 dies before broadcasting; the peer must observe the
        // disconnect and abort instead of hanging forever.
        drop(bundle_tx);

        let err = await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect_err("a peer whose owner never sends must abort");
        assert!(
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_metadata_bundle_aborts_on_shutdown_flag() {
        // compio 0.19 `JoinHandle` yields `Result<T, JoinError>`; the
        // `ResumeUnwind` impl re-raises a task panic and maps cancellation
        // to `None`.
        use compio::runtime::ResumeUnwind;

        let (_bundle_tx, bundle_rx) = crossfire::mpmc::bounded_async::<ServerMetadataBundle>(1);
        let flag = Arc::new(AtomicBool::new(false));

        let waiter = compio::runtime::spawn({
            let flag = Arc::clone(&flag);
            async move { await_metadata_bundle(1, &bundle_rx, &flag, TEST_POLL_INTERVAL).await }
        });

        // Owner has not sent yet, but shutdown was requested; the peer
        // must exit via the flag poll instead of hanging.
        compio::time::sleep(TEST_POLL_INTERVAL / 2).await;
        flag.store(true, Ordering::Relaxed);

        let err = waiter
            .await
            .resume_unwind()
            .expect("waiter task was cancelled")
            .expect_err("shutdown flag must abort the bundle wait");
        assert!(
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 1 }),
            "expected MetadataHandoffAborted on shutdown, got {err:?}"
        );
    }

    #[compio::test]
    async fn await_bootstrap_complete_returns_immediately_for_single_shard() {
        // A single-shard server has no peers to wait on; the owner barrier
        // must not block when `peers == 0`.
        let (_ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));
        await_bootstrap_complete(&ready_rx, 0, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("single-shard server must not block on the barrier");
    }

    #[compio::test]
    async fn await_bootstrap_complete_drains_every_peer_signal() {
        // Two peers report load-complete; shard 0 drains both, then proceeds
        // to bind listeners.
        let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(2);
        let flag = Arc::new(AtomicBool::new(false));
        signal_bootstrap_complete(1, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer 1 must signal load-complete");
        signal_bootstrap_complete(2, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("peer 2 must signal load-complete");
        await_bootstrap_complete(&ready_rx, 2, &flag, TEST_POLL_INTERVAL)
            .await
            .expect("owner must drain both peer signals");
    }

    #[compio::test]
    async fn await_bootstrap_complete_aborts_on_shutdown_flag() {
        use compio::runtime::ResumeUnwind;

        // `_ready_tx` is held so the channel is not disconnected: the owner
        // must exit via the shutdown flag, not a dropped sender.
        let (_ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));

        let owner = compio::runtime::spawn({
            let flag = Arc::clone(&flag);
            async move { await_bootstrap_complete(&ready_rx, 1, &flag, TEST_POLL_INTERVAL).await }
        });

        // The peer never signals, but a sibling failure flips the flag; the
        // owner must abort instead of hanging before listeners.
        compio::time::sleep(TEST_POLL_INTERVAL / 2).await;
        flag.store(true, Ordering::Relaxed);

        let err = owner
            .await
            .resume_unwind()
            .expect("owner task was cancelled")
            .expect_err("shutdown flag must abort the barrier wait");
        assert!(
            matches!(
                err,
                ServerError::ShardBootstrapBarrierAborted { remaining: 1 }
            ),
            "expected ShardBootstrapBarrierAborted, got {err:?}"
        );
    }

    #[compio::test]
    async fn signal_bootstrap_complete_aborts_when_owner_drops_rx() {
        // Shard 0 aborted before draining and dropped its receiver; a peer's
        // signal must surface the disconnect instead of stranding.
        let (ready_tx, ready_rx) = crossfire::mpmc::bounded_async::<u16>(1);
        let flag = Arc::new(AtomicBool::new(false));
        drop(ready_rx);

        let err = signal_bootstrap_complete(2, &ready_tx, &flag, TEST_POLL_INTERVAL)
            .await
            .expect_err("dropped rx must surface as an abort");
        assert!(
            matches!(err, ServerError::MetadataHandoffAborted { shard_id: 2 }),
            "expected MetadataHandoffAborted, got {err:?}"
        );
    }
}
