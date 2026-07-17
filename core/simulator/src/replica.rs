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

use crate::bus::{SharedSimOutbox, SimOutbox};
use crate::deps::{MemStorage, SimJournal, SimMuxStateMachine, SimSnapshot};
use configs::server_ng::NgSystemConfig;
use consensus::{ConsensusClock, LocalPipeline, VsrConsensus};
use iggy_common::IggyByteSize;
use iggy_common::variadic;
use metadata::IggyMetadata;
use metadata::stm::mux::WithFactory;
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use partitions::{IggyPartitions, PartitionsConfig};
use server_common::crypto;
use server_common::sharding::{METADATA_CONSENSUS_NAMESPACE, ShardId};
use server_ng::bootstrap::{ShellHandlers, ShellShardHandle, wire_shell_handlers};
use shard::shards_table::PapayaShardsTable;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

// TODO: Make configurable
const CLUSTER_ID: u128 = 1;

/// Deterministic root credentials the dispatch-shell `SimClient` logs in with.
///
/// Fixed (never env-derived) so the login handshake replays with the seed.
/// Both satisfy the 3..=50 / 3..=100 username/password length bounds
/// `verify_login_credentials` enforces.
pub const SHELL_ROOT_USERNAME: &str = "iggy";
pub const SHELL_ROOT_PASSWORD: &str = "iggy";

// For now there is only one shard per replica,
// we will add support for multiple shards per replica in the future.
//
// `PapayaShardsTable` (the production namespace -> shard routing table)
// instead of the always-`None` `()` impl: each shard owns its own table
// instance, exactly as server-ng wires it. Until rows are seeded the
// router falls back to the deterministic hash assignment, which at one
// shard per replica always resolves to shard 0.
pub type Replica = shard::IggyShard<
    SharedSimOutbox,
    SimJournal<MemStorage>, // MJ: metadata journal
    SimSnapshot,
    SimMuxStateMachine,
    PapayaShardsTable,
>;

/// Read-side handoff bundle for the metadata STM.
///
/// Shard 0 (the sole writer) mints one via `factory_bundle`; every peer shard
/// rebuilds a reader-mode mirror from it with `from_factory_bundle`, exactly as
/// server-ng bootstrap does with its `ServerNgMetadataBundle`.
/// `Clone + Send + Sync`.
pub type SimMetadataBundle = <variadic!(Users, Streams) as WithFactory>::Bundle;

/// Capacity of each shard inbox in the simulator.
///
/// The router drops frames when an inbox is full (recovered by VSR
/// retransmit in production); sized generously so no drop can happen
/// unless a test injects one on purpose. Tests assert the frame-drop
/// metrics stay zero.
pub const SIM_INBOX_CAPACITY: usize = 8192;

/// Build one shard of a sim replica around the real inter-shard mesh.
///
/// `senders`/`inbox` come from [`shard::shard_mesh_channels`], so the
/// shard routes through the same `dispatch` -> inbox -> pump path as
/// production instead of the old `without_inbox` bypass.
///
/// `shell` selects the dispatch handlers. Off is the fast path: inert
/// no-ops, so the simulator drives raw client frames straight into
/// `IggyShard::on_message`. On wires server-ng's real deferred dispatch
/// handlers (via [`wire_shell_handlers`]), exactly as production does, so
/// a client request runs as a task concurrent with the pump.
///
/// Mirrors server-ng bootstrap's single-writer metadata: the consensus
/// group, journal, snapshot, and the only writable metadata STM live on
/// shard 0. Shard 0 mints a [`SimMetadataBundle`] (returned as the second
/// tuple element); every peer shard passes it back in as `reader_bundle`
/// and rebuilds a reader-mode STM that observes shard 0's committed
/// metadata through the shared left-right read handle. So a partition op
/// homing on a peer shard resolves its namespace against consensus-committed
/// streams, rather than a stale per-shard copy. `reader_bundle` is `None`
/// for shard 0 and `Some` for every peer; the return bundle is `Some` only
/// for shard 0.
///
/// # Panics
/// Panics if `senders` is not in canonical order (a bug in the caller's
/// mesh construction), or if `reader_bundle` disagrees with `shard_idx`
/// (peer without a bundle, or shard 0 with one).
#[allow(clippy::too_many_arguments)]
pub fn new_shard(
    replica_id: u8,
    shard_idx: u16,
    name: String,
    bus: &Rc<SimOutbox>,
    replica_count: u8,
    senders: Vec<shard::TaggedSender>,
    inbox: shard::Receiver<shard::ShardFrame>,
    clock: ConsensusClock,
    shell: bool,
    reader_bundle: Option<SimMetadataBundle>,
) -> (Rc<Replica>, Option<SimMetadataBundle>) {
    // Metadata is single-writer, mirroring server-ng bootstrap. Shard 0 owns
    // the only writable STM; every peer shard rebuilds a reader-mode mirror from
    // shard 0's factory bundle and sees committed metadata through the shared
    // left-right read handle (each apply `publish`es, bounding reader staleness
    // to one op). Writes never target a peer, so peers seed nothing locally;
    // `reader_bundle` is `Some` exactly for them.
    debug_assert_eq!(
        shard_idx == 0,
        reader_bundle.is_none(),
        "shard 0 is the metadata writer (no bundle); peers are readers (bundle)"
    );
    let (mux, metadata_bundle) = reader_bundle.map_or_else(
        // Writer shard (shard 0). Seed the root user at slab id 0, matching
        // production bootstrap (`ensure_root_user`); it is undeletable in-apply,
        // so it stays in slot 0 and the workload's users land at slab id 1+, out
        // of any delete attempt. Peer shards inherit it through the read handle,
        // so the seed runs only here. Shell mode logs a client in against this
        // user (the login gate Argon2-checks the stored hash), so seed a real
        // hash of `SHELL_ROOT_PASSWORD`; otherwise a deterministic placeholder,
        // since login never runs and a random per-run argon2 salt would break
        // state equality.
        || {
            let users: Users = UsersInner::new().into();
            let root_password_hash = if shell {
                crypto::hash_with_fixed_salt(SHELL_ROOT_PASSWORD)
            } else {
                "hash".to_string()
            };
            users.ensure_root_user(SHELL_ROOT_USERNAME, &root_password_hash);
            let streams: Streams = StreamsInner::new().into();
            let mux = SimMuxStateMachine::new(variadic!(users, streams));
            let bundle = mux.factory_bundle();
            (mux, Some(bundle))
        },
        // Peer shard: reader-mode mirror over shard 0's shared read handle.
        |bundle| (SimMuxStateMachine::from_factory_bundle(bundle), None),
    );

    // The production metadata consensus namespace, not 0: the router
    // routes Register/Logout and metadata view-change frames to shard 0
    // by comparing against this exact value; anything else hashes to an
    // arbitrary shard with no metadata consensus.
    let metadata_consensus = (shard_idx == 0).then(|| {
        let consensus = VsrConsensus::with_clock(
            CLUSTER_ID,
            replica_id,
            replica_count,
            METADATA_CONSENSUS_NAMESPACE,
            SharedSimOutbox(Rc::clone(bus)),
            LocalPipeline::new(),
            clock.clone(),
        );
        consensus.init();
        consensus
    });
    let metadata_journal = (shard_idx == 0).then(SimJournal::<MemStorage>::default);
    let metadata_snapshot = (shard_idx == 0).then(SimSnapshot::default);

    let metadata = IggyMetadata::new(
        metadata_consensus,
        metadata_journal,
        metadata_snapshot,
        mux,
        None,
    );

    let partitions_config = PartitionsConfig {
        messages_required_to_save: 1000,
        size_of_messages_required_to_save: IggyByteSize::from(4 * 1024 * 1024),
        enforce_fsync: false, //Disable fsync for simulation
        segment_size: IggyByteSize::from(1024 * 1024 * 1024),
        encryptor: None,
    };

    // Shard id is the NODE-LOCAL shard index, never the replica id: the
    // router stamps `target_shard` with the mesh index and
    // `accept_frame_for_self` drops any frame whose stamp differs from
    // `IggyShard::id`. Replica identity lives in `name` and in the VSR
    // ids carried by `PartitionConsensusConfig`.
    let partitions = IggyPartitions::new(ShardId::new(shard_idx), partitions_config);

    // The deferred handlers upgrade this weak self-reference per frame; it
    // stays `None` until the shard is built and downgraded into it below.
    let shard_handle: ShellShardHandle<SharedSimOutbox, SimJournal<MemStorage>, SimSnapshot> =
        Rc::new(RefCell::new(None));
    let ShellHandlers {
        on_replica_message,
        on_client_request,
        on_metadata_submit,
        on_list_clients,
        on_partition_read,
        // Step 6 keeps this to register client sessions; unused shell-off.
        sessions: _,
    } = if shell {
        wire_shell_handlers(
            &SharedSimOutbox(Rc::clone(bus)),
            &shard_handle,
            Arc::new(NgSystemConfig::default()),
        )
    } else {
        ShellHandlers::noop()
    };

    let shard = Rc::new(
        shard::IggyShard::new(
            shard::ShardIdentity::new(shard_idx, name),
            SharedSimOutbox(Rc::clone(bus)),
            on_replica_message,
            on_client_request,
            on_metadata_submit,
            on_list_clients,
            on_partition_read,
            metadata,
            partitions,
            senders,
            inbox,
            PapayaShardsTable::new(),
            shard::PartitionConsensusConfig::with_clock(
                CLUSTER_ID,
                shard::ReplicaTopology::new(replica_id, replica_count),
                SharedSimOutbox(Rc::clone(bus)),
                clock,
            ),
            None,
            shard::metrics::ShardMetrics::for_shard(),
        )
        .expect("sim mesh senders are built in canonical order"),
    );

    // Late-bind the deferred handlers' self-reference. Harmless shell-off:
    // the no-ops never upgrade it.
    *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
    (shard, metadata_bundle)
}
