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

#![allow(clippy::future_not_send)]

use iggy_common::SemanticVersion;
#[cfg(all(feature = "mimalloc", not(feature = "disable-mimalloc")))]
use mimalloc::MiMalloc;
// `--all-features` leaves the allocator off, and cargo then reports the crate
// as an unused dependency.
#[cfg(all(feature = "mimalloc", feature = "disable-mimalloc"))]
use mimalloc as _;

// Both features are checked because `--all-features` turns on `mimalloc` and
// `disable-mimalloc` at once, while `--no-default-features` drops the crate.
#[cfg(all(feature = "mimalloc", not(feature = "disable-mimalloc")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SEMANTIC_VERSION: SemanticVersion = SemanticVersion::parse_const(VERSION);

// Visibility rule: `pub` = named external consumer. main.rs consumes
// `VERSION`, `boot` (including `boot::systemd`) and `server_error`; the
// simulator consumes `shell`, `boot::ServerHost`, the reconciler exports
// (`LifecycleReports`, `reconcile_partition_lifecycles`,
// `reconcile_pending_revocations`), the geo passes (`GeoPasses`,
// `NodePlace`, `peer_log_of`) and the store wiring (`serve_tiered_storage`,
// `serve_peers`, `ServedPeer`, `TierHandlers`, `own_cluster_uid`). Everything
// else is internal to the crate.

// boot: process entry, shard threads, recovery orchestration.
pub mod boot;
pub(crate) mod config_writer;
pub(crate) mod shard_allocator;

// spine: the request path - shell vocabulary, dispatch funnel, per-domain ops.
pub(crate) mod consumer_group;
pub use consumer_group::liveness::run as run_consumer_group_liveness;
pub use dispatch::session_ops::run_heartbeat_verifier;
pub(crate) mod dispatch;
pub(crate) mod namespace;
pub(crate) mod pat;
pub(crate) mod reply_frame;
pub(crate) mod responses;
pub(crate) mod rewrite;
pub(crate) mod session_manager;
pub mod shell;
pub(crate) mod stream_key_rewrapper;
pub(crate) mod stream_keys;

pub(crate) mod users;
pub(crate) mod wire;

// http: the REST spine (role-leaf tree).
pub(crate) mod http;

// background: per-shard maintenance loops.
pub(crate) mod geo_passes;
pub use geo_passes::GeoPasses;
pub(crate) mod geo_peers;
pub use geo_peers::{ServedPeer, serve_peers};
pub(crate) mod geo_topics;
pub(crate) mod incarnation_deletes;
pub(crate) mod origin_recovery;
pub(crate) mod partition_reconciler;
pub(crate) mod peer_manifests;
pub use peer_manifests::NodePlace;
pub(crate) mod peer_positions;
pub use partition_reconciler::{
    GroupOffsetCleanup, LifecycleReports, reconcile_consumer_group_offsets,
    reconcile_partition_lifecycles, reconcile_pending_revocations,
};
pub(crate) mod credential_refresh;
pub(crate) mod gcs_credentials;
pub(crate) mod personal_access_token_cleaner;
pub(crate) mod s3_credentials;
pub(crate) mod segment_cleaner;
pub(crate) mod shadow_topics;
pub use shadow_topics::peer_log_of;
pub(crate) mod snapshot;
pub(crate) mod store_identity;
pub use store_identity::{TierHandlers, own_cluster_uid};
pub(crate) mod sysinfo_printer;
pub(crate) mod tiered_storage;
pub use tiered_storage::serve as serve_tiered_storage;

// support: shared plumbing.
pub(crate) mod cluster_meta;
pub(crate) mod external_auth;
pub(crate) mod partition_helpers;
pub mod server_error;
pub(crate) mod sysinfo_probe;

#[cfg(all(test, feature = "mimalloc", not(feature = "disable-mimalloc")))]
mod tests {
    use super::GLOBAL;
    use iggy_binary_protocol::{Command, GenericHeader};
    use message_bus::framing::read_message;
    use server_common::iobuf::{Frozen, Owned};
    use server_common::{MESSAGE_ALIGN, Message};

    /// A small prepare, and a frame grown past one alignment unit.
    const FRAME_LENS: [usize; 2] = [624, MESSAGE_ALIGN + 1000];

    /// The journal and the repair ring charge `Frozen::allocation_size` of the
    /// backing capacity. That counts the real block only while the backing
    /// capacity is the block that mimalloc reserved.
    #[compio::test]
    #[allow(clippy::cast_possible_truncation)]
    async fn given_journal_buffers_when_mimalloc_allocates_them_should_charge_the_whole_block() {
        for frame_len in FRAME_LENS {
            let frame = Message::<GenericHeader>::new(frame_len).transmute_header(
                |_, h: &mut GenericHeader| {
                    h.command = Command::Ping;
                    h.size = frame_len as u32;
                },
            );
            let mut wire = frame.as_slice();
            let read = read_message(&mut wire, frame_len).await.unwrap();
            let buffers = [
                ("read off the wire", read.into_frozen()),
                ("zeroed", Owned::<MESSAGE_ALIGN>::zeroed(frame_len).into()),
                (
                    "copied",
                    Frozen::from(Owned::copy_from_slice(frame.as_slice())),
                ),
            ];
            for (source, buffer) in buffers {
                let charged = buffer.backing_capacity();
                // SAFETY: mimalloc is the global allocator, and `buffer` keeps
                // its block alive.
                let block = unsafe { GLOBAL.usable_size(buffer.as_slice().as_ptr()) };
                assert_eq!(
                    block, charged,
                    "a {frame_len} B frame {source}: the charge counts {charged} B, mimalloc \
                     reserved {block} B"
                );
            }
        }
    }
}
