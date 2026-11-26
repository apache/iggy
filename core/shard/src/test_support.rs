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

//! Helpers shared by the unit tests of this crate.

use consensus::{PlaneKind, VsrAction, VsrConsensus};
use message_bus::{IggyMessageBus, MessageBus, ReplicaLinkId};

/// One tick of a group alone: advance its clock, then tick it, as the shard
/// drivers do. A partition in a table runs on the table clock, so the other
/// partitions of the table see the tick too.
pub fn advance_and_tick<B: MessageBus>(
    consensus: &VsrConsensus<B>,
    plane: PlaneKind,
) -> Vec<VsrAction> {
    consensus.tick_clock().advance();
    consensus.tick(plane)
}

/// Make `replica` reachable from `bus` over one link that shard `owner`
/// holds, the way an installed one-link bundle does: a frame to `replica`
/// from any other shard is forwarded to `owner`.
pub fn connect_replica_on(bus: &IggyMessageBus, replica: u8, owner: u16) {
    let link = ReplicaLinkId {
        replica,
        link: 0,
        epoch: 0,
    };
    assert!(bus.owner_table().try_claim(link, owner));
    bus.refresh_replica_bundle(replica);
}
