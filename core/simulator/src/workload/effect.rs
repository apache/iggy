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

//! Predicted server-state mutations emitted by op modules on commit.
//!
//! Name-keyed throughout. Server-ng emits empty reply bodies, so the
//! workload cannot recover server-assigned numeric ids; shadow lookups
//! address entities by name (`WireIdentifier::named`). Id-keyed effects
//! return once reply-body parsing lands.

use iggy_common::sharding::IggyNamespace;

#[derive(Debug, Clone)]
pub enum Effect {
    None,
    AddStream {
        name: String,
    },
    RemoveStream {
        name: String,
    },
    AddTopic {
        stream: String,
        name: String,
        partitions: u32,
    },
    RemoveTopic {
        stream: String,
        name: String,
    },
    AddUser {
        name: String,
    },
    RemoveUser {
        name: String,
    },
    AddPat {
        name: String,
    },
    RemovePat {
        name: String,
    },
    AddConsumerGroup {
        stream: String,
        topic: String,
        name: String,
    },
    RemoveConsumerGroup {
        stream: String,
        topic: String,
        name: String,
    },
    SendCommitted {
        ns: IggyNamespace,
        count: u64,
    },
    OffsetStored {
        key: (IggyNamespace, u8, u32),
        value: u64,
    },
    OffsetDeleted {
        key: (IggyNamespace, u8, u32),
    },
    RenameStream {
        old: String,
        new: String,
    },
    RenameTopic {
        stream: String,
        old: String,
        new: String,
    },
    RenameUser {
        old: String,
        new: String,
    },
}

/// Side-effects the driver must run against the simulator. Returned by
/// `Shadow::apply` so the auditor stays pure (no `&mut Simulator`).
#[derive(Debug, Clone)]
pub enum SimCommand {
    /// Reserved hook: no op currently emits this. Will be wired when a
    /// workload-driven namespace-creation op lands (today, namespaces are
    /// pre-seeded by the test fixture via `Simulator::init_partition`).
    InitPartition { ns: IggyNamespace },
}

#[derive(Debug, Default, Clone)]
pub struct ApplyResult {
    pub sim_commands: Vec<SimCommand>,
}
