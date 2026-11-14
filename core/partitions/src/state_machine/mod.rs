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

//! Partition state machines: replicated per-partition state that is not user
//! data, driven by commands that share the partition consensus group's op
//! sequence but live in a log of their own.
//!
//! Every partition carries a [`StateMachines`] registry. A command is a prepare
//! with [`Operation::StateMachineCommand`] whose body names a [`CommandKind`].
//! Each kind has exactly one owning machine, and the registry applies every
//! committed command to its owner in op order.
//!
//! [`Operation::StateMachineCommand`]: iggy_binary_protocol::Operation::StateMachineCommand

mod codec;
mod log;
mod partition;
pub(crate) mod registry;
mod snapshot;
#[cfg(test)]
pub(crate) mod tests;

pub use codec::{CommandDecodeError, CommandEncodeError, MachineCommand};
pub(crate) use log::StateMachineLog;
pub use registry::{
    Applied, CommandKind, MachineId, MachineRestoreError, PartitionStateMachine, RegistrationError,
    StateMachineError, StateMachines,
};
pub use snapshot::{SNAPSHOT_BYTES_MAX, SnapshotDecodeError, StateMachineSnapshot};

/// Commands may hold one slot in this many of a partition's pipeline.
///
/// A busy machine leaves producers the rest. A pipeline shallower than this
/// still gives commands one slot, more than their share: at a depth of one,
/// the only slot.
pub const STATE_MACHINE_PIPELINE_SHARE: usize = 4;

/// Largest command body. Commands describe state, never carry user data.
pub const COMMAND_BYTES_MAX: usize = 64 * 1024;

/// Committed commands a partition keeps in memory so repair can serve them.
///
/// Uncommitted commands, and committed ones above the partition's checkpoint,
/// are never released.
pub const COMMANDS_RETAINED_MAX: usize = 16 * 1024;
/// Memory the retained commands may take, padded allocations included.
pub const COMMANDS_RETAINED_BYTES_MAX: u64 = 16 * 1024 * 1024;

/// The pipeline slots commands may hold in a pipeline of `prepare_queue_max`
/// prepares: their [`STATE_MACHINE_PIPELINE_SHARE`], and at least one, so a
/// pipeline shallower than the share still admits commands.
#[must_use]
pub const fn state_machine_pipeline_slots(prepare_queue_max: usize) -> usize {
    let slots = prepare_queue_max / STATE_MACHINE_PIPELINE_SHARE;
    if slots == 0 { 1 } else { slots }
}
