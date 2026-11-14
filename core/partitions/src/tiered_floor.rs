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

//! The tiered floor, the first partition state machine.
//!
//! The floor is the last offset whose segment is safely in the object store.
//! Every replica applies the same advances in op order, so every replica
//! agrees on what it may evict.
//!
//! Every partition registers the machine, so enabling tiered storage on a topic
//! later needs no registration. Nothing proposes an advance yet, so the floor
//! stays empty and adds nothing to a partition's checkpoint or transfer.

use std::fmt;

use iggy_binary_protocol::RoutedRequestHeader;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;

use crate::IggyPartition;
use crate::state_machine::{
    Applied, CommandEncodeError, CommandKind, MachineCommand, MachineId, MachineRestoreError,
    PartitionStateMachine,
};

#[cfg(test)]
mod tests;

/// The floor machine's id in snapshots, stable within a storage format.
pub const TIERED_FLOOR_MACHINE: MachineId = MachineId(1);

/// The command kind of [`AdvanceTieredFloor`].
pub const ADVANCE_TIERED_FLOOR: CommandKind = CommandKind(1);

/// Bytes in a segment's commit-path hash.
pub const SEGMENT_HASH_SIZE: usize = 32;

/// `start u64 | up_to u64 | hash`, little-endian.
pub const ADVANCE_PAYLOAD_SIZE: usize = 2 * size_of::<u64>() + SEGMENT_HASH_SIZE;

/// Raise the floor over one uploaded segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvanceTieredFloor {
    /// The first offset the advance covers: the segment's first offset, or the
    /// floor plus one when no offset in between holds a message.
    pub start: u64,
    /// The segment's last offset.
    pub up_to: u64,
    /// The segment's commit-path hash. Each replica checks it against its own
    /// copy before evicting, outside the machine.
    pub hash: [u8; SEGMENT_HASH_SIZE],
}

impl AdvanceTieredFloor {
    /// The command payload.
    #[must_use]
    pub const fn encode(&self) -> [u8; ADVANCE_PAYLOAD_SIZE] {
        let mut payload = [0; ADVANCE_PAYLOAD_SIZE];
        let (start, rest) = payload.split_at_mut(size_of::<u64>());
        let (up_to, hash) = rest.split_at_mut(size_of::<u64>());
        start.copy_from_slice(&self.start.to_le_bytes());
        up_to.copy_from_slice(&self.up_to.to_le_bytes());
        hash.copy_from_slice(&self.hash);
        payload
    }
}

impl TryFrom<&[u8]> for AdvanceTieredFloor {
    type Error = AdvanceDecodeError;

    fn try_from(payload: &[u8]) -> Result<Self, Self::Error> {
        let error = AdvanceDecodeError {
            length: payload.len(),
        };
        let (start, rest) = payload.split_first_chunk().ok_or(error)?;
        let (up_to, hash) = rest.split_first_chunk().ok_or(error)?;
        Ok(Self {
            start: u64::from_le_bytes(*start),
            up_to: u64::from_le_bytes(*up_to),
            hash: hash.try_into().map_err(|_| error)?,
        })
    }
}

/// A command payload that is not an [`AdvanceTieredFloor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvanceDecodeError {
    pub length: usize,
}

impl fmt::Display for AdvanceDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tiered floor advance of {} bytes, expected {ADVANCE_PAYLOAD_SIZE}",
            self.length
        )
    }
}

impl std::error::Error for AdvanceDecodeError {}

/// The last offset every replica may treat as stored, monotone for the life of
/// the partition incarnation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TieredFloor {
    tiered_up_to_offset: Option<u64>,
}

impl TieredFloor {
    /// The last tiered offset, `None` before the first advance.
    #[must_use]
    pub const fn tiered_up_to_offset(&self) -> Option<u64> {
        self.tiered_up_to_offset
    }

    /// The first offset an advance may start at. An empty floor counts as
    /// floor plus one at offset zero.
    const fn next_start(&self) -> u64 {
        match self.tiered_up_to_offset {
            Some(floor) => floor.saturating_add(1),
            None => 0,
        }
    }
}

impl PartitionStateMachine for TieredFloor {
    fn id(&self) -> MachineId {
        TIERED_FLOOR_MACHINE
    }

    fn command_kinds(&self) -> &[CommandKind] {
        &[ADVANCE_TIERED_FLOOR]
    }

    fn apply(&mut self, _: u64, command: MachineCommand<'_>) -> Applied {
        let Ok(advance) = AdvanceTieredFloor::try_from(command.payload) else {
            return Applied::Refused;
        };
        // A start past the floor plus one would jump over a segment nobody
        // uploaded.
        if advance.start > advance.up_to || advance.start > self.next_start() {
            return Applied::Refused;
        }
        // A repeated or lower advance is accepted and keeps the floor, so a
        // command retried across a view change is harmless.
        self.tiered_up_to_offset = Some(
            self.tiered_up_to_offset
                .map_or(advance.up_to, |floor| floor.max(advance.up_to)),
        );
        Applied::Changed
    }

    fn snapshot_into(&self, out: &mut Vec<u8>) {
        if let Some(floor) = self.tiered_up_to_offset {
            out.extend_from_slice(&floor.to_le_bytes());
        }
    }

    fn holds_state(&self) -> bool {
        self.tiered_up_to_offset.is_some()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
        self.tiered_up_to_offset = match snapshot {
            [] => None,
            bytes => Some(u64::from_le_bytes(bytes.try_into().map_err(|_| {
                MachineRestoreError {
                    reason: "a tiered floor snapshot is empty or eight bytes",
                }
            })?)),
        };
        Ok(())
    }

    fn reset(&mut self) {
        self.tiered_up_to_offset = None;
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The tiered floor: `None` before the first advance, and on a partition
    /// that does not register the floor machine.
    #[must_use]
    pub fn tiered_floor(&self) -> Option<u64> {
        self.state_machine::<TieredFloor>(TIERED_FLOOR_MACHINE)
            .and_then(TieredFloor::tiered_up_to_offset)
    }

    /// A request committing `advance`, for the primary's `on_request`.
    /// `request` is the caller's own number for it, as for
    /// [`Self::command_request`].
    ///
    /// # Errors
    /// Returns an error when the command exceeds the command size limit, which
    /// a fixed-size advance never does.
    pub fn advance_tiered_floor_request(
        &self,
        advance: &AdvanceTieredFloor,
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        self.command_request(ADVANCE_TIERED_FLOOR, &advance.encode(), request)
    }
}
