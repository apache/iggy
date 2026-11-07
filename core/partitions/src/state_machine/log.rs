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

use std::collections::BTreeMap;
use std::fmt;
use std::ops::RangeBounds;

use iggy_binary_protocol::{Operation, PrepareHeader};
use server_common::iobuf::Frozen;

/// The commands a partition keeps in memory: every uncommitted one, and the
/// committed ones repair may still serve. The durable copy is the partition's
/// state-machine WAL.
#[derive(Debug, Default)]
pub struct StateMachineLog {
    commands: BTreeMap<u64, Frozen<4096>>,
    /// Memory the held prepares take, their padded allocations included.
    bytes: u64,
    /// Every command from this op on is held. Below it they may be released.
    retained_from: u64,
}

impl StateMachineLog {
    /// Hold a command. A second prepare for an op already held is ignored, as
    /// the data journal keeps the first occurrence of an op.
    ///
    /// # Errors
    /// Returns an error when the prepare is not a state-machine command.
    pub fn insert(&mut self, prepare: Frozen<4096>) -> Result<(), StateMachineLogError> {
        let header = header_of(&prepare).ok_or(StateMachineLogError::Malformed)?;
        if header.operation != Operation::StateMachineCommand {
            return Err(StateMachineLogError::NotACommand(header.operation));
        }
        if let std::collections::btree_map::Entry::Vacant(slot) = self.commands.entry(header.op) {
            self.bytes += memory_bytes(&prepare);
            slot.insert(prepare);
        }
        Ok(())
    }

    #[must_use]
    pub fn contains(&self, op: u64) -> bool {
        self.commands.contains_key(&op)
    }

    #[must_use]
    pub fn get(&self, op: u64) -> Option<Frozen<4096>> {
        self.commands.get(&op).cloned()
    }

    #[must_use]
    pub fn header(&self, op: u64) -> Option<PrepareHeader> {
        self.commands.get(&op).and_then(header_of)
    }

    /// Held ops in `ops`, ascending.
    pub fn ops(&self, ops: impl RangeBounds<u64>) -> impl DoubleEndedIterator<Item = u64> + '_ {
        self.commands.range(ops).map(|(op, _)| *op)
    }

    /// Held headers in `ops`, ascending.
    pub fn headers(
        &self,
        ops: impl RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, PrepareHeader)> + '_ {
        self.commands
            .range(ops)
            .filter_map(|(op, prepare)| header_of(prepare).map(|header| (*op, header)))
    }

    #[must_use]
    pub fn last_op(&self) -> Option<u64> {
        self.commands.last_key_value().map(|(op, _)| *op)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    #[cfg(test)]
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Highest commit point any held command stamped.
    #[must_use]
    pub fn max_commit(&self) -> u64 {
        self.commands
            .values()
            .filter_map(header_of)
            .map(|header| header.commit)
            .max()
            .unwrap_or(0)
    }

    /// Drop the held commands in `ops`, returning how many went. They stay
    /// refillable: the retained floor does not move.
    pub fn remove(&mut self, ops: impl RangeBounds<u64>) -> usize {
        let removed: Vec<u64> = self.commands.range(ops).map(|(op, _)| *op).collect();
        for op in &removed {
            if let Some(prepare) = self.commands.remove(op) {
                self.bytes -= memory_bytes(&prepare);
            }
        }
        removed.len()
    }

    /// Forget every command, as an install or a new incarnation does.
    pub fn clear(&mut self) {
        self.commands.clear();
        self.bytes = 0;
        self.retained_from = 0;
    }

    /// Release the oldest commands at or below `through_op`, which the
    /// partition checkpointed, while more than `count_max` are held or they
    /// take more than `bytes_max`. Repair can no longer serve them from
    /// here, and the WAL no longer holds them either.
    ///
    /// Returns whether the held commands are still past a cap, so only a
    /// later checkpoint lets more go.
    pub fn release_checkpointed(
        &mut self,
        through_op: u64,
        count_max: usize,
        bytes_max: u64,
    ) -> bool {
        let over = |log: &Self| log.commands.len() > count_max || log.bytes > bytes_max;
        while over(self) {
            let Some(oldest) = self.commands.first_entry() else {
                break;
            };
            if *oldest.key() > through_op {
                break;
            }
            let (op, prepare) = oldest.remove_entry();
            self.bytes -= memory_bytes(&prepare);
            self.retained_from = self.retained_from.max(op.saturating_add(1));
        }
        over(self)
    }

    /// Every command from this op on is held.
    #[must_use]
    pub const fn retained_from(&self) -> u64 {
        self.retained_from
    }

    /// Mark the commands below `op` as gone without holding any, as a replica
    /// does after restoring machines whose snapshot covers them.
    pub fn set_retained_from(&mut self, op: u64) {
        self.retained_from = self.retained_from.max(op);
    }
}

/// What a held prepare costs in memory: its padded allocation, the way the
/// journal charges the repair bodies it retains, and its map entry.
fn memory_bytes(prepare: &Frozen<4096>) -> u64 {
    Frozen::<4096>::allocation_size(prepare.backing_capacity())
        .and_then(|bytes| bytes.checked_add(size_of::<(u64, Frozen<4096>)>()))
        .map_or(u64::MAX, |bytes| bytes as u64)
}

fn header_of(prepare: &Frozen<4096>) -> Option<PrepareHeader> {
    let bytes = prepare.as_slice().get(..size_of::<PrepareHeader>())?;
    bytemuck::checked::try_from_bytes::<PrepareHeader>(bytes)
        .ok()
        .copied()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMachineLogError {
    Malformed,
    NotACommand(Operation),
}

impl fmt::Display for StateMachineLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("state-machine log entry is not a prepare"),
            Self::NotACommand(operation) => {
                write!(f, "{operation:?} is not a state-machine command")
            }
        }
    }
}

impl std::error::Error for StateMachineLogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::Command;
    use server_common::Message;

    fn prepare(op: u64, operation: Operation, commit: u64) -> Frozen<4096> {
        Message::<PrepareHeader>::new(size_of::<PrepareHeader>())
            .transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = operation;
                header.op = op;
                header.commit = commit;
                header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
            })
            .into_frozen()
    }

    #[test]
    fn given_commands_when_holding_and_releasing_should_track_the_retained_floor() {
        let mut log = StateMachineLog::default();
        for op in [3, 7, 9] {
            log.insert(prepare(op, Operation::StateMachineCommand, op - 1))
                .unwrap();
        }
        assert!(log.contains(7));
        assert_eq!(log.ops(4..=9).collect::<Vec<_>>(), vec![7, 9]);
        assert_eq!(log.max_commit(), 8);
        assert_eq!(log.last_op(), Some(9));

        let one = log.bytes() / 3;
        assert!(!log.release_checkpointed(7, 1, u64::MAX));
        assert_eq!(log.retained_from(), 8);
        assert!(!log.contains(3));
        assert!(log.contains(9));
        assert_eq!(log.bytes(), one);

        assert_eq!(log.remove(9..), 1);
        assert!(log.is_empty());
        assert_eq!(log.retained_from(), 8);
        log.clear();
        assert_eq!(log.retained_from(), 0);
    }

    /// A small prepare still takes a whole padded allocation, and that is
    /// what the byte cap counts.
    #[test]
    fn given_a_small_command_when_held_should_charge_its_allocation() {
        let mut log = StateMachineLog::default();
        let command = prepare(1, Operation::StateMachineCommand, 0);
        assert!(command.len() < 4096);
        log.insert(command).unwrap();
        assert!(log.bytes() >= 4096, "{}", log.bytes());
    }

    #[test]
    fn given_caps_when_releasing_should_keep_everything_above_the_checkpoint() {
        let mut log = StateMachineLog::default();
        for op in 1..=6 {
            log.insert(prepare(op, Operation::StateMachineCommand, 0))
                .unwrap();
        }
        assert!(
            log.release_checkpointed(3, 2, u64::MAX),
            "past the cap until a checkpoint covers more"
        );
        assert_eq!(log.ops(..).collect::<Vec<_>>(), vec![4, 5, 6]);
        assert_eq!(log.retained_from(), 4);
        assert!(!log.release_checkpointed(6, usize::MAX, 0));
        assert!(log.is_empty());
        assert_eq!(log.bytes(), 0);
    }

    #[test]
    fn given_a_held_op_when_inserting_again_should_keep_the_first_copy() {
        let mut log = StateMachineLog::default();
        log.insert(prepare(4, Operation::StateMachineCommand, 1))
            .unwrap();
        log.insert(prepare(4, Operation::StateMachineCommand, 2))
            .unwrap();
        assert_eq!(log.header(4).unwrap().commit, 1);
    }

    #[test]
    fn given_a_data_op_when_inserting_should_refuse_it() {
        let mut log = StateMachineLog::default();
        assert_eq!(
            log.insert(prepare(1, Operation::SendMessages, 0)),
            Err(StateMachineLogError::NotACommand(Operation::SendMessages))
        );
    }
}
