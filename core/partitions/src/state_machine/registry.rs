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

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;

use super::codec::MachineCommand;
use super::snapshot::{SNAPSHOT_BYTES_MAX, SnapshotWriter, StateMachineSnapshot};

/// Identifies one kind of command. Each kind has exactly one owning machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandKind(pub u16);

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifies a machine in snapshots. Stable across releases of one storage
/// format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MachineId(pub u16);

impl fmt::Display for MachineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What a machine did with a committed command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    Changed,
    /// The machine refused the command. It commits as a no-op, so a refusal
    /// never stops a replica.
    Refused,
}

/// A deterministic machine over one partition's committed commands.
///
/// Every replica applies the same commands in the same op order, so apply must
/// read only the command and the machine's own state, never a clock, the disk
/// or anything replica-local, and finish in bounded time: it runs on the
/// partition's pump. Slow work belongs to side effects the owner stages after
/// apply.
///
/// The owner reads the outcome of its commands from the machine's state,
/// through [`StateMachines::machine`]: the reply to a command only says that
/// it committed.
pub trait PartitionStateMachine: Any {
    fn id(&self) -> MachineId;

    /// The command kinds this machine owns. Read once, at registration.
    fn command_kinds(&self) -> &[CommandKind];

    /// Apply the command committed at `op`.
    ///
    /// Commands are server-originated and bypass request deduplication, so an
    /// owner that retries a command whose outcome it never learned, across a
    /// view change for one, can commit it twice at two ops. Apply must
    /// tolerate that: make the command idempotent, or deduplicate on an id the
    /// owner puts in the payload.
    ///
    /// Apply must refuse a command that would grow the state past what
    /// [`Self::snapshot_into`] may write, see there.
    fn apply(&mut self, op: u64, command: MachineCommand<'_>) -> Applied;

    /// Append the machine's whole state, restorable by [`Self::restore`], to
    /// `out`.
    ///
    /// Every machine of a partition together stays within
    /// [`SNAPSHOT_BYTES_MAX`]. A larger snapshot fails the partition's
    /// checkpoint, which stops the partition, and since every replica applies
    /// the same commands, every replica stops on the same op. Bound the state
    /// in [`Self::apply`] instead.
    fn snapshot_into(&self, out: &mut Vec<u8>);

    /// Replace the machine's state with a snapshot it produced.
    ///
    /// # Errors
    /// Returns why the bytes are not a snapshot of this machine.
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError>;

    /// Return to the state before any command.
    fn reset(&mut self);
}

/// Why a machine refused a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachineRestoreError {
    pub reason: &'static str,
}

impl fmt::Display for MachineRestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason)
    }
}

impl std::error::Error for MachineRestoreError {}

/// Every machine of one partition and the op through which they applied.
#[derive(Default)]
pub struct StateMachines {
    /// Ordered by id, so snapshots list machines in one order on every replica.
    machines: Vec<Box<dyn PartitionStateMachine>>,
    owners: BTreeMap<CommandKind, usize>,
    applied_op: u64,
}

impl fmt::Debug for StateMachines {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateMachines")
            .field(
                "machines",
                &self
                    .machines
                    .iter()
                    .map(|machine| machine.id())
                    .collect::<Vec<_>>(),
            )
            .field("applied_op", &self.applied_op)
            .finish_non_exhaustive()
    }
}

impl StateMachines {
    /// Add a machine. Registration happens before the partition restores or
    /// applies any op, and every replica registers the same set.
    ///
    /// # Errors
    /// Returns an error when the id or one of the command kinds is taken, the
    /// machine lists a kind twice, or the registry already restored or
    /// applied an op.
    pub fn register(
        &mut self,
        machine: Box<dyn PartitionStateMachine>,
    ) -> Result<(), RegistrationError> {
        if self.applied_op != 0 {
            return Err(RegistrationError::AfterApply {
                applied_op: self.applied_op,
            });
        }
        let id = machine.id();
        let Err(position) = self
            .machines
            .binary_search_by_key(&id, |registered| registered.id())
        else {
            return Err(RegistrationError::DuplicateMachine(id));
        };
        let kinds = machine.command_kinds();
        for (index, kind) in kinds.iter().enumerate() {
            if let Some(&owner) = self.owners.get(kind) {
                return Err(RegistrationError::DuplicateOwner {
                    kind: *kind,
                    owner: self.machines[owner].id(),
                });
            }
            if kinds[..index].contains(kind) {
                return Err(RegistrationError::DuplicateKind {
                    machine: id,
                    kind: *kind,
                });
            }
        }
        for owner in self.owners.values_mut() {
            if *owner >= position {
                *owner += 1;
            }
        }
        for kind in kinds {
            self.owners.insert(*kind, position);
        }
        self.machines.insert(position, machine);
        Ok(())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.machines.is_empty()
    }

    #[must_use]
    pub fn owns(&self, kind: CommandKind) -> bool {
        self.owners.contains_key(&kind)
    }

    /// The registered machine `id`, when it is a `T`.
    #[must_use]
    pub fn machine<T: PartitionStateMachine>(&self, id: MachineId) -> Option<&T> {
        let index = self
            .machines
            .binary_search_by_key(&id, |machine| machine.id())
            .ok()?;
        let machine: &dyn Any = self.machines[index].as_ref();
        machine.downcast_ref()
    }

    /// The op through which every committed command applied.
    #[must_use]
    pub const fn applied_op(&self) -> u64 {
        self.applied_op
    }

    /// Note that every op through `op` committed with no command among them
    /// above the last applied one, so a snapshot can be cut there.
    pub(crate) fn advance_applied(&mut self, op: u64) {
        self.applied_op = self.applied_op.max(op);
    }

    /// Apply the command committed at `op` to its owner.
    ///
    /// # Errors
    /// Returns an error when `op` does not follow the last applied op or no
    /// machine owns the kind. Neither can happen on a replica that admitted
    /// the command: both mean its history is broken.
    pub(crate) fn apply(
        &mut self,
        op: u64,
        command: MachineCommand<'_>,
    ) -> Result<Applied, StateMachineError> {
        if op <= self.applied_op {
            return Err(StateMachineError::Reapplied {
                op,
                applied_op: self.applied_op,
            });
        }
        let owner = *self
            .owners
            .get(&command.kind)
            .ok_or(StateMachineError::UnownedCommand {
                op,
                kind: command.kind,
            })?;
        let applied = self.machines[owner].apply(op, command);
        self.applied_op = op;
        Ok(applied)
    }

    /// Every machine's state, labelled with `op`: the caller's applied
    /// frontier, at or above the last command applied.
    ///
    /// # Errors
    /// Returns an error when the snapshot exceeds [`SNAPSHOT_BYTES_MAX`]: no
    /// replica could read it back.
    pub(crate) fn snapshot_at(&self, op: u64) -> Result<StateMachineSnapshot, StateMachineError> {
        let snapshot = self.capture(op);
        let length = snapshot.encoded_size();
        if length > SNAPSHOT_BYTES_MAX {
            return Err(StateMachineError::SnapshotTooLarge { length });
        }
        Ok(snapshot)
    }

    /// [`Self::snapshot_at`] without the size bound, for state that never
    /// leaves memory.
    pub(crate) fn capture(&self, op: u64) -> StateMachineSnapshot {
        debug_assert!(op >= self.applied_op, "a snapshot cannot predate its state");
        let mut writer = SnapshotWriter::new(op.max(self.applied_op));
        for machine in &self.machines {
            writer.machine(machine.id(), |out| machine.snapshot_into(out));
        }
        writer.finish()
    }

    /// Whether every machine `snapshot` names is registered. A registered
    /// machine the snapshot lacks starts fresh on restore: it was added after
    /// the snapshot was cut, and no command of its kinds could commit before
    /// it owned them.
    ///
    /// # Errors
    /// Returns an error naming the first machine that is not registered.
    pub(crate) fn check_snapshot(
        &self,
        snapshot: &StateMachineSnapshot,
    ) -> Result<(), StateMachineError> {
        match snapshot.machines().find(|(id, _)| {
            self.machines
                .binary_search_by_key(id, |machine| machine.id())
                .is_err()
        }) {
            Some((id, _)) => Err(StateMachineError::UnregisteredMachine { id }),
            None => Ok(()),
        }
    }

    /// Replace every machine's state with its part of `snapshot`, or the
    /// state before any command when the snapshot has none for it.
    ///
    /// # Errors
    /// Returns an error when the snapshot names a machine that is not
    /// registered or a machine refuses its bytes. The registry is reset then,
    /// so no machine keeps a half-installed state.
    pub(crate) fn restore(
        &mut self,
        snapshot: &StateMachineSnapshot,
    ) -> Result<(), StateMachineError> {
        self.check_snapshot(snapshot)?;
        let mut parts = snapshot.machines().peekable();
        for machine in &mut self.machines {
            let id = machine.id();
            let Some((_, bytes)) = parts.next_if(|(part, _)| *part == id) else {
                machine.reset();
                continue;
            };
            if let Err(source) = machine.restore(bytes) {
                self.reset();
                return Err(StateMachineError::Restore { id, source });
            }
        }
        self.applied_op = snapshot.op();
        Ok(())
    }

    /// Forget every machine's state, as a new partition incarnation starts.
    pub(crate) fn reset(&mut self) {
        for machine in &mut self.machines {
            machine.reset();
        }
        self.applied_op = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationError {
    DuplicateMachine(MachineId),
    DuplicateOwner {
        kind: CommandKind,
        owner: MachineId,
    },
    DuplicateKind {
        machine: MachineId,
        kind: CommandKind,
    },
    AfterApply {
        applied_op: u64,
    },
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateMachine(id) => write!(f, "state machine {id} is already registered"),
            Self::DuplicateOwner { kind, owner } => write!(
                f,
                "command kind {kind} is already owned by state machine {owner}"
            ),
            Self::DuplicateKind { machine, kind } => write!(
                f,
                "state machine {machine} lists command kind {kind} more than once"
            ),
            Self::AfterApply { applied_op } => write!(
                f,
                "state machines must be registered before the partition restores or applies \
                 any op, and it is at op {applied_op}"
            ),
        }
    }
}

impl std::error::Error for RegistrationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMachineError {
    Reapplied {
        op: u64,
        applied_op: u64,
    },
    UnownedCommand {
        op: u64,
        kind: CommandKind,
    },
    UnregisteredMachine {
        id: MachineId,
    },
    SnapshotOp {
        expected: u64,
        found: u64,
    },
    SnapshotTooLarge {
        length: usize,
    },
    Restore {
        id: MachineId,
        source: MachineRestoreError,
    },
}

impl fmt::Display for StateMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reapplied { op, applied_op } => write!(
                f,
                "command at op {op} does not follow the applied op {applied_op}"
            ),
            Self::UnownedCommand { op, kind } => {
                write!(f, "no state machine owns command kind {kind} at op {op}")
            }
            Self::UnregisteredMachine { id } => write!(
                f,
                "snapshot carries state machine {id}, which the partition does not register"
            ),
            Self::SnapshotOp { expected, found } => {
                write!(f, "snapshot is at op {found}, expected op {expected}")
            }
            Self::SnapshotTooLarge { length } => write!(
                f,
                "state machine snapshot of {length} bytes exceeds {SNAPSHOT_BYTES_MAX}"
            ),
            Self::Restore { id, source } => {
                write!(f, "state machine {id} refused its snapshot: {source}")
            }
        }
    }
}

impl std::error::Error for StateMachineError {}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Sums the `u64` payloads of its commands and refuses a zero.
    pub struct CounterMachine {
        id: MachineId,
        kinds: Vec<CommandKind>,
        pub total: u64,
        applied: Vec<u64>,
    }

    impl CounterMachine {
        pub fn new(id: u16, kinds: &[u16]) -> Self {
            Self {
                id: MachineId(id),
                kinds: kinds.iter().copied().map(CommandKind).collect(),
                total: 0,
                applied: Vec::new(),
            }
        }
    }

    impl PartitionStateMachine for CounterMachine {
        fn id(&self) -> MachineId {
            self.id
        }

        fn command_kinds(&self) -> &[CommandKind] {
            &self.kinds
        }

        fn apply(&mut self, op: u64, command: MachineCommand<'_>) -> Applied {
            let Ok(bytes) = <[u8; 8]>::try_from(command.payload) else {
                return Applied::Refused;
            };
            let value = u64::from_le_bytes(bytes);
            if value == 0 {
                return Applied::Refused;
            }
            self.total += value;
            self.applied.push(op);
            Applied::Changed
        }

        fn snapshot_into(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.total.to_le_bytes());
        }

        fn restore(&mut self, snapshot: &[u8]) -> Result<(), MachineRestoreError> {
            let bytes = <[u8; 8]>::try_from(snapshot).map_err(|_| MachineRestoreError {
                reason: "not 8 bytes",
            })?;
            self.total = u64::from_le_bytes(bytes);
            self.applied.clear();
            Ok(())
        }

        fn reset(&mut self) {
            self.total = 0;
            self.applied.clear();
        }
    }

    fn command(kind: u16, payload: &[u8]) -> MachineCommand<'_> {
        MachineCommand {
            kind: CommandKind(kind),
            payload,
        }
    }

    fn counters(ids: &[u16]) -> StateMachines {
        let mut machines = StateMachines::default();
        for id in ids {
            machines
                .register(Box::new(CounterMachine::new(*id, &[*id * 10])))
                .unwrap();
        }
        machines
    }

    #[test]
    fn given_two_machines_when_applying_should_route_each_kind_to_its_owner() {
        let mut machines = StateMachines::default();
        machines
            .register(Box::new(CounterMachine::new(2, &[20, 21])))
            .unwrap();
        machines
            .register(Box::new(CounterMachine::new(1, &[10])))
            .unwrap();

        assert_eq!(
            machines.apply(3, command(10, &5u64.to_le_bytes())),
            Ok(Applied::Changed)
        );
        assert_eq!(
            machines.apply(4, command(21, &7u64.to_le_bytes())),
            Ok(Applied::Changed)
        );
        assert_eq!(
            machines.apply(9, command(20, &0u64.to_le_bytes())),
            Ok(Applied::Refused)
        );
        assert_eq!(machines.applied_op(), 9);
        assert_eq!(
            machines
                .machine::<CounterMachine>(MachineId(2))
                .map(|machine| machine.total),
            Some(7)
        );

        let snapshot = machines.capture(machines.applied_op());
        assert_eq!(snapshot.op(), 9);
        assert_eq!(
            snapshot.machines().collect::<Vec<_>>(),
            vec![
                (MachineId(1), &5u64.to_le_bytes()[..]),
                (MachineId(2), &7u64.to_le_bytes()[..]),
            ]
        );
    }

    struct OtherMachine;

    impl PartitionStateMachine for OtherMachine {
        fn id(&self) -> MachineId {
            MachineId(5)
        }

        fn command_kinds(&self) -> &[CommandKind] {
            &[]
        }

        fn apply(&mut self, _: u64, _: MachineCommand<'_>) -> Applied {
            Applied::Refused
        }

        fn snapshot_into(&self, _: &mut Vec<u8>) {}

        fn restore(&mut self, _: &[u8]) -> Result<(), MachineRestoreError> {
            Ok(())
        }

        fn reset(&mut self) {}
    }

    #[test]
    fn given_a_machine_of_another_type_or_id_when_reading_should_find_none() {
        let mut machines = counters(&[1]);
        machines.register(Box::new(OtherMachine)).unwrap();
        assert!(machines.machine::<CounterMachine>(MachineId(1)).is_some());
        assert!(machines.machine::<OtherMachine>(MachineId(1)).is_none());
        assert!(machines.machine::<CounterMachine>(MachineId(5)).is_none());
        assert!(machines.machine::<OtherMachine>(MachineId(5)).is_some());
        assert!(machines.machine::<CounterMachine>(MachineId(9)).is_none());
    }

    #[test]
    fn given_taken_ids_or_kinds_when_registering_should_refuse() {
        let mut machines = StateMachines::default();
        machines
            .register(Box::new(CounterMachine::new(1, &[10])))
            .unwrap();
        assert_eq!(
            machines.register(Box::new(CounterMachine::new(1, &[11]))),
            Err(RegistrationError::DuplicateMachine(MachineId(1)))
        );
        assert_eq!(
            machines.register(Box::new(CounterMachine::new(2, &[10]))),
            Err(RegistrationError::DuplicateOwner {
                kind: CommandKind(10),
                owner: MachineId(1)
            })
        );
        assert_eq!(
            machines.register(Box::new(CounterMachine::new(3, &[12, 12]))),
            Err(RegistrationError::DuplicateKind {
                machine: MachineId(3),
                kind: CommandKind(12)
            })
        );
        assert!(!machines.owns(CommandKind(12)));
        machines.apply(1, command(10, &1u64.to_le_bytes())).unwrap();
        assert_eq!(
            machines.register(Box::new(CounterMachine::new(4, &[40]))),
            Err(RegistrationError::AfterApply { applied_op: 1 })
        );
    }

    #[test]
    fn given_out_of_order_or_unowned_commands_when_applying_should_report_broken_history() {
        let mut machines = counters(&[1]);
        machines.apply(5, command(10, &1u64.to_le_bytes())).unwrap();
        assert_eq!(
            machines.apply(5, command(10, &1u64.to_le_bytes())),
            Err(StateMachineError::Reapplied {
                op: 5,
                applied_op: 5
            })
        );
        assert_eq!(
            machines.apply(6, command(99, &[])),
            Err(StateMachineError::UnownedCommand {
                op: 6,
                kind: CommandKind(99)
            })
        );
        assert_eq!(machines.applied_op(), 5);
    }

    #[test]
    fn given_a_snapshot_when_restoring_should_refuse_only_unregistered_machines() {
        let mut source = counters(&[1]);
        source.apply(4, command(10, &9u64.to_le_bytes())).unwrap();
        source.advance_applied(8);
        let snapshot = source.capture(source.applied_op());
        assert_eq!(snapshot.op(), 8);

        let mut target = counters(&[1]);
        target.restore(&snapshot).unwrap();
        assert_eq!(target.applied_op(), 8);
        assert_eq!(target.capture(target.applied_op()), snapshot);

        let mut other = counters(&[2]);
        assert_eq!(
            other.restore(&snapshot),
            Err(StateMachineError::UnregisteredMachine { id: MachineId(1) })
        );

        let corrupt = StateMachineSnapshot::from_parts(8, &[(MachineId(1), &[1, 2, 3])]);
        assert_eq!(
            target.restore(&corrupt),
            Err(StateMachineError::Restore {
                id: MachineId(1),
                source: MachineRestoreError {
                    reason: "not 8 bytes"
                }
            })
        );
        assert_eq!(target.applied_op(), 0);
    }

    /// A machine added after the snapshot was cut has no part in it, and no
    /// command of its kinds committed before it, so it starts fresh.
    #[test]
    fn given_a_snapshot_without_a_registered_machine_when_restoring_should_start_it_fresh() {
        let mut source = counters(&[1]);
        source.apply(2, command(10, &9u64.to_le_bytes())).unwrap();
        let snapshot = source.capture(source.applied_op());

        let mut target = counters(&[1, 2]);
        target.apply(1, command(20, &4u64.to_le_bytes())).unwrap();
        target.restore(&snapshot).unwrap();
        assert_eq!(target.applied_op(), 2);
        assert_eq!(
            target
                .capture(target.applied_op())
                .machines()
                .collect::<Vec<_>>(),
            vec![
                (MachineId(1), &9u64.to_le_bytes()[..]),
                (MachineId(2), &0u64.to_le_bytes()[..]),
            ]
        );
    }

    #[test]
    fn given_an_empty_registry_when_snapshotting_should_restore_into_another_empty_one() {
        let mut machines = StateMachines::default();
        machines.advance_applied(12);
        let snapshot = machines.capture(machines.applied_op());
        let mut restored = StateMachines::default();
        restored.restore(&snapshot).unwrap();
        assert_eq!(restored.applied_op(), 12);
        assert!(restored.is_empty());
    }

    #[test]
    fn given_machines_past_the_snapshot_bound_when_snapshotting_should_refuse() {
        struct Large;
        impl PartitionStateMachine for Large {
            fn id(&self) -> MachineId {
                MachineId(1)
            }
            fn command_kinds(&self) -> &[CommandKind] {
                &[]
            }
            fn apply(&mut self, _: u64, _: MachineCommand<'_>) -> Applied {
                Applied::Refused
            }
            fn snapshot_into(&self, out: &mut Vec<u8>) {
                out.resize(out.len() + SNAPSHOT_BYTES_MAX, 0);
            }
            fn restore(&mut self, _: &[u8]) -> Result<(), MachineRestoreError> {
                Ok(())
            }
            fn reset(&mut self) {}
        }
        let mut machines = StateMachines::default();
        machines.register(Box::new(Large)).unwrap();
        assert!(matches!(
            machines.snapshot_at(3),
            Err(StateMachineError::SnapshotTooLarge { length }) if length > SNAPSHOT_BYTES_MAX
        ));
        assert_eq!(machines.capture(3).op(), 3);
    }
}
