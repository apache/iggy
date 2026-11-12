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

//! How a partition drives its state machines: admission and the pipeline
//! share on the primary, apply at commit on every replica, and the snapshot
//! that checkpoints and state transfer carry.

use consensus::{Consensus, Pipeline};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, RoutedRequestHeader};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use message_bus::{AUTO_COMMIT_CLIENT_ID, MessageBus, is_auto_commit_client};
use server_common::Message;
use tracing::{debug, error};

use super::{
    Applied, CommandEncodeError, CommandKind, MachineCommand, MachineId, PartitionStateMachine,
    RegistrationError, StateMachineError, StateMachineSnapshot, StateMachines,
    state_machine_pipeline_slots,
};
use crate::IggyPartition;
use crate::types::FatalCommit;

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Add a state machine to this partition.
    ///
    /// Register at construction, on every replica, before the partition opens
    /// its persistence, restores its receipts checkpoint or installs a
    /// transfer. Restoring a checkpoint moves the applied op even when the
    /// checkpoint carries no machine, and registration is refused from then
    /// on. Every replica registers the same machines. One added by a later
    /// release starts fresh on a checkpoint cut before it existed.
    ///
    /// # Errors
    /// Returns an error when the machine's id or one of its command kinds is
    /// taken, or after the partition restored or applied an op.
    pub fn register_state_machine(
        &mut self,
        machine: Box<dyn PartitionStateMachine>,
    ) -> Result<(), RegistrationError> {
        self.state_machines.register(machine)
    }

    #[must_use]
    pub const fn state_machines(&self) -> &StateMachines {
        &self.state_machines
    }

    /// The registered machine `id`, when it is a `T`: how its owner reads the
    /// state its commands produced.
    #[must_use]
    pub fn state_machine<T: PartitionStateMachine>(&self, id: MachineId) -> Option<&T> {
        self.state_machines.machine(id)
    }

    /// Whether this replica may act on its machines' state as the primary: it
    /// is the primary of a running view, applied every command committed
    /// before that view started, and has not stopped on a fatal failure.
    #[must_use]
    pub fn state_machines_current(&self) -> bool {
        let consensus = self.consensus();
        self.fatal.is_none()
            && consensus.is_primary()
            && consensus.is_normal()
            && !consensus.is_transferring()
            && consensus.applied_view_start()
    }

    /// A request carrying a command of `kind`, for [`Self::on_request`] on
    /// the primary. Commands are server-originated, so the request runs under
    /// the auto-commit client and keeps no receipt.
    ///
    /// `request` is the caller's own number for the command, stamped on its
    /// prepare. The partition does not deduplicate commands by it, see
    /// [`PartitionStateMachine::apply`].
    ///
    /// # Errors
    /// Returns an error when the payload is too large for a command.
    pub fn command_request(
        &self,
        kind: CommandKind,
        payload: &[u8],
        request: u64,
    ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError> {
        let command = MachineCommand { kind, payload };
        let size = size_of::<RoutedRequestHeader>() + command.encoded_len()?;
        let wire_size =
            u32::try_from(size).map_err(|_| CommandEncodeError::TooLarge { length: size })?;
        let mut message = Message::<RoutedRequestHeader>::new(size);
        command.encode_into(&mut message.as_mut_slice()[size_of::<RoutedRequestHeader>()..]);
        let group = self.consensus().group();
        let incarnation = self.created_revision();
        Ok(
            message.transmute_header(|_, header: &mut RoutedRequestHeader| {
                *header = RoutedRequestHeader {
                    command: Command::Request,
                    operation: Operation::StateMachineCommand,
                    size: wire_size,
                    client: AUTO_COMMIT_CLIENT_ID,
                    session: 1,
                    request,
                    group,
                    partition_incarnation: incarnation,
                    ..Default::default()
                };
            }),
        )
    }

    /// Why the primary refuses a command request, if it does.
    pub(crate) fn command_refusal(
        &self,
        header: &RoutedRequestHeader,
        body: &[u8],
    ) -> Option<(IggyError, &'static str)> {
        if !is_auto_commit_client(header.client) {
            return Some((
                IggyError::InvalidCommand,
                "state-machine commands are server-originated",
            ));
        }
        let Ok(command) = MachineCommand::decode(body) else {
            return Some((IggyError::InvalidCommand, "malformed state-machine command"));
        };
        if !self.state_machines.owns(command.kind) {
            return Some((
                IggyError::InvalidCommand,
                "no state machine owns the command kind",
            ));
        }
        if !self.state_machines_current() {
            return Some((
                IggyError::TransientNotAccepted,
                "state machines have not applied the view's history",
            ));
        }
        // A command never waits in the request queue, so the slots it takes
        // are the ones counted here.
        if self.local_request_must_queue()
            || self.uncommitted_commands() >= self.command_pipeline_slots()
        {
            return Some((
                IggyError::TransientNotAccepted,
                "state-machine commands hold their share of the pipeline",
            ));
        }
        None
    }

    /// Commands' share of this partition's pipeline, whose depth is
    /// configured per node.
    pub(crate) fn command_pipeline_slots(&self) -> usize {
        self.consensus()
            .with_pipeline(|pipeline| state_machine_pipeline_slots(pipeline.prepare_queue_max()))
    }

    fn uncommitted_commands(&self) -> usize {
        let commit_min = self.consensus().commit_min();
        self.log
            .journal()
            .inner
            .commands_in(commit_min.saturating_add(1)..)
    }

    /// Check a replicated command before journaling it. Ownership is not
    /// checked here: a replica that does not own a kind its peers own is
    /// misconfigured, which the apply turns into a stop instead of a stall.
    pub(crate) fn check_replicated_command(body: &[u8]) -> Result<(), IggyError> {
        MachineCommand::decode(body)
            .map(|_| ())
            .map_err(|_| IggyError::InvalidCommand)
    }

    /// Whether the committed command at `header` is held and well formed, so
    /// its apply at publication cannot fail on missing bytes.
    pub(crate) fn committed_command_is_held(&self, header: &PrepareHeader) -> bool {
        self.log
            .journal()
            .inner
            .repair_entry(header.op)
            .is_some_and(|prepare| {
                prepare
                    .as_slice()
                    .get(iggy_binary_protocol::HEADER_SIZE..)
                    .is_some_and(|body| MachineCommand::decode(body).is_ok())
            })
    }

    /// Apply the committed command at `header` to its owner, in op order with
    /// every other committed op. A refusal commits as a no-op. A command that
    /// does not follow the applied op or that no machine owns means broken
    /// history, which stops the partition.
    pub(crate) fn apply_committed_command(&mut self, header: &PrepareHeader) -> bool {
        let Some(prepare) = self.log.journal().inner.repair_entry(header.op) else {
            return self.stop_on_command(header, "committed command is not held");
        };
        let Some(Ok(command)) = prepare
            .as_slice()
            .get(iggy_binary_protocol::HEADER_SIZE..)
            .map(MachineCommand::decode)
        else {
            return self.stop_on_command(header, "committed command is malformed");
        };
        match self.state_machines.apply(header.op, command) {
            Ok(Applied::Changed) => true,
            Ok(Applied::Refused) => {
                debug!(
                    namespace_raw = header.group,
                    op = header.op,
                    kind = command.kind.0,
                    "state machine refused a committed command"
                );
                true
            }
            Err(error) => self.stop_on_command(header, &error.to_string()),
        }
    }

    fn stop_on_command(&mut self, header: &PrepareHeader, reason: &str) -> bool {
        error!(
            namespace_raw = header.group,
            op = header.op,
            reason,
            "cannot apply a committed state-machine command"
        );
        self.fatal = Some(FatalCommit {
            namespace_raw: header.group,
            op: header.op,
            operation: Some(Operation::StateMachineCommand),
        });
        false
    }

    /// Every machine at `op`, which must be the applied frontier. `None` for
    /// a partition that registers no machine.
    ///
    /// # Errors
    /// Returns an error when the snapshot is past the size every reader
    /// refuses, so no checkpoint or transfer carries what cannot be restored.
    pub(crate) fn machines_snapshot_at(
        &self,
        op: u64,
    ) -> Result<Option<StateMachineSnapshot>, StateMachineError> {
        if self.state_machines.is_empty() {
            return Ok(None);
        }
        self.state_machines.snapshot_at(op).map(Some)
    }

    /// Check a snapshot an install or a checkpoint carries against the
    /// registered machines, before anything changes. A checkpoint without
    /// one, or without a part for a registered machine, predates that
    /// machine, which then starts fresh.
    pub(crate) fn check_machines_snapshot(
        &self,
        snapshot: Option<&StateMachineSnapshot>,
        op: u64,
    ) -> Result<(), StateMachineError> {
        match snapshot {
            Some(snapshot) if snapshot.op() != op => Err(StateMachineError::SnapshotOp {
                expected: op,
                found: snapshot.op(),
            }),
            Some(snapshot) => self.state_machines.check_snapshot(snapshot),
            None => Ok(()),
        }
    }

    /// Replace every machine's state with `snapshot`, or the state before any
    /// command when the checkpoint carries none.
    pub(crate) fn restore_machines(
        &mut self,
        snapshot: Option<&StateMachineSnapshot>,
        op: u64,
    ) -> Result<(), StateMachineError> {
        let Some(snapshot) = snapshot else {
            self.state_machines.reset();
            self.state_machines.advance_applied(op);
            return Ok(());
        };
        if snapshot.op() != op {
            return Err(StateMachineError::SnapshotOp {
                expected: op,
                found: snapshot.op(),
            });
        }
        self.state_machines.restore(snapshot)
    }
}
