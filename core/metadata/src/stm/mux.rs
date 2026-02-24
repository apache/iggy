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

use crate::stm::snapshot::{FillSnapshot, RestoreSnapshot, SnapshotError};
use iggy_common::Either;
use iggy_common::variadic;
use iggy_common::{header::PrepareHeader, message::Message};

use crate::stm::{State, StateMachine};

// MuxStateMachine that proxies to an tuple of variadic state machines
#[derive(Debug)]
pub struct MuxStateMachine<T>
where
    T: StateMachine,
{
    inner: T,
}

impl<T> MuxStateMachine<T>
where
    T: StateMachine,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> StateMachine for MuxStateMachine<T>
where
    T: StateMachine,
{
    type Input = T::Input;
    type Output = T::Output;
    type Error = T::Error;

    fn update(&self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        self.inner.update(input)
    }
}

// TODO: Figure out how to get around the fact that we need to hardcode the Input/Output type for base case.
// TODO: I think we could move the base case to the impl site of `State`, so this way we know the `Input` and `Output` types.
// Base case of the recursive resolution.
impl StateMachine for () {
    type Input = Message<PrepareHeader>;
    // TODO: Make sure that the `Output` matches to the output type of the rest of list.
    // TODO: Add a trait bound to the output that will allow us to get the response in bytes.
    type Output = ();
    type Error = iggy_common::IggyError;

    fn update(&self, _input: Self::Input) -> Result<Self::Output, Self::Error> {
        Ok(())
    }
}

// Recursive case: process head and recurse on tail
// No Clone bound needed - ownership passes through via `Either`
impl<O, E, S, Rest> StateMachine for variadic!(S, ...Rest)
where
    S: State<Output = O, Error = E>,
    Rest: StateMachine<Input = S::Input, Output = O, Error = E>,
{
    type Input = Rest::Input;
    type Output = O;
    type Error = E;

    fn update(&self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        match self.0.apply(input)? {
            Either::Left(result) => Ok(result),
            Either::Right(input) => self.1.update(input),
        }
    }
}

/// Recursive case for variadic tuple pattern: (Head, Tail)
/// Fills snapshot from head and tail, and restores both on restore.
impl<SnapshotData, Head, Tail> FillSnapshot<SnapshotData> for variadic!(Head, ...Tail)
where
    Head: FillSnapshot<SnapshotData>,
    Tail: FillSnapshot<SnapshotData>,
{
    fn fill_snapshot(&self, snapshot: &mut SnapshotData) -> Result<(), SnapshotError> {
        self.0.fill_snapshot(snapshot)?;
        self.1.fill_snapshot(snapshot)?;
        Ok(())
    }
}

impl<SnapshotData, Head, Tail> RestoreSnapshot<SnapshotData> for variadic!(Head, ...Tail)
where
    Head: RestoreSnapshot<SnapshotData>,
    Tail: RestoreSnapshot<SnapshotData>,
{
    fn restore_snapshot(snapshot: &SnapshotData) -> Result<Self, SnapshotError> {
        let head = Head::restore_snapshot(snapshot)?;
        let tail = Tail::restore_snapshot(snapshot)?;
        Ok((head, tail))
    }
}

impl<SnapshotData, T> FillSnapshot<SnapshotData> for MuxStateMachine<T>
where
    T: StateMachine + FillSnapshot<SnapshotData>,
{
    fn fill_snapshot(&self, snapshot: &mut SnapshotData) -> Result<(), SnapshotError> {
        self.inner.fill_snapshot(snapshot)
    }
}

impl<SnapshotData, T> RestoreSnapshot<SnapshotData> for MuxStateMachine<T>
where
    T: StateMachine + RestoreSnapshot<SnapshotData>,
{
    fn restore_snapshot(snapshot: &SnapshotData) -> Result<Self, SnapshotError> {
        let inner = T::restore_snapshot(snapshot)?;
        Ok(MuxStateMachine::new(inner))
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
    use crate::stm::snapshot::{FillSnapshot, MetadataSnapshot, RestoreSnapshot, Snapshotable};
    use crate::stm::stream::{Streams, StreamsInner};
    use crate::stm::user::{Users, UsersInner};

    #[test]
    fn construct_mux_state_machine_from_states_with_same_output() {
        use crate::stm::StateMachine;
        use crate::stm::mux::MuxStateMachine;
        use crate::stm::stream::{Streams, StreamsInner};
        use crate::stm::user::{Users, UsersInner};
        use iggy_common::header::PrepareHeader;
        use iggy_common::message::Message;

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let mux = MuxStateMachine::new(variadic!(users, streams));

        let input = Message::new(std::mem::size_of::<PrepareHeader>());

        let _ = mux.update(input);
    }

    #[test]
    fn mux_state_machine_snapshot_roundtrip() {
        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();

        let mux = MuxStateMachine::new(variadic!(users, streams, consumer_groups));

        // Fill the typed snapshot
        let mut snapshot = MetadataSnapshot::new(12345);
        mux.fill_snapshot(&mut snapshot).unwrap();

        // Verify all fields are filled
        assert!(snapshot.users.is_some());
        assert!(snapshot.streams.is_some());
        assert!(snapshot.consumer_groups.is_some());

        // Restore and verify
        type MuxTuple = (Users, (Streams, (ConsumerGroups, ())));
        let restored: MuxStateMachine<MuxTuple> =
            MuxStateMachine::restore_snapshot(&snapshot).unwrap();

        // Verify the restored mux produces the same snapshot
        let mut verify_snapshot = MetadataSnapshot::new(0);
        restored.fill_snapshot(&mut verify_snapshot).unwrap();
        assert!(verify_snapshot.users.is_some());
        assert!(verify_snapshot.streams.is_some());
        assert!(verify_snapshot.consumer_groups.is_some());
    }

    #[test]
    fn mux_state_machine_full_envelope_roundtrip() {
        use crate::impls::metadata::IggySnapshot;
        use crate::stm::snapshot::Snapshot;

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();

        type MuxTuple = (Users, (Streams, (ConsumerGroups, ())));
        let mux: MuxStateMachine<MuxTuple> =
            MuxStateMachine::new(variadic!(users, streams, consumer_groups));

        let sequence_number = 12345u64;
        let snapshot = IggySnapshot::create(&mux, sequence_number).unwrap();

        assert_eq!(snapshot.sequence_number(), sequence_number);
        assert!(snapshot.created_at() > 0);

        // Encode to bytes
        let encoded = snapshot.encode().unwrap();
        assert!(!encoded.is_empty());

        // Decode from bytes
        let decoded = IggySnapshot::decode(&encoded).unwrap();
        assert_eq!(decoded.sequence_number(), sequence_number);

        // Verify snapshot fields are present
        assert!(decoded.snapshot().users.is_some());
        assert!(decoded.snapshot().streams.is_some());
        assert!(decoded.snapshot().consumer_groups.is_some());

        // Restore MuxStateMachine from the state side (symmetric with fill_snapshot)
        let restored: MuxStateMachine<MuxTuple> =
            MuxStateMachine::restore_snapshot(decoded.snapshot()).unwrap();

        // Verify restored state
        let mut verify_snapshot = MetadataSnapshot::new(0);
        restored.fill_snapshot(&mut verify_snapshot).unwrap();
        assert!(verify_snapshot.users.is_some());
        assert!(verify_snapshot.streams.is_some());
        assert!(verify_snapshot.consumer_groups.is_some());
    }
}
