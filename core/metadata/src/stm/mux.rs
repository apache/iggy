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

use crate::stm::snapshot::{SnapshotContributor, SnapshotError, SnapshotSection};
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

    fn update(&self, input: Self::Input) -> Self::Output {
        self.inner.update(input)
    }
}

//TODO: Move to common
#[macro_export]
macro_rules! variadic {
    () => ( () );
    (...$a:ident  $(,)? ) => ( $a );
    (...$a:expr  $(,)? ) => ( $a );
    ($a:ident  $(,)? ) => ( ($a, ()) );
    ($a:expr  $(,)? ) => ( ($a, ()) );
    ($a:ident,  $( $b:tt )+) => ( ($a, variadic!( $( $b )* )) );
    ($a:expr,  $( $b:tt )+) => ( ($a, variadic!( $( $b )* )) );
}

// TODO: Figure out how to get around the fact that we need to hardcode the Input/Output type for base case.
// TODO: I think we could move the base case to the impl site of `State`, so this way we know the `Input` and `Output` types.
// Base case of the recursive resolution.
impl StateMachine for () {
    type Input = Message<PrepareHeader>;
    // TODO: Make sure that the `Output` matches to the output type of the rest of list.
    // TODO: Add a trait bound to the output that will allow us to get the response in bytes.
    type Output = ();

    fn update(&self, _input: Self::Input) -> Self::Output {}
}

// Recursive case: process head and recurse on tail
// No Clone bound needed - ownership passes through via Result
impl<O, S, Rest> StateMachine for variadic!(S, ...Rest)
where
    S: State<Output = O>,
    Rest: StateMachine<Input = S::Input, Output = O>,
{
    type Input = Rest::Input;
    type Output = O;

    fn update(&self, input: Self::Input) -> Self::Output {
        match self.0.apply(input) {
            Ok(result) => result,
            Err(input) => self.1.update(input),
        }
    }
}

/// Recursive case for variadic tuple pattern: (Head, Tail)
/// Collects snapshot sections from head and tail, and restores both on restore.
impl<S, Rest> SnapshotContributor for variadic!(S, ...Rest)
where
    S: SnapshotContributor,
    Rest: SnapshotContributor,
{
    fn collect_sections(&self, sections: &mut Vec<SnapshotSection>) -> Result<(), SnapshotError> {
        self.0.collect_sections(sections)?;
        self.1.collect_sections(sections)?;
        Ok(())
    }

    fn restore_from_sections(sections: &[SnapshotSection]) -> Result<Self, SnapshotError> {
        let head = S::restore_from_sections(sections)?;
        let tail = Rest::restore_from_sections(sections)?;
        Ok((head, tail))
    }

    fn known_section_names() -> Vec<&'static str> {
        let mut names = S::known_section_names();
        names.extend(Rest::known_section_names());
        names
    }
}

impl<T> SnapshotContributor for MuxStateMachine<T>
where
    T: StateMachine + SnapshotContributor,
{
    fn collect_sections(&self, sections: &mut Vec<SnapshotSection>) -> Result<(), SnapshotError> {
        self.inner.collect_sections(sections)
    }

    fn restore_from_sections(sections: &[SnapshotSection]) -> Result<Self, SnapshotError> {
        let inner = T::restore_from_sections(sections)?;
        Ok(MuxStateMachine::new(inner))
    }

    fn known_section_names() -> Vec<&'static str> {
        T::known_section_names()
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
    use crate::stm::snapshot::{SnapshotContributor, SnapshotEnvelope, Snapshotable};
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

        mux.update(input);
    }

    #[test]
    fn mux_state_machine_snapshot_roundtrip() {
        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();

        let mux = MuxStateMachine::new(variadic!(users, streams, consumer_groups));

        // Collect all sections
        let mut sections = Vec::new();
        mux.collect_sections(&mut sections).unwrap();

        // Should have 3 sections: users, streams, consumer_groups
        assert_eq!(sections.len(), 3);

        let section_names: Vec<&str> = sections.iter().map(|s| s.name.as_str()).collect();
        assert!(section_names.contains(&"users"));
        assert!(section_names.contains(&"streams"));
        assert!(section_names.contains(&"consumer_groups"));

        // Restore and verify
        type MuxTuple = (Users, (Streams, (ConsumerGroups, ())));
        let restored: MuxStateMachine<MuxTuple> =
            MuxStateMachine::restore_from_sections(&sections).unwrap();

        // Verify the restored mux has empty state machines
        let mut verify_sections = Vec::new();
        restored.collect_sections(&mut verify_sections).unwrap();
        assert_eq!(verify_sections.len(), 3);
    }

    #[test]
    fn mux_state_machine_full_envelope_roundtrip() {
        use crate::impls::metadata::{IggySnapshot, MetadataSnapshot};

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();

        type MuxTuple = (Users, (Streams, (ConsumerGroups, ())));
        let mux: MuxStateMachine<MuxTuple> =
            MuxStateMachine::new(variadic!(users, streams, consumer_groups));

        let commit_number = 12345u64;
        let snapshot = IggySnapshot::create(&mux, commit_number).unwrap();

        assert_eq!(snapshot.commit_number(), commit_number);
        assert!(snapshot.created_at() > 0);

        // Encode to bytes
        let encoded = snapshot.encode().unwrap();
        assert!(!encoded.is_empty());

        // Decode from bytes
        let decoded = IggySnapshot::decode(&encoded).unwrap();
        assert_eq!(decoded.commit_number(), commit_number);
        assert_eq!(decoded.envelope().sections.len(), 3);

        // Restore MuxStateMachine
        let restored: MuxStateMachine<MuxTuple> = decoded.restore().unwrap();

        // Verify restored state
        let mut sections = Vec::new();
        restored.collect_sections(&mut sections).unwrap();
        assert_eq!(sections.len(), 3);
    }
}
