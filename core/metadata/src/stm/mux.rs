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

// Base case: panics if no state machine in the chain accepted the input.
impl StateMachine for () {
    type Input = Message<PrepareHeader>;
    type Output = ();

    fn update(&self, _input: Self::Input) -> Self::Output {
        panic!("unhandled command: no state machine accepted the input");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::State;
    use crate::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
    use crate::stm::mux::MuxStateMachine;
    use crate::stm::snapshot::{FillSnapshot, MetadataSnapshot, RestoreSnapshot, Snapshotable};
    use crate::stm::stream::{Streams, StreamsInner};
    use crate::stm::user::{Users, UsersInner};
    use bytes::{Bytes, BytesMut};
    use iggy_common::BytesSerializable;
    use iggy_common::header::{Command2, Operation, PrepareHeader};
    use iggy_common::message::Message;
    use std::mem;

    fn build_prepare_message(operation: Operation, body: Bytes) -> Message<PrepareHeader> {
        let header_size = mem::size_of::<PrepareHeader>();
        let total_size = header_size + body.len();
        let mut buffer = BytesMut::zeroed(total_size);

        let header = bytemuck::from_bytes_mut::<PrepareHeader>(&mut buffer[..header_size]);
        header.command = Command2::Prepare;
        header.operation = operation;
        header.size = total_size as u32;

        buffer[header_size..].copy_from_slice(&body);
        Message::<PrepareHeader>::from_bytes(buffer.freeze()).unwrap()
    }

    #[test]
    fn create_stream_applies_and_is_readable() {
        let streams: Streams = StreamsInner::new().into();

        let cmd = iggy_common::create_stream::CreateStream {
            name: "test".to_string(),
        };
        let msg = build_prepare_message(Operation::CreateStream, cmd.to_bytes());

        let result = streams.apply(msg);
        assert!(result.is_ok());

        streams.with_state(|inner| {
            assert_eq!(inner.items.len(), 1);
            assert_eq!(inner.index.len(), 1);

            let (id, stream) = inner.items.iter().next().unwrap();
            assert_eq!(stream.name.as_ref(), "test");
            assert_eq!(stream.id, id);
            assert_eq!(inner.index.get("test"), Some(&id));
        });
    }

    #[test]
    fn create_user_applies_and_is_readable() {
        let users: Users = UsersInner::new().into();

        let cmd = iggy_common::create_user::CreateUser {
            username: "admin".to_string(),
            password: "secret123".to_string(),
            status: iggy_common::UserStatus::Active,
            permissions: None,
        };
        let msg = build_prepare_message(Operation::CreateUser, cmd.to_bytes());

        let result = users.apply(msg);
        assert!(result.is_ok());

        users.with_state(|inner| {
            assert_eq!(inner.items.len(), 1);
            assert_eq!(inner.index.len(), 1);

            let (id, user) = inner.items.iter().next().unwrap();
            assert_eq!(user.username.as_ref(), "admin");
            assert_eq!(user.id, id as iggy_common::UserId);
            assert_eq!(user.status, iggy_common::UserStatus::Active);
            assert_eq!(inner.index.get("admin"), Some(&(id as iggy_common::UserId)));
            assert!(
                inner
                    .personal_access_tokens
                    .contains_key(&(id as iggy_common::UserId))
            );
        });
    }

    #[test]
    #[should_panic(expected = "unhandled command: no state machine accepted the input")]
    fn unhandled_operation_falls_through() {
        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let mux = MuxStateMachine::new(variadic!(users, streams));

        let input = Message::new(mem::size_of::<PrepareHeader>());

        mux.update(input);
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
