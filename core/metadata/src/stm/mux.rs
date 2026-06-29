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
use iggy_binary_protocol::PrepareHeader;
use iggy_common::Either;
use iggy_common::variadic;
use server_common::Message;

use crate::stm::{State, StateMachine};

/// Cross-thread read-side handoff for a state machine.
///
/// The owning shard (typically shard 0) materialises a `Bundle` after
/// recovering the WAL and hands one clone to every peer shard. Peer
/// shards then call [`WithFactory::from_factory_bundle`] on their own
/// runtime to construct a reader-mode state machine — no WAL replay,
/// no shared `!Sync` `ReadHandle`, just `Send + Sync` factories per
/// state.
///
/// The variadic tuple impl below recurses over the state list, so a
/// `MuxStateMachine<variadic!(Users, Streams)>` ends up
/// with `Bundle = (Users::Bundle, (Streams::Bundle, ()))`.
pub trait WithFactory: Sized {
    type Bundle: Clone + Send + Sync;
    fn factory_bundle(&self) -> Self::Bundle;
    fn from_factory_bundle(bundle: Self::Bundle) -> Self;
}

impl WithFactory for () {
    type Bundle = ();
    fn factory_bundle(&self) -> Self::Bundle {}
    fn from_factory_bundle((): Self::Bundle) -> Self {}
}

impl<S, Rest> WithFactory for variadic!(S, ...Rest)
where
    S: WithFactory,
    Rest: WithFactory,
{
    type Bundle = (S::Bundle, Rest::Bundle);

    fn factory_bundle(&self) -> Self::Bundle {
        (self.0.factory_bundle(), self.1.factory_bundle())
    }

    fn from_factory_bundle(bundle: Self::Bundle) -> Self {
        (
            S::from_factory_bundle(bundle.0),
            Rest::from_factory_bundle(bundle.1),
        )
    }
}

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
    #[must_use]
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    #[must_use]
    pub const fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T> Default for MuxStateMachine<T>
where
    T: StateMachine + Default,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> MuxStateMachine<T>
where
    T: StateMachine + WithFactory,
{
    /// Mint a factory bundle from the owning state machine so peer shards
    /// can rebuild a reader-mode mirror on their own runtimes.
    #[must_use]
    pub fn factory_bundle(&self) -> T::Bundle {
        self.inner.factory_bundle()
    }

    /// Reconstruct a reader-mode `MuxStateMachine` from a bundle minted
    /// on the writer-side shard. Must be invoked on the destination
    /// runtime so the inner `!Sync` `ReadHandle`s are created in place.
    #[must_use]
    pub fn from_factory_bundle(bundle: T::Bundle) -> Self {
        Self::new(T::from_factory_bundle(bundle))
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

// Base case of the recursive resolution.
impl StateMachine for () {
    type Input = Message<PrepareHeader>;
    type Output = crate::stm::result::ApplyReply;
    type Error = iggy_common::IggyError;

    fn update(&self, _input: Self::Input) -> Result<Self::Output, Self::Error> {
        Ok(crate::stm::result::ApplyReply::default())
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
        Ok(Self::new(inner))
    }
}

#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::stm::snapshot::{FillSnapshot, MetadataSnapshot, RestoreSnapshot, Snapshotable};
    use crate::stm::stream::{Streams, StreamsInner};
    use crate::stm::user::{Users, UsersInner};

    #[test]
    fn construct_mux_state_machine_from_states_with_same_output() {
        use crate::stm::StateMachine;
        use crate::stm::mux::MuxStateMachine;
        use crate::stm::stream::{Streams, StreamsInner};
        use crate::stm::user::{Users, UsersInner};
        use iggy_binary_protocol::PrepareHeader;
        use server_common::Message;

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let mux = MuxStateMachine::new(variadic!(users, streams));

        let input = Message::new(std::mem::size_of::<PrepareHeader>());

        let _ = mux.update(input);
    }

    #[test]
    fn mux_state_machine_snapshot_roundtrip() {
        type MuxTuple = (Users, (Streams, ()));

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();

        let mux = MuxStateMachine::new(variadic!(users, streams));

        // Fill the typed snapshot
        let mut snapshot = MetadataSnapshot::new(12345);
        mux.fill_snapshot(&mut snapshot).unwrap();

        // Verify all fields are filled
        assert!(snapshot.users.is_some());
        assert!(snapshot.streams.is_some());

        let restored: MuxStateMachine<MuxTuple> =
            MuxStateMachine::restore_snapshot(&snapshot).unwrap();

        // Verify the restored mux produces the same snapshot
        let mut verify_snapshot = MetadataSnapshot::new(0);
        restored.fill_snapshot(&mut verify_snapshot).unwrap();
        assert!(verify_snapshot.users.is_some());
        assert!(verify_snapshot.streams.is_some());
    }

    #[test]
    fn mux_state_machine_full_envelope_roundtrip() {
        use crate::impls::metadata::IggySnapshot;
        use crate::stm::snapshot::Snapshot;

        type MuxTuple = (Users, (Streams, ()));

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();

        let mux: MuxStateMachine<MuxTuple> = MuxStateMachine::new(variadic!(users, streams));

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

        // Restore MuxStateMachine from the state side (symmetric with fill_snapshot)
        let restored: MuxStateMachine<MuxTuple> =
            MuxStateMachine::restore_snapshot(decoded.snapshot()).unwrap();

        // Verify restored state
        let mut verify_snapshot = MetadataSnapshot::new(0);
        restored.fill_snapshot(&mut verify_snapshot).unwrap();
        assert!(verify_snapshot.users.is_some());
        assert!(verify_snapshot.streams.is_some());
    }

    #[test]
    fn mux_state_machine_default_initializes_empty_state() {
        type MuxTuple = (Users, (Streams, ()));

        let mux = MuxStateMachine::<MuxTuple>::default();

        let mut verify_snapshot = MetadataSnapshot::new(0);
        mux.fill_snapshot(&mut verify_snapshot).unwrap();
        assert!(verify_snapshot.users.is_some());
        assert!(verify_snapshot.streams.is_some());
    }
}
