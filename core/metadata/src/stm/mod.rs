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

pub mod authz;
pub mod consumer_group;
pub mod mux;
pub mod result;
pub mod snapshot;
pub mod stream;
pub mod user;
use iggy_common::Either;
use left_right::{Absorb, ReadHandle, ReadHandleFactory, WriteHandle};
use std::cell::Cell;
use std::cell::UnsafeCell;
use std::sync::Arc;

pub use left_right::ReadHandleFactory as LeftRightFactory;

pub struct WriteCell<T, O>
where
    T: Absorb<O>,
{
    inner: UnsafeCell<WriteHandle<T, O>>,
}

impl<T, O> std::fmt::Debug for WriteCell<T, O>
where
    T: Absorb<O>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCell").finish_non_exhaustive()
    }
}

impl<T, O> WriteCell<T, O>
where
    T: Absorb<O>,
{
    pub const fn new(write: WriteHandle<T, O>) -> Self {
        Self {
            inner: UnsafeCell::new(write),
        }
    }

    /// # Panics
    /// Panics if the inner `UnsafeCell` pointer is null (should never happen
    /// unless the writer was not properly initialized).
    pub fn apply(&self, cmd: O) {
        let hdl = unsafe {
            self.inner
                .get()
                .as_mut()
                .expect("[apply]: called on uninit writer")
        };
        hdl.append(cmd).publish();
    }
}

/// Parses type-erased input into a command. Macro-generated.
/// Returns:
/// - `Ok(Either::Left(cmd))` if applicable
/// - `Ok(Either::Right(input))` to pass ownership back
/// - `Err(error)` for malformed payload/parse errors
#[allow(clippy::missing_errors_doc)]
pub trait Command {
    type Cmd;
    type Input;
    type Error;

    fn parse(input: Self::Input) -> Result<Either<Self::Cmd, Self::Input>, Self::Error>;
}

/// Per-command handler for a given state type.
///
/// Each command implements it for the state it mutates, returning an
/// [`ApplyReply`]: a `code` (0 = success) plus the typed reply `body` to thread
/// into the Reply message.
///
/// Apply MUST be deterministic across replicas: both left/right buffers recompute
/// it independently and must agree. So `timestamp` (the primary's stamp from the
/// enclosing `PrepareHeader`, replicated to every replica) is the only wall-clock
/// source a handler may record, never `IggyTimestamp::now()`; and the result
/// `code` must be a pure function of `(state, command)`, never of clocks or
/// allocator reads.
pub trait StateHandler {
    type State;
    fn apply(
        &self,
        state: &mut Self::State,
        timestamp: iggy_common::IggyTimestamp,
    ) -> result::ApplyReply;
}

#[derive(Debug)]
pub struct LeftRight<T, C>
where
    T: Absorb<C>,
{
    write: Option<WriteCell<T, C>>,
    #[allow(unused)]
    read: Arc<ReadHandle<T>>,
}

impl<T, C> LeftRight<T, C>
where
    T: Absorb<C>,
{
    /// # Panics
    /// Panics if the read handle has been dropped (should never happen in normal operation).
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.read.enter().expect("read handle should be accessible");
        f(&*guard)
    }

    /// Produce a `Send + Sync` factory that can mint additional [`ReadHandle`]s
    /// on other threads. Use this to share the read side across compio shard
    /// runtimes without sharing the (`!Sync`) [`ReadHandle`] itself.
    #[must_use]
    pub fn factory(&self) -> ReadHandleFactory<T> {
        self.read.factory()
    }

    /// Build a reader-only [`LeftRight`] from a factory minted on the owning
    /// thread. The new [`ReadHandle`] is materialised here (the caller's
    /// thread), keeping the `!Sync` discipline local. `write` stays `None`,
    /// so [`Self::try_apply`] returns
    /// [`ApplyOnReaderError::ReaderOnly`] instead of mutating state if the
    /// routing layer ever dispatches a write to this instance.
    ///
    /// Takes the factory by value so the constructor reads as ownership
    /// transfer from the owning shard to the peer-shard reader.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn from_factory(factory: ReadHandleFactory<T>) -> Self {
        Self {
            write: None,
            read: Arc::new(factory.handle()),
        }
    }
}

/// Error returned by [`LeftRight::try_apply`] when invoked on a
/// reader-only instance built via [`LeftRight::from_factory`].
///
/// A reader-only [`LeftRight`] holds no [`WriteCell`] (`write` is `None`)
/// because peer shards rebuild their state machine from a
/// [`ReadHandleFactory`] minted on the owning shard. Hitting this error
/// means the routing layer dispatched a write to a non-owner shard,
/// which is a programming error - the namespace-to-shard mapping owns
/// the invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApplyOnReaderError {
    /// `try_apply` was called on a reader-only [`LeftRight`] (no write
    /// handle). The command was not applied; state is unchanged.
    ReaderOnly,
}

impl std::fmt::Display for ApplyOnReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReaderOnly => f.write_str(
                "apply invoked on reader-only LeftRight (no write handle on this shard)",
            ),
        }
    }
}

impl std::error::Error for ApplyOnReaderError {}

impl<T> From<T> for LeftRight<T, <T as Command>::Cmd>
where
    T: Absorb<<T as Command>::Cmd> + Clone + Command,
{
    fn from(inner: T) -> Self {
        let (write, read) = {
            let (w, r) = left_right::new_from_empty(inner);
            (WriteCell::new(w).into(), r.into())
        };
        Self { write, read }
    }
}

impl<T> LeftRight<T, <T as Command>::Cmd>
where
    T: Absorb<<T as Command>::Cmd> + Clone + Command,
{
    /// Apply `cmd` to the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyOnReaderError::ReaderOnly`] when invoked on an
    /// instance built via [`Self::from_factory`] (peer-shard reader). The
    /// command is dropped without mutating state - the caller is the
    /// routing layer and the invariant violation must be surfaced, not
    /// hidden behind a panic.
    pub fn try_apply(&self, cmd: <T as Command>::Cmd) -> Result<(), ApplyOnReaderError> {
        let cell = self.write.as_ref().ok_or(ApplyOnReaderError::ReaderOnly)?;
        cell.apply(cmd);
        Ok(())
    }
}

/// Public interface for state handlers.
#[allow(clippy::missing_errors_doc)]
pub trait State {
    type Output;
    type Input;
    type Error;

    fn apply(&self, input: Self::Input) -> Result<Either<Self::Output, Self::Input>, Self::Error>;
}

#[allow(clippy::missing_errors_doc)]
pub trait StateMachine {
    type Input;
    type Output;
    type Error;
    fn update(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug)]
pub struct ConsensusGroupAllocator {
    highest: Cell<u64>,
}

impl ConsensusGroupAllocator {
    #[must_use]
    pub const fn new(initial_highest: u64) -> Self {
        Self {
            highest: Cell::new(initial_highest),
        }
    }

    #[must_use]
    pub const fn highest(&self) -> u64 {
        self.highest.get()
    }

    pub fn observe(&self, assigned: u64) {
        if assigned > self.highest.get() {
            self.highest.set(assigned);
        }
    }

    #[must_use]
    ///
    /// # Panics
    /// Panics if allocating `count` more group IDs would overflow `u64`.
    pub fn allocate_many(&self, count: usize) -> Vec<u64> {
        let mut allocated = Vec::with_capacity(count);
        let mut current = self.highest.get();
        for _ in 0..count {
            current = current.checked_add(1).expect("consensus group id overflow");
            allocated.push(current);
        }
        self.highest.set(current);
        allocated
    }
}

/// Generates the state's inner struct and wrapper type.
///
/// # Generated items
/// - `{$state}Inner` struct with the specified fields (the data)
/// - `$state` wrapper struct (contains `LeftRight` storage)
/// - `From<LeftRight<...>>` impl for `$state`
/// - `From<{$state}Inner>` impl for `$state`
///
/// The command enum, parsing, dispatch, and Absorb impl are generated
/// by `collect_handlers!` separately, keeping state definition decoupled
/// from the set of operations.
#[macro_export]
macro_rules! define_state {
    (
        $state:ident {
            $($field_name:ident : $field_type:ty),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, Default)]
            pub struct [<$state Inner>] {
                $(
                    pub $field_name: $field_type,
                )*
                pub last_result: Option<$crate::stm::result::ApplyReply>,
            }

            impl [<$state Inner>] {
                pub fn new() -> Self {
                    Self::default()
                }
            }

            #[derive(Debug)]
            pub struct $state {
                inner: $crate::stm::LeftRight<[<$state Inner>], [<$state Command>]>,
            }

            impl From<$crate::stm::LeftRight<[<$state Inner>], [<$state Command>]>> for $state {
                fn from(inner: $crate::stm::LeftRight<[<$state Inner>], [<$state Command>]>) -> Self {
                    Self { inner }
                }
            }

            impl From<[<$state Inner>]> for $state {
                fn from(inner: [<$state Inner>]) -> Self {
                    let left_right: $crate::stm::LeftRight<[<$state Inner>], [<$state Command>]> = inner.into();
                    left_right.into()
                }
            }

            impl Default for $state {
                fn default() -> Self {
                    [<$state Inner>]::new().into()
                }
            }

            impl $state {
                /// Mint a `Send + Sync` [`ReadHandleFactory`] for this state.
                /// Allows the read side to be carried across shard threads
                /// without sharing the underlying `!Sync` [`ReadHandle`].
                #[must_use]
                pub fn factory(&self) -> $crate::stm::LeftRightFactory<[<$state Inner>]> {
                    self.inner.factory()
                }

                /// Construct a reader-only state wrapper from a factory minted
                /// by the writer-side shard. The thread that calls this owns
                /// the resulting [`ReadHandle`]; calling `apply` on the
                /// returned wrapper panics because `write` is `None`.
                #[must_use]
                pub fn from_factory(
                    factory: $crate::stm::LeftRightFactory<[<$state Inner>]>,
                ) -> Self {
                    Self {
                        inner: $crate::stm::LeftRight::from_factory(factory),
                    }
                }
            }

            impl $crate::stm::mux::WithFactory for $state {
                type Bundle = $crate::stm::LeftRightFactory<[<$state Inner>]>;

                fn factory_bundle(&self) -> Self::Bundle {
                    self.factory()
                }

                fn from_factory_bundle(bundle: Self::Bundle) -> Self {
                    Self::from_factory(bundle)
                }
            }
        }
    };
}

/// Generates the command enum, parsing, dispatch, State, and Absorb for a state type.
///
/// # Generated items
/// - `{$state}Command` enum with one variant per operation
/// - `Command` impl for `{$state}Inner` (parses `Message<PrepareHeader>`)
/// - `{$state}Inner::dispatch()` method (routes each variant to `StateHandler::apply()`)
/// - `State` impl for `$state` wrapper
/// - `Absorb<{$state}Command>` impl for `{$state}Inner`
///
/// # Requirements
/// Each listed operation must have a corresponding `{Operation}Request` wire type
/// that implements `WireDecode` and `StateHandler<State = {$state}Inner>`.
#[macro_export]
macro_rules! collect_handlers {
    (
        $state:ident {
            $($operation:ident),* $(,)?
        }
    ) => {
        paste::paste! {
            #[derive(Debug, Clone)]
            pub enum [<$state Command>] {
                $(
                    $operation([<$operation Request>], ::iggy_common::IggyTimestamp),
                )*
            }

            impl $crate::stm::Command for [<$state Inner>] {
                type Cmd = [<$state Command>];
                type Input = ::server_common::Message<::iggy_binary_protocol::PrepareHeader>;
                type Error = ::iggy_common::IggyError;

                fn parse(input: Self::Input) -> Result<::iggy_common::Either<Self::Cmd, Self::Input>, Self::Error> {
                    use ::iggy_binary_protocol::WireDecode;
                    use ::iggy_common::Either;
                    use ::iggy_binary_protocol::{Operation, PrepareHeader};
                    match input.header().operation {
                        $(
                            Operation::$operation => {
                                // TODO: FIXME, zero allocation operation construction.
                                let header = *input.header();
                                let body = ::bytes::Bytes::copy_from_slice(
                                    &input.as_slice()[core::mem::size_of::<PrepareHeader>()..header.size as usize]
                                );
                                let cmd = [<$operation Request>]::decode_from(&body)
                                    .map_err(|_| ::iggy_common::IggyError::InvalidCommand)?;
                                let ts = ::iggy_common::IggyTimestamp::from(header.timestamp);
                                Ok(Either::Left([<$state Command>]::$operation(cmd, ts)))
                            },
                        )*
                        _ => Ok(Either::Right(input)),
                    }
                }
            }

            impl [<$state Inner>] {
                fn dispatch(&mut self, cmd: &[<$state Command>]) {
                    self.last_result = Some(match cmd {
                        $(
                            [<$state Command>]::$operation(payload, ts) => {
                                $crate::stm::StateHandler::apply(payload, self, *ts)
                            },
                        )*
                    });
                }
            }

            impl $crate::stm::State for $state {
                type Input = <[<$state Inner>] as $crate::stm::Command>::Input;
                type Output = $crate::stm::result::ApplyReply;
                type Error = <[<$state Inner>] as $crate::stm::Command>::Error;

                fn apply(&self, input: Self::Input) -> Result<::iggy_common::Either<Self::Output, Self::Input>, Self::Error> {
                    use ::iggy_common::Either;

                    match <[<$state Inner>] as $crate::stm::Command>::parse(input)? {
                        Either::Left(cmd) => {
                            self.inner.try_apply(cmd).map_err(|err| {
                                ::tracing::error!(
                                    state = stringify!($state),
                                    "metadata write dispatched to reader-only \
                                     state machine: {err}; the routing layer must \
                                     own namespace-to-shard mapping for this state"
                                );
                                ::iggy_common::IggyError::InvalidConfiguration
                            })?;
                            let result = self.inner.read(|state| {
                                state.last_result.clone().unwrap_or_default()
                            });
                            Ok(Either::Left(result))
                        }
                        Either::Right(input) => {
                            Ok(Either::Right(input))
                        }
                    }
                }
            }

            impl left_right::Absorb<[<$state Command>]> for [<$state Inner>] {
                fn absorb_first(&mut self, cmd: &mut [<$state Command>], _other: &Self) {
                    self.dispatch(cmd);
                }

                fn absorb_second(&mut self, cmd: [<$state Command>], _other: &Self) {
                    self.dispatch(&cmd);
                }

                fn sync_with(&mut self, first: &Self) {
                    *self = first.clone();
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use left_right::Absorb;

    #[derive(Debug, Default, Clone)]
    struct Counter(u64);

    #[derive(Debug, Clone, Copy)]
    enum CounterCmd {
        Inc,
    }

    impl Absorb<CounterCmd> for Counter {
        fn absorb_first(&mut self, _cmd: &mut CounterCmd, _other: &Self) {
            self.0 += 1;
        }

        fn absorb_second(&mut self, _cmd: CounterCmd, _other: &Self) {
            self.0 += 1;
        }

        fn sync_with(&mut self, first: &Self) {
            *self = first.clone();
        }
    }

    impl Command for Counter {
        type Cmd = CounterCmd;
        type Input = ();
        type Error = ();

        fn parse(_input: Self::Input) -> Result<Either<Self::Cmd, Self::Input>, Self::Error> {
            Ok(Either::Left(CounterCmd::Inc))
        }
    }

    #[test]
    fn try_apply_on_writer_mutates_state() {
        let lr: LeftRight<Counter, CounterCmd> = Counter::default().into();
        lr.try_apply(CounterCmd::Inc)
            .expect("writer must accept apply");
        assert_eq!(lr.read(|c| c.0), 1);
    }

    #[test]
    fn try_apply_on_reader_returns_reader_only_error() {
        // Reader-only LeftRight (built from a factory minted on the writer)
        // must surface `ApplyOnReaderError::ReaderOnly` instead of mutating
        // state or panicking - the routing layer is the only caller and the
        // invariant violation has to be visible to it.
        let writer: LeftRight<Counter, CounterCmd> = Counter::default().into();
        let reader = LeftRight::<Counter, CounterCmd>::from_factory(writer.factory());
        let err = reader
            .try_apply(CounterCmd::Inc)
            .expect_err("reader-only instance must reject try_apply");
        assert_eq!(err, ApplyOnReaderError::ReaderOnly);
        assert_eq!(
            reader.read(|c| c.0),
            0,
            "reader state must be unchanged after a rejected try_apply"
        );
    }
}
