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

pub mod consumer_group;
pub mod mux;
pub mod snapshot;
pub mod stream;
pub mod user;

use iggy_common::Either;
use left_right::*;
use std::cell::UnsafeCell;
use std::sync::Arc;

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
    pub fn new(write: WriteHandle<T, O>) -> Self {
        Self {
            inner: UnsafeCell::new(write),
        }
    }

    pub fn apply(&self, cmd: O) {
        let hdl = unsafe {
            self.inner
                .get()
                .as_mut()
                .expect("[apply]: called on uninit writer, for cmd: {cmd}")
        };
        hdl.append(cmd).publish();
    }
}

/// Parses type-erased input into a command. Macro-generated.
/// Returns:
/// - `Ok(Either::Left(cmd))` if applicable
/// - `Ok(Either::Right(input))` to pass ownership back
/// - `Err(error)` for malformed payload/parse errors
pub trait Command {
    type Cmd;
    type Input;
    type Error;

    fn parse(input: Self::Input) -> Result<Either<Self::Cmd, Self::Input>, Self::Error>;
}

/// Per-command handler for a given state type.
/// Each command struct implements this for the state it mutates.
pub trait StateHandler {
    type State;
    fn apply(&self, state: &mut Self::State);
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
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.read.enter().expect("read handle should be accessible");
        f(&*guard)
    }
}

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
    pub fn do_apply(&self, cmd: <T as Command>::Cmd) {
        self.write
            .as_ref()
            .expect("no write handle - not the owner shard")
            .apply(cmd);
    }
}

/// Public interface for state handlers.
pub trait State {
    type Output;
    type Input;
    type Error;

    fn apply(&self, input: Self::Input) -> Result<Either<Self::Output, Self::Input>, Self::Error>;
}

pub trait StateMachine {
    type Input;
    type Output;
    type Error;
    fn update(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}

/// Generates the state's inner struct and wrapper type.
///
/// # Generated items
/// - `{$state}Inner` struct with the specified fields (the data)
/// - `$state` wrapper struct (contains LeftRight storage)
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
/// Each listed operation type must implement `StateHandler<{$state}Inner>`.
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
                    $operation($operation),
                )*
            }

            impl $crate::stm::Command for [<$state Inner>] {
                type Cmd = [<$state Command>];
                type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;
                type Error = ::iggy_common::IggyError;

                fn parse(input: Self::Input) -> Result<::iggy_common::Either<Self::Cmd, Self::Input>, Self::Error> {
                    use ::iggy_common::BytesSerializable;
                    use ::iggy_common::Either;
                    use ::iggy_common::header::Operation;

                    match input.header().operation {
                        $(
                            Operation::$operation => {
                                let body = input.body_bytes();
                                let cmd = $operation::from_bytes(body)?;
                                Ok(Either::Left([<$state Command>]::$operation(cmd)))
                            },
                        )*
                        _ => Ok(Either::Right(input)),
                    }
                }
            }

            impl [<$state Inner>] {
                fn dispatch(&mut self, cmd: &[<$state Command>]) {
                    match cmd {
                        $(
                            [<$state Command>]::$operation(payload) => {
                                $crate::stm::StateHandler::apply(payload, self);
                            },
                        )*
                    }
                }
            }

            impl $crate::stm::State for $state {
                type Input = <[<$state Inner>] as $crate::stm::Command>::Input;
                type Output = ();
                type Error = <[<$state Inner>] as $crate::stm::Command>::Error;

                fn apply(&self, input: Self::Input) -> Result<::iggy_common::Either<Self::Output, Self::Input>, Self::Error> {
                    use ::iggy_common::Either;

                    match <[<$state Inner>] as $crate::stm::Command>::parse(input)? {
                        Either::Left(cmd) => {
                            self.inner.do_apply(cmd);
                            Ok(Either::Left(()))
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
