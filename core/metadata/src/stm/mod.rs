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
pub mod stream;
pub mod user;

use std::cell::UnsafeCell;

// ============================================================================
// WriteCell - Interior mutability wrapper for WriteHandle
// ============================================================================

pub struct WriteCell<T: left_right::Absorb<O>, O> {
    inner: UnsafeCell<left_right::WriteHandle<T, O>>,
}

impl<T: left_right::Absorb<O>, O> WriteCell<T, O> {
    pub fn new(write: left_right::WriteHandle<T, O>) -> Self {
        Self {
            inner: UnsafeCell::new(write),
        }
    }

    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut left_right::WriteHandle<T, O>) -> R,
    {
        // SAFETY: WriteCell is !Sync (via UnsafeCell), so only one thread can access.
        // The caller ensures exclusive access through the &self borrow.
        unsafe { f(&mut *self.inner.get()) }
    }
}

// ============================================================================
// Traits - All use &self, implementations use interior mutability
// ============================================================================

/// Parses input into a command.
pub trait Command {
    type Cmd;
    type Input;

    fn into_command(input: &Self::Input) -> Option<Self::Cmd>;
}

/// Dispatches a command to mutate state.
/// Takes `&mut self` - implementations use interior mutability.
pub trait Dispatch {
    type Cmd;
    type Output;

    fn dispatch(&self, cmd: &Self::Cmd) -> Self::Output;
}

/// Applies a command through a state wrapper.
/// Takes `&self` - implementations use interior mutability.
pub trait ApplyState {
    type Cmd;
    type Output;
    type Inner: Command<Cmd = Self::Cmd>;

    fn do_apply(&self, cmd: Self::Cmd) -> Option<Self::Output>;
}

/// Public interface for state machines.
/// Takes `&self` - implementations use interior mutability.
pub trait State {
    type Output;
    type Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output>;
}

impl<T> State for T
where
    T: ApplyState,
{
    type Output = T::Output;
    type Input = <T::Inner as Command>::Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output> {
        let cmd = T::Inner::into_command(input)?;
        self.do_apply(cmd)
    }
}

pub(crate) trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: &Self::Input, output: &mut Vec<Self::Output>);
}

#[macro_export]
macro_rules! define_state {
    (
        $state:ident,
        $inner:ident {
            $($field_name:ident : $field_type:ty),* $(,)?
        },
        $command:ident,
        [$($operation:ident),* $(,)?]
    ) => {
        #[derive(Debug, Clone, Default)]
        pub struct $inner {
            $(
                pub $field_name: $field_type,
            )*
        }

        #[derive(Debug)]
        pub enum $command {
            $(
                $operation($operation),
            )*
        }

        impl Clone for $command
        where
            $($operation: Clone,)*
        {
            fn clone(&self) -> Self {
                match self {
                    $(
                        $command::$operation(payload) => $command::$operation(payload.clone()),
                    )*
                }
            }
        }

        /// State wrapper with interior mutability for write access.
        pub struct $state {
            write: Option<$crate::stm::WriteCell<$inner, $command>>,
            read: ::std::sync::Arc<::left_right::ReadHandle<$inner>>,
        }

        impl $state {
            /// Get a clone of the read handle.
            pub fn read_handle(&self) -> ::std::sync::Arc<::left_right::ReadHandle<$inner>> {
                self.read.clone()
            }

            /// Get read access to the inner state.
            pub fn read(&self) -> Option<::left_right::ReadGuard<'_, $inner>> {
                self.read.enter()
            }

            /// Check if this instance has write capability.
            pub fn has_write(&self) -> bool {
                self.write.is_some()
            }
        }

        impl From<$inner> for $state {
            fn from(inner: $inner) -> Self {
                let (write, read) = ::left_right::new_from_empty(inner);
                let write = Some($crate::stm::WriteCell::new(write));
                let read = ::std::sync::Arc::new(read);
                Self { write, read }
            }
        }

        impl From<::std::sync::Arc<::left_right::ReadHandle<$inner>>> for $state {
            /// Create a read-only instance from a read handle.
            fn from(read: ::std::sync::Arc<::left_right::ReadHandle<$inner>>) -> Self {
                Self { write: None, read }
            }
        }

        impl ::std::fmt::Debug for $state {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self.read.enter() {
                    Some(guard) => f.debug_struct(stringify!($state))
                        .field("has_write", &self.write.is_some())
                        .field("inner", &*guard)
                        .finish(),
                    None => f.debug_struct(stringify!($state)).finish_non_exhaustive(),
                }
            }
        }

        impl $crate::stm::Command for $inner {
            type Cmd = $command;
            type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;

            fn into_command(input: &Self::Input) -> Option<Self::Cmd> {
                use ::iggy_common::BytesSerializable;
                use ::iggy_common::header::Operation;

                let body = input.body_bytes();
                match input.header().operation {
                    $(
                        Operation::$operation => {
                            Some($command::$operation(
                                $operation::from_bytes(body).unwrap()
                            ))
                        },
                    )*
                    _ => None,
                }
            }
        }

        impl ::left_right::Absorb<$command> for $inner
        where
            $inner: $crate::stm::Dispatch<Cmd = $command>,
        {
            fn absorb_first(&mut self, cmd: &mut $command, _other: &Self) {
                use $crate::stm::Dispatch;
                self.dispatch(cmd);
            }

            fn absorb_second(&mut self, cmd: $command, _other: &Self) {
                use $crate::stm::Dispatch;
                self.dispatch(&cmd);
            }

            fn sync_with(&mut self, first: &Self) {
                *self = first.clone();
            }

            fn drop_first(self: Box<Self>) {}

            fn drop_second(self: Box<Self>) {}
        }

        impl $crate::stm::ApplyState for $state
        where
            $inner: $crate::stm::Dispatch<Cmd = $command>,
        {
            type Cmd = $command;
            type Output = ();
            type Inner = $inner;

            fn do_apply(&self, cmd: Self::Cmd) -> Option<Self::Output> {
                let write_cell = self.write.as_ref()?;
                write_cell.with(|write| {
                    write.append(cmd).publish();
                });
                Some(())
            }
        }
    };
}
