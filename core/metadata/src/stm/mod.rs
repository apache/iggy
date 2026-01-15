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

    pub fn apply(&self, cmd: O) {
        // SAFETY: WriteCell is !Sync (via UnsafeCell), so only one thread can access.
        // The caller ensures exclusive access through the &self borrow.
        unsafe {
            (*self.inner.get()).append(cmd).publish();
        }
    }
}

/// Parses input into a command.
pub trait Command {
    type Cmd;
    type Input;

    fn into_command(input: &Self::Input) -> Option<Self::Cmd>;
}

/// Handles a command to mutate state.
pub trait Handle: Command {
    fn handle(&mut self, cmd: &<Self as Command>::Cmd);
}

/// Applies a command through a state wrapper.
pub trait ApplyState {
    type Inner: Command;
    type Output;

    fn do_apply(&self, cmd: <Self::Inner as Command>::Cmd) -> Self::Output;
}

/// Public interface for state machines.
pub trait State {
    type Output;
    type Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output>;
}

impl<T: ApplyState> State for T {
    type Output = T::Output;
    type Input = <T::Inner as Command>::Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output> {
        T::Inner::into_command(input).map(|cmd| self.do_apply(cmd))
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
                let (write, read) = { let (w, r) = ::left_right::new_from_empty(inner); (Some($crate::stm::WriteCell::new(w)), ::std::sync::Arc::new(r)) };
                Self { write, read }
            }
        }

        impl From<::std::sync::Arc<::left_right::ReadHandle<$inner>>> for $state {
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
            $inner: $crate::stm::Handle,
        {
            fn absorb_first(&mut self, cmd: &mut $command, _other: &Self) {
                use $crate::stm::Handle;
                self.handle(cmd);
            }

            fn absorb_second(&mut self, cmd: $command, _other: &Self) {
                use $crate::stm::Handle;
                self.handle(&cmd);
            }

            fn sync_with(&mut self, first: &Self) {
                *self = first.clone();
            }

            fn drop_first(self: Box<Self>) {}

            fn drop_second(self: Box<Self>) {}
        }

        impl $crate::stm::ApplyState for $state
        where
            $inner: $crate::stm::Handle,
        {
            type Inner = $inner;
            type Output = ();

            fn do_apply(&self, cmd: $command) -> Self::Output {
                self.write.as_ref().expect("[do_apply]: no write handle").apply(cmd);
            }
        }
    };
}
