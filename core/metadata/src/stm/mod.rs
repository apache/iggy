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

use left_right::*;
use std::cell::UnsafeCell;
use std::sync::Arc;

pub struct WriteCell<T, O>
where
    T: Absorb<O>,
{
    inner: UnsafeCell<WriteHandle<T, O>>,
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
pub trait Command {
    type Cmd;
    type Input;

    fn parse(input: &Self::Input) -> Option<Self::Cmd>;
}

/// Handles commands. User-implemented business logic.
pub trait Handler: Command {
    fn handle(&mut self, cmd: &Self::Cmd);
}

/// Storage abstraction: applies commands to inner state.
pub trait ApplyState {
    type Inner: Handler;
    type Output;

    fn do_apply(&self, cmd: <Self::Inner as Command>::Cmd) -> Self::Output;
}

pub struct LeftRight<T, C>
where
    T: Absorb<C>,
{
    write: Option<WriteCell<T, C>>,
    read: Arc<ReadHandle<T>>,
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

impl<T> ApplyState for LeftRight<T, <T as Command>::Cmd>
where
    T: Absorb<<T as Command>::Cmd> + Clone + Handler,
{
    type Inner = T;
    type Output = ();

    fn do_apply(&self, cmd: <Self::Inner as Command>::Cmd) -> Self::Output {
        self.write
            .as_ref()
            .expect("no write handle - not the owner shard")
            .apply(cmd);
    }
}

/// Public interface for state machines.
pub trait State {
    type Output;
    type Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output>;
}

pub trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: &Self::Input, output: &mut Vec<Self::Output>);
}

/// Generates a state machine with pluggable storage.
///
/// # Generated items
/// - `$inner` struct with the specified fields (the data)
/// - `$command` enum with variants for each operation
/// - `$state<S: ApplyState<Inner = $inner>>` wrapper struct (storage-agnostic)
/// - `Command` impl for `$inner` (parsing)
/// - `Absorb` impl for `$inner` (delegates to `Handler::handle`)
/// - `State` impl for `$state<S>`
/// - `From<S>` impl for `$state<S>`
///
/// # User must implement
/// - `Handler` for `$inner` (business logic)
///
/// # Example
/// ```ignore
/// define_state! {
///     Streams,
///     StreamsInner {
///         index: AHashMap<String, usize>,
///         items: Slab<Stream>,
///     },
///     StreamsCommand,
///     [CreateStream, UpdateStream, DeleteStream]
/// }
///
/// // User implements Handler manually:
/// impl Handler for StreamsInner {
///     fn handle(&mut self, cmd: &StreamsCommand) {
///         match cmd {
///             StreamsCommand::CreateStream(payload) => { /* ... */ }
///             // ...
///         }
///     }
/// }
/// ```
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

        #[derive(Debug, Clone)]
        pub enum $command {
            $(
                $operation($operation),
            )*
        }

        pub struct $state<S: $crate::stm::ApplyState<Inner = $inner>> {
            inner: S,
        }

        impl<S: $crate::stm::ApplyState<Inner = $inner>> From<S> for $state<S> {
            fn from(storage: S) -> Self {
                Self { inner: storage }
            }
        }

        impl<S> $crate::stm::State for $state<S>
        where
            S: $crate::stm::ApplyState<Inner = $inner>,
        {
            type Input = <$inner as $crate::stm::Command>::Input;
            type Output = S::Output;

            fn apply(&self, input: &Self::Input) -> Option<Self::Output> {
                <$inner as $crate::stm::Command>::parse(input)
                    .map(|cmd| self.inner.do_apply(cmd))
            }
        }

        impl $crate::stm::Command for $inner {
            type Cmd = $command;
            type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;

            fn parse(input: &Self::Input) -> Option<Self::Cmd> {
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

        /*
        impl ::left_right::Absorb<$command> for $inner
        where
            $inner: $crate::stm::Handler,
        {
            fn absorb_first(&mut self, cmd: &mut $command, _other: &Self) {
                <Self as $crate::stm::Handler>::handle(self, cmd);
            }

            fn absorb_second(&mut self, cmd: $command, _other: &Self) {
                <Self as $crate::stm::Handler>::handle(self, &cmd);
            }

            fn sync_with(&mut self, first: &Self) {
                *self = first.clone();
            }

            fn drop_first(self: Box<Self>) {}
            fn drop_second(self: Box<Self>) {}
        }
        */
    };
}

// This macro is really sad, but we can't do blanket impl from below, due to orphan rule.
// impl<T> Absorb<T::Cmd> for T
// where
//     T: Handler + Clone,
// {
//     fn absorb_first(&mut self, cmd: &mut T::Cmd, _other: &Self) {
//         self.handle(cmd);

//     }

//     fn absorb_second(&mut self, cmd: T::Cmd, _other: &Self) {
//         self.handle(&cmd);
//     }

//     fn sync_with(&mut self, first: &Self) {
//         *self = first.clone();
//     }

//     fn drop_first(self: Box<Self>) {}
//     fn drop_second(self: Box<Self>) {}
// }
#[macro_export]
macro_rules! impl_absorb {
    ($inner:ident, $cmd:ident) => {
        impl left_right::Absorb<$cmd> for $inner {
            fn absorb_first(&mut self, cmd: &mut $cmd, _other: &Self) {
                self.handle(cmd);
            }

            fn absorb_second(&mut self, cmd: $cmd, _other: &Self) {
                self.handle(&cmd);
            }

            fn sync_with(&mut self, first: &Self) {
                *self = first.clone();
            }
        }
    };
}

/*
*/
