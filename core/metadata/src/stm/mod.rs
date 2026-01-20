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

pub struct LeftRight<T, C>
where
    T: Absorb<C>,
{
    write: Option<WriteCell<T, C>>,
    #[allow(unused)]
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

impl<T> LeftRight<T, <T as Command>::Cmd>
where
    T: Absorb<<T as Command>::Cmd> + Clone + Handler,
{
    pub fn do_apply(&self, cmd: <T as Command>::Cmd) {
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

/// Generates a state machine with convention-based storage.
///
/// # Generated items
/// - `{$state}Inner` struct with the specified fields (the data)
/// - `{$state}Command` enum with variants for each operation
/// - `$state` wrapper struct (non-generic, contains LeftRight storage)
/// - `Command` impl for `{$state}Inner` (parsing)
/// - `State` impl for `$state`
/// - `From<LeftRight<...>>` impl for `$state`
///
/// # User must implement
/// - `Handler` for `{$state}Inner` (business logic)
/// - `impl_absorb!` for `{$state}Inner` and `{$state}Command`
///
/// # Example
/// ```ignore
/// define_state! {
///     Streams {
///         index: AHashMap<String, usize>,
///         items: Slab<Stream>,
///     },
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
///
/// // User implements Absorb via macro:
/// impl_absorb!(StreamsInner, StreamsCommand);
/// ```
// TODO: The `operation` argument can be removed, once we create an trait for mapping.
#[macro_export]
macro_rules! define_state {
    (
        $state:ident {
            $($field_name:ident : $field_type:ty),* $(,)?
        },
        [$($operation:ident),* $(,)?]
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

            #[derive(Debug, Clone)]
            pub enum [<$state Command>] {
                $(
                    $operation($operation),
                )*
            }

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

            impl $crate::stm::State for $state {
                type Input = <[<$state Inner>] as $crate::stm::Command>::Input;
                type Output = ();

                fn apply(&self, input: &Self::Input) -> Option<Self::Output> {
                    <[<$state Inner>] as $crate::stm::Command>::parse(input)
                        .map(|cmd| self.inner.do_apply(cmd))
                }
            }

            impl $crate::stm::Command for [<$state Inner>] {
                type Cmd = [<$state Command>];
                type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;

                fn parse(input: &Self::Input) -> Option<Self::Cmd> {
                    use ::iggy_common::BytesSerializable;
                    use ::iggy_common::header::Operation;

                    let body = input.body_bytes();
                    match input.header().operation {
                        $(
                            Operation::$operation => {
                                Some([<$state Command>]::$operation(
                                    $operation::from_bytes(body).unwrap()
                                ))
                            },
                        )*
                        _ => None,
                    }
                }
            }
        }
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
