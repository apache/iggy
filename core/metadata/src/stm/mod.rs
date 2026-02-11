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

use iggy_common::IggyTimestamp;
use left_right::*;
use std::cell::{Cell, UnsafeCell};
use std::sync::Arc;

/// Per-command handler for a given state type.
/// Each command struct implements this for the state it mutates.
///
/// `now` is captured once before the command enters `left_right`, ensuring
/// both copies see the same timestamp (deterministic replay).
pub trait StateHandler {
    type State;
    type Output;
    fn apply(&self, state: &mut Self::State, now: IggyTimestamp) -> Self::Output;
}

/// Parses type-erased input into a command. Macro-generated.
/// Returns `Ok(cmd)` if applicable, `Err(input)` to pass ownership back.
pub trait Command {
    type Cmd;
    type Input;
    type Output;

    fn parse(input: Self::Input) -> Result<Self::Cmd, Self::Input>;
}

/// Public interface for state machines.
/// Returns `Ok(output)` if applicable, `Err(input)` to pass ownership back.
pub trait State {
    type Output;
    type Input;

    fn apply(&self, input: Self::Input) -> Result<Self::Output, Self::Input>;
}

pub trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: Self::Input) -> Self::Output;
}

/// Wraps a command with a raw pointer to a stack-allocated output slot.
///
/// Used to smuggle return values out of `left_right`'s `Absorb` trait,
/// which returns void. The pointer targets a stack local in `WriteCell::apply()`
/// and is only written during the synchronous `publish()` call.
#[doc(hidden)]
pub struct CommandEnvelope<C, R> {
    pub(crate) cmd: C,
    pub(crate) output: *mut Option<R>,
    pub(crate) now: IggyTimestamp,
}

// Safety: The pointer targets a stack local in WriteCell::apply() and is only
// dereferenced during the synchronous append()+publish() sequence on the same
// thread. Single-writer architecture ensures no concurrent access.
unsafe impl<C: Send, R: Send> Send for CommandEnvelope<C, R> {}

pub struct WriteCell<T, C, R>
where
    T: Absorb<CommandEnvelope<C, R>>,
{
    inner: UnsafeCell<WriteHandle<T, CommandEnvelope<C, R>>>,
    #[cfg(debug_assertions)]
    in_apply: Cell<bool>,
}

impl<T, C, R> std::fmt::Debug for WriteCell<T, C, R>
where
    T: Absorb<CommandEnvelope<C, R>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCell").finish_non_exhaustive()
    }
}

impl<T, C, R> WriteCell<T, C, R>
where
    T: Absorb<CommandEnvelope<C, R>>,
{
    pub fn new(write: WriteHandle<T, CommandEnvelope<C, R>>) -> Self {
        Self {
            inner: UnsafeCell::new(write),
            #[cfg(debug_assertions)]
            in_apply: Cell::new(false),
        }
    }

    pub fn apply(&self, cmd: C) -> R {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self.in_apply.replace(true),
                "WriteCell::apply() called reentrantly — this would alias &mut WriteHandle"
            );
        }

        let mut output: Option<R> = None;
        let envelope = CommandEnvelope {
            cmd,
            output: &mut output,
            now: IggyTimestamp::now(),
        };
        let hdl = unsafe {
            self.inner
                .get()
                .as_mut()
                .expect("[apply]: called on uninit writer")
        };
        hdl.append(envelope).publish();

        #[cfg(debug_assertions)]
        self.in_apply.set(false);

        output.expect("[apply]: absorb did not produce output")
    }
}

#[derive(Debug)]
pub struct LeftRight<T, C, R>
where
    T: Absorb<CommandEnvelope<C, R>>,
{
    write: Option<WriteCell<T, C, R>>,
    read: Arc<ReadHandle<T>>,
}

// TODO: Expose Arc<ReadHandle<T>> for cross-thread lock-free reads from non-owner shards.

impl<T, C, R> LeftRight<T, C, R>
where
    T: Absorb<CommandEnvelope<C, R>>,
{
    pub fn read<F, Ret>(&self, f: F) -> Ret
    where
        F: FnOnce(&T) -> Ret,
    {
        let guard = self.read.enter().expect("read handle should be accessible");
        f(&*guard)
    }
}

impl<T> From<T> for LeftRight<T, <T as Command>::Cmd, <T as Command>::Output>
where
    T: Absorb<CommandEnvelope<<T as Command>::Cmd, <T as Command>::Output>> + Clone + Command,
{
    fn from(inner: T) -> Self {
        let (write, read) = {
            let (w, r) = left_right::new_from_empty(inner);
            (WriteCell::new(w).into(), r.into())
        };
        Self { write, read }
    }
}

impl<T> LeftRight<T, <T as Command>::Cmd, <T as Command>::Output>
where
    T: Absorb<CommandEnvelope<<T as Command>::Cmd, <T as Command>::Output>> + Clone + Command,
{
    pub fn do_apply(&self, cmd: <T as Command>::Cmd) -> <T as Command>::Output {
        self.write
            .as_ref()
            .expect("no write handle - not the owner shard")
            .apply(cmd)
    }

    pub fn with_state<F, Ret>(&self, f: F) -> Ret
    where
        F: FnOnce(&T) -> Ret,
    {
        let guard = self
            .read
            .enter()
            .expect("ReadHandle dropped - WriteHandle was deallocated");
        f(&guard)
    }
}

/// Generates the state's inner struct.
///
/// # Generated items
/// - `{$state}Inner` struct with the specified fields (the data)
///
/// The wrapper struct, `From` impls, command enum, parsing, dispatch, and
/// Absorb impl are all generated by `collect_handlers!`, which knows the
/// output type.
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
        }
    };
}

/// Generates the command enum, wrapper struct, parsing, dispatch, State, and
/// envelope-aware Absorb for a state type.
///
/// # Syntax
/// ```ignore
/// collect_handlers! { Streams { Op1, Op2 } }           // Output = ()
/// collect_handlers! { Streams -> OutputType { Op1 } }   // Output = OutputType
/// ```
///
/// # Generated items
/// - `$state` wrapper struct (contains LeftRight storage)
/// - `From<LeftRight<...>>` impl for `$state`
/// - `From<{$state}Inner>` impl for `$state`
/// - `{$state}Command` enum with one variant per operation
/// - `Command` impl for `{$state}Inner` (parses `Message<PrepareHeader>`)
/// - `{$state}Inner::dispatch()` method (routes each variant to `StateHandler::apply()`)
/// - `State` impl for `$state` wrapper
/// - `Absorb<CommandEnvelope<{$state}Command, Output>>` impl for `{$state}Inner`
///
/// # Requirements
/// Each listed operation type must implement `StateHandler<State = {$state}Inner, Output = $output>`.
#[macro_export]
macro_rules! collect_handlers {
    // Default output: ()
    (
        $state:ident {
            $($operation:ident),* $(,)?
        }
    ) => {
        $crate::__collect_handlers_impl!($state -> () { $($operation),* });
    };
    // Explicit output type
    (
        $state:ident -> $output:ty {
            $($operation:ident),* $(,)?
        }
    ) => {
        $crate::__collect_handlers_impl!($state -> $output { $($operation),* });
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __collect_handlers_impl {
    (
        $state:ident -> $output:ty {
            $($operation:ident),*
        }
    ) => {
        paste::paste! {
            // --- wrapper struct + From impls (moved from define_state!) ---

            #[derive(Debug)]
            pub struct $state {
                inner: $crate::stm::LeftRight<
                    [<$state Inner>],
                    [<$state Command>],
                    $output,
                >,
            }

            impl From<$crate::stm::LeftRight<[<$state Inner>], [<$state Command>], $output>> for $state {
                fn from(inner: $crate::stm::LeftRight<[<$state Inner>], [<$state Command>], $output>) -> Self {
                    Self { inner }
                }
            }

            impl $state {
                pub fn with_state<F, Ret>(&self, f: F) -> Ret
                where
                    F: FnOnce(&[<$state Inner>]) -> Ret,
                {
                    self.inner.with_state(f)
                }
            }

            impl From<[<$state Inner>]> for $state {
                fn from(inner: [<$state Inner>]) -> Self {
                    let left_right: $crate::stm::LeftRight<
                        [<$state Inner>],
                        [<$state Command>],
                        $output,
                    > = inner.into();
                    left_right.into()
                }
            }

            // --- command enum ---

            #[derive(Debug, Clone)]
            pub enum [<$state Command>] {
                $(
                    $operation($operation),
                )*
            }

            // --- Command trait ---

            impl $crate::stm::Command for [<$state Inner>] {
                type Cmd = [<$state Command>];
                type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;
                type Output = $output;

                fn parse(input: Self::Input) -> Result<Self::Cmd, Self::Input> {
                    use ::iggy_common::BytesSerializable;
                    use ::iggy_common::header::Operation;

                    match input.header().operation {
                        $(
                            Operation::$operation => {
                                let body = input.body_bytes();
                                // FIXME: return a parse error instead of panicking on malformed input
                                Ok([<$state Command>]::$operation(
                                    $operation::from_bytes(body).unwrap()
                                ))
                            },
                        )*
                        _ => Err(input),
                    }
                }
            }

            // --- dispatch ---

            impl [<$state Inner>] {
                fn dispatch(&mut self, cmd: &[<$state Command>], now: ::iggy_common::IggyTimestamp) -> $output {
                    match cmd {
                        $(
                            [<$state Command>]::$operation(payload) => {
                                $crate::stm::StateHandler::apply(payload, self, now)
                            },
                        )*
                    }
                }
            }

            // --- State trait ---

            impl $crate::stm::State for $state {
                type Input = <[<$state Inner>] as $crate::stm::Command>::Input;
                type Output = $output;

                fn apply(&self, input: Self::Input) -> Result<Self::Output, Self::Input> {
                    let cmd = <[<$state Inner>] as $crate::stm::Command>::parse(input)?;
                    Ok(self.inner.do_apply(cmd))
                }
            }

            // --- Absorb<CommandEnvelope<...>> ---

            impl left_right::Absorb<$crate::stm::CommandEnvelope<[<$state Command>], $output>>
                for [<$state Inner>]
            {
                fn absorb_first(
                    &mut self,
                    envelope: &mut $crate::stm::CommandEnvelope<[<$state Command>], $output>,
                    _other: &Self,
                ) {
                    let result = self.dispatch(&envelope.cmd, envelope.now);
                    if !envelope.output.is_null() {
                        // Safety: pointer targets WriteCell::apply()'s stack local,
                        // still alive during synchronous publish().
                        unsafe { *envelope.output = Some(result) };
                        // Nullify so the stale copy in absorb_second won't write
                        // through a dangling pointer.
                        envelope.output = std::ptr::null_mut();
                    }
                }

                fn absorb_second(
                    &mut self,
                    mut envelope: $crate::stm::CommandEnvelope<[<$state Command>], $output>,
                    _other: &Self,
                ) {
                    let result = self.dispatch(&envelope.cmd, envelope.now);
                    if !envelope.output.is_null() {
                        // Safety: on the `extend` (first-publish) path, this is
                        // absorb_second and the pointer is still live.
                        unsafe { *envelope.output = Some(result) };
                        envelope.output = std::ptr::null_mut();
                    }
                }

                fn sync_with(&mut self, first: &Self) {
                    *self = first.clone();
                }
            }
        }
    };
}
