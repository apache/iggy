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

pub trait Container {
    type Inner;
}

pub trait Command {
    type Cmd;
    type Input;

    fn into_command(input: &Self::Input) -> Option<Self::Cmd>;
}

pub trait Handle: Command {
    fn handle(&mut self, cmd: &Self::Cmd);
}

pub trait ApplyState: Container {
    type Output;

    fn apply_cmd(&self, cmd: <<Self as Container>::Inner as Command>::Cmd) -> Self::Output
    where
        <Self as Container>::Inner: Command;
}

pub trait Factory<T> {
    type Constructable;

    fn create(&self, inner: impl FnOnce() -> T) -> Self::Constructable;
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

impl<T, C> LeftRight<T, C>
where
    T: Absorb<C>,
{
    pub fn read(&self) -> Arc<ReadHandle<T>> {
        self.read.clone()
    }
}

impl<T, C> Container for LeftRight<T, C>
where
    T: Absorb<C>,
{
    type Inner = T;
}

impl<T, C> ApplyState for LeftRight<T, C>
where
    T: Command<Cmd = C> + Absorb<C>,
{
    type Output = ();

    fn apply_cmd(&self, cmd: C) -> Self::Output {
        self.write
            .as_ref()
            .expect("no write handle - not the owner shard")
            .apply(cmd);
    }
}

#[derive(Default)]
pub struct LeftRightFactory;

impl<T> Factory<T> for LeftRightFactory
where
    T: Absorb<<T as Command>::Cmd> + Clone + Command,
{
    type Constructable = LeftRight<T, <T as Command>::Cmd>;

    fn create(&self, inner: impl FnOnce() -> T) -> Self::Constructable {
        LeftRight::new(inner())
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
        #[derive(Debug, Clone)]
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

        /// State wrapper with interior mutability for write access.
        pub struct $state {
            write: Option<$crate::stm::WriteCell<$inner, $command>>,
            read: ::std::sync::Arc<::left_right::ReadHandle<$inner>>,
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
                f.debug_struct(stringify!($state))
                    .field("write", &self.write)
                    .field("inner", &self.read.enter())
                    .finish()
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
                self.write.as_ref().expect("[do_apply]: no write handle, not handled on shard0").apply(cmd);
            }
        }
    };
}
