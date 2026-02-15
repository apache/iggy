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

use super::traits::Command;
use iggy_common::IggyTimestamp;
use left_right::*;
use std::cell::{Cell, UnsafeCell};
use std::sync::Arc;

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
