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

use super::NoShutdown;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::task_registry::registry::TaskRegistry;
use iggy_common::IggyError;
use std::ops::{AsyncFn, AsyncFnOnce};
use std::time::Duration;

pub struct PeriodicBuilder<'a, Tick, OnShutdown = NoShutdown> {
    reg: &'a TaskRegistry,
    name: &'static str,
    critical: bool,
    period: Option<Duration>,
    last_on_shutdown: bool,
    tick_fn: Option<Tick>,
    on_shutdown: Option<OnShutdown>,
}

impl<'a> PeriodicBuilder<'a, (), NoShutdown> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            critical: false,
            period: None,
            last_on_shutdown: false,
            tick_fn: None,
            on_shutdown: None,
        }
    }
}

impl<'a, Tick, OnShutdown> PeriodicBuilder<'a, Tick, OnShutdown> {
    pub fn every(mut self, d: Duration) -> Self {
        self.period = Some(d);
        self
    }

    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn last_tick_on_shutdown(mut self, v: bool) -> Self {
        self.last_on_shutdown = v;
        self
    }

    pub fn on_shutdown<NewShutdown>(self, f: NewShutdown) -> PeriodicBuilder<'a, Tick, NewShutdown>
    where
        NewShutdown: AsyncFnOnce(Result<(), IggyError>) + 'static,
    {
        PeriodicBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            period: self.period,
            last_on_shutdown: self.last_on_shutdown,
            tick_fn: self.tick_fn,
            on_shutdown: Some(f),
        }
    }
}

impl<'a> PeriodicBuilder<'a, ()> {
    pub fn tick<NewTick>(self, f: NewTick) -> PeriodicBuilder<'a, NewTick>
    where
        NewTick: AsyncFn(ShutdownToken) -> Result<(), IggyError> + 'static,
    {
        PeriodicBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            period: self.period,
            last_on_shutdown: self.last_on_shutdown,
            tick_fn: Some(f),
            on_shutdown: self.on_shutdown,
        }
    }
}

impl<'a, Tick> PeriodicBuilder<'a, Tick, NoShutdown>
where
    Tick: AsyncFn(ShutdownToken) -> Result<(), IggyError> + 'static,
{
    pub fn spawn(self) {
        let period = self.period.expect("period required - use .every()");
        let tick_fn = self.tick_fn.expect("tick function required - use .tick()");

        self.reg.spawn_periodic_closure(
            self.name,
            period,
            self.critical,
            self.last_on_shutdown,
            tick_fn,
            Some(|_| async {}),
        );
    }
}

impl<'a, Tick, OnShutdown> PeriodicBuilder<'a, Tick, OnShutdown>
where
    Tick: AsyncFn(ShutdownToken) -> Result<(), IggyError> + 'static,
    OnShutdown: AsyncFnOnce(Result<(), IggyError>) + 'static,
{
    pub fn spawn(self) {
        let period = self.period.expect("period required - use .every()");
        let tick_fn = self.tick_fn.expect("tick function required - use .tick()");

        self.reg.spawn_periodic_closure(
            self.name,
            period,
            self.critical,
            self.last_on_shutdown,
            tick_fn,
            self.on_shutdown,
        );
    }
}
