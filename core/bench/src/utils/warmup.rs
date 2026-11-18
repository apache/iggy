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

use iggy::prelude::IggyDuration;
use tokio::time::Instant;

/// The warmup of one benchmark run, with one end for every actor of the run.
///
/// A producer counts a batch as measured when it starts to send it at or after the end. A consumer
/// counts a batch as measured when it receives it at or after the end. A batch reaches a consumer
/// only after its producer started to send it, so a consumer never takes a measured batch inside
/// its warmup. With an end per actor, a consumer that started later did, and then waited for the
/// rest of the run for a batch its producer never sent.
#[derive(Debug, Clone, Copy)]
pub struct BenchmarkWarmup {
    duration: IggyDuration,
    end: Instant,
}

impl BenchmarkWarmup {
    /// Starts the warmup now. A run starts it once, before it spawns its actors, so the setup of
    /// each actor counts against the warmup.
    pub fn start(duration: IggyDuration) -> Self {
        Self {
            duration,
            end: Instant::now() + duration.get_duration(),
        }
    }

    pub const fn duration(&self) -> IggyDuration {
        self.duration
    }

    pub const fn end(&self) -> Instant {
        self.end
    }

    pub fn is_enabled(&self) -> bool {
        !self.duration.is_zero()
    }

    /// Compares with the clock directly. A timer for the end can fire up to a millisecond late,
    /// and a batch received in that gap belongs to the measured phase.
    pub fn is_over(&self) -> bool {
        Instant::now() >= self.end
    }
}
