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

//! `PollMessages` op: an offset read as a consumer, or as the fixture group on a
//! fixture namespace. Targets only `Served`; the readback oracle checks what the
//! reply carries. PRNG draw order:
//!
//! 1. namespace pick
//! 2. group or consumer draw (only in a run with fixture groups)
//! 3. `consumer_id` range draw (consumer polls only)
//! 4. start offset range draw
//! 5. count range draw

use iggy_binary_protocol::{KIND_CONSUMER, KIND_CONSUMER_GROUP, RoutedRequestHeader};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::{FIXTURE_GROUP_ID, Shadow};

/// Most messages one poll asks for.
pub const POLL_COUNT_MAX: u32 = 16;

/// Chance that a poll on a fixture namespace reads as the fixture group.
const GROUP_POLL_PROBABILITY: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct Input {
    pub ns: IggyNamespace,
    pub consumer_kind: u8,
    pub consumer_id: u32,
    pub offset: u64,
    pub count: u32,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    Served,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Served];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256PlusPlus,
    options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Served => {
            let ns = shadow.pick_namespace(prng)?;
            let group = options.weights.draws_fixture_groups()
                && options.namespaces.contains(&ns)
                && prng.random_bool(GROUP_POLL_PROBABILITY);
            let (consumer_kind, consumer_id) = if group {
                (KIND_CONSUMER_GROUP, FIXTURE_GROUP_ID)
            } else {
                (
                    KIND_CONSUMER,
                    prng.random_range(0..options.consumer_pool_size.max(1)),
                )
            };
            // Up to one past the last send the client saw commit, so a poll also
            // reads at the committed end.
            let offset = prng.random_range(0..=shadow.sends_committed(ns));
            let count = prng.random_range(1..=POLL_COUNT_MAX);
            Some(Input {
                ns,
                consumer_kind,
                consumer_id,
                offset,
                count,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    client.poll_messages_at(
        input.ns,
        input.consumer_kind,
        input.consumer_id,
        input.offset,
        input.count,
    )
}

#[must_use]
pub const fn classify_reply(_code: u32) -> Outcome {
    Outcome::Served
}

#[must_use]
pub const fn predicted_effect(_input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Served => Effect::None,
    }
}
