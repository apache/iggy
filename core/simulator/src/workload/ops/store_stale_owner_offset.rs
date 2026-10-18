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

//! An offset store carrying authority captured before an owner handoff.

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::{CapturedOwner, FIXTURE_GROUP_ID, Shadow};
use iggy_binary_protocol::{AckLevel, KIND_CONSUMER_GROUP, RoutedRequestHeader};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;

#[derive(Debug, Clone)]
pub struct Input {
    pub captured: CapturedOwner,
    pub offset: u64,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    Refused,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Refused];

pub fn sample(
    shadow: &mut Shadow,
    _outcome: Outcome,
    prng: &mut Xoshiro256PlusPlus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    let captured = shadow.pick_stale_owner(prng)?;
    let high = shadow.sends_committed(captured.namespace);
    (high > 0).then(|| Input {
        captured,
        offset: prng.random_range(0..high),
    })
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    let captured = input.captured;
    client
        .store_consumer_offset(
            captured.namespace,
            KIND_CONSUMER_GROUP,
            FIXTURE_GROUP_ID,
            input.offset,
            AckLevel::Quorum,
        )
        .transmute_header(|mut source, target: &mut RoutedRequestHeader| {
            source.partition_incarnation = captured.context.incarnation;
            source.owner_generation = captured.owner.generation;
            source.minimum_metadata_op = captured.context.metadata_op;
            source.session = captured.owner.session;
            *target = source;
        })
}

#[must_use]
pub const fn classify_reply(_code: u32) -> Outcome {
    Outcome::Refused
}

#[must_use]
pub const fn predicted_effect(_input: &Input, _outcome: Outcome) -> Effect {
    Effect::None
}
