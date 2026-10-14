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

//! `LeaveFixtureGroup` op. Targets only `Reached`.
//!
//! Leaves the fixture group of a fixture topic. The shadow keeps no
//! membership to move: until its successor installs, a client that left can still
//! commit as the owner, so only the joins that [`Shadow::finish_group_join`]
//! records bound who may. Samples nothing in a run without fixture groups.

use iggy_binary_protocol::RoutedRequestHeader;
use metadata::stm::result::LeaveConsumerGroupResult;
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::{FIXTURE_GROUP_NAME, Shadow};

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    /// The client is not a member.
    Reached,
    Refused,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Reached];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256PlusPlus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Reached => {
            let fixture = shadow.pick_fixture_group(prng, true)?;
            let (stream, topic) = shadow.fixture_names(fixture);
            Some(Input { stream, topic })
        }
        Outcome::Refused => None,
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    client.leave_consumer_group(&input.stream, &input.topic, FIXTURE_GROUP_NAME)
}

/// A client that was no member reached the goal of a leave too.
#[must_use]
pub const fn classify_reply(code: u32) -> Outcome {
    if code == 0 || code == LeaveConsumerGroupResult::ConsumerGroupMemberNotFound as u32 {
        Outcome::Reached
    } else {
        Outcome::Refused
    }
}

#[must_use]
pub const fn predicted_effect(_input: &Input, _outcome: Outcome) -> Effect {
    Effect::None
}
