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

//! `JoinFixtureGroup` op. Targets only `Reached`.
//!
//! Joins the fixture group of a fixture topic, or creates the group again when a
//! replacement dropped it with its topic. Samples nothing in a run without
//! fixture groups, which every raw run is: the raw path does not attach the
//! member identity that a replicated join needs.

use iggy_binary_protocol::RoutedRequestHeader;
use metadata::stm::result::CreateConsumerGroupResult;
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::{FIXTURE_GROUP_NAME, GroupJoin, Shadow};

#[derive(Debug, Clone)]
pub struct Input {
    pub join: GroupJoin,
    pub stream: String,
    pub topic: String,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    /// The client is a member, or the group exists.
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
            let join = shadow.begin_group_join(prng)?;
            let (stream, topic) = shadow.fixture_names(join.fixture);
            Some(Input {
                join,
                stream,
                topic,
            })
        }
        Outcome::Refused => None,
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    if input.join.create {
        client.create_consumer_group(&input.stream, &input.topic, FIXTURE_GROUP_NAME)
    } else {
        client.join_consumer_group(&input.stream, &input.topic, FIXTURE_GROUP_NAME)
    }
}

/// A group already there means a create reached its goal. A join is idempotent,
/// so it answers 0 for a member too.
#[must_use]
pub const fn classify_reply(code: u32) -> Outcome {
    if code == 0 || code == CreateConsumerGroupResult::NameAlreadyExists as u32 {
        Outcome::Reached
    } else {
        Outcome::Refused
    }
}

/// The group moves in [`Shadow::finish_group_join`] instead, which needs the
/// client of the reply and also hears of uncommitted ends.
#[must_use]
pub const fn predicted_effect(_input: &Input, _outcome: Outcome) -> Effect {
    Effect::None
}
