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

//! `ReplaceFixtureTopic` op. Targets only `Reached`.
//!
//! Deletes a live fixture topic, or recreates a deleted one under its seeded
//! names, so its namespaces keep their ids under a new incarnation.
//! [`Shadow::begin_fixture_toggle`] picks the topic and the half.

use iggy_binary_protocol::RoutedRequestHeader;
use metadata::stm::result::{CreateTopicResult, DeleteTopicResult};
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::{Shadow, Toggle};

#[derive(Debug, Clone)]
pub struct Input {
    /// Index of the fixture topic in the shadow.
    pub fixture: usize,
    pub stream: String,
    pub topic: String,
    pub toggle: Toggle,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    /// The topic is now what the toggle asked for.
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
            let (fixture, toggle) = shadow.begin_fixture_toggle(prng)?;
            let (stream, topic) = shadow.fixture_names(fixture);
            Some(Input {
                fixture,
                stream,
                topic,
                toggle,
            })
        }
        Outcome::Refused => None,
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    match input.toggle {
        Toggle::Delete => client.delete_topic(&input.stream, &input.topic),
        Toggle::Create { partitions_count } => {
            client.create_topic(&input.stream, &input.topic, partitions_count)
        }
    }
}

/// A topic already gone, or already there, means the toggle reached its goal.
///
/// A probe asks again after a toggle of unknown fate, so either code proves that
/// the first toggle committed. Each code belongs to one half, and `on_reply`
/// rejects a code the half does not declare.
#[must_use]
pub const fn classify_reply(code: u32) -> Outcome {
    if code == 0 || already_reached(code) {
        Outcome::Reached
    } else {
        Outcome::Refused
    }
}

/// Whether `code` says the topic was already what the toggle asked for.
///
/// Only a probe or a resend can meet that: a first delete of a live fixture
/// finds its topic, and a first create after a committed delete does not.
#[must_use]
pub const fn already_reached(code: u32) -> bool {
    code == DeleteTopicResult::TopicNotFound as u32
        || code == CreateTopicResult::NameAlreadyExists as u32
}

/// The fixture moves in [`Shadow::finish_fixture_toggle`] instead, which needs
/// the metadata op of the reply and also hears of uncommitted ends.
#[must_use]
pub const fn predicted_effect(_input: &Input, _outcome: Outcome) -> Effect {
    Effect::None
}
