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

//! `PurgeTopic` op. Targets `Ok` (live topic), `StreamNotFound` (fabricated
//! parent stream), or `TopicNotFound` (live stream, fabricated topic).

use iggy_binary_protocol::RequestHeader;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::PurgeTopicResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Ok, Outcome::StreamNotFound, Outcome::TopicNotFound];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => shadow
            .pick_topic_pair(prng)
            .map(|(stream, topic)| Input { stream, topic }),
        Outcome::StreamNotFound => Some(Input {
            stream: shadow.fabricate_absent_name("stream"),
            topic: shadow.fabricate_absent_name("topic"),
        }),
        Outcome::TopicNotFound => {
            let stream = shadow.pick_stream_name(prng)?;
            Some(Input {
                stream,
                topic: shadow.fabricate_absent_name("topic"),
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.purge_topic(&input.stream, &input.topic)
}

/// Decode committed `code` into this op's outcome.
///
/// # Panics
/// On an undeclared code; `on_reply` rejects unrecognized codes first.
#[must_use]
pub const fn classify_reply(code: u32) -> Outcome {
    Outcome::from_u32(code).expect("on_reply rejects unrecognized result codes before classify")
}

#[must_use]
pub const fn predicted_effect(_input: &Input, _outcome: Outcome) -> Effect {
    // Purge clears messages but leaves the topic live; see `purge_stream` for
    // why per-namespace message counts are not modeled in the shadow yet.
    Effect::None
}
