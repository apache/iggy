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

//! `CreateConsumerGroup` op.
//!
//! Targets `Ok` (fresh group under a live stream/topic), `StreamNotFound`
//! (a fabricated parent stream), `TopicNotFound` (a live stream with a
//! fabricated topic), or `NameAlreadyExists` (an existing group name).

use iggy_binary_protocol::RequestHeader;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::CreateConsumerGroupResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
    pub name: String,
}

pub const OUTCOMES: &[Outcome] = &[
    Outcome::Ok,
    Outcome::StreamNotFound,
    Outcome::TopicNotFound,
    Outcome::NameAlreadyExists,
];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => {
            let (stream, topic) = shadow.pick_topic_pair(prng)?;
            let name = shadow.fresh_name("cg");
            Some(Input {
                stream,
                topic,
                name,
            })
        }
        Outcome::StreamNotFound => Some(Input {
            stream: shadow.fabricate_absent_name("stream"),
            topic: shadow.fabricate_absent_name("topic"),
            name: shadow.fresh_name("cg"),
        }),
        Outcome::TopicNotFound => {
            let stream = shadow.pick_stream_name(prng)?;
            Some(Input {
                stream,
                topic: shadow.fabricate_absent_name("topic"),
                name: shadow.fresh_name("cg"),
            })
        }
        Outcome::NameAlreadyExists => {
            let (stream, topic, name) = shadow.pick_consumer_group_triple(prng)?;
            Some(Input {
                stream,
                topic,
                name,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.create_consumer_group(&input.stream, &input.topic, &input.name)
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
pub fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Ok => Effect::AddConsumerGroup {
            stream: input.stream.clone(),
            topic: input.topic.clone(),
            name: input.name.clone(),
        },
        // A committed rejection is a no-op: an absent parent stream or topic, or
        // a duplicate group name, leaves the shadow unchanged.
        Outcome::StreamNotFound | Outcome::TopicNotFound | Outcome::NameAlreadyExists => {
            Effect::None
        }
    }
}
