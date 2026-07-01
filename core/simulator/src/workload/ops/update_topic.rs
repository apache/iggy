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

//! `UpdateTopic` op.
//!
//! Targets `Ok` (rename live topic to fresh name), `StreamNotFound` (fabricated
//! parent stream), or `TopicNotFound` (live stream, fabricated topic).
//! `NameAlreadyExists` not targeted.

use iggy_binary_protocol::RequestHeader;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::UpdateTopicResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
    pub new_name: String,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Ok, Outcome::StreamNotFound, Outcome::TopicNotFound];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => {
            let (stream, topic) = shadow.pick_topic_pair(prng)?;
            let new_name = shadow.fresh_name("topic");
            Some(Input {
                stream,
                topic,
                new_name,
            })
        }
        Outcome::StreamNotFound => Some(Input {
            stream: shadow.fabricate_absent_name("stream"),
            topic: shadow.fabricate_absent_name("topic"),
            new_name: shadow.fresh_name("topic"),
        }),
        Outcome::TopicNotFound => {
            let stream = shadow.pick_stream_name(prng)?;
            Some(Input {
                stream,
                topic: shadow.fabricate_absent_name("topic"),
                new_name: shadow.fresh_name("topic"),
            })
        }
        Outcome::NameAlreadyExists => {
            unreachable!("update_topic does not target NameAlreadyExists")
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.update_topic(&input.stream, &input.topic, &input.new_name)
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
        Outcome::Ok => Effect::RenameTopic {
            stream: input.stream.clone(),
            old: input.topic.clone(),
            new: input.new_name.clone(),
        },
        _ => Effect::None,
    }
}
