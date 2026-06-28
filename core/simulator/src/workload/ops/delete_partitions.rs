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

//! `DeletePartitions` op.
//!
//! Targets `Ok` (a live topic), `StreamNotFound` (a fabricated parent stream),
//! or `TopicNotFound` (a live stream with a fabricated topic). Partition counts
//! are not tracked in the shadow, so every outcome predicts `Effect::None`.

use iggy_binary_protocol::RequestHeader;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::DeletePartitionsResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
    pub partitions_count: u32,
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
            let partitions_count = 1 + prng.random_range(0..4u32);
            Some(Input {
                stream,
                topic,
                partitions_count,
            })
        }
        Outcome::StreamNotFound => {
            let stream = shadow.fabricate_absent_name("stream");
            let topic = shadow.fabricate_absent_name("topic");
            let partitions_count = 1 + prng.random_range(0..4u32);
            Some(Input {
                stream,
                topic,
                partitions_count,
            })
        }
        Outcome::TopicNotFound => {
            let stream = shadow.pick_stream_name(prng)?;
            let topic = shadow.fabricate_absent_name("topic");
            let partitions_count = 1 + prng.random_range(0..4u32);
            Some(Input {
                stream,
                topic,
                partitions_count,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.delete_partitions(&input.stream, &input.topic, input.partitions_count)
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
    // Partition counts are not tracked per topic in the shadow.
    Effect::None
}
