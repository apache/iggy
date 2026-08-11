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
//! Targets `Ok` (a live topic, count within its shadow-tracked partition
//! count), `StreamNotFound` (a fabricated parent stream), `TopicNotFound` (a
//! live stream with a fabricated topic), or `InvalidPartitionsCount` (a live
//! topic, count one past its partition count - the server commits the typed
//! rejection instead of acking a silent no-op). A committed `Ok` shrinks the
//! shadow's per-topic partition count.

use iggy_binary_protocol::{MAX_PARTITIONS_PER_REQUEST, RoutedRequestHeader};
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

pub const OUTCOMES: &[Outcome] = &[
    Outcome::Ok,
    Outcome::StreamNotFound,
    Outcome::TopicNotFound,
    Outcome::InvalidPartitionsCount,
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
            let live = *shadow
                .topic_partitions
                .get(&(stream.clone(), topic.clone()))?;
            if live == 0 {
                // Any nonzero count would over-delete; that is the
                // `InvalidPartitionsCount` target, not `Ok`.
                return None;
            }
            let partitions_count = 1 + prng.random_range(0..live.min(4));
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
        Outcome::InvalidPartitionsCount => {
            let (stream, topic) = shadow.pick_topic_pair(prng)?;
            let live = *shadow
                .topic_partitions
                .get(&(stream.clone(), topic.clone()))?;
            // One past the live count is the smallest guaranteed over-delete.
            // Past the per-request cap the pre-consensus gate would answer
            // `TooManyPartitions` instead, so the target is unrealizable.
            let partitions_count = live.checked_add(1)?;
            if partitions_count > MAX_PARTITIONS_PER_REQUEST {
                return None;
            }
            Some(Input {
                stream,
                topic,
                partitions_count,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
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
pub fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Ok => Effect::RemovePartitions {
            stream: input.stream.clone(),
            topic: input.topic.clone(),
            count: input.partitions_count,
        },
        _ => Effect::None,
    }
}
