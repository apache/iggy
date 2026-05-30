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

//! `CreateTopic` op. Parent stream from shadow, fresh topic name.

use iggy_binary_protocol::{ReplyHeader, RequestHeader};
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub name: String,
    pub partitions_count: u32,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    Success,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Success];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Success => {
            let stream = shadow.pick_stream_name(prng)?;
            let name = shadow.fresh_name("topic");
            let partitions_count = 1 + prng.random_range(0..4u32);
            Some(Input {
                stream,
                name,
                partitions_count,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.create_topic(&input.stream, &input.name, input.partitions_count)
}

#[must_use]
pub const fn classify_reply(_reply: &ReplyHeader) -> Outcome {
    Outcome::Success
}

#[must_use]
pub fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Success => Effect::AddTopic {
            stream: input.stream.clone(),
            name: input.name.clone(),
            partitions: input.partitions_count,
        },
    }
}
