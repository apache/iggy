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

//! `StoreConsumerOffset` op. Pre-`AckLevel` manual encoding. Live
//! namespace via shadow, fabricated consumer kind/id. Samples Success.

use iggy_binary_protocol::RequestHeader;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

#[derive(Debug, Clone)]
pub struct Input {
    pub ns: IggyNamespace,
    pub consumer_kind: u8,
    pub consumer_id: u32,
    pub offset: u64,
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
    options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Success => {
            let ns = shadow.pick_namespace(prng)?;
            let consumer_kind: u8 = u8::from(prng.random::<bool>());
            let consumer_id: u32 = prng.random_range(0..options.consumer_pool_size.max(1));
            // Draw against the configured ceiling, then clamp to committed
            // reality so the offset is reachable. Clamping post-draw keeps
            // the PRNG draw order (and determinism hash baseline) intact
            // while staying valid once server-ng validates offsets.
            let raw: u64 = prng.random_range(0..options.max_offset.max(1));
            let high = shadow.sends_committed(ns).max(1);
            let offset = raw % high;
            Some(Input {
                ns,
                consumer_kind,
                consumer_id,
                offset,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.store_consumer_offset(
        input.ns,
        input.consumer_kind,
        input.consumer_id,
        input.offset,
    )
}

#[must_use]
pub const fn classify_reply(_code: u32) -> Outcome {
    Outcome::Success
}

#[must_use]
pub const fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Success => Effect::OffsetStored {
            key: (input.ns, input.consumer_kind, input.consumer_id),
            value: input.offset,
        },
    }
}
