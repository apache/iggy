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

//! `SendMessages` op. Samples only `Success`. PRNG draw order:
//!
//! 1. namespace pick (`pick_namespace`)
//! 2. batch length (`prng.random_range(0..span)`)
//! 3. one `prng.random()` per payload to disambiguate body bytes

use bytes::Bytes;
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
    pub batch_len: u32,
    /// `Bytes` so `build_message` does refcount bumps, not allocations.
    pub payloads: Vec<Bytes>,
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
            let span = options.batch_size_span.max(1);
            let batch_len = options.batch_size_min + prng.random_range(0..span);
            let mut payloads: Vec<Bytes> = Vec::with_capacity(batch_len as usize);
            for i in 0..batch_len {
                let r: u32 = prng.random();
                payloads.push(Bytes::from(format!("wl-msg-{i:04x}-{r:08x}")));
            }
            Some(Input {
                ns,
                batch_len,
                payloads,
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.send_messages(input.ns, &input.payloads)
}

#[must_use]
pub const fn classify_reply(_code: u32) -> Outcome {
    Outcome::Success
}

#[must_use]
pub fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Success => Effect::SendCommitted {
            ns: input.ns,
            count: u64::from(input.batch_len),
        },
    }
}
