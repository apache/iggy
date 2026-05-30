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

//! `DeleteSegments` op.
//!
//! Currently disabled at sample time. `DeleteSegments` is a partition
//! operation (`Operation::is_partition() == true`, see
//! `core/binary_protocol/src/consensus/operation.rs`) but the simulator's
//! name-keyed `Shadow` has no `(stream, topic) -> IggyNamespace` binding,
//! so the request cannot be routed to the right partition (wire-level
//! namespace defaults to 0, which is the metadata plane and has no
//! partition handler). Sampling returns `None` until the shadow grows
//! a topic-to-namespace index; the op stays in `Action` and the
//! dispatch table so the surface compiles, and so the v2.4 outcome-
//! expansion lands cleanly when the shadow is upgraded.

use iggy_binary_protocol::{ReplyHeader, RequestHeader};
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

#[derive(Debug, Clone)]
pub struct Input {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
    pub segments_count: u32,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome {
    Success,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Success];

pub const fn sample(
    _shadow: &mut Shadow,
    _outcome: Outcome,
    _prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    // Disabled until shadow grows a topic-to-namespace index; routing
    // via the partition namespace requires it. See module docs.
    None
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.delete_segments(
        &input.stream,
        &input.topic,
        input.partition_id,
        input.segments_count,
    )
}

#[must_use]
pub const fn classify_reply(_reply: &ReplyHeader) -> Outcome {
    Outcome::Success
}

#[must_use]
pub const fn predicted_effect(_input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Success => Effect::None,
    }
}
