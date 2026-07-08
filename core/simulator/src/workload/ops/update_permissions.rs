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

//! `UpdatePermissions` op. Targets `Ok` (live user) or `UserNotFound`
//! (fabricated user). No permissions payload, so every outcome is `Effect::None`.

use iggy_binary_protocol::RequestHeader;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::UpdatePermissionsResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub user: String,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Ok, Outcome::UserNotFound];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => shadow.pick_user_name(prng).map(|user| Input { user }),
        Outcome::UserNotFound => Some(Input {
            user: shadow.fabricate_absent_name("user"),
        }),
        // Not a targeted outcome (absent from `OUTCOMES`); never sampled.
        Outcome::CannotChangePermissions => None,
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.update_permissions(&input.user)
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
    Effect::None
}
