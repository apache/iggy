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

//! `UpdateUser` op. Existing username from shadow plus a fresh new name.

use iggy_binary_protocol::{Message, ReplyHeader, RequestHeader};
use rand_xoshiro::Xoshiro256Plus;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

#[derive(Debug, Clone)]
pub struct Input {
    pub user: String,
    pub new_username: Option<String>,
    pub status: Option<u8>,
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
            let user = shadow.pick_user_name(prng)?;
            let new_username = Some(shadow.fresh_name("user"));
            Some(Input {
                user,
                new_username,
                status: Some(1),
            })
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.update_user(&input.user, input.new_username.as_deref(), input.status)
}

#[must_use]
pub const fn classify_reply(_reply: &ReplyHeader) -> Outcome {
    Outcome::Success
}

#[must_use]
pub fn predicted_effect(input: &Input, outcome: Outcome) -> Effect {
    match outcome {
        Outcome::Success => {
            input
                .new_username
                .as_ref()
                .map_or(Effect::None, |new| Effect::RenameUser {
                    old: input.user.clone(),
                    new: new.clone(),
                })
        }
    }
}
