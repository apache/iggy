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

//! `ChangePassword` op. Targets `Ok` (rotate live user's password) or
//! `UserNotFound` (fabricated user).

use iggy_binary_protocol::RequestHeader;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::ChangePasswordResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub user: String,
    pub current_password: String,
    pub new_password: String,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Ok, Outcome::UserNotFound];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256Plus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => {
            let user = shadow.pick_user_name(prng)?;
            // After the first rotation `pw-{user}` is stale; read the shadow.
            let current_password = shadow.password_for(&user)?.to_string();
            let r: u32 = prng.random();
            Some(Input {
                user,
                current_password,
                new_password: format!("pw-{r:08x}"),
            })
        }
        Outcome::UserNotFound => Some(Input {
            user: shadow.fabricate_absent_name("user"),
            current_password: String::new(),
            new_password: "pw-missing".to_string(),
        }),
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RequestHeader> {
    client.change_password(&input.user, &input.current_password, &input.new_password)
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
        Outcome::Ok => Effect::PasswordChanged {
            user: input.user.clone(),
            new_password: input.new_password.clone(),
        },
        Outcome::UserNotFound => Effect::None,
    }
}
