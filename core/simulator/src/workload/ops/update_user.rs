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

//! `UpdateUser` op. Targets `Ok` (rename live user to fresh name and set it
//! active or inactive) or `UserNotFound` (fabricated user).
//! `UsernameAlreadyExists` not targeted.

use iggy_binary_protocol::RoutedRequestHeader;
use iggy_common::UserStatus;
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

pub use metadata::stm::result::UpdateUserResult as Outcome;

#[derive(Debug, Clone)]
pub struct Input {
    pub user: String,
    pub new_username: Option<String>,
    pub status: Option<UserStatus>,
    /// Current password from shadow; carried via `Effect::RenameUser` so the
    /// shadow re-keys `passwords` under `new_username`.
    pub current_password: String,
}

pub const OUTCOMES: &[Outcome] = &[Outcome::Ok, Outcome::UserNotFound];

pub fn sample(
    shadow: &mut Shadow,
    outcome: Outcome,
    prng: &mut Xoshiro256PlusPlus,
    _options: &WorkloadOptions,
) -> Option<Input> {
    match outcome {
        Outcome::Ok => {
            let user = shadow.pick_user_name(prng)?;
            let current_password = shadow.password_for(&user)?.to_string();
            let status = if prng.random_bool(0.5) {
                UserStatus::Inactive
            } else {
                UserStatus::Active
            };
            Some(Input {
                user,
                new_username: Some(shadow.fresh_name("user")),
                status: Some(status),
                current_password,
            })
        }
        Outcome::UserNotFound => Some(Input {
            user: shadow.fabricate_absent_name("user"),
            new_username: Some(shadow.fresh_name("user")),
            status: Some(UserStatus::Active),
            current_password: String::new(),
        }),
        Outcome::UsernameAlreadyExists => {
            unreachable!("update_user does not target UsernameAlreadyExists")
        }
        Outcome::InvalidUsername => {
            unreachable!("update_user does not target InvalidUsername")
        }
        Outcome::InvalidOptionValue | Outcome::OptionsBlockTooLarge => {
            unreachable!("the simulator only sends an empty update options block")
        }
        Outcome::InvalidUserStatus => {
            unreachable!("update_user only sends a valid status")
        }
        Outcome::CannotModifyRootUser => {
            unreachable!("the simulator only selects non-root users")
        }
    }
}

#[must_use]
pub fn build_message(client: &SimClient, input: &Input) -> Message<RoutedRequestHeader> {
    client.update_user(&input.user, input.new_username.as_deref(), input.status)
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
        Outcome::Ok => input
            .new_username
            .as_ref()
            .map_or(Effect::None, |new| Effect::RenameUser {
                old: input.user.clone(),
                new: new.clone(),
                password: input.current_password.clone(),
            }),
        Outcome::UserNotFound
        | Outcome::UsernameAlreadyExists
        | Outcome::InvalidUsername
        | Outcome::InvalidUserStatus
        | Outcome::InvalidOptionValue
        | Outcome::OptionsBlockTooLarge
        | Outcome::CannotModifyRootUser => Effect::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::ids::IdPermutation;
    use rand_xoshiro::rand_core::SeedableRng;
    use server_common::sharding::IggyNamespace;

    const SEED: u64 = 0x5553_4552;

    #[test]
    fn given_live_users_when_sampling_updates_should_draw_both_statuses() {
        let namespaces = vec![IggyNamespace::new(1, 1, 0)];
        let options = WorkloadOptions::new(SEED, 3, namespaces.clone());
        let mut shadow = Shadow::new(namespaces, IdPermutation::Identity);
        let mut prng = Xoshiro256PlusPlus::seed_from_u64(SEED);
        let mut statuses = Vec::new();
        for _ in 0..32 {
            let name = shadow.fresh_name("user");
            shadow.apply(Effect::AddUser { name });
            let input = sample(&mut shadow, Outcome::Ok, &mut prng, &options).expect("a live user");
            statuses.push(input.status);
        }
        assert!(statuses.contains(&Some(UserStatus::Active)));
        assert!(statuses.contains(&Some(UserStatus::Inactive)));
    }
}
