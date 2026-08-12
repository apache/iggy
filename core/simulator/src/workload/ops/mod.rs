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

//! Op modules: one file per server command.
//!
//! Each module exposes:
//! - `Input`: sampled request parameters
//! - `Outcome`: expected reply classes
//! - `OUTCOMES`: declared outcome variants for weight tables
//! - `sample`: synthesize an `Input` for a target `Outcome`
//! - `build_message`: encode `Input` into the wire request
//! - `classify_reply`: decode reply into a declared outcome
//! - `predicted_effect`: predicted shadow mutation on commit
//!
//! Dispatch via the [`op_dispatch!`] macro; missing variants are a compile
//! error.

pub mod change_password;
pub mod create_consumer_group;
pub mod create_partitions;
pub mod create_personal_access_token;
pub mod create_stream;
pub mod create_topic;
pub mod create_user;
pub mod delete_consumer_group;
pub mod delete_consumer_offset;
pub mod delete_consumer_offset_2;
pub mod delete_partitions;
pub mod delete_personal_access_token;
pub mod delete_segments;
pub mod delete_stream;
pub mod delete_topic;
pub mod delete_user;
pub mod purge_stream;
pub mod purge_topic;
pub mod send_messages;
pub mod store_consumer_offset;
pub mod store_consumer_offset_2;
pub mod update_permissions;
pub mod update_stream;
pub mod update_topic;
pub mod update_user;

use iggy_binary_protocol::RequestHeader;
use rand_xoshiro::Xoshiro256Plus;
use server_common::Message;

use crate::client::SimClient;
use crate::workload::actions::Action;
use crate::workload::effect::Effect;
use crate::workload::options::WorkloadOptions;
use crate::workload::shadow::Shadow;

/// Generates per-op enums (`InFlightInput`, `InFlightOutcome`) plus four
/// dispatch fns over a fixed `(Action, module)` table. Missing variants
/// are a compile error via the exhaustive `match` arms.
macro_rules! op_dispatch {
    ( $( $variant:ident => $module:ident ),* $(,)? ) => {
        /// Per-op `Input` packaged into one enum so the auditor stores
        /// heterogeneous in-flight entries without trait-object machinery.
        #[derive(Debug, Clone)]
        pub enum InFlightInput {
            $( $variant($module::Input), )*
        }

        #[derive(Debug, Copy, Clone, Eq, PartialEq)]
        pub enum InFlightOutcome {
            $( $variant($module::Outcome), )*
        }

        /// In-flight entry recorded on submit, removed on reply.
        ///
        /// `request_namespace` is the `header.namespace` the request was
        /// submitted with; the auditor cross-checks it against the
        /// reply's namespace so a misrouted reply cannot update the
        /// wrong VSR group's bookkeeping.
        #[derive(Debug, Clone)]
        pub struct InFlight {
            pub action: Action,
            pub input: InFlightInput,
            pub outcome: InFlightOutcome,
            pub request_namespace: u64,
        }

        /// Sample an `Input` for `action` realizing the outcome at `outcome_id`
        /// within the op's `OUTCOMES`. `None` when sampling preconditions fail
        /// (e.g. no live namespace).
        pub fn sample(
            action: Action,
            shadow: &mut Shadow,
            prng: &mut Xoshiro256Plus,
            options: &WorkloadOptions,
            outcome_id: usize,
        ) -> Option<(InFlightInput, InFlightOutcome)> {
            match action {
                $(
                    Action::$variant => {
                        let outcome = $module::OUTCOMES[outcome_id];
                        let input = $module::sample(shadow, outcome, prng, options)?;
                        Some((
                            InFlightInput::$variant(input),
                            InFlightOutcome::$variant(outcome),
                        ))
                    }
                )*
            }
        }

        /// Number of declared outcomes for `action` (its `OUTCOMES` length).
        /// The workload targets one by picking an `outcome_id` in `0..count`.
        #[must_use]
        pub const fn outcome_count(action: Action) -> usize {
            match action {
                $( Action::$variant => $module::OUTCOMES.len(), )*
            }
        }

        #[must_use]
        pub fn build_message(client: &SimClient, input: &InFlightInput) -> Message<RequestHeader> {
            match input {
                $( InFlightInput::$variant(i) => $module::build_message(client, i), )*
            }
        }

        /// Classify a reply into its op's declared outcome.
        ///
        /// Decodes the committed result `code` (read off the reply body upstream;
        /// `0` for the partition plane, which has no result section) through the
        /// op's result enum. `Workload::on_reply` rejects an unrecognized code
        /// before this runs.
        #[must_use]
        pub const fn classify_reply(action: Action, code: u32) -> InFlightOutcome {
            match action {
                $( Action::$variant => InFlightOutcome::$variant($module::classify_reply(code)), )*
            }
        }

        /// # Panics
        /// Panics if `input` and `outcome` carry mismatched op variants.
        /// Workload bug: the auditor pairs them at sample-time.
        #[must_use]
        pub fn predicted_effect(input: &InFlightInput, outcome: &InFlightOutcome) -> Effect {
            match (input, outcome) {
                $(
                    (InFlightInput::$variant(i), InFlightOutcome::$variant(o)) => {
                        $module::predicted_effect(i, *o)
                    }
                )*
                (input, outcome) => panic!(
                    "input/outcome op variant mismatch: input={input:?}, outcome={outcome:?}"
                ),
            }
        }
    };
}

op_dispatch! {
    // First three positions lock the hash baseline (do not reorder).
    CreateStream              => create_stream,
    SendMessages              => send_messages,
    StoreConsumerOffset2      => store_consumer_offset_2,
    // Append-only; mirrors actions::Action declaration order.
    DeleteStream              => delete_stream,
    UpdateStream              => update_stream,
    PurgeStream               => purge_stream,
    CreateTopic               => create_topic,
    UpdateTopic               => update_topic,
    DeleteTopic               => delete_topic,
    PurgeTopic                => purge_topic,
    CreatePartitions          => create_partitions,
    DeletePartitions          => delete_partitions,
    DeleteSegments            => delete_segments,
    CreateConsumerGroup       => create_consumer_group,
    DeleteConsumerGroup       => delete_consumer_group,
    CreateUser                => create_user,
    UpdateUser                => update_user,
    DeleteUser                => delete_user,
    ChangePassword            => change_password,
    UpdatePermissions         => update_permissions,
    CreatePersonalAccessToken => create_personal_access_token,
    DeletePersonalAccessToken => delete_personal_access_token,
    StoreConsumerOffset       => store_consumer_offset,
    DeleteConsumerOffset      => delete_consumer_offset,
    DeleteConsumerOffset2     => delete_consumer_offset_2,
}
