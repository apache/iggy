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

use iggy_common::IggyTimestamp;

/// Per-command handler for a given state type.
/// Each command struct implements this for the state it mutates.
///
/// `now` is captured once before the command enters `left_right`, ensuring
/// both copies see the same timestamp (deterministic replay).
pub trait StateHandler {
    type State;
    type Output;
    fn apply(&self, state: &mut Self::State, now: IggyTimestamp) -> Self::Output;
}

/// Parses type-erased input into a command. Macro-generated.
/// Returns `Ok(cmd)` if applicable, `Err(input)` to pass ownership back.
pub trait Command {
    type Cmd;
    type Input;
    type Output;

    fn parse(input: Self::Input) -> Result<Self::Cmd, Self::Input>;
}

/// Public interface for state machines.
/// Returns `Ok(output)` if applicable, `Err(input)` to pass ownership back.
pub trait State {
    type Output;
    type Input;

    fn apply(&self, input: Self::Input) -> Result<Self::Output, Self::Input>;
}

pub trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: Self::Input) -> Self::Output;
}
