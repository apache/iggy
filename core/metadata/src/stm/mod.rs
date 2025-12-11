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

pub mod consumer_group;
pub mod stream;
pub mod user;

pub mod mux;
// TODO: Add more state machines.

pub trait State {
    type Output;
    // Apply the state machine logic and return an optional output.
    // The output is optional, as we model the `StateMachine`, as an variadic list,
    // where not all state machines will produce an output for every input event.
    fn apply(&self) -> Option<Self::Output>;
}

pub trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: &Self::Input, output: &mut Vec<Self::Output>);
}
