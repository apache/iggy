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

use std::env;

/// Reads a variable the suite cannot run without.
///
/// A default here would turn a dropped compose variable into a run against
/// whatever happens to listen on the fallback address, so a missing value
/// aborts the suite instead.
fn required_env(name: &str) -> String {
    match env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => panic!("{name} must be set; run the suite via scripts/run-bdd-tests.sh"),
    }
}

pub fn server_address() -> String {
    required_env("IGGY_TCP_ADDRESS")
}

pub fn leader_address() -> String {
    required_env("IGGY_TCP_ADDRESS_LEADER")
}

pub fn follower_address() -> String {
    required_env("IGGY_TCP_ADDRESS_FOLLOWER")
}

pub fn root_username() -> String {
    required_env("IGGY_ROOT_USERNAME")
}

pub fn root_password() -> String {
    required_env("IGGY_ROOT_PASSWORD")
}
