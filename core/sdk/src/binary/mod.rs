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

use iggy_binary_protocol::codes::{
    LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGIN_USER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGOUT_USER_CODE,
};
use iggy_common::IggyError;
pub use iggy_common::{BinaryClient, BinaryTransport};

/// Auth/session codes rejected by the raw binary path. Must go through the
/// typed `login_user` / `logout_user` methods to keep session state correct.
pub(crate) const SESSION_CONTROL_CODES: [u32; 5] = [
    LOGIN_USER_CODE,
    LOGOUT_USER_CODE,
    LOGIN_REGISTER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
    LOGIN_REGISTER_WITH_PAT_CODE,
];

pub(crate) fn validate_binary_request_code(code: u32) -> Result<(), IggyError> {
    if SESSION_CONTROL_CODES.contains(&code) {
        Err(IggyError::InvalidCommand)
    } else {
        Ok(())
    }
}
