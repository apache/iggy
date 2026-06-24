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

//! User password-hashing request rewriting.
//!
//! Passwords arrive raw on the wire. The metadata STM stores
//! `CreateUserRequest::password` / `ChangePasswordRequest::new_password`
//! verbatim as the credential hash, and login verifies against it with
//! Argon2, so the stored value must already be a PHC hash. Hashing inside
//! the replicated `apply` would call `OsRng` for the Argon2 salt on every
//! replica and diverge state (and a raw password would never parse,
//! panicking `verify_password`). So the primary hashes once here and
//! replicates the hash, mirroring the PAT mint in [`crate::pat`].

use crate::wire::{request_body, rewrite_request_body};
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::users::{ChangePasswordRequest, CreateUserRequest};
use iggy_binary_protocol::{Operation, RequestHeader};
use iggy_common::IggyError;
use server_common::{Message, crypto};

/// Replace a raw wire password with its Argon2 hash before replication.
///
/// Only touches `CreateUser` and `ChangePassword`; every other operation
/// passes through unchanged.
pub(crate) fn maybe_rewrite_user_password_request(
    request: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError> {
    let operation = request.header().operation;
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::CreateUser => {
            let mut wire =
                CreateUserRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            wire.password = crypto::hash_password(&wire.password);
            wire.to_bytes()
        }
        Operation::ChangePassword => {
            let mut wire =
                ChangePasswordRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            wire.new_password = crypto::hash_password(&wire.new_password);
            wire.to_bytes()
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}
