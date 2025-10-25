/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use iggy_common::UserId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Identity {
    pub token_id: String,
    pub token_expiry: u64,
    pub user_id: UserId,
    pub ip_address: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JwtClaims {
    pub jti: String,
    pub iss: String,
    pub aud: String,
    pub sub: u32,
    pub iat: u64,
    pub exp: u64,
    pub nbf: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevokedAccessToken {
    pub id: String,
    pub expiry: u64,
}

#[derive(Debug)]
pub struct GeneratedToken {
    pub user_id: UserId,
    pub access_token: String,
    pub access_token_expiry: u64,
}
