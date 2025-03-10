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

use super::user_info::UserId;
use serde::{Deserialize, Serialize};

/// `IdentityInfo` represents the information about an identity.
/// It consists of the following fields:
/// - `user_id`: the unique identifier (numeric) of the user.
/// - `access_token`: the optional access token, used only by HTTP transport.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    /// The unique identifier (numeric) of the user.
    pub user_id: UserId,
    /// The optional tokens, used only by HTTP transport.
    pub access_token: Option<TokenInfo>,
}

/// `TokenInfo` represents the details of the access token.
/// It consists of the following fields:
/// - `token`: the value of token.
/// - `expiry`: the expiry of token.
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    /// The value of token.
    pub token: String,
    /// The expiry of token.
    pub expiry: u64,
}
