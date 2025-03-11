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

use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};

/// `RawPersonalAccessToken` represents the raw personal access token - the secured token which is returned only once during the creation.
/// It consists of the following fields:
/// - `token`: the unique token that should be securely stored by the user and can be used for authentication.
#[derive(Debug, Serialize, Deserialize)]
pub struct RawPersonalAccessToken {
    /// The unique token that should be securely stored by the user and can be used for authentication.
    pub token: String,
}

/// `PersonalAccessToken` represents the personal access token. It does not contain the token itself, but the information about the token.
/// It consists of the following fields:
/// - `name`: the unique name of the token.
/// - `expiry`: the optional expiry of the token.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersonalAccessTokenInfo {
    /// The unique name of the token.
    pub name: String,
    /// The optional expiry of the token.
    pub expiry_at: Option<IggyTimestamp>,
}
