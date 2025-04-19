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

use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU32;

/// `UserId` represents the unique identifier (numeric) of the user.
pub type UserId = u32;
/// `AtomicUserId` represents the unique identifier (numeric) of the user
/// which can be safely modified concurrently across threads
pub type AtomicUserId = AtomicU32;

/// `UserInfo` represents the basic information about the user.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the user.
/// - `created_at`: the timestamp when the user was created.
/// - `status`: the status of the user.
/// - `username`: the username of the user.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    /// The unique identifier (numeric) of the user.
    pub id: UserId,
    /// The timestamp when the user was created.
    pub created_at: IggyTimestamp,
    /// The status of the user.
    pub status: UserStatus,
    /// The username of the user.
    pub username: String,
}

/// `UserInfoDetails` represents the detailed information about the user.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the user.
/// - `created_at`: the timestamp when the user was created.
/// - `status`: the status of the user.
/// - `username`: the username of the user.
/// - `permissions`: the optional permissions of the user.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfoDetails {
    /// The unique identifier (numeric) of the user.
    pub id: UserId,
    /// The timestamp when the user was created.
    pub created_at: IggyTimestamp,
    /// The status of the user.
    pub status: UserStatus,
    /// The username of the user.
    pub username: String,
    /// The optional permissions of the user.
    pub permissions: Option<Permissions>,
}
