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

//! Type-safe authentication proof system using proof-carrying code pattern.
//!
//! The [`Auth`] type can only be constructed via [`IggyShard::auth()`],
//! ensuring any code holding an `Auth` value has passed authentication.

use iggy_common::UserId;

/// Proof of successful authentication.
///
/// # Invariants
/// - `user_id` is always valid (never `u32::MAX`)
/// - User was authenticated at construction time
#[derive(Clone, Copy, Debug)]
pub struct Auth {
    user_id: UserId,
    // Private field prevents external construction - only `Auth::new()` can create this.
    _sealed: (),
}

impl Auth {
    /// Only call after verifying user is authenticated.
    #[inline]
    pub(crate) fn new(user_id: UserId) -> Self {
        debug_assert!(user_id != u32::MAX, "Auth created with invalid user_id");
        Self {
            user_id,
            _sealed: (),
        }
    }

    #[inline]
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
}

/// Sealed marker trait for authentication requirements.
pub trait AuthRequirement: private::Sealed {
    const NAME: &'static str;
}

/// Command requires authentication - handler receives [`Auth`] proof token.
pub struct Authenticated;

impl AuthRequirement for Authenticated {
    const NAME: &'static str = "Authenticated";
}

impl private::Sealed for Authenticated {}

/// Command does NOT require authentication (Ping, LoginUser, LoginWithPersonalAccessToken).
pub struct Unauthenticated;

impl AuthRequirement for Unauthenticated {
    const NAME: &'static str = "Unauthenticated";
}

impl private::Sealed for Unauthenticated {}

mod private {
    pub trait Sealed {}
}
