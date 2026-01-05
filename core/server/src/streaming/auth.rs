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

//! Type-safe authentication proof system.
//!
//! This module provides compile-time guarantees for authentication by using
//! proof-carrying code patterns. The [`Auth`] type can only be constructed
//! via [`IggyShard::auth()`], ensuring that any code path holding an `Auth`
//! value has passed authentication checks.

use iggy_common::UserId;

/// Proof of successful authentication.
///
/// This type can ONLY be constructed via [`IggyShard::auth()`].
/// The `_sealed` field prevents external construction, ensuring that
/// possession of an `Auth` value proves authentication has occurred.
///
/// # Invariants
/// - `user_id` is always a valid, authenticated user ID (never `u32::MAX`)
/// - The user was authenticated at the time of `Auth` construction
#[derive(Clone, Copy, Debug)]
pub struct Auth {
    user_id: UserId,
    /// Zero-sized private field that prevents external construction.
    /// The only way to create an `Auth` is through `Auth::new()` which is `pub(crate)`.
    _sealed: (),
}

impl Auth {
    /// Creates a new authentication proof.
    ///
    /// # Safety Contract (not unsafe, but must be upheld)
    /// This MUST only be called after verifying that the user is authenticated.
    /// Callers are responsible for ensuring `user_id` corresponds to a valid,
    /// authenticated user.
    #[inline]
    pub(crate) fn new(user_id: UserId) -> Self {
        debug_assert!(user_id != u32::MAX, "Auth created with invalid user_id");
        Self {
            user_id,
            _sealed: (),
        }
    }

    /// Returns the authenticated user's ID.
    #[inline]
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
}

/// Marker trait for authentication requirements.
///
/// This trait is sealed and cannot be implemented outside this module.
/// It defines whether a command requires authentication and what token
/// type the handler receives.
pub trait AuthRequirement: private::Sealed {
    /// A descriptive name for logging/debugging.
    const NAME: &'static str;
}

/// Marker type indicating a command requires authentication.
///
/// Handlers with this requirement will receive an [`Auth`] proof token
/// and can pass it to shard methods that require authentication.
pub struct Authenticated;

impl AuthRequirement for Authenticated {
    const NAME: &'static str = "Authenticated";
}

impl private::Sealed for Authenticated {}

/// Marker type indicating a command does NOT require authentication.
///
/// Used for commands like `Ping`, `LoginUser`, and `LoginWithPersonalAccessToken`
/// that must work before authentication.
pub struct Unauthenticated;

impl AuthRequirement for Unauthenticated {
    const NAME: &'static str = "Unauthenticated";
}

impl private::Sealed for Unauthenticated {}

mod private {
    /// Sealed trait pattern - prevents external implementations of AuthRequirement.
    pub trait Sealed {}
}
