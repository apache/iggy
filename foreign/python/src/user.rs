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

use iggy::prelude::{
    UserInfo as RustUserInfo, UserInfoDetails as RustUserInfoDetails, UserStatus as RustUserStatus,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::permissions::Permissions;

/// The status of a user account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum UserStatus {
    /// The user account is active and can be used.
    Active,
    /// The user account is inactive and cannot be used.
    Inactive,
}

impl From<UserStatus> for RustUserStatus {
    fn from(status: UserStatus) -> Self {
        match status {
            UserStatus::Active => RustUserStatus::Active,
            UserStatus::Inactive => RustUserStatus::Inactive,
        }
    }
}

impl From<RustUserStatus> for UserStatus {
    fn from(status: RustUserStatus) -> Self {
        match status {
            RustUserStatus::Active => UserStatus::Active,
            RustUserStatus::Inactive => UserStatus::Inactive,
        }
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct UserInfo {
    pub(crate) inner: RustUserInfo,
}

impl From<RustUserInfo> for UserInfo {
    fn from(user: RustUserInfo) -> Self {
        Self { inner: user }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl UserInfo {
    /// The unique identifier (numeric) of the user.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// The timestamp when the user was created, in microseconds since the Unix epoch.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// The status of the user.
    #[getter]
    pub fn status(&self) -> UserStatus {
        self.inner.status.into()
    }

    /// The username of the user.
    #[getter]
    pub fn username(&self) -> String {
        self.inner.username.to_string()
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct UserInfoDetails {
    pub(crate) inner: RustUserInfoDetails,
}

impl From<RustUserInfoDetails> for UserInfoDetails {
    fn from(user: RustUserInfoDetails) -> Self {
        Self { inner: user }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl UserInfoDetails {
    /// The unique identifier (numeric) of the user.
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// The timestamp when the user was created, in microseconds since the Unix epoch.
    #[getter]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// The status of the user.
    #[getter]
    pub fn status(&self) -> UserStatus {
        self.inner.status.into()
    }

    /// The username of the user.
    #[getter]
    pub fn username(&self) -> String {
        self.inner.username.to_string()
    }

    /// The permissions of the user, or `None` when the user has none assigned.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "Permissions | None"))]
    pub fn permissions(&self) -> Option<Permissions> {
        self.inner.permissions.clone().map(Permissions::from)
    }
}
