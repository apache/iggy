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

use std::sync::Arc;

use ext_php_rs::{boxed::ZBox, exception::PhpResult, php_class, php_impl, types::ZendHashTable};
use iggy::prelude::UserInfo as RustUserInfo;

use crate::options::options_array;

/// One user of a `UsersPage`.
#[php_class]
#[php(name = "Iggy\\UserInfo")]
#[derive(Clone)]
pub struct UserInfo {
    // The Rust type is not Clone, and each read of the page's items copies the user.
    inner: Arc<RustUserInfo>,
}

impl From<RustUserInfo> for UserInfo {
    fn from(user: RustUserInfo) -> Self {
        Self {
            inner: Arc::new(user),
        }
    }
}

#[php_impl]
impl UserInfo {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    /// Creation time in microseconds.
    #[php(getter, change_case = "snake_case")]
    pub fn created_at(&self) -> u64 {
        self.inner.created_at.as_micros()
    }

    /// `active` or `inactive`.
    #[php(getter)]
    pub fn status(&self) -> String {
        self.inner.status.to_string()
    }

    #[php(getter)]
    pub fn username(&self) -> String {
        self.inner.username.clone()
    }

    /// Options the creating client set, as option name to scalar value.
    #[php(getter)]
    pub fn options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, true)
    }

    /// Options the server filled in.
    #[php(getter, change_case = "snake_case")]
    pub fn derived_options(&self) -> PhpResult<ZBox<ZendHashTable>> {
        options_array(&self.inner.options, false)
    }
}
