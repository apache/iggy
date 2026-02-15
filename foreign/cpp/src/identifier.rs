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

use iggy::prelude::{IdKind, Identifier as RustIdentifier};
use std::result::Result;
use std::sync::Arc;

pub(crate) struct Identifier {
    pub(crate) inner: Arc<RustIdentifier>,
}

pub(crate) fn identifier_from_named(identifier_name: String) -> Result<*mut Identifier, String> {
    let identifier = RustIdentifier::named(identifier_name.as_str()).map_err(|error| {
        format!(
            "Could not create named identifier '{}': {error}",
            identifier_name
        )
    })?;
    Ok(Box::into_raw(Box::new(Identifier::new(identifier))))
}

pub(crate) fn identifier_from_numeric(identifier_id: u32) -> Result<*mut Identifier, String> {
    let identifier = RustIdentifier::numeric(identifier_id).map_err(|error| {
        format!(
            "Could not create numeric identifier {}: {error}",
            identifier_id
        )
    })?;
    Ok(Box::into_raw(Box::new(Identifier::new(identifier))))
}

impl Identifier {
    pub(crate) fn new(inner: RustIdentifier) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn as_rust(&self) -> &RustIdentifier {
        &self.inner
    }

    pub(crate) fn get_value(&self) -> String {
        match self.inner.kind {
            IdKind::Numeric => self
                .inner
                .get_u32_value()
                .map(|v| v.to_string())
                .unwrap_or_else(|_| "0".to_string()),
            IdKind::String => String::from_utf8_lossy(&self.inner.value).to_string(),
        }
    }
}

pub(crate) unsafe fn delete_identifier(identifier: *mut Identifier) {
    if identifier.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(identifier));
    }
}
