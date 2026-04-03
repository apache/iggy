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

use crate::ffi;
use iggy::prelude::{IdKind, Identifier as RustIdentifier, Validatable};

impl From<RustIdentifier> for ffi::Identifier {
    fn from(identifier: RustIdentifier) -> Self {
        let kind = match identifier.kind {
            IdKind::Numeric => "numeric".to_string(),
            IdKind::String => "string".to_string(),
        };

        ffi::Identifier {
            kind,
            length: identifier.length,
            value: identifier.value,
        }
    }
}

impl TryFrom<ffi::Identifier> for RustIdentifier {
    type Error = String;

    fn try_from(identifier: ffi::Identifier) -> Result<Self, Self::Error> {
        let kind = match identifier.kind.as_str() {
            "numeric" => IdKind::Numeric,
            "string" => IdKind::String,
            _ => {
                return Err(format!(
                    "unsupported identifier kind '{}'. Expected 'numeric' or 'string'.",
                    identifier.kind
                ));
            }
        };

        let rust_identifier = RustIdentifier {
            kind,
            length: identifier.length,
            value: identifier.value,
        };

        rust_identifier
            .validate()
            .map_err(|error| format!("invalid identifier: {error}"))?;

        Ok(rust_identifier)
    }
}

impl ffi::Identifier {
    pub fn from_string(&mut self, id: String) -> Result<(), String> {
        *self = RustIdentifier::named(&id)
            .map(ffi::Identifier::from)
            .map_err(|error| format!("Could not create string identifier: {error}"))?;
        Ok(())
    }

    pub fn from_numeric(&mut self, id: u32) -> Result<(), String> {
        *self = RustIdentifier::numeric(id)
            .map(ffi::Identifier::from)
            .map_err(|error| format!("Could not create numeric identifier: {error}"))?;
        Ok(())
    }
}
