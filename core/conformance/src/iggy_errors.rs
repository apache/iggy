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

//! Error names in test files, mapped to the codes that agents return.
//!
//! The names are the snake_case `IggyError` names, the same names that the
//! shared codec fixtures use.

use iggy_common::IggyErrorDiscriminants;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Above every code that `IggyError` defines.
const LAST_CODE: u32 = 65_535;

/// The code of an `IggyError` name, such as `unauthenticated` for 40.
#[must_use]
pub fn code_of(name: &str) -> Option<u32> {
    codes_by_name().get(name).copied()
}

/// The `IggyError` name of a code, or `None` for a code it does not define.
#[must_use]
pub fn name_of(code: u32) -> Option<&'static str> {
    IggyErrorDiscriminants::from_repr(code).map(Into::into)
}

fn codes_by_name() -> &'static HashMap<&'static str, u32> {
    static CODES: OnceLock<HashMap<&'static str, u32>> = OnceLock::new();
    CODES.get_or_init(|| {
        (0..=LAST_CODE)
            .filter_map(|code| name_of(code).map(|name| (name, code)))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_known_names_when_mapping_should_return_their_codes() {
        assert_eq!(code_of("unauthenticated"), Some(40));
        assert_eq!(code_of("transient_not_accepted"), Some(58));
        assert_eq!(code_of("operation_uncertain"), Some(14006));
        assert_eq!(code_of("lane_blocked"), Some(14007));
        assert_eq!(name_of(14009), Some("partition_lane_blocked"));
        assert_eq!(code_of("no_such_error"), None);
    }
}
