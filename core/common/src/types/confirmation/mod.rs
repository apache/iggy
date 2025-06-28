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

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Clone, Copy, Debug, Default, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum Confirmation {
    #[default]
    Wait,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_to_string() {
        assert_eq!(Confirmation::Wait.to_string(), "wait");
        assert_eq!(Confirmation::NoWait.to_string(), "no_wait");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(Confirmation::from_str("wait").unwrap(), Confirmation::Wait);
        assert_eq!(
            Confirmation::from_str("no_wait").unwrap(),
            Confirmation::NoWait
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(Confirmation::default(), Confirmation::Wait);
    }
}
