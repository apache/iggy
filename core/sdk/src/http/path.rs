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

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, PercentEncode, utf8_percent_encode};

const PATH_SEGMENT: &AsciiSet = NON_ALPHANUMERIC;

pub(crate) fn encode_segment(value: &str) -> PercentEncode<'_> {
    utf8_percent_encode(value, PATH_SEGMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_plain_id_when_encoding_should_pass_through() {
        assert_eq!(encode_segment("1").to_string(), "1");
    }

    #[test]
    fn given_reserved_characters_when_encoding_should_percent_encode() {
        assert_eq!(encode_segment("my stream").to_string(), "my%20stream");
        assert_eq!(encode_segment("my/topic").to_string(), "my%2Ftopic");
    }

    #[test]
    fn given_dot_segments_when_encoding_should_never_pass_through_literally() {
        assert_eq!(encode_segment(".").to_string(), "%2E");
        assert_eq!(encode_segment("..").to_string(), "%2E%2E");
    }
}
