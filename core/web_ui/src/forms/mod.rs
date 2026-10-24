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

//! Modal forms that create, change and delete resources.

mod stream;
mod topic;

pub use stream::{NewStreamForm, StreamSettingsForm};
pub use topic::{AddPartitionsForm, DeletePartitionsForm, NewTopicForm, TopicSettingsForm};

use std::str::FromStr;

use crate::models::OptionInputs;

/// Collects the option fields the user filled in. Blank ones are left out so
/// the server applies its default.
fn non_blank_options<const N: usize>(fields: [(&str, String); N]) -> OptionInputs {
    fields
        .into_iter()
        .filter_map(|(key, value)| {
            let value = value.trim();
            (!value.is_empty()).then(|| (key.to_owned(), value.to_owned()))
        })
        .collect()
}

/// A partitions count typed into a form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionsCount(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Partitions count must be a whole number above 0")]
struct InvalidPartitionsCount;

impl FromStr for PartitionsCount {
    type Err = InvalidPartitionsCount;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.trim().parse::<u32>() {
            Ok(count) if count > 0 => Ok(Self(count)),
            _ => Err(InvalidPartitionsCount),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_blank_fields_should_leave_them_out() {
        let options = non_blank_options([
            ("message_expiry", " 7 days ".to_owned()),
            ("max_topic_size", "   ".to_owned()),
        ]);
        assert_eq!(
            options,
            OptionInputs::from([("message_expiry".to_owned(), "7 days".to_owned())])
        );
    }

    #[test]
    fn given_count_text_should_accept_only_positive_integers() {
        assert_eq!(" 3 ".parse(), Ok(PartitionsCount(3)));
        for text in ["0", "-1", "two", ""] {
            assert_eq!(text.parse::<PartitionsCount>(), Err(InvalidPartitionsCount));
        }
    }
}
