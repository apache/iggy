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

use std::future::Future;
use std::str::FromStr;

use dioxus::prelude::*;

use crate::api::ApiError;
use crate::auth::Session;
use crate::components::end_session;
use crate::models::OptionInputs;

/// The server's `MAX_NAME_LENGTH`, in bytes.
const MAX_NAME_BYTES: usize = 255;
/// The server's `MAX_PARTITIONS_COUNT`, per request.
const MAX_PARTITIONS_COUNT: u32 = 1000;

/// Runs a form's request while `busy` holds the form still, and shows a
/// failure in `error`. A 401 also ends the session, as `ErrorView` does.
async fn run_request<T>(
    session: Signal<Option<Session>>,
    mut busy: Signal<bool>,
    mut error: Signal<Option<String>>,
    request: impl Future<Output = Result<T, ApiError>>,
) -> Option<T> {
    busy.set(true);
    error.set(None);
    let result = request.await;
    busy.set(false);
    match result {
        Ok(value) => Some(value),
        Err(failure) => {
            error.set(Some(failure.to_string()));
            if failure == ApiError::Unauthorized {
                end_session(session);
            }
            None
        }
    }
}

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

/// A stream or topic name typed into a form, trimmed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResourceName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum InvalidName {
    #[error("Name must not be blank")]
    Blank,
    #[error("Name must not exceed {MAX_NAME_BYTES} bytes")]
    TooLong,
}

impl FromStr for ResourceName {
    type Err = InvalidName;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.trim() {
            "" => Err(InvalidName::Blank),
            name if name.len() > MAX_NAME_BYTES => Err(InvalidName::TooLong),
            name => Ok(Self(name.to_owned())),
        }
    }
}

/// The number of partitions to add or delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionsCount(u32);

/// The number of partitions a new topic starts with. Zero is allowed, and
/// partitions can be added later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitialPartitionsCount(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Partitions count must be a whole number from {min} to {MAX_PARTITIONS_COUNT}")]
struct InvalidPartitionsCount {
    min: u32,
}

impl FromStr for PartitionsCount {
    type Err = InvalidPartitionsCount;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        count_in_range(text, 1).map(Self)
    }
}

impl FromStr for InitialPartitionsCount {
    type Err = InvalidPartitionsCount;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        count_in_range(text, 0).map(Self)
    }
}

fn count_in_range(text: &str, min: u32) -> Result<u32, InvalidPartitionsCount> {
    text.trim()
        .parse::<u32>()
        .ok()
        .filter(|count| (min..=MAX_PARTITIONS_COUNT).contains(count))
        .ok_or(InvalidPartitionsCount { min })
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
    fn given_count_text_should_accept_only_counts_the_server_allows() {
        assert_eq!(" 3 ".parse(), Ok(PartitionsCount(3)));
        assert_eq!("1000".parse(), Ok(PartitionsCount(1000)));
        for text in ["0", "1001", "-1", "two", ""] {
            assert_eq!(
                text.parse::<PartitionsCount>(),
                Err(InvalidPartitionsCount { min: 1 })
            );
        }
    }

    #[test]
    fn given_new_topic_count_should_allow_zero_partitions() {
        assert_eq!("0".parse(), Ok(InitialPartitionsCount(0)));
        assert_eq!(
            "1001".parse::<InitialPartitionsCount>(),
            Err(InvalidPartitionsCount { min: 0 })
        );
    }

    #[test]
    fn given_name_text_should_trim_and_enforce_the_server_length() {
        assert_eq!(" orders ".parse(), Ok(ResourceName("orders".to_owned())));
        assert_eq!("   ".parse::<ResourceName>(), Err(InvalidName::Blank));
        assert_eq!(
            "ł".repeat(128).parse::<ResourceName>(),
            Err(InvalidName::TooLong)
        );
        assert!("a".repeat(MAX_NAME_BYTES).parse::<ResourceName>().is_ok());
    }
}
