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

use crate::error::IggyError;
use crate::types::message::polling_kind::PollingKind;
use crate::utils::timestamp::IggyTimestamp;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, json::JsonString, serde_as};
use std::fmt::Display;

/// Default value for the polling strategy.
const DEFAULT_POLLING_STRATEGY_VALUE: u64 = 0;

/// `PollingStrategy` specifies from where to start polling messages.
/// It has the following kinds:
/// - `Offset` - start polling from the specified offset.
/// - `Timestamp` - start polling from the specified timestamp.
/// - `First` - start polling from the first message in the partition.
/// - `Last` - start polling from the last message in the partition.
/// - `Next` - start polling from the next message after the last polled message based on the stored consumer offset.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub struct PollingStrategy {
    /// Kind of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "PollingStrategy::default_kind")]
    pub kind: PollingKind,
    /// Value of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "PollingStrategy::default_value")]
    pub value: u64,
    /// Partition incarnation and owner captured with a continuation offset. A stale
    /// context rejects the poll instead of interpreting that offset in another incarnation.
    #[serde_as(as = "Option<JsonString>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<PartitionContext>,
    /// The partition of a continuation, because a context names no partition. Not on the
    /// wire: every request names its partition in a field of its own.
    #[serde(skip)]
    partition_id: Option<u32>,
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::Offset,
            value: 0,
        }
    }
}

impl Display for PollingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.value)
    }
}

impl PollingStrategy {
    /// Poll messages from the specified offset.
    pub fn offset(value: u64) -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::Offset,
            value,
        }
    }

    /// Poll messages from the specified timestamp.
    pub fn timestamp(value: IggyTimestamp) -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::Timestamp,
            value: value.into(),
        }
    }

    /// Poll messages from the first message in the partition.
    pub fn first() -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::First,
            value: 0,
        }
    }

    /// Poll messages from the last message in the partition.
    pub fn last() -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::Last,
            value: 0,
        }
    }

    /// Poll messages after the consumer offset stored on the server.
    ///
    /// Advance that offset with automatic commits or explicit offset stores.
    /// A missing response to a poll with automatic commits can leave it ahead
    /// of messages the caller received. See the
    /// [poll recovery contract](crate::MessageClient::poll_messages) before retrying.
    pub fn next() -> Self {
        Self {
            context: None,
            partition_id: None,
            kind: PollingKind::Next,
            value: 0,
        }
    }

    /// Change the value of the polling strategy, affects only `Offset` and `Timestamp` kinds.
    pub fn set_value(&mut self, value: u64) {
        if self.kind == PollingKind::Offset || self.kind == PollingKind::Timestamp {
            self.value = value;
        }
    }

    /// Returns default value of the polling strategy.
    pub fn default_value() -> u64 {
        DEFAULT_POLLING_STRATEGY_VALUE
    }

    /// Returns default kind of the polling strategy.
    pub fn default_kind() -> PollingKind {
        PollingKind::default()
    }

    /// A continuation of `partition_id` from `kind` and `value`, under the `context` that
    /// served the batch before it.
    #[doc(hidden)]
    pub fn continuation(
        partition_id: u32,
        kind: PollingKind,
        value: u64,
        context: PartitionContext,
    ) -> Self {
        Self {
            kind,
            value,
            context: Some(context),
            partition_id: Some(partition_id),
        }
    }

    #[doc(hidden)]
    pub fn context(&self) -> Option<PartitionContext> {
        self.context
    }

    /// The partition of a continuation. `None` for a strategy of any partition.
    #[doc(hidden)]
    pub fn partition_id(&self) -> Option<u32> {
        self.partition_id
    }

    /// Refuses a continuation of another partition, which would read `partition_id` at an
    /// offset of its own partition. A strategy without a partition fits every partition.
    ///
    /// # Errors
    /// [`IggyError::InvalidCommand`] for a continuation of another partition.
    #[doc(hidden)]
    pub fn validate_partition(&self, partition_id: u32) -> Result<(), IggyError> {
        match self.partition_id {
            Some(bound) if bound != partition_id => Err(IggyError::InvalidCommand),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{Error as ValueError, MapDeserializer};

    const PARTITION: u32 = 3;
    const OFFSET: u64 = 5;
    const CONTEXT: PartitionContext = PartitionContext {
        incarnation: 1,
        owner_generation: 2,
        metadata_op: 3,
    };
    /// The JSON and HTTP query forms of `Offset(OFFSET)` under `CONTEXT`, as of `5c252f08d`.
    const JSON: &str = r#"{"kind":"offset","value":"5","context":"{\"incarnation\":1,\"owner_generation\":2,\"metadata_op\":3}"}"#;
    const QUERY: &str = "kind=offset&value=5&context=%7B%22incarnation%22%3A1%2C%22owner_generation%22%3A2%2C%22metadata_op%22%3A3%7D";

    /// The HTTP poll query flattens its strategy, as the server reads it.
    #[derive(Deserialize)]
    struct PollQuery {
        #[serde(flatten)]
        strategy: PollingStrategy,
    }

    fn from_query(query: &str) -> PollingStrategy {
        let pairs = url::form_urlencoded::parse(query.as_bytes());
        PollQuery::deserialize(MapDeserializer::<_, ValueError>::new(pairs))
            .unwrap()
            .strategy
    }

    #[test]
    fn given_a_continuation_when_validating_a_partition_should_refuse_another_partition() {
        let bound = PollingStrategy::continuation(PARTITION, PollingKind::Offset, OFFSET, CONTEXT);

        assert_eq!(bound.validate_partition(PARTITION), Ok(()));
        assert_eq!(
            bound.validate_partition(PARTITION + 1),
            Err(IggyError::InvalidCommand)
        );
        for unbound in [PollingStrategy::offset(OFFSET), PollingStrategy::next()] {
            assert_eq!(unbound.partition_id(), None);
            assert_eq!(unbound.validate_partition(PARTITION + 1), Ok(()));
        }
    }

    #[test]
    fn given_a_strategy_when_serialized_should_keep_the_json_form_without_its_partition() {
        assert_eq!(
            serde_json::to_string(&PollingStrategy::offset(OFFSET)).unwrap(),
            r#"{"kind":"offset","value":"5"}"#
        );
        let bound = PollingStrategy::continuation(PARTITION, PollingKind::Offset, OFFSET, CONTEXT);
        assert_eq!(serde_json::to_string(&bound).unwrap(), JSON);

        let deserialized: PollingStrategy = serde_json::from_str(JSON).unwrap();

        assert_eq!(
            (
                deserialized.kind,
                deserialized.value,
                deserialized.context(),
                deserialized.partition_id()
            ),
            (PollingKind::Offset, OFFSET, Some(CONTEXT), None)
        );
    }

    #[test]
    fn given_a_poll_query_when_read_should_take_the_strategy_and_its_context() {
        let strategy = from_query(QUERY);
        assert_eq!(
            (
                strategy.kind,
                strategy.value,
                strategy.context(),
                strategy.partition_id()
            ),
            (PollingKind::Offset, OFFSET, Some(CONTEXT), None)
        );
        assert_eq!(
            from_query("kind=next"),
            PollingStrategy::next(),
            "a query without a context reads a strategy without one"
        );
    }
}
