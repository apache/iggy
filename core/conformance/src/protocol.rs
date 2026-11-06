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

//! The JSON-lines protocol between the orchestrator and an SDK agent.
//!
//! The agent writes a `hello` line first. Then the orchestrator writes one
//! request per line and the agent answers each with a line that carries the
//! same `id`. The agent writes its logs to stderr only.

use crate::format::{Credentials, Transport};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The protocol version, which also versions the canonical result forms.
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloLine {
    pub hello: Hello,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub sdk: String,
    pub sdk_version: String,
    pub protocol: u32,
    pub transports: Vec<Transport>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLine {
    pub id: u64,
    #[serde(flatten)]
    pub request: Request,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Creates an entity. Only clients are created this way.
    Create {
        kind: EntityKind,
        entity: String,
        args: Value,
    },
    /// Calls a method of an entity and answers with its result.
    Call {
        entity: String,
        method: String,
        args: Value,
        /// Keeps the entity that the call creates under this id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        save_as: Option<String>,
    },
    /// Starts a call as a task and answers at once.
    Start {
        task: String,
        entity: String,
        method: String,
        args: Value,
    },
    /// Answers with the result of a task once it ends.
    Await { task: String },
    /// Cancels a task, as an application drops a call it no longer wants.
    Cancel { task: String },
    /// Closes an entity.
    Close { entity: String },
    /// Closes every entity and cancels every task.
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Client,
    Reader,
    Consumer,
}

/// The arguments of `create` for a client.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateClientArgs {
    pub transport: Transport,
    pub address: String,
    /// Signs in after the connect. `None` leaves the client signed out.
    pub login: Option<Credentials>,
}

/// One answer line: exactly one of the four answer fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseLine {
    pub id: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AgentError>,
    /// The SDK has no such method, argument or entity kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported: Option<String>,
    /// The agent itself failed, for example on an argument it cannot read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// An error that the SDK returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentError {
    pub code: u32,
    /// The retained operation of the errors 14006 to 14010.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<u64>,
    /// The partition of `partition_lane_blocked`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// An answer, decoded from a [`ResponseLine`].
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Result(Value),
    Error(AgentError),
    Unsupported(String),
    Failure(String),
}

impl Reply {
    #[must_use]
    pub fn into_line(self, id: u64) -> ResponseLine {
        let mut line = ResponseLine {
            id,
            result: None,
            error: None,
            unsupported: None,
            failure: None,
        };
        match self {
            Self::Result(value) => line.result = Some(value),
            Self::Error(error) => line.error = Some(error),
            Self::Unsupported(reason) => line.unsupported = Some(reason),
            Self::Failure(reason) => line.failure = Some(reason),
        }
        line
    }
}

impl TryFrom<ResponseLine> for Reply {
    type Error = String;

    fn try_from(line: ResponseLine) -> Result<Self, String> {
        match (line.result, line.error, line.unsupported, line.failure) {
            (Some(value), None, None, None) => Ok(Self::Result(value)),
            (None, Some(error), None, None) => Ok(Self::Error(error)),
            (None, None, Some(reason), None) => Ok(Self::Unsupported(reason)),
            (None, None, None, Some(reason)) => Ok(Self::Failure(reason)),
            _ => Err(format!(
                "answer {} must carry exactly one of result, error, unsupported and failure",
                line.id
            )),
        }
    }
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn given_a_null_result_when_encoding_should_keep_the_result_key() {
        let line = Reply::Result(Value::Null).into_line(3);
        let text = serde_json::to_string(&line).unwrap();
        assert_eq!(text, r#"{"id":3,"result":null}"#);
        let decoded: ResponseLine = serde_json::from_str(&text).unwrap();
        assert_eq!(
            Reply::try_from(decoded).unwrap(),
            Reply::Result(Value::Null)
        );
    }

    #[test]
    fn given_two_answer_fields_when_decoding_should_fail() {
        let line: ResponseLine =
            serde_json::from_value(json!({"id": 1, "result": 1, "unsupported": "x"})).unwrap();
        assert!(Reply::try_from(line).is_err());
    }

    #[test]
    fn given_a_call_when_encoding_should_flatten_the_operation() {
        let line = RequestLine {
            id: 2,
            request: Request::Call {
                entity: "client0".to_owned(),
                method: "get_stream".to_owned(),
                args: json!({"stream_id": 1}),
                save_as: None,
            },
        };
        assert_eq!(
            serde_json::to_value(&line).unwrap(),
            json!({"id": 2, "op": "call", "entity": "client0", "method": "get_stream", "args": {"stream_id": 1}})
        );
    }
}
