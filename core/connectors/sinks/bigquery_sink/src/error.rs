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

//! Failure classification for BigQuery calls.
//!
//! Only transient failures are retried. Retrying a permanent one (bad
//! schema, missing permission) cannot succeed and only delays the error.

use std::fmt;

use gcloud_gax::grpc::{Code, Status};
use iggy_connector_sdk::Error;

/// An `AppendRows` call that did not append. Row-level errors are not part
/// of this type: they are an expected outcome the caller handles by
/// dropping the reported rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendError {
    /// The call failed, or the response carried an error status.
    Rpc { code: Code, message: String },
    /// Append was attempted before the write stream was initialized.
    MissingStream,
    /// The response stream closed without a response.
    ResponseStreamClosed,
}

/// A `tables.get` call that did not return a usable schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableError {
    Http { status: u16, body: String },
    Transport(String),
    Token(String),
}

impl AppendError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            AppendError::Rpc { code, .. } => is_retryable_code(*code),
            // The append may or may not have landed. Retrying risks a
            // duplicate, which the delivery guarantees allow.
            AppendError::ResponseStreamClosed => true,
            AppendError::MissingStream => false,
        }
    }
}

impl TableError {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            TableError::Http { status, .. } => *status == 429 || *status >= 500,
            TableError::Transport(_) | TableError::Token(_) => true,
        }
    }
}

/// gRPC codes retried for connection, timeout, capacity and token failures.
pub(crate) fn is_retryable_code(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::Unknown
            | Code::Unauthenticated
    )
}

impl From<Status> for AppendError {
    fn from(status: Status) -> Self {
        AppendError::Rpc {
            code: status.code(),
            message: status.message().to_owned(),
        }
    }
}

impl From<AppendError> for Error {
    fn from(error: AppendError) -> Self {
        if error.is_retryable() {
            Error::CannotStoreData(error.to_string())
        } else {
            Error::PermanentHttpError(error.to_string())
        }
    }
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendError::Rpc { code, message } => write!(f, "AppendRows {code:?}: {message}"),
            AppendError::MissingStream => write!(f, "AppendRows write stream is not initialized"),
            AppendError::ResponseStreamClosed => {
                write!(f, "AppendRows stream closed without a response")
            }
        }
    }
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableError::Http { status, body } => {
                write!(f, "tables.get returned HTTP {status}: {body}")
            }
            TableError::Transport(reason) => write!(f, "tables.get failed: {reason}"),
            TableError::Token(reason) => write!(f, "cannot obtain an access token: {reason}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc(code: Code) -> AppendError {
        AppendError::Rpc {
            code,
            message: "boom".into(),
        }
    }

    #[test]
    fn given_transient_codes_should_be_retryable() {
        for code in [
            Code::Unavailable,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::Internal,
            Code::Aborted,
            Code::ResourceExhausted,
            Code::Unknown,
            Code::Unauthenticated,
        ] {
            assert!(rpc(code).is_retryable(), "{code:?}");
        }
    }

    #[test]
    fn given_permanent_codes_should_not_be_retryable() {
        for code in [
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::NotFound,
            Code::FailedPrecondition,
        ] {
            assert!(!rpc(code).is_retryable(), "{code:?}");
        }
    }

    #[test]
    fn given_missing_response_should_be_retryable() {
        assert!(AppendError::ResponseStreamClosed.is_retryable());
        assert!(!AppendError::MissingStream.is_retryable());
    }

    #[test]
    fn given_status_should_keep_code_and_message() {
        let error = AppendError::from(Status::permission_denied("no access"));
        assert_eq!(
            error,
            AppendError::Rpc {
                code: Code::PermissionDenied,
                message: "no access".into()
            }
        );
    }

    #[test]
    fn given_append_error_should_map_to_sdk_error_by_retryability() {
        assert!(matches!(
            Error::from(rpc(Code::Unavailable)),
            Error::CannotStoreData(_)
        ));
        assert!(matches!(
            Error::from(rpc(Code::InvalidArgument)),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_table_failures_should_retry_only_transient_errors() {
        let http = |status| TableError::Http {
            status,
            body: String::new(),
        };
        assert!(http(429).is_retryable());
        assert!(http(503).is_retryable());
        assert!(!http(403).is_retryable());
        assert!(!http(404).is_retryable());
        assert!(TableError::Transport("reset".into()).is_retryable());
        assert!(TableError::Token("token service unavailable".into()).is_retryable());
    }
}
