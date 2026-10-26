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

use crate::lsn::{InvalidLsnLength, Lsn, Position};
use std::fmt;

const LSN_OUT_OF_RANGE: u32 = 313;
const INVALID_OBJECT_NAME: u32 = 208;

#[derive(Debug)]
pub(crate) enum MssqlError {
    Connect(String),
    Timeout,
    Server {
        number: u32,
        message: String,
    },
    Protocol(String),
    /// An event could not be written as JSON. The connection is still usable.
    Encoding(String),
    /// The driver could not decode a value, for example an unpaired UTF-16 surrogate in an nvarchar.
    Decode(String),
    /// A value of a change row after `after` could not be decoded.
    Undecodable {
        table: String,
        after: Position,
        message: String,
    },
    PayloadTooLarge {
        table: String,
        position: Position,
        bytes: usize,
        limit: u32,
    },
    Conversion {
        table: String,
        column: String,
        position: Position,
        reason: ConversionError,
    },
    CdcNotEnabled {
        database: String,
    },
    TableNotFound {
        table: String,
    },
    TableNotCaptured {
        table: String,
    },
    DuplicateTable {
        table: String,
        configured: Vec<String>,
    },
    UnsupportedType {
        table: String,
        /// `(column, type)` pairs.
        columns: Vec<(String, String)>,
    },
    NoUsableInstance {
        table: String,
        instances: Vec<(String, Lsn)>,
        next_needed: Lsn,
    },
    InstanceMissing {
        table: String,
        capture_instance: String,
    },
    LsnGap {
        table: String,
        next_needed: Lsn,
        min_lsn: Lsn,
    },
    DatabaseChanged {
        reason: String,
    },
    MissingMetadata(&'static str),
}

impl MssqlError {
    /// The connection is unsafe to reuse after these, a decode error included. Server errors leave it usable.
    pub(crate) fn breaks_connection(&self) -> bool {
        matches!(
            self,
            Self::Connect(_)
                | Self::Protocol(_)
                | Self::Timeout
                | Self::Decode(_)
                | Self::Undecodable { .. }
        )
    }

    /// The change that blocks progress, for an error that repeats on every poll until the data changes.
    pub(crate) fn stuck_position(&self) -> Option<Position> {
        match self {
            Self::Conversion { position, .. } | Self::PayloadTooLarge { position, .. } => {
                Some(*position)
            }
            Self::Undecodable { after, .. } => Some(*after),
            _ => None,
        }
    }

    /// Cleanup raised the minimum LSN above the query's start. SQL Server reports it with a misleading message.
    pub(crate) fn is_lsn_out_of_range(&self) -> bool {
        matches!(self, Self::Server { number, .. } if *number == LSN_OUT_OF_RANGE)
    }

    /// The change function no longer exists, so the capture instance is gone.
    pub(crate) fn is_missing_object(&self) -> bool {
        matches!(self, Self::Server { number, .. } if *number == INVALID_OBJECT_NAME)
    }
}

/// Why one value could not become JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConversionError {
    UnexpectedVariant { expected: &'static str },
    InvalidNumeric,
    UnpairedUpdate { operation: u8 },
}

impl fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedVariant { expected } => {
                write!(formatter, "unexpected value type, expected {expected}")
            }
            Self::InvalidNumeric => write!(formatter, "invalid numeric value"),
            Self::UnpairedUpdate { operation } => {
                write!(
                    formatter,
                    "update row with operation {operation} has no partner"
                )
            }
        }
    }
}

impl fmt::Display for MssqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(message) => write!(formatter, "connection failed: {message}"),
            Self::Timeout => write!(formatter, "query timed out"),
            Self::Server { number, message } => {
                write!(formatter, "server error {number}: {message}")
            }
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::Encoding(message) => write!(formatter, "JSON encoding failed: {message}"),
            Self::Decode(message) => write!(formatter, "cannot decode a value: {message}"),
            Self::Undecodable {
                table,
                after,
                message,
            } => write!(
                formatter,
                "cannot decode a change of {table} after {after}: {message}"
            ),
            Self::PayloadTooLarge {
                table,
                position,
                bytes,
                limit,
            } => write!(
                formatter,
                "change of {table} at {position} is {bytes} bytes as JSON, above the Iggy limit of {limit}"
            ),
            Self::Conversion {
                table,
                column,
                position,
                reason,
            } => write!(
                formatter,
                "cannot convert {table}.{column} at {position}: {reason}"
            ),
            Self::CdcNotEnabled { database } => write!(
                formatter,
                "CDC is not enabled on database '{database}'. Check Database= in the connection string and run sys.sp_cdc_enable_db"
            ),
            Self::TableNotFound { table } => write!(formatter, "table {table} does not exist"),
            Self::TableNotCaptured { table } => write!(
                formatter,
                "table {table} has no CDC capture instance. Run sys.sp_cdc_enable_table"
            ),
            Self::DuplicateTable { table, configured } => write!(
                formatter,
                "configured tables {} all resolve to {table}",
                configured.join(", ")
            ),
            Self::UnsupportedType { table, columns } => {
                write!(formatter, "table {table} has columns of unsupported types:")?;
                for (column, sql_type) in columns {
                    write!(formatter, " {column} ({sql_type})")?;
                }
                Ok(())
            }
            Self::NoUsableInstance {
                table,
                instances,
                next_needed,
            } => {
                write!(
                    formatter,
                    "no capture instance of {table} holds every change after {next_needed}. Instances:"
                )?;
                if instances.is_empty() {
                    write!(formatter, " none")?;
                }
                for (name, min_lsn) in instances {
                    write!(formatter, " {name} (min {min_lsn})")?;
                }
                if !instances.is_empty() && instances.iter().all(|(_, min_lsn)| min_lsn.is_zero()) {
                    write!(
                        formatter,
                        ". A zero minimum means the instance is gone or the login cannot read its change data"
                    )?;
                }
                Ok(())
            }
            Self::InstanceMissing {
                table,
                capture_instance,
            } => write!(
                formatter,
                "capture instance {capture_instance} of {table} no longer exists, or the login can no longer read its change data"
            ),
            Self::LsnGap {
                table,
                next_needed,
                min_lsn,
            } => write!(
                formatter,
                "changes of {table} were cleaned up: next needed LSN {next_needed} is below the minimum {min_lsn}. Reset the connector state to resume"
            ),
            Self::MissingMetadata(what) => write!(
                formatter,
                "cannot read {what}. Check the connector's permissions"
            ),
            Self::DatabaseChanged { reason } => write!(
                formatter,
                "database no longer matches the checkpoint: {reason}. Reset the connector state to resume"
            ),
        }
    }
}

impl From<tiberius::error::Error> for MssqlError {
    fn from(error: tiberius::error::Error) -> Self {
        match error {
            tiberius::error::Error::Server(token) => Self::Server {
                number: token.code(),
                message: token.message().to_string(),
            },
            tiberius::error::Error::Io { message, .. } => Self::Connect(message),
            tiberius::error::Error::Tls(message) => Self::Connect(format!("TLS: {message}")),
            tiberius::error::Error::Routing { host, port } => Self::Connect(format!(
                "server redirected to {host}:{port}, which is not supported (Azure SQL redirect)"
            )),
            // A bad value is a data problem that holds progress, not a protocol fault.
            error @ (tiberius::error::Error::Utf8
            | tiberius::error::Error::Utf16
            | tiberius::error::Error::Encoding(_)
            | tiberius::error::Error::Conversion(_)) => Self::Decode(error.to_string()),
            // tiberius 0.13 reports an invalid nvarchar as Protocol("nvarchar: invalid UTF-16 sequence").
            tiberius::error::Error::Protocol(message)
                if message.contains("invalid UTF-16") || message.contains("invalid UTF-8") =>
            {
                Self::Decode(message.into_owned())
            }
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<InvalidLsnLength> for MssqlError {
    fn from(error: InvalidLsnLength) -> Self {
        Self::Protocol(format!("LSN: {error}"))
    }
}

impl From<MssqlError> for iggy_connector_sdk::Error {
    fn from(error: MssqlError) -> Self {
        match error {
            MssqlError::Conversion { .. }
            | MssqlError::Encoding(_)
            | MssqlError::Decode(_)
            | MssqlError::Undecodable { .. }
            | MssqlError::PayloadTooLarge { .. } => Self::Serialization(error.to_string()),
            _ => Self::Connection(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(number: u32) -> MssqlError {
        MssqlError::Server {
            number,
            message: "message".to_string(),
        }
    }

    #[test]
    fn given_conversion_error_should_map_to_serialization() {
        let error = MssqlError::Conversion {
            table: "dbo.users".to_string(),
            column: "balance".to_string(),
            position: Position::new(Lsn::ZERO, Lsn::ZERO),
            reason: ConversionError::InvalidNumeric,
        };

        assert!(matches!(
            iggy_connector_sdk::Error::from(error),
            iggy_connector_sdk::Error::Serialization(_)
        ));
    }

    #[test]
    fn given_server_error_should_map_to_connection() {
        assert!(matches!(
            iggy_connector_sdk::Error::from(server(1205)),
            iggy_connector_sdk::Error::Connection(_)
        ));
    }

    #[test]
    fn given_io_protocol_or_timeout_should_break_connection() {
        assert!(MssqlError::Connect("reset".to_string()).breaks_connection());
        assert!(MssqlError::Protocol("bad token".to_string()).breaks_connection());
        assert!(MssqlError::Timeout.breaks_connection());
        assert!(!server(1205).breaks_connection());
    }

    #[test]
    fn given_error_313_should_be_lsn_out_of_range() {
        assert!(server(313).is_lsn_out_of_range());
        assert!(!server(208).is_lsn_out_of_range());
    }

    #[test]
    fn given_only_zero_minimums_should_explain_them() {
        let error = MssqlError::NoUsableInstance {
            table: "dbo.users".to_string(),
            instances: vec![("dbo_users".to_string(), Lsn::ZERO)],
            next_needed: Lsn::ZERO,
        };

        assert!(
            error
                .to_string()
                .contains("the login cannot read its change data")
        );
    }

    #[test]
    fn given_utf16_decode_error_should_be_decode() {
        let error = MssqlError::from(tiberius::error::Error::Utf16);

        assert!(matches!(error, MssqlError::Decode(_)));
        assert!(error.breaks_connection());
    }

    #[test]
    fn given_invalid_nvarchar_protocol_error_should_be_decode() {
        let error = MssqlError::from(tiberius::error::Error::Protocol(
            "nvarchar: invalid UTF-16 sequence".into(),
        ));

        assert!(matches!(error, MssqlError::Decode(_)));
    }

    #[test]
    fn given_other_protocol_error_should_stay_protocol() {
        let error = MssqlError::from(tiberius::error::Error::Protocol("bad token".into()));

        assert!(matches!(error, MssqlError::Protocol(_)));
    }

    #[test]
    fn given_undecodable_change_should_report_stuck_position() {
        let after = Position::new(Lsn::ZERO, Lsn::MAX);
        let error = MssqlError::Undecodable {
            table: "dbo.orders".to_string(),
            after,
            message: "UTF-16 error".to_string(),
        };

        assert_eq!(error.stuck_position(), Some(after));
    }

    #[test]
    fn given_error_208_should_be_missing_object() {
        assert!(server(208).is_missing_object());
        assert!(!server(313).is_missing_object());
    }
}
