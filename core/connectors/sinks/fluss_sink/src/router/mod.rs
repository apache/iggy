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

mod multi_table;
mod static_table;

use fluss::metadata::TablePath;
use iggy_connector_sdk::Error as ConnectorError;
use thiserror::Error;

use crate::{ResolvedFlussSinkConfig, config::RouterType, schema_catalog, writer};

pub(crate) use multi_table::MultiTableRouter;
pub(crate) use static_table::StaticTableRouter;

#[derive(Debug)]
pub(crate) enum Router {
    StaticTable(StaticTableRouter),
    MultiTable(MultiTableRouter),
}

impl Router {
    pub(crate) fn from_config(config: &ResolvedFlussSinkConfig) -> Router {
        match config.router_type {
            RouterType::Static => Router::StaticTable(StaticTableRouter::new(config)),
            RouterType::Multi => Router::MultiTable(MultiTableRouter::new(config)),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("Failed to init router because: [{reason}]")]
    Init { reason: String },
    #[error(transparent)]
    WriterError(writer::WriterError),
    #[error(transparent)]
    SchemaCatalog(schema_catalog::Error),
    #[error("Failed to extract string value for [{key}] for message: [{id}] because: [{reason}]")]
    ExtractStringField {
        id: u128,
        key: String,
        reason: String,
    },
    #[error("Failed to create table path: [{reason}]")]
    FailedToExtractTablePath { reason: String },
    #[error("Failed to create record batch: [{reason}]")]
    FailedToCreateRecordBatch { reason: String },
    #[error("Failed to resolve schema for table [{table_path}]: [{reason}]")]
    FailedToResolveSchema {
        table_path: TablePath,
        reason: String,
    },
}

impl From<writer::WriterError> for Error {
    fn from(error: writer::WriterError) -> Self {
        Self::WriterError(error)
    }
}

impl From<schema_catalog::Error> for Error {
    fn from(value: schema_catalog::Error) -> Self {
        match value {
            schema_catalog::Error::InferSchemaFailed { table_path, reason } => {
                Self::FailedToResolveSchema { table_path, reason }
            }
            _ => Error::SchemaCatalog(value),
        }
    }
}

impl From<Error> for ConnectorError {
    fn from(error: Error) -> Self {
        let message = error.to_string();
        match error {
            Error::Init { .. } => Self::InitError(message),
            Error::WriterError(source)
            | Error::SchemaCatalog(schema_catalog::Error::Writer(source)) => source.into(),
            Error::SchemaCatalog(_)
            | Error::FailedToCreateRecordBatch { .. }
            | Error::FailedToResolveSchema { .. } => Self::SchemaMismatch(message),
            Error::ExtractStringField { .. } | Error::FailedToExtractTablePath { .. } => {
                Self::InvalidRecordValue(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use fluss::{
        error::{Error as FlussError, RpcError},
        metadata::TablePath,
    };
    use iggy_connector_sdk::Error as ConnectorError;

    use super::{Error, Router};
    use crate::{ResolvedFlussSinkConfig, config::RouterType, schema_catalog, writer::WriterError};

    #[test]
    fn given_router_type_when_selecting_should_create_matching_router() {
        let static_config = ResolvedFlussSinkConfig::default();
        let multi_config = ResolvedFlussSinkConfig {
            router_type: RouterType::Multi,
            ..ResolvedFlussSinkConfig::default()
        };

        assert!(matches!(
            Router::from_config(&static_config),
            Router::StaticTable(_)
        ));
        assert!(matches!(
            Router::from_config(&multi_config),
            Router::MultiTable(_)
        ));
    }

    #[test]
    fn given_writer_error_when_routed_should_preserve_connector_variant() {
        for expected in [
            ConnectorError::SchemaMismatch("incompatible columns".into()),
            ConnectorError::InvalidConfigValue("invalid acks".into()),
            ConnectorError::InvalidRecordValue("null key".into()),
            ConnectorError::Connection("disconnected".into()),
            ConnectorError::CannotStoreData("backpressure".into()),
            ConnectorError::InitError("not initialized".into()),
        ] {
            let direct = Error::WriterError(WriterError::Connector(expected.clone()));
            let catalog = Error::SchemaCatalog(schema_catalog::Error::Writer(
                WriterError::Connector(expected.clone()),
            ));
            assert_eq!(ConnectorError::from(direct), expected);
            assert_eq!(ConnectorError::from(catalog), expected);
        }
    }

    #[test]
    fn given_invalid_table_when_routed_should_return_schema_mismatch_with_context() {
        let writer_error = WriterError::CreateTable {
            table_path: TablePath::new("fluss", "orders"),
            source: Box::new(FlussError::invalid_table("invalid primary key")),
        };
        let expected = ConnectorError::SchemaMismatch(writer_error.to_string());
        let actual = ConnectorError::from(Error::SchemaCatalog(schema_catalog::Error::Writer(
            writer_error,
        )));
        assert_eq!(actual, expected);
    }

    #[test]
    fn given_rpc_failure_when_routed_should_return_connection_error() {
        let error = Error::WriterError(WriterError::GetTableFailed {
            table_path: TablePath::new("fluss", "orders"),
            source: Box::new(FlussError::from(RpcError::ConnectionError(
                "disconnected".into(),
            ))),
        });
        assert!(matches!(
            ConnectorError::from(error),
            ConnectorError::Connection(_)
        ));
    }

    #[test]
    fn given_schema_conversion_failure_when_routed_should_return_schema_mismatch() {
        for error in [
            Error::FailedToCreateRecordBatch {
                reason: "expected integer".into(),
            },
            Error::SchemaCatalog(schema_catalog::Error::FlussToArrowSchemaFailed {
                reason: "unsupported column type".into(),
            }),
            Error::FailedToResolveSchema {
                table_path: TablePath::new("fluss", "orders"),
                reason: "missing schema".into(),
            },
        ] {
            assert!(matches!(
                ConnectorError::from(error),
                ConnectorError::SchemaMismatch(_)
            ));
        }
    }

    #[test]
    fn given_invalid_route_when_routed_should_return_invalid_record_value() {
        let error = Error::FailedToExtractTablePath {
            reason: "missing database".into(),
        };
        assert!(matches!(
            ConnectorError::from(error),
            ConnectorError::InvalidRecordValue(_)
        ));
    }
}
