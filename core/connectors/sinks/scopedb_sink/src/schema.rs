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

use std::collections::BTreeMap;

use iggy_connector_sdk::Error;
use scopedb_client::{CatalogTable, DataType};

use crate::{Settings, WriteMode};

const METADATA: [(&str, DataType); 6] = [
    ("iggy_stream", DataType::String),
    ("iggy_topic", DataType::String),
    ("iggy_partition_id", DataType::UInt),
    ("iggy_offset", DataType::UInt),
    ("iggy_timestamp", DataType::Timestamp),
    ("iggy_id", DataType::String),
];

#[derive(Debug)]
pub(crate) struct Layout {
    pub writable: BTreeMap<String, DataType>,
    pub raw_type: Option<DataType>,
}

impl Layout {
    pub(crate) fn new(table: CatalogTable, settings: &Settings) -> Result<Self, Error> {
        let writable: BTreeMap<_, _> = table
            .columns
            .into_iter()
            .filter(|column| column.expression.is_none())
            .map(|column| (column.name, column.data_type))
            .collect();
        if settings.include_metadata {
            for (name, expected) in METADATA {
                let actual = writable.get(name).ok_or_else(|| {
                    Error::SchemaMismatch(format!("missing writable metadata column '{name}'"))
                })?;
                if *actual != expected && !(expected == DataType::UInt && *actual == DataType::Int)
                {
                    return Err(Error::SchemaMismatch(format!(
                        "metadata column '{name}' must be {expected:?}"
                    )));
                }
            }
        }
        if settings.include_headers
            && !matches!(
                writable.get("iggy_headers"),
                Some(DataType::Object | DataType::Any)
            )
        {
            return Err(Error::SchemaMismatch(
                "iggy_headers must be a writable object or any column".into(),
            ));
        }
        let raw_type = if settings.mode == WriteMode::Raw {
            if (settings.include_metadata
                && METADATA
                    .iter()
                    .any(|(name, _)| *name == settings.payload_column))
                || (settings.include_headers && settings.payload_column == "iggy_headers")
            {
                return Err(Error::SchemaMismatch(
                    "payload_column conflicts with enabled metadata".into(),
                ));
            }
            let data_type = writable
                .get(&settings.payload_column)
                .copied()
                .ok_or_else(|| {
                    Error::SchemaMismatch("payload_column is absent or computed".into())
                })?;
            if !matches!(
                data_type,
                DataType::Binary | DataType::String | DataType::Object | DataType::Any
            ) {
                return Err(Error::SchemaMismatch(
                    "raw payload_column must be binary, string, object or any".into(),
                ));
            }
            Some(data_type)
        } else {
            None
        };
        Ok(Self { writable, raw_type })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScopeDBSinkConfig;

    #[test]
    fn given_computed_payload_when_opening_raw_mode_should_reject_schema() {
        let config: ScopeDBSinkConfig = toml::from_str(
            "endpoint='http://localhost'\ntable='events'\nmode='raw'\ninclude_metadata=false",
        )
        .expect("valid config");
        let settings = Settings::from_config(1, &config);
        let table: CatalogTable = serde_json::from_value(serde_json::json!({
            "name":"events", "columns":[{"name":"payload","data_type":"string","expression":"'constant'"}]
        })).expect("valid catalog");
        assert!(matches!(
            Layout::new(table, &settings),
            Err(Error::SchemaMismatch(_))
        ));
    }
}
