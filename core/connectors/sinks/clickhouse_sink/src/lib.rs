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

use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{Error, sink_connector};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;

mod binary;
mod body;
mod client;
mod schema;
mod sink;

sink_connector!(ClickHouseSink);

const DEFAULT_DATABASE: &str = "default";
const DEFAULT_USERNAME: &str = "default";
const DEFAULT_PASSWORD: &str = "";
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseSinkConfig {
    pub url: String,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub table: String,
    /// "json_each_row" (default), "row_binary", or "string"
    pub insert_format: Option<String>,
    /// "json_each_row" (default), "csv", or "tsv" — only used when insert_format = "string"
    pub string_format: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InsertFormat {
    #[default]
    JsonEachRow,
    RowBinary,
    StringPassthrough,
}

impl InsertFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("row_binary") => InsertFormat::RowBinary,
            Some("string") => InsertFormat::StringPassthrough,
            _ => InsertFormat::JsonEachRow,
        }
    }

    pub fn clickhouse_format_name(&self, string_fmt: StringFormat) -> &'static str {
        match self {
            InsertFormat::JsonEachRow => "JSONEachRow",
            InsertFormat::RowBinary => "RowBinaryWithDefaults",
            InsertFormat::StringPassthrough => string_fmt.clickhouse_format_name(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StringFormat {
    #[default]
    JsonEachRow,
    Csv,
    Tsv,
}

impl StringFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("csv") => StringFormat::Csv,
            Some("tsv") => StringFormat::Tsv,
            _ => StringFormat::JsonEachRow,
        }
    }

    pub fn clickhouse_format_name(&self) -> &'static str {
        match self {
            StringFormat::JsonEachRow => "JSONEachRow",
            StringFormat::Csv => "CSV",
            StringFormat::Tsv => "TSV",
        }
    }

    /// CSV and TSV rows must end with a newline; JSON rows are already delimited.
    pub fn requires_newline(&self) -> bool {
        matches!(self, StringFormat::Csv | StringFormat::Tsv)
    }
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    errors_count: u64,
}

#[derive(Debug)]
pub struct ClickHouseSink {
    id: u32,
    config: ClickHouseSinkConfig,
    client: Option<client::ClickHouseClient>,
    table_schema: Option<Vec<schema::Column>>,
    insert_format: InsertFormat,
    string_format: StringFormat,
    retry_delay: Duration,
    state: Mutex<State>,
}

impl ClickHouseSink {
    pub fn new(id: u32, config: ClickHouseSinkConfig) -> Self {
        let insert_format = InsertFormat::from_config(config.insert_format.as_deref());
        let string_format = StringFormat::from_config(config.string_format.as_deref());
        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = HumanDuration::from_str(delay_str)
            .map(|d| d.into())
            .unwrap_or_else(|_| Duration::from_secs(1));

        ClickHouseSink {
            id,
            config,
            client: None,
            table_schema: None,
            insert_format,
            string_format,
            retry_delay,
            state: Mutex::new(State {
                messages_processed: 0,
                errors_count: 0,
            }),
        }
    }

    pub fn database(&self) -> &str {
        self.config.database.as_deref().unwrap_or(DEFAULT_DATABASE)
    }

    pub fn username(&self) -> &str {
        self.config.username.as_deref().unwrap_or(DEFAULT_USERNAME)
    }

    pub fn password(&self) -> &str {
        self.config.password.as_deref().unwrap_or(DEFAULT_PASSWORD)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(
            self.config
                .timeout_seconds
                .unwrap_or(DEFAULT_TIMEOUT_SECONDS),
        )
    }

    pub fn max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    pub fn verbose(&self) -> bool {
        self.config.verbose_logging.unwrap_or(false)
    }

    fn get_client(&self) -> Result<&client::ClickHouseClient, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::InitError("ClickHouse client not initialised".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            url: "http://localhost:8123".into(),
            database: None,
            username: None,
            password: None,
            table: "events".into(),
            insert_format: None,
            string_format: None,
            timeout_seconds: None,
            max_retries: None,
            retry_delay: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_no_insert_format_should_default_to_json_each_row() {
        let sink = ClickHouseSink::new(1, test_config());
        assert_eq!(sink.insert_format, InsertFormat::JsonEachRow);
    }

    #[test]
    fn given_row_binary_insert_format_should_parse_correctly() {
        let mut config = test_config();
        config.insert_format = Some("row_binary".into());
        let sink = ClickHouseSink::new(1, config);
        assert_eq!(sink.insert_format, InsertFormat::RowBinary);
    }

    #[test]
    fn given_string_insert_format_should_parse_correctly() {
        let mut config = test_config();
        config.insert_format = Some("string".into());
        let sink = ClickHouseSink::new(1, config);
        assert_eq!(sink.insert_format, InsertFormat::StringPassthrough);
    }

    #[test]
    fn given_csv_string_format_should_parse_correctly() {
        let mut config = test_config();
        config.insert_format = Some("string".into());
        config.string_format = Some("csv".into());
        let sink = ClickHouseSink::new(1, config);
        assert_eq!(sink.string_format, StringFormat::Csv);
    }

    #[test]
    fn given_no_retry_delay_should_default_to_one_second() {
        let sink = ClickHouseSink::new(1, test_config());
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config();
        config.retry_delay = Some("500ms".into());
        let sink = ClickHouseSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }

    #[test]
    fn given_no_database_should_use_default() {
        let sink = ClickHouseSink::new(1, test_config());
        assert_eq!(sink.database(), DEFAULT_DATABASE);
    }

    #[test]
    fn given_json_format_should_return_correct_clickhouse_name() {
        assert_eq!(
            InsertFormat::JsonEachRow.clickhouse_format_name(StringFormat::JsonEachRow),
            "JSONEachRow"
        );
    }

    #[test]
    fn given_row_binary_format_should_return_row_binary_with_defaults() {
        assert_eq!(
            InsertFormat::RowBinary.clickhouse_format_name(StringFormat::JsonEachRow),
            "RowBinaryWithDefaults"
        );
    }

    #[test]
    fn given_string_format_csv_should_return_csv() {
        assert_eq!(
            InsertFormat::StringPassthrough.clickhouse_format_name(StringFormat::Csv),
            "CSV"
        );
    }
}
