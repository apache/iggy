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

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
	ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tiberius::{Client, Config, Query, Row};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, warn};
use uuid::Uuid;


source_connector!(MSSQLSource);

// Define a new type for the client with the generic type incorporated
pub type DBClient = Client<Compat<TcpStream>>;

pub(crate) type Lsn = [u8; 10];

/// Convert LSN parameter into a binary string
pub(crate) fn lsn_to_hex(lsn: &Lsn) -> String {

	let mut hex_string = String::with_capacity(lsn.len() * 2);
	// Append the leading 0s and push a hex upper case representation
	hex_string.push_str("0x");
	for byte in lsn.iter() {
		hex_string.push_str(&format!("{:02X}", byte));
	}
	hex_string
}


#[derive(Debug)]
pub struct TableCapture {
	capture_instance: String,
	columns: String,
	start_lsn: Lsn
}

#[derive(Debug)]
pub struct MSSQLSource {
	pub id: u32,
	client: Mutex<Option<DBClient>>,
	config: MSSQLSourceConfig,
	state: Mutex<State>,
	verbose: bool,
	poll_interval: Duration,
	capture_columns: HashMap<String, TableCapture>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MSSQLSourceConfig {
	// Example jdbc:sqlserver://localhost;encrypt=true;user=MyUserName;password=<password>;
	// See https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver15
	#[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
	pub connection_string: SecretString,
	pub database: String,
	pub schema: Option<String>,
	pub mode: String,
	pub tables: Vec<String>,
	pub poll_interval: Option<String>,
	pub batch_size: Option<u32>,
	pub tracking_column: Option<String>,
	pub initial_offset: Option<String>,
	// pub max_connections: Option<u32>,
	pub enable_cdc: Option<bool>,
	pub custom_query: Option<String>,
	pub snake_case_columns: Option<bool>,
	pub include_metadata: Option<bool>,
	pub capture_operations: Option<Vec<String>>,
	pub delete_after_read: Option<bool>,
	pub processed_column: Option<String>,
	pub primary_key_column: Option<String>,
	pub payload_column: Option<String>,
	pub payload_format: Option<String>,
	pub verbose_logging: Option<bool>,
	pub max_retries: Option<u32>,
	pub retry_delay: Option<String>,
	pub cdc_schema: Option<String>,
	pub cdc_role: Option<String>,
	pub capture_table_columns: HashMap<String, String>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
	#[default]
	Json,
	Bytea,
	Text,
	JsonDirect,
}

impl PayloadFormat {
	fn from_config(s: Option<&str>) -> Self {
		match s.map(|s| s.to_lowercase()).as_deref() {
			Some("bytea") | Some("raw") => PayloadFormat::Bytea,
			Some("text") => PayloadFormat::Text,
			Some("json_direct") | Some("jsonb") | Some("jsonb_direct") => PayloadFormat::JsonDirect,
			_ => PayloadFormat::Json,
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
	last_poll_lsn: Option<Lsn>,
	start_lsn: Option<Lsn>,
	end_lsn: Option<Lsn>,
	last_poll_time: DateTime<Utc>,
	tracking_offsets: HashMap<String, String>,
	processed_rows: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRecord {
	pub table_name: String,
	pub operation_type: String,
	pub timestamp: DateTime<Utc>,
	pub data: serde_json::Value,
	pub old_data: Option<serde_json::Value>,
}


const CONNECTOR_NAME: &str = "MSSQL source";

impl MSSQLSource {
	pub fn new(id: u32, config: MSSQLSourceConfig, state: Option<ConnectorState>) -> Self {
		let verbose = config.verbose_logging.unwrap_or(false);
		let restored_state = state
			.and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
			.inspect(|s| {
				info!(
					"Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
					 Tracking offsets: {:?}, processed rows: {}",
					s.tracking_offsets, s.processed_rows
				);
			});

		let interval_str = config.poll_interval.as_deref().unwrap_or("10s");
		let poll_interval = HumanDuration::from_str(interval_str)
			.map(|duration| duration.into())
			.unwrap_or_else(|_| Duration::from_secs(10));
		let capture_columns = HashMap::new();
		MSSQLSource {
			id,
			client: Mutex::new(None),
			config,
			state: Mutex::new(restored_state.unwrap_or(State {
				last_poll_lsn: None,
				start_lsn: None,
				end_lsn: None,
				last_poll_time: Utc::now(),
				tracking_offsets: HashMap::new(),
				processed_rows: 0
			})),
			verbose,
			poll_interval,
			capture_columns
		}
	}

	fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
		ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
	}
}

#[async_trait]
impl Source for MSSQLSource {
	async fn open(&mut self) -> Result<(), Error> {
		info!(
			"Opening MSSQL source connector with ID: {}. Mode: {}, Tables: {:?}",
			self.id, self.config.mode, self.config.tables
		);

		self.connect().await?;

		match self.config.mode.as_str() {
			"cdc" => {
				self.setup_cdc().await?;
				info!(
					"MSSQL CDC mode enabled for connector ID: {}",
					self.id
				);
			}
			"polling" => {
				info!(
					"MSSQL polling mode not enabled for connector ID: {}",
					self.id
				);
				return Err(Error::InitError(format!(
					"Polling mode is not available. Use CDC."
				)));
			}
			_ => {
				return Err(Error::InitError(format!(
					"Invalid mode '{}'. Supported modes: 'polling', 'cdc'",
					self.config.mode
				)));
			}
		}

		info!(
			"MSSQL source connector with ID: {} opened successfully",
			self.id
		);
		Ok(())
	}

	async fn poll(&self) -> Result<ProducedMessages, Error> {
		let poll_interval = self.poll_interval;
		tokio::time::sleep(poll_interval).await;

		let messages = match self.config.mode.as_str() {
			// "polling" => self.poll_tables().await?,
			"cdc" => self.poll_cdc().await?,
			_ => {
				error!("Invalid mode: {}", self.config.mode);
				return Err(Error::InvalidConfig);
			}
		};

		let state = self.state.lock().await;
		if self.verbose {
			info!(
				"MSSQL source connector ID: {} produced {} messages. Total processed: {}",
				self.id,
				messages.len(),
				state.processed_rows
			);
		} else {
			debug!(
				"MSSQL source connector ID: {} produced {} messages. Total processed: {}",
				self.id,
				messages.len(),
				state.processed_rows
			);
		}

		let schema = match self.payload_format() {
			PayloadFormat::Bytea => Schema::Raw,
			PayloadFormat::Text => Schema::Text,
			PayloadFormat::JsonDirect | PayloadFormat::Json => Schema::Json,
		};

		let persisted_state = self.serialize_state(&state);

		Ok(ProducedMessages {
			schema,
			messages,
			state: persisted_state,
		})
	}

	async fn close(&mut self) -> Result<(), Error> {
		let mut lock = self.client.lock().await;

		let client = lock.take();

		let _ = match client {
			Some(x) => x.close().await,
			None => Ok(())
		};
		info!(
			"MSSQL connection closed for connector ID: {}",
			self.id
		);

		let state = self.state.lock().await;
		info!(
			"MSSQL source connector ID: {} closed. Total rows processed: {}",
			self.id, state.processed_rows
		);
		Ok(())
	}
}

impl MSSQLSource {
	async fn connect(&mut self) -> Result<(), Error> {

		let redacted = redact_connection_string(self.config.connection_string.expose_secret());

		info!("Connecting to MSSQL with 1 connection: {redacted}");
		let config = Config::from_jdbc_string(self.config.connection_string.expose_secret())
			.map_err(|e| Error::InitError(format!("Invalid connection string configuration: {e}")))?;

		let tcp = TcpStream::connect(config.get_addr())
			.await
			.map_err(|e| Error::InitError(
				format!("Cannot connect to TCP stream: {e}")))?;

		if tcp.set_nodelay(true).is_ok() { } else {
			warn!("Cannot set no delay on the TCP socket!");
		}

		// To be able to use Tokio's tcp, we're using the `compat_write` from
		// the `TokioAsyncWriteCompatExt` to get a stream compatible with the
		// traits from the `futures` crate.
		let mut client = Client::connect(
			config,
			tcp.compat_write()
		).await
		.map_err(|e| Error::InitError(format!("Failed to connect to MSSQL server: {e}")))?;

		client.query("SELECT 1", &[])
			.await
			.map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

		{
			let mut lock = self.client.lock().await;
			*lock = Some(client);
		}

		info!("Connected to MSSQL database");
		Ok(())
	}

	async fn setup_cdc(&mut self) -> Result<(), Error> {

		let database = self.config.database.clone();
		let cdc_tables = if self.config.tables.is_empty() {
			let res = self.query(Query::new("SELECT name FROM sys.tables WHERE is_tracked_by_cdc = 1;"))
				.await?;
			res
				.into_iter()
				.map(|r| r.get::<&str, _>("name").unwrap().to_string())
				.collect()
		} else {
		   self.config.tables.clone()
		}.join("','");

		let query = Query::new(
			format!(
				"SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 0 AND name IN ('{cdc_tables}');"));
		let non_capture_tables = self.query(query).await?;

		if !non_capture_tables.is_empty() {
			warn!("Not all desired tables are monitored by CDC!");
			if !self.config.enable_cdc.unwrap_or(false) {
				error!("Update config to enable_cdc=true in order to monitor the indicated tables!");
				return Ok(());
			}
			warn!("Adding tables to the CDC monitor");
			let schema = self.config.cdc_schema.clone().unwrap_or("dbo".to_string());
			for row in non_capture_tables {
				let table = row.get("name").unwrap_or("");
				let cdc_role = match &self.config.cdc_role {
					Some(x) => format!("N'{x}'"),
					None => "NULL".to_string()
				};
				let query = format!(r#"
					EXEC sys.sp_cdc_enable_table
					@source_schema = N'{schema}',
					@source_name   = N'{table}',
					@role_name	 = {cdc_role}"#);

				self.query(Query::new(query)).await?;
			}
		}

		let table_columns = self.query(Query::new(
			format!(r#"
				SELECT
					OBJECT_NAME(ct.object_id, DB_ID('{database}')) as capture_table,
					c.object_id,
					ct.capture_instance as source_table,
					ct.start_lsn,
					c.columns
				FROM [{database}].[cdc].[change_tables] as ct
				JOIN (SELECT
					object_id,
					STRING_AGG(column_name, ',') as columns
					FROM [{database}].[cdc].[captured_columns]
					GROUP BY object_id) as c
				ON c.object_id = ct.object_id
				JOIN sys.tables as all_tables
				ON all_tables.object_id = ct.object_id
				WHERE all_tables.name IN ('{cdc_tables}');"#
			)
		)).await?;

		let schema_prefix = format!("{}_", self.config.schema.clone().unwrap_or("dbo".to_string()));
		for row in table_columns {
			let capture_table: &str = row
				.try_get::<&str, _>("capture_table")
				.map_err(|e| Error::InvalidRecord)?
				.ok_or(Error::InvalidRecord)?;
			let capture_table = capture_table.replacen(&schema_prefix, "", 1);

			let	source_table: &str = row.get::<&str, _>("source_table").unwrap();
			let source_table = source_table.replacen(&schema_prefix, "", 1);

			let	start_lsn: Lsn = row.get::<&[u8], _>("start_lsn").unwrap().try_into().unwrap();

			let columns: String = match self.config.capture_table_columns.get(&source_table) {
				Some(x) => x.to_string(),
				None => row.get::<&str, _>("columns").unwrap().to_string()
			};
			self.capture_columns.insert(source_table, 
				TableCapture {
					capture_instance: capture_table,
					columns, // column names are already joined with "," at query level
					start_lsn
			});
		}

		info!("MSSQL CDC setup completed");
		Ok(())
	}


	async fn poll_cdc(&self) -> Result<Vec<ProducedMessage>, Error> {

		let database = self.config.database.clone();
		let query = format!("SELECT [{database}].sys.fn_cdc_get_max_lsn() as lsn;");

		let new_lsn: Lsn = self.query(Query::new(query))
			.await?[0]
			.get::<&[u8], _>("lsn")
			.unwrap()
			.try_into()
			.unwrap();
		let new_lsn_hex = lsn_to_hex(&new_lsn);

		let mut state = self.state.lock().await;
		if state.last_poll_lsn.is_some() && state.last_poll_lsn.unwrap() == new_lsn {
			return Ok(vec![]);
		}

		let base_filter = match state.last_poll_lsn {
			Some(x) => {
				let last_poll_lsn = lsn_to_hex(&x);
				format!("[__$start_lsn] > {last_poll_lsn} AND [__$start_lsn] <= {new_lsn_hex}")
			}
			None => format!("[__$start_lsn] <= {new_lsn_hex}")
		};

		let capture_ops = self
			.config
			.capture_operations
			.as_ref()
			.map(|ops| ops.iter().map(|s| s.as_str()).collect::<Vec<_>>())
			.unwrap_or_else(|| vec!["INSERT", "UPDATE", "DELETE"])
			.into_iter()
			.flat_map(|e| match e {
				"INSERT" => vec!["2"],
				"UPDATE" => vec!["3","4"], // 3 is for delete and 4 for insert (in atomic operation UPDATE)
				"DELETE" => vec!["1"],
				e => {
					warn!("Invalid capture operation {e}. This will be ignored");
					vec![]
				}
			})
			.collect::<Vec<_>>()
			.join(",");
		let base_filter = format!("{base_filter} AND [__$operation] IN ({capture_ops})");

		let database = self.config.database.clone();

		let mut result = Vec::new();
		let default_columns = "*".to_string();

		for (table, data) in &self.capture_columns {

			// Get columns from config, otherwise from the monitored ones or fallback to *
			let columns = match self.config.capture_table_columns.get::<String>(table) {
				Some(x) => x,
				None => match self.capture_columns.get(table) {
					Some(x) => &x.columns,
					None => &default_columns
				}
			};

			let first_table_lsn = lsn_to_hex(&data.start_lsn);
			let query_filter = format!("{base_filter} AND {first_table_lsn} <= [__$start_lsn]");

			let changes_query = format!(
				r#"SELECT 
						TODATETIMEOFFSET(
						[{database}].sys.fn_cdc_map_lsn_to_time([__$start_lsn]),
						DATEPART(TZOFFSET, SYSDATETIMEOFFSET())
					) as event_timestamp,
					-- '{table}' as table,
					[__$start_lsn]
					,[__$end_lsn]
					,[__$seqval]
					,[__$update_mask]
					,min([__$operation]) as [__$operation]
					,[__$command_id]
					,(
						SELECT
							{columns}
						FROM [{database}].[cdc].[{table}] t_json
						WHERE t.[__$start_lsn] = t_json.[__$start_lsn]
							AND t.[__$update_mask] = t_json.[__$update_mask]
							AND t.[__$command_id]= t_json.[__$command_id]
						ORDER BY t_json.[__$command_id] ASC
						FOR JSON AUTO
					) as data
				FROM [{database}].cdc.[{table}] t
				WHERE {query_filter}
				GROUP BY [__$start_lsn], [__$command_id], [__$seqval], [__$end_lsn], [__$update_mask]
				ORDER BY [__$start_lsn] ASC, [__$command_id] ASC;
				"#);

			let rows = self.query(Query::new(changes_query)).await?;
			result.push((table, rows));
		}

		let mut messages = Vec::new();
		for (table_name, row) in result {
			for r in row {
				let data: String = match r.try_get::<&str, _>("data") {
					Ok(Some(x)) => x.to_string(),
					Ok(None) => return Err(Error::InvalidRecord),
					Err(_) => return Err(Error::InvalidRecord)
				};

				let operation_type = match r.try_get::<i32, _>("__$operation").map_err(|_| Error::InvalidRecord)? {
						Some(1) => "DELETE".to_string(),
						Some(2) => "INSERT".to_string(),
						Some(3) => "UPDATE".to_string(),
						Some(4) => "UPDATE".to_string(),
						_ => {
							error!("Operation type not recognized!");
							return Err(Error::InvalidRecord);
						}
					};

				let timestamp = match r.get::<&str, _>("event_timestamp") {
					Some(x) => match DateTime::parse_from_str(x, "%Y-%m-%d %H:%M:%S.%f %z") {
						Ok(y) => y.with_timezone(&Utc),
						Err(y) => {
							warn!("Cannot parse datetime! {y}");
							Utc::now()
						}
					}
					None => Utc::now()
				};

				let parsed_data = serde_json::from_str(&data)
					.map_err(|_| Error::InvalidRecord)?;
				let change_record = DatabaseRecord {
					table_name: table_name.to_string(),
					operation_type,
					timestamp,
					data: serde_json::Value::Object(parsed_data),
					old_data: None
				};
				let payload =
					simd_json::to_vec(&change_record).map_err(|_| Error::InvalidRecord)?;

				let message = ProducedMessage {
					id: Some(Uuid::new_v4().as_u128()),
					headers: None,
					checksum: None,
					timestamp: Some(Utc::now().timestamp_millis() as u64),
					origin_timestamp: Some(Utc::now().timestamp_millis() as u64),
					payload
				};
				messages.push(message);
			}
		}

		// Update state with minimal lock time
		if !messages.is_empty() {
			state.processed_rows += messages.len() as u64;
			state.last_poll_lsn = Some(new_lsn);
		}

		if self.verbose {
			info!("CDC: Fetched {} change records", messages.len());
		} else {
			debug!("CDC: Fetched {} change records", messages.len());
		}
		Ok(messages)
	}

	async fn query(&self, q: Query<'_>) -> Result<Vec<Row>, Error> {

		let mut lock = self.client.lock().await;

		if lock.is_none() {
			return Err(Error::InitError("No connection to the database!".to_string()));
		}
		let mut conn = lock.as_mut().unwrap();
		q.query(conn)
			.await
			.map_err(|e| Error::InitError(format!("Failed to query the database: {e}")))?
			.into_first_result()
			.await
			.map_err(|e| Error::InitError(format!("Queried tracked tables, but cannot elaborate result. {e}")))

	}

	fn payload_format(&self) -> PayloadFormat {
		if let Some(ref payload_col) = self.config.payload_column
			&& !payload_col.is_empty()
		{
			return PayloadFormat::from_config(self.config.payload_format.as_deref());
		}
		PayloadFormat::Json
	}	
}


fn redact_connection_string(conn_str: &str) -> String {
	if let Some(scheme_end) = conn_str.find(";") {
		let scheme_address = &conn_str[..scheme_end];
		return format!("{scheme_address}***");
	}
	let preview: String = conn_str.chars().take(4).collect();
	format!("{preview}***")
}



#[cfg(test)]
mod tests {
	use super::*;

	fn test_config() -> MSSQLSourceConfig {
		MSSQLSourceConfig {			
			connection_string: SecretString::from("jdbc:sqlserver://localhost;encrypt=true;user=MyUserName;password=<password>;"),
			database: "dbo".to_string(),
			mode: "cdc".to_string(),
			tables: vec!["users".to_string()],
			poll_interval: Some("5s".to_string()),
			batch_size: Some(500),
			tracking_column: None,
			initial_offset: None,
			enable_cdc: Some(true),
			custom_query: None,
			snake_case_columns: Some(true),
			include_metadata: None,
			capture_operations: None,
			delete_after_read: None,
			processed_column: None,
			primary_key_column: None,
			payload_column: None,
			payload_format: Some("json".to_string()),
			verbose_logging: None,
			max_retries: None,
			retry_delay: None,
			cdc_schema: None,
			cdc_role: None,
			capture_table_columns: HashMap::new()
		}
	}

	#[test]
	fn given_connection_string_with_credentials_should_redact() {
		let conn = "jdbc:sqlserver://localhost;encrypt=true;user=MyUserName;password=<password>;";
		let redacted = redact_connection_string(conn);
		assert_eq!(redacted, "jdbc:sqlserver://localhost***");
	}
}
