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

use regex::Regex;
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use humantime::Duration as HumanDuration;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
	ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tiberius::{Client, Config, AuthMethod, Query, Row};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, warn};
use uuid::Uuid;


source_connector!(MSSQLSource);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";


// Define a new type for the client with the generic type incorporated
pub type DBClient = Client<Compat<TcpStream>>;

pub(crate) type Lsn = [u8; 10];

/// Convert LSN parameter into a binary string
pub(crate) fn lsn_to_hex(lsn: &Lsn) -> String {

	let mut hex_string = String::with_capacity(lsn.len() * 2);
	// Append the leading 0s and push a hex upper case representation
	hex_string.push_str("0x");
	for byte in lsn.into_iter() {
		hex_string.push_str(&format!("{:02X}", byte));
	}
	hex_string
}


#[derive(Debug)]
pub struct MSSQLSource {
	pub id: u32,
	client: Mutex<Option<DBClient>>,
	config: MSSQLSourceConfig,
	state: Mutex<State>,
	verbose: bool,
	retry_delay: Duration,
	poll_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MSSQLSourceConfig {
	// Example jdbc:sqlserver://localhost;encrypt=true;user=MyUserName;password=<password>;
	// See https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver15
	#[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
	pub connection_string: SecretString,
	pub database: String,
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

#[derive(Clone, Copy)]
struct RowProcessingConfig<'a> {
	table: &'a str,
	tracking_column: &'a str,
	pk_column: &'a str,
	payload_format: PayloadFormat,
	payload_col: &'a str,
	snake_case_columns: bool,
	include_metadata: bool,
}

struct ProcessedRow {
	message: ProducedMessage,
	max_offset: Option<String>,
	row_pk: Option<String>,
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

		let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
		let retry_delay = HumanDuration::from_str(delay_str)
			.map(|duration| duration.into())
			.unwrap_or_else(|_| Duration::from_secs(1));
		let interval_str = config.poll_interval.as_deref().unwrap_or("10s");
		let poll_interval = HumanDuration::from_str(interval_str)
			.map(|duration| duration.into())
			.unwrap_or_else(|_| Duration::from_secs(10));
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
			retry_delay,
			poll_interval
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
				info!("Poll interval: {:?}", self.poll_interval);
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

		let mut client = lock.take();

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
		}.join(",");

		let mut query = Query::new("SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 0 AND name IN (@P1);");
		query.bind(&cdc_tables);
		let non_capture_tables = self.query(query).await?;

		if !non_capture_tables.is_empty() {
			warn!("Not all desired tables are monitored by CDC!");
			if !self.config.enable_cdc.unwrap_or(false) {
				error!("Update config to enable_cdc=true in order to monitor them!");
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

		// For each monitored table, get the capture columns
		let table_columns = self.query(Query::new(
			format!(r#"
				SELECT OBJECT_NAME(ct.object_id, DB_ID('{database}')) as table_name, c.object_id, ct.capture_instance, ct.start_lsn, c.columns FROM [{database}].[cdc].[change_tables] as ct JOIN (SELECT object_id, STRING_AGG(column_name, ',') as columns FROM [{database}].[cdc].[captured_columns] GROUP BY object_id) as c ON c.object_id = ct.object_id;"#
			)
		)).await?;

		for row in table_columns {
			let table_name = row.get::<&str, _>("table_name").unwrap().into();
			let	first_lsn: Lsn = row.get::<&[u8], _>("start_lsn").unwrap().try_into().unwrap();
			let	data_table = row.get::<&str, _>("capture_instance").unwrap().to_string();
			let	columns = row.get::<&str, _>("columns").unwrap().to_string();

			let columns = match self.config.capture_table_columns.get(&table_name) {
				Some(x) => x,
				None => &columns
			};
			self.config.capture_table_columns.insert(table_name, columns.clone());
		}

		info!("MSSQL CDC setup completed");
		Ok(())
	}


	async fn poll_cdc(&self) -> Result<Vec<ProducedMessage>, Error> {

		let q = replace_all_instances(
			&"#db",
			&"SELECT [#db].sys.fn_cdc_get_max_lsn() as lsn;",
			&self.config.database
		);

		let new_lsn: Lsn = self.query(Query::new(q))
			.await?[0]
			.get::<&[u8], _>("lsn")
			.unwrap()
			.try_into()
			.unwrap();
		let mut state = self.state.lock().await;
		if state.last_poll_lsn.is_some() && state.last_poll_lsn.unwrap() == new_lsn {
			return Ok(vec![]);
		}

		let n_lsn = lsn_to_hex(&new_lsn);
		let query_filter = if state.last_poll_lsn.is_some() {
			let last_poll_lsn = lsn_to_hex(&state.last_poll_lsn.unwrap());
			format!("[__$start_lsn] > {last_poll_lsn} AND [__$start_lsn] <= {n_lsn}")
		} else {
			format!("[__$start_lsn] <= {n_lsn}")
		};

		let capture_ops = self
			.config
			.capture_operations
			.as_ref()
			.map(|ops| ops.iter().map(|s| s.as_str()).collect::<Vec<_>>())
			.unwrap_or_else(|| vec!["INSERT", "UPDATE", "DELETE"])
			.into_iter()
			.map(|e| match e {
				"INSERT" => vec![2],
				"UPDATE" => vec![3,4], // 3 is for delete and 4 for insert (in atomic operation UPDATE)
				"DELETE" => vec![1],
				e => {
					warn!("Invalid capture operation {e}. This will be ignored");
					vec![]
				}
			})
			.flatten()
			.collect::<Vec<_>>();

		let tables: Vec<String> = self.config.capture_table_columns.values().cloned().collect();
		let db = self.config.database.clone();

		let mut result = Vec::new();
		let default_columns = "*".to_string();
		for table in tables {
			let columns = match self.config.capture_table_columns.get::<String>(&table) {
				Some(x) => x,
				None => &default_columns
			};
			let changes_query = format!(
				r#"SELECT 
						TODATETIMEOFFSET(
						[#db].sys.fn_cdc_map_lsn_to_time([__$start_lsn]),
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
						FROM [{db}].[cdc].[{table}] t_json
						WHERE t.[__$start_lsn] = t_json.[__$start_lsn]
							AND t.[__$update_mask] = t_json.[__$update_mask]
							AND t.[__$command_id]= t_json.[__$command_id]
						ORDER BY t_json.[__$command_id] ASC
						FOR JSON AUTO
					) as data
				FROM [{db}].cdc.[{table}] t
				WHERE {query_filter}
				GROUP BY [__$start_lsn], [__$command_id], [__$seqval], [__$end_lsn], [__$update_mask]
				ORDER BY [__$start_lsn] ASC, [__$command_id] ASC;";
				"#);

			let rows = self.query(Query::new(changes_query)).await?;
			result.push((table, rows));
		}

		let mut messages = Vec::new();
		for (table_name, row) in result {
			for r in row {
				let data: String = match r.get::<&str, _>("data") {
					Some(x) => x.to_string(),
					None => return Err(Error::InvalidRecord)
				};

				let operation_type = match r.get::<i64, _>("[__$operation]").unwrap_or(0) {
						1 => "DELETE".to_string(),
						2 => "INSERT".to_string(),
						3 => "UPDATE".to_string(),
						4 => "UPDATE".to_string(),
						e => {
							error!("Operation type not recognized! {e}");
							return Err(Error::InvalidRecord)
						}
					};

				let timestamp = match r.get::<&str, _>("event_timestamp") {
					Some(x) => match DateTime::parse_from_str(x, "%Y-%m-%d %H:%M:%S %:z") {
						Ok(y) => y.with_timezone(&Utc),
						Err(y) => {
							warn!("Cannot parse datetime! {y}");
							Utc::now()
						}
					}
					None => Utc::now()
				};

				let parsed_data = parse_record_data(&data);
				let change_record = DatabaseRecord {
					table_name: table_name.to_string(),
					operation_type,
					timestamp: Utc::now(),
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
			// let mut state = self.state.lock().await;
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


	/*
	async fn poll_tables(&self) -> Result<Vec<ProducedMessage>, Error> {
		let pool = self.get_pool()?;
		let mut messages = Vec::new();

		let batch_size = self.config.batch_size.unwrap_or(1000);
		let tracking_column = self.config.tracking_column.as_deref().unwrap_or("id");
		let pk_column = self
			.config
			.primary_key_column
			.as_deref()
			.unwrap_or(tracking_column);

		let row_config = RowProcessingConfig {
			table: "",
			tracking_column,
			pk_column,
			payload_format: self.payload_format(),
			payload_col: self.config.payload_column.as_deref().unwrap_or(""),
			snake_case_columns: self.config.snake_case_columns.unwrap_or(false),
			include_metadata: self.config.include_metadata.unwrap_or(true),
		};

		// Collect state updates to apply after processing
		let mut state_updates: Vec<(String, String)> = Vec::new();
		let mut total_processed: u64 = 0;

		for table in &self.config.tables {
			let table_config = RowProcessingConfig {
				table,
				..row_config
			};

			// Get last offset with minimal lock time
			let last_offset = {
				let state = self.state.lock().await;
				state.tracking_offsets.get(table).cloned()
			};

			let query = if let Some(custom_query) = &self.config.custom_query {
				self.validate_custom_query(custom_query)?;
				self.substitute_query_params(custom_query, table, &last_offset, batch_size)
			} else {
				self.build_polling_query(table, tracking_column, &last_offset, batch_size)?
			};

			// Database I/O without holding the lock
			let rows = with_retry(
				|| sqlx::query(sqlx::AssertSqlSafe(query.as_str())).fetch_all(pool),
				self.get_max_retries(),
				self.retry_delay.as_millis() as u64,
			)
			.await?;

			let mut max_offset: Option<String> = None;
			let mut processed_ids: Vec<String> = Vec::new();

			for row in rows {
				let processed = self.process_row(&row, &table_config)?;

				if let Some(pk) = processed.row_pk {
					processed_ids.push(pk);
				}
				if let Some(offset) = processed.max_offset {
					max_offset = Some(offset);
				}

				messages.push(processed.message);
				total_processed += 1;
			}

			// Database I/O without holding the lock
			if !processed_ids.is_empty() {
				self.mark_or_delete_processed_rows(pool, table, pk_column, &processed_ids)
					.await?;
			}

			// Collect offset update for later
			if let Some(offset) = max_offset {
				state_updates.push((table.clone(), offset));
			}

			if self.verbose {
				info!("Fetched {} rows from table '{table}'", messages.len());
			} else {
				debug!("Fetched {} rows from table '{table}'", messages.len());
			}
		}

		// Apply all state updates with a single lock acquisition
		{
			let mut state = self.state.lock().await;
			state.processed_rows += total_processed;
			for (table, offset) in state_updates {
				state.tracking_offsets.insert(table, offset);
			}
			state.last_poll_time = Utc::now();
		}

		Ok(messages)
	}

	async fn mark_or_delete_processed_rows(
		&self,
		pool: &Pool<Postgres>,
		table: &str,
		pk_column: &str,
		ids: &[String],
	) -> Result<(), Error> {
		if ids.is_empty() {
			return Ok(());
		}

		let quoted_table = quote_qualified_identifier(table)?;
		let quoted_pk = quote_identifier(pk_column)?;

		let ids_list = ids
			.iter()
			.map(|id| {
				if id.parse::<i64>().is_ok() {
					id.clone()
				} else {
					format!("'{}'", id.replace('\'', "''"))
				}
			})
			.collect::<Vec<_>>()
			.join(", ");

		if self.config.delete_after_read.unwrap_or(false) {
			let delete_query =
				format!("DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({ids_list})");

			if self.verbose {
				info!("Deleting {} processed rows from '{table}'", ids.len());
			} else {
				debug!("Deleting {} processed rows from '{table}'", ids.len());
			}

			sqlx::query(sqlx::AssertSqlSafe(delete_query))
				.execute(pool)
				.await
				.map_err(|e| {
					error!("Failed to delete processed rows: {e}");
					Error::InvalidRecord
				})?;
		} else if let Some(processed_col) = &self.config.processed_column {
			let quoted_processed = quote_identifier(processed_col)?;
			let update_query = format!(
				"UPDATE {quoted_table} SET {quoted_processed} = TRUE WHERE {quoted_pk} IN ({ids_list})"
			);

			if self.verbose {
				info!("Marking {} rows as processed in '{table}'", ids.len());
			} else {
				debug!("Marking {} rows as processed in '{table}'", ids.len());
			}

			sqlx::query(sqlx::AssertSqlSafe(update_query))
				.execute(pool)
				.await
				.map_err(|e| {
					error!("Failed to mark rows as processed: {e}");
					Error::InvalidRecord
				})?;
		}

		Ok(())
	}
	*/

	async fn query(&self, q: Query<'_>) -> Result<Vec<Row>, Error> {

		let mut lock = self.client.lock().await;

		if lock.is_none() {
			return Err(Error::InitError("No connection to the database!".to_string()));
		}
		let mut conn = lock.as_mut().unwrap();
		q.query(&mut conn)
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

	fn get_max_retries(&self) -> u32 {
		self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
	}

	/*
	fn build_polling_query(
		&self,
		table: &str,
		tracking_column: &str,
		last_offset: &Option<String>,
		batch_size: u32,
	) -> Result<String, Error> {
		let quoted_table = quote_qualified_identifier(table)?;
		let quoted_tracking = quote_identifier(tracking_column)?;

		let base_query = format!("SELECT * FROM {quoted_table}");

		let mut conditions = Vec::new();

		if let Some(offset) = last_offset {
			conditions.push(format!(
				"{quoted_tracking} > {}",
				format_offset_value(offset)
			));
		} else if let Some(initial) = &self.config.initial_offset {
			conditions.push(format!(
				"{quoted_tracking} > {}",
				format_offset_value(initial)
			));
		}

		if let Some(processed_col) = &self.config.processed_column {
			let quoted_processed = quote_identifier(processed_col)?;
			conditions.push(format!("{quoted_processed} = FALSE"));
		}

		let where_clause = if conditions.is_empty() {
			String::new()
		} else {
			format!(" WHERE {}", conditions.join(" AND "))
		};

		let order_clause = format!(" ORDER BY {quoted_tracking} ASC");
		let limit_clause = format!(" LIMIT {batch_size}");

		Ok(format!(
			"{base_query}{where_clause}{order_clause}{limit_clause}"
		))
	}

	fn validate_custom_query(&self, query: &str) -> Result<(), Error> {
		let query_upper = query.to_uppercase();
		if !query_upper.contains("SELECT") {
			warn!("Custom query should contain SELECT statement");
		}
		if query.contains("$table") && self.config.tables.is_empty() {
			return Err(Error::InvalidConfig);
		}
		Ok(())
	}

	fn substitute_query_params(
		&self,
		query: &str,
		table: &str,
		last_offset: &Option<String>,
		batch_size: u32,
	) -> String {
		let offset_value = last_offset
			.clone()
			.or_else(|| self.config.initial_offset.clone())
			.unwrap_or_default();

		let now = Utc::now();

		query
			.replace("$table", table)
			.replace("$offset", &offset_value)
			.replace("$limit", &batch_size.to_string())
			.replace("$now", &now.to_rfc3339())
			.replace("$now_unix", &now.timestamp().to_string())
	}

	fn parse_logical_replication_message(
		&self,
		data: &str,
		capture_ops: &[&str],
	) -> Option<DatabaseRecord> {
		if data.starts_with("BEGIN") || data.starts_with("COMMIT") {
			return None;
		}

		if data.starts_with("INSERT:") && capture_ops.contains(&"INSERT") {
			return self.parse_insert_message(data);
		}

		if data.starts_with("UPDATE:") && capture_ops.contains(&"UPDATE") {
			return self.parse_update_message(data);
		}

		if data.starts_with("DELETE:") && capture_ops.contains(&"DELETE") {
			return self.parse_delete_message(data);
		}

		None
	}

	fn parse_insert_message(&self, data: &str) -> Option<DatabaseRecord> {
		if let Some(table_start) = data.find("table ")
			&& let Some(colon_pos) = data[table_start..].find(':')
		{
			let table_part = &data[table_start + 6..table_start + colon_pos];
			let table_name = table_part
				.split('.')
				.next_back()
				.unwrap_or(table_part)
				.to_string();

			let data_part = &data[table_start + colon_pos + 1..];
			let parsed_data = parse_record_data(data_part);

			return Some(DatabaseRecord {
				table_name,
				operation_type: "INSERT".to_string(),
				timestamp: Utc::now(),
				data: serde_json::Value::Object(parsed_data),
				old_data: None,
			});
		}
		None
	}

	fn parse_update_message(&self, data: &str) -> Option<DatabaseRecord> {
		if let Some(table_start) = data.find("table ")
			&& let Some(colon_pos) = data[table_start..].find(':')
		{
			let table_part = &data[table_start + 6..table_start + colon_pos];
			let table_name = table_part
				.split('.')
				.next_back()
				.unwrap_or(table_part)
				.to_string();

			let data_part = &data[table_start + colon_pos + 1..];
			let parsed_data = parse_record_data(data_part);

			return Some(DatabaseRecord {
				table_name,
				operation_type: "UPDATE".to_string(),
				timestamp: Utc::now(),
				data: serde_json::Value::Object(parsed_data),
				old_data: None,
			});
		}
		None
	}

	fn parse_delete_message(&self, data: &str) -> Option<DatabaseRecord> {
		if let Some(table_start) = data.find("table ")
			&& let Some(colon_pos) = data[table_start..].find(':')
		{
			let table_part = &data[table_start + 6..table_start + colon_pos];
			let table_name = table_part
				.split('.')
				.next_back()
				.unwrap_or(table_part)
				.to_string();

			let data_part = &data[table_start + colon_pos + 1..];
			let parsed_data = parse_record_data(data_part);

			return Some(DatabaseRecord {
				table_name,
				operation_type: "DELETE".to_string(),
				timestamp: Utc::now(),
				data: serde_json::Value::Object(parsed_data),
				old_data: None,
			});
		}
		None
	}
	*/
	/*
	fn process_row(
		&self,
		row: &sqlx::postgres::PgRow,
		config: &RowProcessingConfig,
	) -> Result<ProcessedRow, Error> {
		let mut row_pk: Option<String> = None;
		let mut max_offset: Option<String> = None;
		let mut extracted_payload: Option<Vec<u8>> = None;
		let mut data = serde_json::Map::new();

		for (i, column) in row.columns().iter().enumerate() {
			let column_name = if config.snake_case_columns {
				to_snake_case(column.name())
			} else {
				column.name().to_string()
			};

			if !config.payload_col.is_empty() && column.name() == config.payload_col {
				extracted_payload =
					Some(self.extract_payload_column(row, i, config.payload_format)?);
				continue;
			}

			let value = extract_column_value(row, i)?;
			data.insert(column_name.clone(), value.clone());

			if column.name() == config.tracking_column {
				if let serde_json::Value::String(ref s) = value {
					max_offset = Some(s.clone());
				} else if let serde_json::Value::Number(ref n) = value {
					max_offset = Some(n.to_string());
				}
			}

			if column.name() == config.pk_column {
				if let serde_json::Value::String(ref s) = value {
					row_pk = Some(s.clone());
				} else if let serde_json::Value::Number(ref n) = value {
					row_pk = Some(n.to_string());
				}
			}
		}

		let payload = if let Some(bytes) = extracted_payload {
			bytes
		} else {
			let record = if config.include_metadata {
				DatabaseRecord {
					table_name: config.table.to_string(),
					operation_type: "SELECT".to_string(),
					timestamp: Utc::now(),
					data: serde_json::Value::Object(data),
					old_data: None,
				}
			} else {
				let mut simple_record = serde_json::Map::new();
				simple_record.insert("data".to_string(), serde_json::Value::Object(data));
				DatabaseRecord {
					table_name: config.table.to_string(),
					operation_type: "SELECT".to_string(),
					timestamp: Utc::now(),
					data: serde_json::Value::Object(simple_record),
					old_data: None,
				}
			};
			simd_json::to_vec(&record).map_err(|_| Error::InvalidRecord)?
		};

		let message = ProducedMessage {
			id: Some(Uuid::new_v4().as_u128()),
			headers: None,
			checksum: None,
			timestamp: Some(Utc::now().timestamp_millis() as u64),
			origin_timestamp: Some(Utc::now().timestamp_millis() as u64),
			payload,
		};

		Ok(ProcessedRow {
			message,
			max_offset,
			row_pk,
		})
	}
	*/
	/*
	fn extract_payload_column(
		&self,
		row: &sqlx::postgres::PgRow,
		column_index: usize,
		format: PayloadFormat,
	) -> Result<Vec<u8>, Error> {
		match format {
			PayloadFormat::Bytea => {
				let bytes: Option<Vec<u8>> = row
					.try_get(column_index)
					.map_err(|_| Error::InvalidRecord)?;
				Ok(bytes.unwrap_or_default())
			}
			PayloadFormat::Text => {
				let text: Option<String> = row
					.try_get(column_index)
					.map_err(|_| Error::InvalidRecord)?;
				Ok(text.unwrap_or_default().into_bytes())
			}
			PayloadFormat::JsonDirect => {
				let json_value: Option<serde_json::Value> = row
					.try_get(column_index)
					.map_err(|_| Error::InvalidRecord)?;
				simd_json::to_vec(&json_value.unwrap_or(serde_json::Value::Null))
					.map_err(|_| Error::InvalidRecord)
			}
			PayloadFormat::Json => {
				let bytes: Option<Vec<u8>> = row
					.try_get(column_index)
					.map_err(|_| Error::InvalidRecord)?;
				Ok(bytes.unwrap_or_default())
			}
		}
	}
	*/

	
}
/*
fn extract_column_value(
	row: &sqlx::postgres::PgRow,
	column_index: usize,
) -> Result<serde_json::Value, Error> {
	let column = &row.columns()[column_index];
	let type_name = column.type_info().name();

	match type_name {
		"BOOL" => {
			let value: Option<bool> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(serde_json::Value::Bool)
				.unwrap_or(serde_json::Value::Null))
		}
		"INT2" => {
			let value: Option<i16> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|v| serde_json::Value::from(v as i64))
				.unwrap_or(serde_json::Value::Null))
		}
		"INT4" => {
			let value: Option<i32> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|v| serde_json::Value::from(v as i64))
				.unwrap_or(serde_json::Value::Null))
		}
		"OID" => {
			let value: Option<Oid> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|v| serde_json::Value::from(v.0 as u64))
				.unwrap_or(serde_json::Value::Null))
		}
		"INT8" => {
			let value: Option<i64> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(serde_json::Value::from)
				.unwrap_or(serde_json::Value::Null))
		}
		"FLOAT4" => {
			let value: Option<f32> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|v| serde_json::Value::from(v as f64))
				.unwrap_or(serde_json::Value::Null))
		}
		"FLOAT8" => {
			let value: Option<f64> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(serde_json::Value::from)
				.unwrap_or(serde_json::Value::Null))
		}
		"NUMERIC" => {
			let value: Option<String> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.and_then(|s| s.parse::<f64>().ok())
				.map(serde_json::Value::from)
				.unwrap_or(serde_json::Value::Null))
		}
		"VARCHAR" | "TEXT" | "CHAR" | "NAME" | "BPCHAR" => {
			let value: Option<String> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(serde_json::Value::String)
				.unwrap_or(serde_json::Value::Null))
		}
		"DATE" => {
			let value: Option<NaiveDate> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|d| serde_json::Value::String(d.to_string()))
				.unwrap_or(serde_json::Value::Null))
		}
		"TIME" => {
			let value: Option<NaiveTime> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|t| serde_json::Value::String(t.to_string()))
				.unwrap_or(serde_json::Value::Null))
		}
		"TIMETZ" => {
			let value: Option<PgTimeTz> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|tz| serde_json::Value::String(format!("{}{}", tz.time, tz.offset)))
				.unwrap_or(serde_json::Value::Null))
		}
		"TIMESTAMP" => {
			let value: Option<NaiveDateTime> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|dt| serde_json::Value::String(dt.to_string()))
				.unwrap_or(serde_json::Value::Null))
		}
		"TIMESTAMPTZ" => {
			let value: Option<DateTime<Utc>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|dt| serde_json::Value::String(dt.to_rfc3339()))
				.unwrap_or(serde_json::Value::Null))
		}
		"INTERVAL" => {
			let value: Option<PgInterval> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|iv| serde_json::Value::String(format_pg_interval(&iv)))
				.unwrap_or(serde_json::Value::Null))
		}
		"UUID" => {
			let value: Option<Uuid> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|u| serde_json::Value::String(u.to_string()))
				.unwrap_or(serde_json::Value::Null))
		}
		"JSON" | "JSONB" => {
			let value: Option<serde_json::Value> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value.unwrap_or(serde_json::Value::Null))
		}
		"BYTEA" => {
			let value: Option<Vec<u8>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|bytes| {
					use base64::Engine;
					serde_json::Value::String(
						base64::engine::general_purpose::STANDARD.encode(&bytes),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"BOOL[]" => {
			let value: Option<Vec<Option<bool>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(serde_json::Value::Bool)
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"INT2[]" => {
			let value: Option<Vec<Option<i16>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|n| serde_json::Value::from(n as i64))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"INT4[]" => {
			let value: Option<Vec<Option<i32>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|n| serde_json::Value::from(n as i64))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"OID[]" => {
			let value: Option<Vec<Option<Oid>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|n| serde_json::Value::from(n.0 as u64))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"INT8[]" => {
			let value: Option<Vec<Option<i64>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(serde_json::Value::from)
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"FLOAT4[]" => {
			let value: Option<Vec<Option<f32>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|n| serde_json::Value::from(n as f64))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"FLOAT8[]" => {
			let value: Option<Vec<Option<f64>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(serde_json::Value::from)
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"TEXT[]" | "VARCHAR[]" | "CHAR[]" => {
			let value: Option<Vec<Option<String>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(serde_json::Value::String)
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"UUID[]" => {
			let value: Option<Vec<Option<Uuid>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|u| serde_json::Value::String(u.to_string()))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"JSON[]" | "JSONB[]" => {
			let value: Option<Vec<Option<serde_json::Value>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| v.unwrap_or(serde_json::Value::Null))
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"DATE[]" => {
			let value: Option<Vec<Option<NaiveDate>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|d| serde_json::Value::String(d.to_string()))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"TIME[]" => {
			let value: Option<Vec<Option<NaiveTime>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|t| serde_json::Value::String(t.to_string()))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"TIMESTAMP[]" => {
			let value: Option<Vec<Option<NaiveDateTime>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|dt| serde_json::Value::String(dt.to_string()))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"TIMESTAMPTZ[]" => {
			let value: Option<Vec<Option<DateTime<Utc>>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|dt| serde_json::Value::String(dt.to_rfc3339()))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		"INTERVAL[]" => {
			let value: Option<Vec<Option<PgInterval>>> = row
				.try_get(column_index)
				.map_err(|_| Error::InvalidRecord)?;
			Ok(value
				.map(|arr| {
					serde_json::Value::Array(
						arr.into_iter()
							.map(|v| {
								v.map(|iv| serde_json::Value::String(format_pg_interval(&iv)))
									.unwrap_or(serde_json::Value::Null)
							})
							.collect(),
					)
				})
				.unwrap_or(serde_json::Value::Null))
		}
		_ => {
			let column_name = column.name();
			warn!(
				"Column '{column_name}' has unrecognized Postgres type '{type_name}', \
				 attempting raw text extraction"
			);
			let raw = row.try_get_raw(column_index).map_err(|e| {
				error!("Failed to read column '{column_name}' (type '{type_name}'): {e}");
				Error::InvalidRecordValue(format!(
					"column '{column_name}' has unsupported Postgres type '{type_name}'"
				))
			})?;
			if raw.is_null() {
				return Ok(serde_json::Value::Null);
			}
			match raw.as_str() {
				Ok(text) => Ok(serde_json::Value::String(text.to_owned())),
				Err(_) => {
					use base64::Engine;
					let bytes = raw.as_bytes().map_err(|e| {
						error!(
							"Failed to read column '{column_name}' \
							 (type '{type_name}') as bytes: {e}"
						);
						Error::InvalidRecordValue(format!(
							"column '{column_name}' has unsupported Postgres type '{type_name}'"
						))
					})?;
					Ok(serde_json::Value::String(
						base64::engine::general_purpose::STANDARD.encode(bytes),
					))
				}
			}
		}
	}
}
*/
/*
fn format_pg_interval(interval: &PgInterval) -> String {
	let mut parts = Vec::new();

	let years = interval.months / 12;
	let months = interval.months % 12;

	if years != 0 {
		parts.push(format!(
			"{years} year{}",
			if years.unsigned_abs() != 1 { "s" } else { "" }
		));
	}
	if months != 0 {
		parts.push(format!(
			"{months} mon{}",
			if months.unsigned_abs() != 1 { "s" } else { "" }
		));
	}
	if interval.days != 0 {
		parts.push(format!(
			"{} day{}",
			interval.days,
			if interval.days.unsigned_abs() != 1 {
				"s"
			} else {
				""
			}
		));
	}
	if interval.microseconds != 0 || parts.is_empty() {
		let negative = interval.microseconds < 0;
		let abs_us = interval.microseconds.unsigned_abs();
		let total_secs = abs_us / 1_000_000;
		let remaining_us = abs_us % 1_000_000;
		let hours = total_secs / 3600;
		let mins = (total_secs % 3600) / 60;
		let secs = total_secs % 60;
		let sign = if negative { "-" } else { "" };
		if remaining_us != 0 {
			parts.push(format!(
				"{sign}{:02}:{:02}:{:02}.{:06}",
				hours, mins, secs, remaining_us
			));
		} else {
			parts.push(format!("{sign}{hours:02}:{mins:02}:{secs:02}"));
		}
	}

	parts.join(" ")
}
*/
/*
fn quote_identifier(name: &str) -> Result<String, Error> {
	if name.is_empty() {
		return Err(Error::InvalidConfigValue(
			"identifier must not be empty".to_string(),
		));
	}
	if name.contains('\0') {
		return Err(Error::InvalidConfigValue(format!(
			"identifier '{name}' contains NUL byte"
		)));
	}
	let escaped = name.replace('"', "\"\"");
	Ok(format!("\"{escaped}\""))
}

/// Quote a possibly schema-qualified identifier like `public.users` as
/// `"public"."users"`. Each dot-separated segment is validated and quoted
/// independently so that schema-qualified table names survive intact.
fn quote_qualified_identifier(name: &str) -> Result<String, Error> {
	if !name.contains('.') {
		return quote_identifier(name);
	}
	let parts: Result<Vec<_>, _> = name.split('.').map(quote_identifier).collect();
	Ok(parts?.join("."))
}
*/
fn format_offset_value(value: &str) -> String {
	if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
		value.to_string()
	} else {
		format!("'{}'", value.replace('\'', "''"))
	}
}

fn to_snake_case(input: &str) -> String {
	let mut result = String::new();
	let mut prev_was_uppercase = false;

	for (i, ch) in input.chars().enumerate() {
		if ch.is_uppercase() {
			if i > 0 && !prev_was_uppercase {
				result.push('_');
			}
			if let Some(lowercase_ch) = ch.to_lowercase().next() {
				result.push(lowercase_ch);
			} else {
				result.push(ch);
			}
			prev_was_uppercase = true;
		} else {
			result.push(ch);
			prev_was_uppercase = false;
		}
	}

	result
}

fn parse_record_data(data: &str) -> serde_json::Map<String, serde_json::Value> {
	let mut result = serde_json::Map::new();

	for part in data.split_whitespace() {
		if let Some(bracket_pos) = part.find('[')
			&& let Some(_close_bracket) = part.find(']')
			&& let Some(colon_pos) = part.find(':')
		{
			let column_name = &part[..bracket_pos];
			let value_str = &part[colon_pos + 1..];

			let cleaned_value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
				&value_str[1..value_str.len() - 1]
			} else {
				value_str
			};

			let value = if let Ok(num) = cleaned_value.parse::<i64>() {
				serde_json::Value::Number(serde_json::Number::from(num))
			} else if let Ok(float) = cleaned_value.parse::<f64>() {
				serde_json::Value::Number(
					serde_json::Number::from_f64(float).unwrap_or(serde_json::Number::from(0)),
				)
			} else if cleaned_value.eq_ignore_ascii_case("true") {
				serde_json::Value::Bool(true)
			} else if cleaned_value.eq_ignore_ascii_case("false") {
				serde_json::Value::Bool(false)
			} else {
				serde_json::Value::String(cleaned_value.to_string())
			};

			result.insert(column_name.to_string(), value);
		}
	}

	result
}
/*
async fn with_retry<T, F, Fut>(operation: F, max_retries: u32, delay_ms: u64) -> Result<T, Error>
where
	F: Fn() -> Fut,
	Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
	let mut attempts = 0;
	loop {
		match operation().await {
			Ok(result) => return Ok(result),
			Err(e) => {
				attempts += 1;
				if attempts >= max_retries || !is_transient_error(&e) {
					error!("Database operation failed after {attempts} attempts: {e}");
					return Err(Error::InvalidRecord);
				}
				warn!(
					"Transient database error (attempt {attempts}/{max_retries}): {e}. Retrying in {delay_ms}ms..."
				);
				tokio::time::sleep(Duration::from_millis(delay_ms * attempts as u64)).await;
			}
		}
	}
}
*/
/*
fn is_transient_error(e: &sqlx::Error) -> bool {
	match e {
		sqlx::Error::Io(_) => true,
		sqlx::Error::PoolTimedOut => true,
		sqlx::Error::PoolClosed => false,
		sqlx::Error::Protocol(_) => false,
		sqlx::Error::Database(db_err) => db_err.code().is_some_and(|code| {
			matches!(
				code.as_ref(),
				"40001" | "40P01" | "57P01" | "57P02" | "57P03" | "08000" | "08003" | "08006"
			)
		}),
		_ => false,
	}
}
*/

fn redact_connection_string(conn_str: &str) -> String {
	if let Some(scheme_end) = conn_str.find(";") {
		let scheme_address = &conn_str[..scheme_end];
		return format!("{scheme_address}***");
	}
	let preview: String = conn_str.chars().take(4).collect();
	format!("{preview}***")
}


pub fn replace_all_instances(r: &str, original_string: &str, replacement: &str) -> String {
	Regex::new(r)
		.unwrap()
		.replace_all(original_string, String::from(replacement))
		.to_string()
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

	/*
	#[test]
	fn given_last_offset_polling_query_should_be_built() {
		let src = PostgresSource::new(1, test_config(), None);
		let query = src
			.build_polling_query("users", "updated_at", &Some("2024-01-01".to_string()), 500)
			.expect("Failed to build query");
		assert_eq!(
			query,
			"SELECT * FROM \"users\" WHERE \"updated_at\" > '2024-01-01' ORDER BY \"updated_at\" ASC LIMIT 500"
		);
	}

	#[test]
	fn given_initial_offset_polling_query_should_be_built() {
		let mut config = test_config();
		config.tracking_column = Some("id".to_string());
		config.initial_offset = Some("100".to_string());
		let src = PostgresSource::new(1, config, None);
		let query = src
			.build_polling_query("users", "id", &None, 1000)
			.expect("Failed to build query");
		assert_eq!(
			query,
			"SELECT * FROM \"users\" WHERE \"id\" > 100 ORDER BY \"id\" ASC LIMIT 1000"
		);
	}

	#[test]
	fn given_processed_column_polling_query_should_include_filter() {
		let mut config = test_config();
		config.processed_column = Some("is_processed".to_string());
		let src = PostgresSource::new(1, config, None);
		let query = src
			.build_polling_query("events", "id", &None, 100)
			.expect("Failed to build query");
		assert!(query.contains("\"is_processed\" = FALSE"));
	}

	#[test]
	fn given_numeric_offset_should_not_quote_value() {
		let src = PostgresSource::new(1, test_config(), None);
		let query = src
			.build_polling_query("users", "id", &Some("42".to_string()), 100)
			.expect("Failed to build query");
		assert!(query.contains("\"id\" > 42"));
		assert!(!query.contains("'42'"));
	}

	#[test]
	fn given_special_chars_in_identifier_should_escape() {
		let result = quote_identifier("table\"name").expect("Failed to quote");
		assert_eq!(result, "\"table\"\"name\"");
	}

	#[test]
	fn given_empty_identifier_should_fail() {
		let result = quote_identifier("");
		assert!(result.is_err());
	}

	#[test]
	fn given_unqualified_name_should_quote_as_single_identifier() {
		let result = quote_qualified_identifier("users").expect("Failed to quote");
		assert_eq!(result, "\"users\"");
	}

	#[test]
	fn given_schema_qualified_name_should_quote_each_segment() {
		let result = quote_qualified_identifier("public.users").expect("Failed to quote");
		assert_eq!(result, "\"public\".\"users\"");
	}

	#[test]
	fn given_qualified_name_with_quote_chars_should_escape_each_segment() {
		let result = quote_qualified_identifier("my\"schema.my\"table").expect("Failed to quote");
		assert_eq!(result, "\"my\"\"schema\".\"my\"\"table\"");
	}

	#[test]
	fn given_qualified_name_with_empty_segment_should_fail() {
		assert!(quote_qualified_identifier("public.").is_err());
		assert!(quote_qualified_identifier(".users").is_err());
	}

	#[test]
	fn given_insert_message_should_parse_correctly() {
		let mut config = test_config();
		config.mode = "cdc".to_string();
		let src = MSSQLSource::new(1, config, None);

		let data = "INSERT: table public.users: id[1] name['Alice'] active[true]";
		let rec = src
			.parse_logical_replication_message(data, &["INSERT"])
			.unwrap();
		assert_eq!(rec.table_name, "users");
		assert_eq!(rec.operation_type, "INSERT");
	}

	#[test]
	fn given_update_message_should_parse_correctly() {
		let mut config = test_config();
		config.mode = "cdc".to_string();
		let src = PostgresSource::new(1, config, None);

		let data = "UPDATE: table public.orders: id[42] total[99.5]";
		let rec = src
			.parse_logical_replication_message(data, &["UPDATE"])
			.unwrap();
		assert_eq!(rec.table_name, "orders");
		assert_eq!(rec.operation_type, "UPDATE");
	}

	#[test]
	fn given_delete_message_should_parse_correctly() {
		let mut config = test_config();
		config.mode = "cdc".to_string();
		let src = PostgresSource::new(1, config, None);

		let data = "DELETE: table public.products: id[7]";
		let rec = src
			.parse_logical_replication_message(data, &["DELETE"])
			.unwrap();
		assert_eq!(rec.table_name, "products");
		assert_eq!(rec.operation_type, "DELETE");
	}

	#[test]
	fn given_custom_query_params_should_substitute_correctly() {
		let mut config = test_config();
		config.initial_offset = Some("0".to_string());
		let src = PostgresSource::new(1, config, None);

		let query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit";
		let result = src.substitute_query_params(query, "events", &Some("100".to_string()), 50);

		assert!(result.contains("FROM events"));
		assert!(result.contains("id > 100"));
		assert!(result.contains("LIMIT 50"));
	}

	#[test]
	fn given_custom_query_with_time_params_should_substitute_correctly() {
		let src = PostgresSource::new(1, test_config(), None);

		let query = "SELECT * FROM $table WHERE created_at < '$now'";
		let result = src.substitute_query_params(query, "logs", &None, 100);

		assert!(result.contains("FROM logs"));
		assert!(!result.contains("$now"));
	}

	#[test]
	fn given_no_last_offset_should_use_initial_offset() {
		let mut config = test_config();
		config.initial_offset = Some("500".to_string());
		let src = PostgresSource::new(1, config, None);

		let query = "SELECT * FROM $table WHERE id > $offset";
		let result = src.substitute_query_params(query, "data", &None, 100);

		assert!(result.contains("id > 500"));
	}
*/
	#[test]
	fn given_connection_string_with_credentials_should_redact() {
		let conn = "jdbc:sqlserver://localhost;encrypt=true;user=MyUserName;password=<password>;";
		let redacted = redact_connection_string(conn);
		assert_eq!(redacted, "jdbc:sqlserver://localhost***");
	}
/*
	#[test]
	fn given_connection_string_without_scheme_should_redact() {
		let conn = "localhost:5432/db";
		let redacted = redact_connection_string(conn);
		assert_eq!(redacted, "loc***");
	}

	#[test]
	fn given_postgresql_scheme_should_redact() {
		let conn = "postgresql://admin:secret123@db.example.com:5432/mydb";
		let redacted = redact_connection_string(conn);
		assert_eq!(redacted, "postgresql://adm***");
	}

	#[test]
	fn given_persisted_state_should_restore_tracking_offsets() {
		let state = State {
			last_poll_time: Utc::now(),
			tracking_offsets: HashMap::from([
				("users".to_string(), "100".to_string()),
				("orders".to_string(), "2024-01-15T10:30:00Z".to_string()),
			]),
			processed_rows: 500,
		};

		let connector_state =
			ConnectorState::serialize(&state, "test", 1).expect("Failed to serialize state");

		let src = PostgresSource::new(1, test_config(), Some(connector_state));

		let runtime = tokio::runtime::Runtime::new().unwrap();
		runtime.block_on(async {
			let restored = src.state.lock().await;
			assert_eq!(
				restored.tracking_offsets.get("users"),
				Some(&"100".to_string())
			);
			assert_eq!(
				restored.tracking_offsets.get("orders"),
				Some(&"2024-01-15T10:30:00Z".to_string())
			);
			assert_eq!(restored.processed_rows, 500);
		});
	}

	#[test]
	fn given_no_state_should_start_fresh() {
		let src = PostgresSource::new(1, test_config(), None);

		let runtime = tokio::runtime::Runtime::new().unwrap();
		runtime.block_on(async {
			let state = src.state.lock().await;
			assert!(state.tracking_offsets.is_empty());
			assert_eq!(state.processed_rows, 0);
		});
	}

	#[test]
	fn given_invalid_state_should_start_fresh() {
		let invalid_state = ConnectorState(b"not valid json".to_vec());
		let src = PostgresSource::new(1, test_config(), Some(invalid_state));

		let runtime = tokio::runtime::Runtime::new().unwrap();
		runtime.block_on(async {
			let state = src.state.lock().await;
			assert!(state.tracking_offsets.is_empty());
			assert_eq!(state.processed_rows, 0);
		});
	}

	#[test]
	fn state_should_be_serializable_and_deserializable() {
		let original = State {
			last_poll_time: DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
				.unwrap()
				.with_timezone(&Utc),
			tracking_offsets: HashMap::from([("table1".to_string(), "42".to_string())]),
			processed_rows: 1000,
		};

		let connector_state =
			ConnectorState::serialize(&original, "test", 1).expect("Failed to serialize state");
		let deserialized: State = connector_state
			.deserialize("test", 1)
			.expect("Failed to deserialize state");

		assert_eq!(original.last_poll_time, deserialized.last_poll_time);
		assert_eq!(original.tracking_offsets, deserialized.tracking_offsets);
		assert_eq!(original.processed_rows, deserialized.processed_rows);
	}

	#[test]
	fn given_zero_interval_should_format_as_zero_time() {
		let interval = PgInterval {
			months: 0,
			days: 0,
			microseconds: 0,
		};
		assert_eq!(format_pg_interval(&interval), "00:00:00");
	}

	#[test]
	fn given_interval_with_months_and_days_should_format_correctly() {
		let interval = PgInterval {
			months: 14,
			days: 3,
			microseconds: 0,
		};
		assert_eq!(format_pg_interval(&interval), "1 year 2 mons 3 days");
	}

	#[test]
	fn given_interval_with_time_should_format_correctly() {
		let interval = PgInterval {
			months: 0,
			days: 0,
			microseconds: 3_661_000_000,
		};
		assert_eq!(format_pg_interval(&interval), "01:01:01");
	}

	#[test]
	fn given_interval_with_microseconds_should_format_fractional_seconds() {
		let interval = PgInterval {
			months: 0,
			days: 1,
			microseconds: 500_000,
		};
		assert_eq!(format_pg_interval(&interval), "1 day 00:00:00.500000");
	}

	#[test]
	fn given_singular_units_should_omit_plural_suffix() {
		let interval = PgInterval {
			months: 13,
			days: 1,
			microseconds: 0,
		};
		assert_eq!(format_pg_interval(&interval), "1 year 1 mon 1 day");
	}

	#[test]
	fn given_negative_microseconds_should_format_with_sign() {
		let interval = PgInterval {
			months: 0,
			days: 0,
			microseconds: -1_500_000,
		};
		assert_eq!(format_pg_interval(&interval), "-00:00:01.500000");
	}

	#[test]
	fn given_negative_hours_should_format_with_sign() {
		let interval = PgInterval {
			months: 0,
			days: 0,
			microseconds: -3_600_000_000,
		};
		assert_eq!(format_pg_interval(&interval), "-01:00:00");
	}
	*/
}
