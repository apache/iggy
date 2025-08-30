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

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tokio::sync::Mutex;
use tracing::{error, info};

sink_connector!(PostgresSink);

#[derive(Debug)]
pub struct PostgresSink {
    pub id: u32,
    pool: Option<Pool<Postgres>>,
    config: PostgresSinkConfig,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSinkConfig {
    pub connection_string: String,
    pub target_table: String,
    pub batch_size: Option<u32>,
    pub max_connections: Option<u32>,
    pub auto_create_table: Option<bool>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    insertion_errors: u64,
}

impl PostgresSink {
    pub fn new(id: u32, config: PostgresSinkConfig) -> Self {
        PostgresSink {
            id,
            pool: None,
            config,
            state: Mutex::new(State {
                messages_processed: 0,
                insertion_errors: 0,
            }),
        }
    }

    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(10);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(&self.config.connection_string)
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to PostgreSQL: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!(
            "Connected to PostgreSQL database with {} max connections",
            max_connections
        );
        Ok(())
    }

    async fn ensure_table_exists(&self) -> Result<(), Error> {
        if !self.config.auto_create_table.unwrap_or(false) {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;
        let table_name = &self.config.target_table;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);

        let mut create_table_sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (");
        create_table_sql.push_str("id DECIMAL(39, 0) PRIMARY KEY");

        if include_metadata {
            create_table_sql.push_str(", iggy_offset BIGINT");
            create_table_sql.push_str(", iggy_timestamp TIMESTAMP WITH TIME ZONE");
            create_table_sql.push_str(", iggy_stream TEXT");
            create_table_sql.push_str(", iggy_topic TEXT");
            create_table_sql.push_str(", iggy_partition_id INTEGER");
        }

        if include_checksum {
            create_table_sql.push_str(", iggy_checksum BIGINT");
        }

        if include_origin_timestamp {
            create_table_sql.push_str(", iggy_origin_timestamp TIMESTAMP WITH TIME ZONE");
        }

        create_table_sql.push_str(", payload BYTEA");
        create_table_sql.push_str(", created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())");

        sqlx::query(&create_table_sql)
            .execute(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create table '{table_name}': {e}")))?;

        info!("Ensured table '{}' exists", table_name);
        Ok(())
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;
        let batch_size = self.config.batch_size.unwrap_or(100) as usize;

        for batch in messages.chunks(batch_size) {
            if let Err(e) = self
                .insert_batch(batch, topic_metadata, messages_metadata, pool)
                .await
            {
                let mut state = self.state.lock().await;
                state.insertion_errors += batch.len() as u64;
                error!("Failed to insert batch: {}", e);
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;

        info!(
            "PostgreSQL sink ID: {} processed {} messages to table '{}'",
            self.id,
            messages.len(),
            self.config.target_table
        );

        Ok(())
    }

    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        pool: &Pool<Postgres>,
    ) -> Result<(), Error> {
        let table_name = &self.config.target_table;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);

        for message in messages {
            let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
                error!("Failed to convert payload to bytes: {}", e);
                Error::InvalidRecord
            })?;

            let mut query = format!("INSERT INTO {table_name} (id");
            let mut values = "($1".to_string();
            let mut param_count = 1;

            if include_metadata {
                query.push_str(
                    ", iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id",
                );
                for i in 2..=6 {
                    values.push_str(&format!(", ${i}"));
                }
                param_count = 6;
            }

            if include_checksum {
                query.push_str(", iggy_checksum");
                param_count += 1;
                values.push_str(&format!(", ${param_count}"));
            }

            if include_origin_timestamp {
                query.push_str(", iggy_origin_timestamp");
                param_count += 1;
                values.push_str(&format!(", ${param_count}"));
            }

            query.push_str(", payload");
            param_count += 1;
            values.push_str(&format!(", ${param_count}"));

            query.push_str(&format!(") VALUES {values}"));
            query.push(')');

            let message_id_str = message.id.to_string();
            let mut query_obj = sqlx::query(&query).bind(message_id_str);

            if include_metadata {
                let timestamp = DateTime::from_timestamp(
                    (message.timestamp / 1_000_000) as i64,
                    ((message.timestamp % 1_000_000) * 1_000) as u32,
                )
                .unwrap_or_else(Utc::now);

                query_obj = query_obj
                    .bind(message.offset as i64)
                    .bind(timestamp)
                    .bind(&topic_metadata.stream)
                    .bind(&topic_metadata.topic)
                    .bind(messages_metadata.partition_id as i32);
            }

            if include_checksum {
                query_obj = query_obj.bind(message.checksum as i64);
            }

            if include_origin_timestamp {
                let origin_timestamp = DateTime::from_timestamp(
                    (message.origin_timestamp / 1_000_000) as i64,
                    ((message.origin_timestamp % 1_000_000) * 1_000) as u32,
                )
                .unwrap_or_else(Utc::now);
                query_obj = query_obj.bind(origin_timestamp);
            }

            query_obj = query_obj.bind(payload_bytes);

            query_obj.execute(pool).await.map_err(|e| {
                error!("Failed to insert message: {}", e);
                Error::InvalidRecord
            })?;
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening PostgreSQL sink connector with ID: {}. Target table: {}",
            self.id, self.config.target_table
        );
        self.connect().await?;
        self.ensure_table_exists().await?;
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.process_messages(topic_metadata, &messages_metadata, &messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing PostgreSQL sink connector with ID: {}", self.id);
        let state = self.state.lock().await;
        info!(
            "PostgreSQL sink ID: {} processed {} messages with {} errors",
            self.id, state.messages_processed, state.insertion_errors
        );
        Ok(())
    }
}
