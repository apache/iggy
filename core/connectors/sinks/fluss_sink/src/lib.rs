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
use fluss::metadata::TablePath;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::{schema::FlussTableLayout, writer::FlussWriter};

mod config;
mod schema;
mod writer;
pub use config::{FlussSinkConfig, PayloadFormat};

sink_connector!(FlussSink);

#[derive(Debug)]
struct State {
    invocations_count: u64,
    insertion_errors: u64,
    messages_processed: u64,
}

#[derive(Debug)]
pub struct FlussSink {
    id: u32,
    state: Mutex<State>,
    fluss_writer: writer::FlussWriter,
    fluss_config: FlussSinkConfig,
    table_layout: Option<FlussTableLayout>,
    table_path: TablePath,
}

impl FlussSink {
    pub fn new(id: u32, config: FlussSinkConfig) -> Self {
        let table_path =
            TablePath::new(config.target_database.clone(), config.target_table.clone());
        Self {
            id,
            state: Mutex::new(State {
                invocations_count: 0,
                messages_processed: 0,
                insertion_errors: 0,
            }),
            fluss_writer: FlussWriter::new(config.clone()),
            fluss_config: config,
            table_layout: None,
            table_path,
        }
    }
}

#[async_trait]
impl Sink for FlussSink {
    async fn open(&mut self) -> Result<(), Error> {
        let table_layout = FlussTableLayout::from_config(&self.fluss_config);
        self.fluss_writer.connect().await.map_err(Error::from)?;

        self.fluss_writer
            .ensure_table_exists(&self.table_path, &table_layout)
            .await
            .map_err(Error::from)?;

        self.table_layout = Some(table_layout);
        info!("Opened Fluss sink connector ID: {}", self.id);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let invocation = {
            let mut state = self.state.lock().await;
            state.invocations_count += 1;
            state.invocations_count
        };

        debug!(
            "Fluss sink connector ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition_id: {}, current_offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );

        let table_layout = self
            .table_layout
            .as_ref()
            .ok_or_else(|| Error::InitError("Fluss table layout is not initialized".to_string()))?;

        let result = if self.fluss_config.use_arrow_batch {
            self.fluss_writer
                .write_to_table_arrow(
                    &self.table_path,
                    messages_metadata,
                    messages,
                    topic_metadata,
                    table_layout,
                )
                .await
        } else {
            self.fluss_writer
                .write_to_table(
                    &self.table_path,
                    messages_metadata,
                    messages,
                    topic_metadata,
                    table_layout,
                )
                .await
        };

        match result {
            Ok(r) => {
                let mut state = self.state.lock().await;
                state.insertion_errors += r.insertion_errors;
                state.messages_processed += r.messages_processed;
                Ok(())
            }

            Err(error) => Err(error.into()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Fluss sink ID: {} processed {} messages with {} errors",
            self.id, state.messages_processed, state.insertion_errors
        );
        self.fluss_writer.close().await.map_err(Into::into)
    }
}
