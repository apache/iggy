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
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use tokio::sync::Mutex;
use tracing::info;

use crate::{router::Router, writer::FlussWriter};

mod config;
mod router;
mod schema_catalog;
mod static_schema;
mod writer;
pub(crate) use config::ResolvedFlussSinkConfig;
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
    writer: writer::FlussWriter,
    router: Router,
    verbose: bool,
}

impl FlussSink {
    pub fn new(id: u32, config: FlussSinkConfig) -> Self {
        let config = ResolvedFlussSinkConfig::from(config);
        let verbose = config.verbose_logging;
        let writer = FlussWriter::new(config.clone());
        Self {
            id,
            state: Mutex::new(State {
                invocations_count: 0,
                messages_processed: 0,
                insertion_errors: 0,
            }),
            writer,
            router: Router::from_config(&config),
            verbose,
        }
    }
}

#[async_trait]
impl Sink for FlussSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.writer.connect().await?;
        if let Router::StaticTable(router) = &self.router {
            router.init(&self.writer).await?;
        }
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

        if self.verbose {
            info!(
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
        }

        let result = match &self.router {
            Router::StaticTable(router) => {
                router
                    .route(&self.writer, topic_metadata, messages_metadata, messages)
                    .await
            }
            Router::MultiTable(router) => router.route(&self.writer, messages).await,
        };

        match result {
            Ok(r) => {
                {
                    let mut state = self.state.lock().await;
                    state.insertion_errors += r.errors;
                    state.messages_processed += r.appended + r.inserted;
                }
                Ok(())
            }

            Err(error) => Err(error.into()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        {
            let state = self.state.lock().await;
            info!(
                "Fluss sink ID: {} processed {} messages with {} errors",
                self.id, state.messages_processed, state.insertion_errors
            );
        }
        self.writer.close().await.map_err(Into::into)
    }
}
