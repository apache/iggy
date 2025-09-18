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
use chrono::Utc;
use iggy_connector_sdk::{
    sink_connector, transforms::Transform, ConsumedMessage, DecodedMessage, Error,
    MessagesMetadata, Payload, Sink, StreamDecoder, StreamEncoder, TopicMetadata,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

mod config;
mod config_loader;
mod flink_client;
mod state;

use config::FlinkSinkConfig;
use flink_client::{FlinkClient, FlinkRecord};
use state::CheckpointState;

sink_connector!(FlinkSink);

pub struct FlinkSink {
    id: u32,
    config: FlinkSinkConfig,
    client: Arc<Mutex<FlinkClient>>,
    state: Arc<RwLock<CheckpointState>>,
    buffer: Arc<Mutex<Vec<FlinkRecord>>>,
    active_job_id: Arc<RwLock<Option<String>>>,
    transforms: Vec<Arc<dyn Transform>>,
    decoder: Option<Arc<dyn StreamDecoder>>,
    encoder: Option<Arc<dyn StreamEncoder>>,
}

impl std::fmt::Debug for FlinkSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlinkSink")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("transforms_count", &self.transforms.len())
            .finish()
    }
}

impl FlinkSink {
    pub fn new(id: u32, config: FlinkSinkConfig) -> Self {
        // Initialize transforms from config
        let mut transforms = Vec::new();
        for transform_cfg in &config.transforms {
            if let Ok(transform) = iggy_connector_sdk::transforms::from_config(
                transform_cfg.r#type,
                &transform_cfg.config,
            ) {
                transforms.push(transform);
            }
        }

        // Initialize decoder and encoder only if schema conversion is needed
        let decoder = config.input_schema.as_ref().map(|schema| schema.decoder());
        let encoder = config.output_schema.as_ref().map(|schema| schema.encoder());

        let client = Arc::new(Mutex::new(FlinkClient::new(config.clone())));
        let state = Arc::new(RwLock::new(CheckpointState::new()));
        let buffer = Arc::new(Mutex::new(Vec::with_capacity(config.batch_size)));
        let active_job_id = Arc::new(RwLock::new(None));

        FlinkSink {
            id,
            config,
            client,
            state,
            buffer,
            active_job_id,
            transforms,
            decoder,
            encoder,
        }
    }

    async fn flush_buffer(&self) -> Result<(), Error> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let records = buffer.drain(..).collect::<Vec<_>>();
        let batch_size = records.len();

        info!(
            "Flushing {} records to Flink job: {}",
            batch_size, self.config.job_name
        );

        let client = self.client.lock().await;
        match client.send_batch(records).await {
            Ok(_) => {
                debug!("Successfully sent {} records to Flink", batch_size);
                self.update_checkpoint().await?;
                Ok(())
            }
            Err(e) => {
                error!("Failed to send batch to Flink: {}", e);
                Err(Error::HttpRequestFailed(e.to_string()))
            }
        }
    }

    async fn update_checkpoint(&self) -> Result<(), Error> {
        let mut state = self.state.write().await;
        state.last_checkpoint = Utc::now();
        state.records_processed += 1;

        if self.config.enable_checkpointing {
            if let Some(job_id) = &self.config.job_id {
                let client = self.client.lock().await;
                client.trigger_checkpoint(job_id).await.map_err(|e| {
                    error!("Failed to trigger Flink checkpoint: {}", e);
                    Error::HttpRequestFailed(e.to_string())
                })?;
            }
        }

        Ok(())
    }

    async fn convert_decoded_message(&self, message: DecodedMessage) -> Result<FlinkRecord, Error> {
        // Encode message if encoder is configured
        let final_payload = if let Some(ref encoder) = self.encoder {
            // Encode the payload using configured encoder
            let encoded = encoder.encode(message.payload.clone())?;
            Payload::Raw(encoded)
        } else {
            message.payload
        };

        let payload_json = match final_payload {
            Payload::Json(value) => {
                // Convert simd_json::Value to serde_json::Value
                let json_str =
                    simd_json::to_string(&value).map_err(|_| Error::InvalidJsonPayload)?;
                serde_json::from_str(&json_str).map_err(|_| Error::InvalidJsonPayload)?
            }
            Payload::Text(text) => {
                serde_json::from_str(&text).map_err(|_| Error::InvalidJsonPayload)?
            }
            Payload::Raw(bytes) => {
                let text = String::from_utf8(bytes).map_err(|_| Error::InvalidTextPayload)?;
                serde_json::from_str(&text).map_err(|_| Error::InvalidJsonPayload)?
            }
            _ => {
                warn!("Unsupported payload format for Flink sink");
                return Err(Error::InvalidPayloadType);
            }
        };

        // Convert HeaderKey/HeaderValue to String/String
        let headers = if let Some(iggy_headers) = message.headers {
            iggy_headers
                .into_iter()
                .filter_map(|(k, v)| match v.as_str() {
                    Ok(value) => Some((k.as_str().to_string(), value.to_string())),
                    Err(_) => None,
                })
                .collect()
        } else {
            HashMap::new()
        };

        Ok(FlinkRecord {
            id: message
                .id
                .map_or_else(|| "unknown".to_string(), |id| id.to_string()),
            timestamp: message.timestamp.unwrap_or(0),
            headers,
            payload: payload_json,
            offset: message.offset.unwrap_or(0),
            checksum: message.checksum.unwrap_or(0),
        })
    }
}

#[async_trait]
impl Sink for FlinkSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Flink sink connector with ID: {} for cluster: {}",
            self.id, self.config.flink_cluster_url
        );

        // Test connection to Flink cluster
        let mut client = self.client.lock().await;
        let overview = client.check_cluster_health().await.map_err(|e| {
            error!("Failed to connect to Flink cluster: {}", e);
            Error::InitError(format!("Cannot connect to Flink cluster: {}", e))
        })?;

        info!(
            "Connected to Flink cluster v{} with {} taskmanagers and {}/{} available slots",
            overview.flink_version,
            overview.taskmanagers,
            overview.slots_available,
            overview.slots_total
        );

        // List all current jobs for monitoring
        match client.list_jobs().await {
            Ok(jobs) => {
                info!("Found {} jobs in Flink cluster", jobs.len());
                for job in &jobs {
                    info!("  - Job {}: {} ({})", job.jid, job.name, job.state);
                }
            }
            Err(e) => {
                warn!("Could not list jobs: {}", e);
            }
        }

        // Handle job creation or verification
        if let Some(job_id) = &self.config.job_id {
            // Verify existing job
            let job_status = client.verify_job_exists(job_id).await.map_err(|e| {
                error!("Flink job {} not found: {}", job_id, e);
                Error::InitError(format!("Job not found: {}", e))
            })?;

            if job_status.state != "RUNNING" {
                warn!(
                    "Job {} is in state {}, expected RUNNING",
                    job_id, job_status.state
                );
            }

            let mut active_job_id = self.active_job_id.write().await;
            *active_job_id = Some(job_id.clone());
            info!("Using existing Flink job: {} ({})", job_id, job_status.name);
        } else if let Some(jar_path) = &self.config.jar_path {
            // Upload and submit a new job
            info!("Uploading JAR from: {}", jar_path);
            match client.upload_jar(jar_path).await {
                Ok(jar_id) => {
                    info!("JAR uploaded successfully with ID: {}", jar_id);

                    // Submit job with the uploaded JAR
                    let job_config = serde_json::json!({
                        "entryClass": self.config.entry_class.as_ref().unwrap_or(&"org.apache.flink.streaming.examples.WordCount".to_string()),
                        "parallelism": self.config.parallelism.unwrap_or(1),
                        "programArgs": self.config.program_args.as_ref().unwrap_or(&"".to_string())
                    });

                    match client.submit_job(&jar_id, job_config).await {
                        Ok(job_id) => {
                            info!("Job submitted successfully with ID: {}", job_id);
                            let mut active_job_id = self.active_job_id.write().await;
                            *active_job_id = Some(job_id);
                        }
                        Err(e) => {
                            error!("Failed to submit job: {}", e);
                            return Err(Error::InitError(format!("Job submission failed: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to upload JAR: {}", e);
                    return Err(Error::InitError(format!("JAR upload failed: {}", e)));
                }
            }
        }

        // Start the periodic flush task if configured
        if self.config.auto_flush_interval_ms > 0 {
            let interval = Duration::from_millis(self.config.auto_flush_interval_ms);
            let sink = Arc::new(self.clone_for_task());

            tokio::spawn(async move {
                let mut interval_timer = tokio::time::interval(interval);
                loop {
                    interval_timer.tick().await;
                    if let Err(e) = sink.flush_buffer().await {
                        error!("Auto-flush failed: {}", e);
                    }
                }
            });
        }

        info!("Flink sink connector initialized successfully");
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "Flink sink {} received {} messages from {}/{}",
            self.id,
            messages.len(),
            topic_metadata.stream,
            topic_metadata.topic
        );

        let mut buffer = self.buffer.lock().await;

        for message in messages {
            // Decode message if decoder is configured, otherwise use as-is
            let mut decoded_msg = if let Some(ref decoder) = self.decoder {
                // Decode from raw bytes using configured decoder
                let raw_bytes = message.payload.try_into_vec()?;
                let decoded_payload = decoder.decode(raw_bytes)?;
                DecodedMessage {
                    id: Some(message.id),
                    timestamp: Some(message.timestamp),
                    origin_timestamp: Some(message.origin_timestamp),
                    headers: message.headers.clone(),
                    offset: Some(message.offset),
                    checksum: Some(message.checksum),
                    payload: decoded_payload,
                }
            } else {
                // Use message payload directly
                DecodedMessage {
                    id: Some(message.id),
                    timestamp: Some(message.timestamp),
                    origin_timestamp: Some(message.origin_timestamp),
                    headers: message.headers.clone(),
                    offset: Some(message.offset),
                    checksum: Some(message.checksum),
                    payload: message.payload.clone(),
                }
            };

            // Apply transforms
            let mut skip_message = false;
            for transform in &self.transforms {
                let msg_to_transform = DecodedMessage {
                    id: decoded_msg.id,
                    offset: decoded_msg.offset,
                    checksum: decoded_msg.checksum,
                    timestamp: decoded_msg.timestamp,
                    origin_timestamp: decoded_msg.origin_timestamp,
                    headers: decoded_msg.headers.clone(),
                    payload: decoded_msg.payload.clone(),
                };

                match transform.transform(topic_metadata, msg_to_transform) {
                    Ok(Some(transformed)) => decoded_msg = transformed,
                    Ok(None) => {
                        debug!("Message filtered by transform");
                        skip_message = true;
                        break;
                    }
                    Err(e) => {
                        warn!("Transform failed: {}", e);
                        if !self.config.skip_errors {
                            return Err(e);
                        }
                        skip_message = true;
                        break;
                    }
                }
            }

            if skip_message {
                continue;
            }

            // Convert to FlinkRecord after transforms
            match self.convert_decoded_message(decoded_msg).await {
                Ok(record) => {
                    buffer.push(record);

                    // Flush if buffer is full
                    if buffer.len() >= self.config.batch_size {
                        drop(buffer); // Release lock before flushing
                        self.flush_buffer().await?;
                        buffer = self.buffer.lock().await;
                    }
                }
                Err(e) => {
                    warn!("Failed to convert message: {}", e);
                    if !self.config.skip_errors {
                        return Err(e);
                    }
                }
            }
        }

        // Update state with current offset
        let mut state = self.state.write().await;
        state.last_offset = messages_metadata.current_offset;
        state.partition_id = messages_metadata.partition_id;

        // Periodically fetch job metrics if a job is active
        if state.records_processed % 10000 == 0 {
            if let Some(job_id) = self.active_job_id.read().await.as_ref() {
                let client = self.client.lock().await;
                let metrics = vec![
                    "numRecordsIn".to_string(),
                    "numRecordsOut".to_string(),
                    "numBytesIn".to_string(),
                    "numBytesOut".to_string(),
                ];

                match client.get_job_metrics(job_id, metrics).await {
                    Ok(metrics_data) => {
                        debug!("Job {} metrics: {:?}", job_id, metrics_data);
                    }
                    Err(e) => {
                        debug!("Failed to fetch job metrics: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing Flink sink connector with ID: {}", self.id);

        // Final flush of any remaining messages
        self.flush_buffer().await?;

        // Cancel job if we created one
        let active_job_id = self.active_job_id.read().await;
        if let Some(job_id) = active_job_id.as_ref() {
            if self.config.jar_path.is_some() {
                // Only cancel if we created the job
                let client = self.client.lock().await;
                match client.cancel_job(job_id).await {
                    Ok(_) => info!("Successfully cancelled Flink job: {}", job_id),
                    Err(e) => warn!("Failed to cancel Flink job {}: {}", job_id, e),
                }
            }
        }

        // Save final checkpoint if enabled
        if self.config.enable_checkpointing {
            if let Some(job_id) = &self.config.job_id {
                let client = self.client.lock().await;
                client.trigger_savepoint(job_id, None).await.map_err(|e| {
                    error!("Failed to create final savepoint: {}", e);
                    Error::HttpRequestFailed(e.to_string())
                })?;
            }
        }

        let state = self.state.read().await;
        info!(
            "Flink sink closed. Total records processed: {}",
            state.records_processed
        );

        Ok(())
    }
}

impl FlinkSink {
    fn clone_for_task(&self) -> Self {
        FlinkSink {
            id: self.id,
            config: self.config.clone(),
            client: Arc::clone(&self.client),
            state: self.state.clone(),
            buffer: self.buffer.clone(),
            active_job_id: self.active_job_id.clone(),
            transforms: self.transforms.clone(),
            decoder: self
                .config
                .input_schema
                .as_ref()
                .map(|schema| schema.decoder()),
            encoder: self
                .config
                .output_schema
                .as_ref()
                .map(|schema| schema.encoder()),
        }
    }
}

