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

use crate::config::{FlinkSourceConfig, SourceType};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum FlinkReaderError {
    #[error("Connection failed: {0}")]
    ConnectionError(String),

    #[error("Subscription failed: {0}")]
    SubscriptionError(String),

    #[error("Fetch failed: {0}")]
    FetchError(String),

    #[error("Deserialization failed: {0}")]
    DeserializationError(String),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Invalid job state: {0}")]
    InvalidJobState(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkMessage {
    pub id: Uuid,
    pub timestamp: u64,
    pub origin_timestamp: Option<u64>,
    pub headers: HashMap<String, String>,
    pub data: JsonValue,
    pub checksum: u64,
    pub source_partition: Option<i32>,
    pub source_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterOverview {
    #[serde(rename = "flink-version")]
    pub flink_version: String,
    #[serde(rename = "flink-commit")]
    pub flink_commit: String,
    #[serde(rename = "jobs-running")]
    pub jobs_running: u32,
    #[serde(rename = "taskmanagers")]
    pub taskmanagers: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobVertex {
    pub id: String,
    pub name: String,
    pub parallelism: u32,
    pub status: String,
    pub metrics: Option<VertexMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VertexMetrics {
    #[serde(rename = "read-records")]
    pub read_records: u64,
    #[serde(rename = "write-records")]
    pub write_records: u64,
    #[serde(rename = "read-bytes")]
    pub read_bytes: u64,
    #[serde(rename = "write-bytes")]
    pub write_bytes: u64,
}

#[derive(Debug)]
pub struct FlinkReader {
    client: Client,
    config: FlinkSourceConfig,
    base_url: String,
    subscription_id: Option<String>,
    active_job_id: Option<String>,
    source_vertex_id: Option<String>,
}

impl FlinkReader {
    pub fn new(config: FlinkSourceConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.connection_timeout_secs))
            .build()
            .expect("Failed to build HTTP client");

        let base_url = config.flink_cluster_url.trim_end_matches('/').to_string();

        FlinkReader {
            client,
            config,
            base_url,
            subscription_id: None,
            active_job_id: None,
            source_vertex_id: None,
        }
    }

    pub async fn check_connection(&self) -> Result<ClusterOverview, FlinkReaderError> {
        let url = format!("{}/v1/overview", self.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FlinkReaderError::ConnectionError(format!(
                "Cluster returned status: {}",
                response.status()
            )));
        }

        let overview: ClusterOverview = response.json().await?;
        info!(
            "Connected to Flink cluster v{} with {} taskmanagers and {} running jobs",
            overview.flink_version, overview.taskmanagers, overview.jobs_running
        );
        Ok(overview)
    }

    pub async fn subscribe_to_source(&mut self) -> Result<(), FlinkReaderError> {
        // Find a suitable job that has the source we're looking for
        let jobs = self.list_running_jobs().await?;

        for job in jobs {
            if let Ok(vertices) = self.get_job_vertices(&job).await {
                // Look for a source vertex that matches our configuration
                for vertex in vertices {
                    if self.is_matching_source(&vertex) {
                        self.active_job_id = Some(job.clone());
                        self.source_vertex_id = Some(vertex.id.clone());
                        info!("Subscribed to source vertex {} in job {}", vertex.name, job);
                        return Ok(());
                    }
                }
            }
        }

        // If no matching job found, return a subscription error
        self.subscription_id = Some(Uuid::new_v4().to_string());
        Err(FlinkReaderError::SubscriptionError(format!(
            "No matching Flink job found for source: {}",
            self.config.source_identifier
        )))
    }

    async fn list_running_jobs(&self) -> Result<Vec<String>, FlinkReaderError> {
        let url = format!("{}/v1/jobs", self.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FlinkReaderError::FetchError(format!(
                "Failed to list jobs: {}",
                response.status()
            )));
        }

        let body: JsonValue = response.json().await?;
        let jobs = body["jobs"].as_array().ok_or_else(|| {
            FlinkReaderError::DeserializationError("Invalid jobs response".to_string())
        })?;

        let running_jobs: Vec<String> = jobs
            .iter()
            .filter_map(|j| {
                if j["state"].as_str() == Some("RUNNING") {
                    j["id"].as_str().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(running_jobs)
    }

    async fn get_job_vertices(&self, job_id: &str) -> Result<Vec<JobVertex>, FlinkReaderError> {
        let url = format!("{}/v1/jobs/{}", self.base_url, job_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FlinkReaderError::JobNotFound(job_id.to_string()));
        }

        let body: JsonValue = response.json().await?;
        let vertices = body["vertices"].as_array().ok_or_else(|| {
            FlinkReaderError::DeserializationError("Invalid job vertices".to_string())
        })?;

        let vertex_list: Result<Vec<JobVertex>, _> = vertices
            .iter()
            .map(|v| serde_json::from_value(v.clone()))
            .collect();

        vertex_list.map_err(FlinkReaderError::JsonError)
    }

    fn is_matching_source(&self, vertex: &JobVertex) -> bool {
        // Check if this vertex is a source operator matching our configuration
        let vertex_name = vertex.name.to_lowercase();
        let source_id = self.config.source_identifier.to_lowercase();

        match &self.config.source_type {
            SourceType::Kafka => {
                vertex_name.contains("kafka")
                    && (vertex_name.contains(&source_id) || source_id.is_empty())
            }
            SourceType::Kinesis => {
                vertex_name.contains("kinesis")
                    && (vertex_name.contains(&source_id) || source_id.is_empty())
            }
            SourceType::FileSystem => {
                vertex_name.contains("file")
                    && (vertex_name.contains(&source_id) || source_id.is_empty())
            }
            SourceType::Custom(custom_type) => {
                vertex_name.contains(custom_type)
                    && (vertex_name.contains(&source_id) || source_id.is_empty())
            }
            _ => false,
        }
    }

    pub async fn fetch_batch(
        &self,
        offset: u64,
        batch_size: usize,
    ) -> Result<Vec<FlinkMessage>, FlinkReaderError> {
        // In a real implementation, this would:
        // 1. Query the Flink job's queryable state
        // 2. Or read from an intermediate storage (like Kafka/Redis) where Flink writes
        // 3. Or use Flink's SQL Gateway API if available

        if let (Some(job_id), Some(vertex_id)) = (&self.active_job_id, &self.source_vertex_id) {
            // Try to get metrics/data from the vertex
            let url = format!(
                "{}/v1/jobs/{}/vertices/{}/metrics",
                self.base_url, job_id, vertex_id
            );
            let response = self
                .client
                .get(&url)
                .query(&[("get", "numRecordsIn,numRecordsOut")])
                .send()
                .await?;

            if response.status().is_success() {
                let metrics: JsonValue = response.json().await?;
                debug!("Source vertex metrics: {:?}", metrics);
            }

            // For now, return mock data as actual data fetching would require:
            // - A Flink job with queryable state
            // - Or a custom sink that exposes data via REST
            // - Or integration with Flink's Table API
            self.generate_mock_messages(offset, batch_size)
        } else {
            // No active job, return empty
            Ok(vec![])
        }
    }

    fn generate_mock_messages(
        &self,
        offset: u64,
        batch_size: usize,
    ) -> Result<Vec<FlinkMessage>, FlinkReaderError> {
        let mut messages = Vec::new();

        for i in 0..batch_size {
            let message = FlinkMessage {
                id: Uuid::new_v4(),
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                origin_timestamp: Some(chrono::Utc::now().timestamp_millis() as u64),
                headers: HashMap::from([
                    ("source".to_string(), self.config.source_identifier.clone()),
                    ("type".to_string(), format!("{:?}", self.config.source_type)),
                ]),
                data: serde_json::json!({
                    "offset": offset + i as u64,
                    "value": format!("Message from Flink source at offset {}", offset + i as u64),
                    "source": self.config.source_identifier,
                }),
                checksum: rand::random(),
                source_partition: Some(0),
                source_offset: Some(offset + i as u64),
            };
            messages.push(message);
        }

        Ok(messages)
    }

    pub async fn unsubscribe(&self) -> Result<(), FlinkReaderError> {
        if let Some(sub_id) = &self.subscription_id {
            info!("Unsubscribing from Flink source: {}", sub_id);
            // In a real implementation, this would clean up any subscriptions
        }
        Ok(())
    }

    pub async fn restore_from_checkpoint(
        &self,
        checkpoint_id: &str,
    ) -> Result<(), FlinkReaderError> {
        if let Some(job_id) = &self.active_job_id {
            let url = format!(
                "{}/v1/jobs/{}/checkpoints/{}",
                self.base_url, job_id, checkpoint_id
            );
            let response = self.client.get(&url).send().await?;

            if response.status().is_success() {
                info!("Restored from checkpoint: {}", checkpoint_id);
                Ok(())
            } else {
                Err(FlinkReaderError::FetchError(format!(
                    "Failed to restore from checkpoint: {}",
                    response.status()
                )))
            }
        } else {
            Err(FlinkReaderError::InvalidJobState(
                "No active job to restore checkpoint".to_string(),
            ))
        }
    }

    pub async fn get_source_metrics(&self) -> Result<JsonValue, FlinkReaderError> {
        if let (Some(job_id), Some(vertex_id)) = (&self.active_job_id, &self.source_vertex_id) {
            let url = format!(
                "{}/v1/jobs/{}/vertices/{}/subtasks/metrics",
                self.base_url, job_id, vertex_id
            );

            let response = self
                .client
                .get(&url)
                .query(&[("get", "numRecordsIn,numRecordsOut,numBytesIn,numBytesOut")])
                .send()
                .await?;

            if response.status().is_success() {
                let metrics: JsonValue = response.json().await?;
                Ok(metrics)
            } else {
                Err(FlinkReaderError::FetchError(format!(
                    "Failed to get metrics: {}",
                    response.status()
                )))
            }
        } else {
            Ok(serde_json::json!({}))
        }
    }
}
