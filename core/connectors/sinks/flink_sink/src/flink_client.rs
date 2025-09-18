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

use crate::config::{AuthType, FlinkSinkConfig, SinkType};
use reqwest::{multipart, Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum FlinkClientError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Cluster unavailable")]
    ClusterUnavailable,

    #[error("Authentication failed")]
    #[allow(dead_code)]
    AuthenticationFailed,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Job submission failed: {0}")]
    JobSubmissionFailed(String),

    #[error("JAR upload failed: {0}")]
    JarUploadFailed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkRecord {
    pub id: String,
    pub timestamp: u64,
    pub headers: HashMap<String, String>,
    pub payload: JsonValue,
    pub offset: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatus {
    pub jid: String,
    pub name: String,
    pub state: String,
    #[serde(rename = "start-time")]
    pub start_time: i64,
    #[serde(rename = "end-time")]
    pub end_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JarUploadResponse {
    pub filename: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSubmitResponse {
    pub jobid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterOverview {
    #[serde(rename = "flink-version")]
    pub flink_version: String,
    #[serde(rename = "flink-commit")]
    pub flink_commit: String,
    #[serde(rename = "jobs-running")]
    pub jobs_running: u32,
    #[serde(rename = "jobs-finished")]
    pub jobs_finished: u32,
    #[serde(rename = "jobs-cancelled")]
    pub jobs_cancelled: u32,
    #[serde(rename = "jobs-failed")]
    pub jobs_failed: u32,
    #[serde(rename = "taskmanagers")]
    pub taskmanagers: u32,
    #[serde(rename = "slots-total")]
    pub slots_total: u32,
    #[serde(rename = "slots-available")]
    pub slots_available: u32,
}

#[derive(Debug, Clone)]
pub struct FlinkClient {
    client: Client,
    config: FlinkSinkConfig,
    base_url: String,
    jar_id: Option<String>,
}

impl FlinkClient {
    pub fn new(config: FlinkSinkConfig) -> Self {
        let mut client_builder =
            Client::builder().timeout(Duration::from_secs(config.connection_timeout_secs));

        // Configure TLS if enabled
        if let Some(tls) = &config.tls {
            if tls.enabled {
                client_builder = client_builder.danger_accept_invalid_certs(!tls.verify_hostname);
            }
        }

        let client = client_builder.build().expect("Failed to build HTTP client");

        let base_url = config.flink_cluster_url.trim_end_matches('/').to_string();

        FlinkClient {
            client,
            config,
            base_url,
            jar_id: None,
        }
    }

    pub async fn check_cluster_health(&self) -> Result<ClusterOverview, FlinkClientError> {
        let url = format!("{}/v1/overview", self.base_url);
        let response = self.make_request(reqwest::Method::GET, &url, None).await?;

        if response.status().is_success() {
            let overview: ClusterOverview = response.json().await?;
            info!(
                "Flink cluster is healthy - version: {}, running jobs: {}, available slots: {}/{}",
                overview.flink_version,
                overview.jobs_running,
                overview.slots_available,
                overview.slots_total
            );
            Ok(overview)
        } else {
            error!("Flink cluster health check failed: {}", response.status());
            Err(FlinkClientError::ClusterUnavailable)
        }
    }

    pub async fn verify_job_exists(&self, job_id: &str) -> Result<JobStatus, FlinkClientError> {
        let url = format!("{}/v1/jobs/{}", self.base_url, job_id);
        let response = self.make_request(reqwest::Method::GET, &url, None).await?;

        if response.status() == 404 {
            return Err(FlinkClientError::JobNotFound(job_id.to_string()));
        }

        if !response.status().is_success() {
            return Err(FlinkClientError::InvalidResponse(format!(
                "Job verification failed: {}",
                response.status()
            )));
        }

        let job_status: JobStatus = response.json().await?;
        info!("Job {} exists with state: {}", job_id, job_status.state);
        Ok(job_status)
    }

    pub async fn upload_jar(&mut self, jar_path: &str) -> Result<String, FlinkClientError> {
        let url = format!("{}/v1/jars/upload", self.base_url);

        // Read the JAR file
        let mut file = File::open(jar_path).await.map_err(|e| {
            FlinkClientError::JarUploadFailed(format!("Failed to open JAR file: {}", e))
        })?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await.map_err(|e| {
            FlinkClientError::JarUploadFailed(format!("Failed to read JAR file: {}", e))
        })?;

        // Create multipart form
        let file_name = std::path::Path::new(jar_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("connector.jar");

        let part = multipart::Part::bytes(buffer)
            .file_name(file_name.to_string())
            .mime_str("application/x-java-archive")
            .map_err(|e| FlinkClientError::JarUploadFailed(format!("Invalid MIME type: {}", e)))?;

        let form = multipart::Form::new().part("jarfile", part);

        // Upload the JAR
        let response = self.client.post(&url).multipart(form).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(FlinkClientError::JarUploadFailed(format!(
                "Upload failed with status {}: {}",
                status, body
            )));
        }

        let upload_response: JarUploadResponse = response.json().await?;
        self.jar_id = Some(upload_response.filename.clone());
        info!("Successfully uploaded JAR: {}", upload_response.filename);
        Ok(upload_response.filename)
    }

    pub async fn submit_job(
        &self,
        jar_id: &str,
        job_config: JsonValue,
    ) -> Result<String, FlinkClientError> {
        let url = format!("{}/v1/jars/{}/run", self.base_url, jar_id);

        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("allowNonRestoredState".to_string(), "false".to_string());

        // Extract parameters from job_config JSON
        if let Some(entry_class) = job_config["entryClass"].as_str() {
            params.insert("entry-class".to_string(), entry_class.to_string());
        }

        if let Some(parallelism) = job_config["parallelism"].as_u64() {
            params.insert("parallelism".to_string(), parallelism.to_string());
        }

        if let Some(program_args) = job_config["programArgs"].as_str() {
            if !program_args.is_empty() {
                params.insert("program-args".to_string(), program_args.to_string());
            }
        }

        if let Some(savepoint_path) = job_config["savepointPath"].as_str() {
            params.insert("savepointPath".to_string(), savepoint_path.to_string());
        }

        let response = self
            .make_request_with_params(reqwest::Method::POST, &url, None, params)
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(FlinkClientError::JobSubmissionFailed(format!(
                "Job submission failed with status {}: {}",
                status, body
            )));
        }

        let submit_response: JobSubmitResponse = response.json().await?;
        info!("Successfully submitted job: {}", submit_response.jobid);
        Ok(submit_response.jobid)
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobStatus>, FlinkClientError> {
        let url = format!("{}/v1/jobs", self.base_url);
        let response = self.make_request(reqwest::Method::GET, &url, None).await?;

        if !response.status().is_success() {
            return Err(FlinkClientError::InvalidResponse(format!(
                "Failed to list jobs: {}",
                response.status()
            )));
        }

        let body: JsonValue = response.json().await?;
        if let Some(jobs) = body["jobs"].as_array() {
            let job_list: Result<Vec<JobStatus>, _> = jobs
                .iter()
                .map(|j| serde_json::from_value(j.clone()))
                .collect();
            return job_list.map_err(FlinkClientError::SerializationError);
        }

        Ok(vec![])
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<(), FlinkClientError> {
        let url = format!("{}/v1/jobs/{}", self.base_url, job_id);
        let response = self
            .make_request(
                reqwest::Method::PATCH,
                &url,
                Some(&serde_json::json!({"state": "CANCELLED"})),
            )
            .await?;

        if !response.status().is_success() {
            return Err(FlinkClientError::InvalidResponse(format!(
                "Failed to cancel job: {}",
                response.status()
            )));
        }

        info!("Successfully cancelled job: {}", job_id);
        Ok(())
    }

    pub async fn send_batch(&self, records: Vec<FlinkRecord>) -> Result<(), FlinkClientError> {
        let endpoint = self.get_sink_endpoint();
        let batch_payload = self.prepare_batch_payload(records)?;

        let mut retries = 0;
        while retries <= self.config.max_retries {
            match self.send_batch_internal(&endpoint, &batch_payload).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if retries == self.config.max_retries {
                        return Err(e);
                    }
                    retries += 1;
                    debug!(
                        "Retry {}/{} after error: {}",
                        retries, self.config.max_retries, e
                    );
                    tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
                }
            }
        }

        Ok(())
    }

    async fn send_batch_internal(
        &self,
        endpoint: &str,
        payload: &JsonValue,
    ) -> Result<(), FlinkClientError> {
        let response = self
            .make_request(reqwest::Method::POST, endpoint, Some(payload))
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(FlinkClientError::InvalidResponse(format!(
                "Failed to send batch: {} - {}",
                status, body
            )));
        }

        Ok(())
    }

    pub async fn trigger_checkpoint(&self, job_id: &str) -> Result<String, FlinkClientError> {
        let url = format!("{}/v1/jobs/{}/checkpoints", self.base_url, job_id);

        // Trigger checkpoint using proper API format
        let response = self
            .make_request(
                reqwest::Method::POST,
                &url,
                Some(&serde_json::json!({
                    "checkpointType": "FULL"
                })),
            )
            .await?;

        if response.status() == 202 {
            // Checkpoint triggered successfully
            let body: JsonValue = response.json().await?;
            if let Some(request_id) = body["request-id"].as_str() {
                info!(
                    "Checkpoint triggered for job {} with request-id: {}",
                    job_id, request_id
                );
                return Ok(request_id.to_string());
            }
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("Failed to trigger checkpoint: {} - {}", status, body);
        }

        Ok(String::new())
    }

    pub async fn get_job_metrics(
        &self,
        job_id: &str,
        metrics: Vec<String>,
    ) -> Result<JsonValue, FlinkClientError> {
        let mut params = HashMap::new();
        params.insert("get".to_string(), metrics.join(","));

        let response = self
            .make_request_with_params(
                reqwest::Method::GET,
                &format!("{}/v1/jobs/{}/metrics", self.base_url, job_id),
                None,
                params,
            )
            .await?;

        let body: JsonValue = response.json().await?;
        Ok(body)
    }

    pub async fn trigger_savepoint(
        &self,
        job_id: &str,
        target_directory: Option<&str>,
    ) -> Result<String, FlinkClientError> {
        let url = format!("{}/v1/jobs/{}/savepoints", self.base_url, job_id);
        let target_dir = target_directory.unwrap_or("/tmp/savepoints");

        let response = self
            .make_request(
                reqwest::Method::POST,
                &url,
                Some(&serde_json::json!({
                    "target-directory": target_dir,
                    "cancel-job": false
                })),
            )
            .await?;

        let status = response.status();
        if status == 202 {
            // Savepoint triggered successfully
            let body: JsonValue = response.json().await?;
            if let Some(request_id) = body["request-id"].as_str() {
                info!(
                    "Savepoint triggered for job {} with request-id: {}",
                    job_id, request_id
                );

                // Poll for completion
                let status_url = format!(
                    "{}/v1/jobs/{}/savepoints/{}",
                    self.base_url, job_id, request_id
                );
                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let status_response = self
                        .make_request(reqwest::Method::GET, &status_url, None)
                        .await?;

                    if status_response.status().is_success() {
                        let status_body: JsonValue = status_response.json().await?;
                        if let Some(status) = status_body["status"]["id"].as_str() {
                            if status == "COMPLETED" {
                                if let Some(location) =
                                    status_body["operation"]["location"].as_str()
                                {
                                    info!("Savepoint completed at: {}", location);
                                    return Ok(location.to_string());
                                }
                            } else if status == "FAILED" {
                                let failure_cause = status_body["operation"]["failure-cause"]
                                    .as_str()
                                    .unwrap_or("Unknown error");
                                return Err(FlinkClientError::InvalidResponse(format!(
                                    "Savepoint failed: {}",
                                    failure_cause
                                )));
                            }
                        }
                    }
                }

                return Err(FlinkClientError::InvalidResponse(
                    "Savepoint operation timed out".to_string(),
                ));
            }
        }

        Err(FlinkClientError::InvalidResponse(format!(
            "Failed to trigger savepoint: {}",
            status
        )))
    }

    fn get_sink_endpoint(&self) -> String {
        match &self.config.sink_type {
            SinkType::Kafka => {
                format!("{}/v1/kafka/produce/{}", self.base_url, self.config.target)
            }
            SinkType::Jdbc => {
                format!("{}/v1/jdbc/insert/{}", self.base_url, self.config.target)
            }
            SinkType::Elasticsearch => {
                format!(
                    "{}/v1/elasticsearch/index/{}",
                    self.base_url, self.config.target
                )
            }
            SinkType::Custom(endpoint) => {
                format!("{}/{}", self.base_url, endpoint)
            }
            _ => {
                format!("{}/v1/sink/{}", self.base_url, self.config.target)
            }
        }
    }

    fn prepare_batch_payload(
        &self,
        records: Vec<FlinkRecord>,
    ) -> Result<JsonValue, FlinkClientError> {
        let payload = match &self.config.sink_type {
            SinkType::Kafka => {
                serde_json::json!({
                    "records": records.into_iter().map(|r| {
                        serde_json::json!({
                            "key": r.id,
                            "value": r.payload,
                            "headers": r.headers,
                            "timestamp": r.timestamp
                        })
                    }).collect::<Vec<_>>()
                })
            }
            SinkType::Jdbc => {
                serde_json::json!({
                    "table": self.config.target,
                    "rows": records.into_iter().map(|r| r.payload).collect::<Vec<_>>()
                })
            }
            SinkType::Elasticsearch => {
                serde_json::json!({
                    "index": self.config.target,
                    "documents": records.into_iter().map(|r| {
                        serde_json::json!({
                            "_id": r.id,
                            "_source": r.payload
                        })
                    }).collect::<Vec<_>>()
                })
            }
            _ => {
                serde_json::json!({
                    "data": records
                })
            }
        };

        Ok(payload)
    }

    async fn make_request_with_params(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&JsonValue>,
        params: HashMap<String, String>,
    ) -> Result<Response, FlinkClientError> {
        let mut request = self.client.request(method, url);

        // Add query parameters
        if !params.is_empty() {
            request = request.query(&params);
        }

        self.configure_request(request, body).await
    }

    async fn make_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<&JsonValue>,
    ) -> Result<Response, FlinkClientError> {
        let request = self.client.request(method, url);
        self.configure_request(request, body).await
    }

    async fn configure_request(
        &self,
        mut request: reqwest::RequestBuilder,
        body: Option<&JsonValue>,
    ) -> Result<Response, FlinkClientError> {
        // Add authentication headers if configured
        if let Some(auth) = &self.config.auth {
            request = match &auth.auth_type {
                AuthType::Basic => {
                    if let (Some(user), Some(pass)) = (&auth.username, &auth.password) {
                        request.basic_auth(user, Some(pass))
                    } else {
                        request
                    }
                }
                AuthType::Bearer => {
                    if let Some(token) = &auth.token {
                        request.bearer_auth(token)
                    } else {
                        request
                    }
                }
                _ => request,
            };
        }

        // Add custom headers from properties
        for (key, value) in &self.config.properties {
            if key.starts_with("header.") {
                let header_name = key.strip_prefix("header.").unwrap();
                request = request.header(header_name, value);
            }
        }

        // Add body if provided
        if let Some(body) = body {
            request = request.json(body);
        }

        Ok(request.send().await?)
    }
}
