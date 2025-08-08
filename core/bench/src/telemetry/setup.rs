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

use super::{BenchMetrics, MetricsHandle, buffer};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

pub struct TelemetryConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub export_interval: Duration,
    pub export_timeout: Duration,
    pub buffer_size: usize,
    pub buffer_flush_interval: Duration,
    pub service_name: String,
    pub service_version: String,
    pub benchmark_id: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".to_string(),
            export_interval: Duration::from_secs(1),
            export_timeout: Duration::from_secs(10),
            buffer_size: 10000,
            buffer_flush_interval: Duration::from_millis(100),
            service_name: "iggy-bench".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            benchmark_id: String::new(), // Will be set when creating config
        }
    }
}

pub struct TelemetryContext {
    pub metrics: Arc<BenchMetrics>,
    pub buffer: Arc<buffer::MetricsBuffer>,
    processor_handle: Option<tokio::task::JoinHandle<()>>,
    stats_handle: Option<tokio::task::JoinHandle<()>>,
    meter_provider: Arc<Mutex<Option<SdkMeterProvider>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TelemetryContext {
    // Async for API consistency with future async operations
    #[allow(clippy::unused_async)]
    #[allow(clippy::cognitive_complexity)]
    pub async fn init(config: TelemetryConfig) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        if !config.enabled {
            info!("OpenTelemetry metrics disabled");
            return Ok(None);
        }

        info!(
            "Initializing OpenTelemetry metrics with endpoint: {}",
            config.endpoint
        );

        // Verify connection to OTLP endpoint with retries (up to 10 seconds)
        Self::verify_connection_with_retry(&config.endpoint, 10, Duration::from_secs(1)).await?;

        // Create resource with benchmark metadata
        let resource = Resource::builder()
            .with_attribute(KeyValue::new("service.name", config.service_name.clone()))
            .with_attribute(KeyValue::new(
                "service.version",
                config.service_version.clone(),
            ))
            .with_attribute(KeyValue::new("benchmark.id", config.benchmark_id.clone()))
            .with_attribute(KeyValue::new("telemetry.sdk.name", "opentelemetry"))
            .with_attribute(KeyValue::new("telemetry.sdk.language", "rust"))
            .build();

        // Configure OTLP exporter
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.export_timeout)
            .build()?;

        // Create periodic reader for metrics export
        let reader = PeriodicReader::builder(exporter)
            .with_interval(config.export_interval)
            .build();

        // Build meter provider
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        // Set as global provider
        global::set_meter_provider(meter_provider.clone());

        // Get meter and create metrics
        let meter = global::meter("iggy-bench");
        let metrics = Arc::new(BenchMetrics::new(&meter));

        // Create metrics buffer
        let (buffer, processor) =
            buffer::MetricsBuffer::new(config.buffer_size, config.buffer_flush_interval);
        let buffer = Arc::new(buffer);

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        // Start processor task with graceful shutdown
        let metrics_clone = metrics.clone();
        let processor_handle = tokio::spawn(async move {
            processor.run(metrics_clone).await;
        });

        // Start buffer stats reporter with graceful shutdown
        let buffer_clone = buffer.clone();
        let stats_handle = tokio::spawn(async move {
            let reporter = buffer::BufferStatsReporter::new(buffer_clone, Duration::from_secs(10));
            tokio::select! {
                _ = reporter.run() => {},
                _ = &mut shutdown_rx => {
                    info!("Stats reporter received shutdown signal");
                }
            }
        });

        info!("OpenTelemetry metrics initialized successfully");

        Ok(Some(Self {
            metrics,
            buffer,
            processor_handle: Some(processor_handle),
            stats_handle: Some(stats_handle),
            meter_provider: Arc::new(Mutex::new(Some(meter_provider))),
            shutdown_tx: Some(shutdown_tx),
        }))
    }

    pub fn create_handle(&self, actor_type: &str, actor_id: u32, stream_id: u32) -> MetricsHandle {
        MetricsHandle::new(
            self.metrics.clone(),
            self.buffer.clone(),
            actor_type,
            actor_id,
            stream_id,
        )
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn shutdown(mut self) {
        info!("Shutting down OpenTelemetry metrics");

        // Signal shutdown to background tasks
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // First, close the buffer to signal no more events
        // This is done by dropping the buffer which drops the sender
        drop(self.buffer);

        // Wait for processor to finish processing remaining events
        if let Some(handle) = self.processor_handle.take() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {
                    info!("Metrics processor shut down cleanly");
                }
                Ok(Err(e)) => {
                    error!("Metrics processor task failed: {}", e);
                }
                Err(_) => {
                    warn!("Metrics processor shutdown timed out after 5 seconds");
                }
            }
        }

        // Wait for stats reporter to finish
        if let Some(handle) = self.stats_handle.take() {
            // Stats reporter should exit quickly due to shutdown signal
            match tokio::time::timeout(Duration::from_secs(1), handle).await {
                Ok(Ok(())) => {
                    info!("Stats reporter shut down cleanly");
                }
                Ok(Err(e)) if !e.is_cancelled() => {
                    error!("Stats reporter task failed: {}", e);
                }
                _ => {
                    // Task was cancelled or timed out, which is fine
                }
            }
        }

        // Shutdown meter provider with mutex protection
        let mut provider_guard = self.meter_provider.lock().unwrap();
        if let Some(provider) = provider_guard.take() {
            // Force a final flush before shutdown
            if let Err(e) = provider.force_flush() {
                error!("Error flushing meter provider: {}", e);
            }

            // Shutdown the provider
            if let Err(e) = provider.shutdown() {
                error!("Error shutting down meter provider: {}", e);
            }

            // Clear the global meter provider
            // Note: OpenTelemetry doesn't provide a way to unset the global provider,
            // but shutting it down ensures it won't accept new metrics
        }
        drop(provider_guard);

        info!("OpenTelemetry metrics shutdown complete");
    }

    /// Verify that we can connect to the OTLP endpoint
    async fn verify_connection_with_retry(
        endpoint: &str,
        max_attempts: u32,
        retry_delay: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Verifying connection to OTLP endpoint: {}", endpoint);

        for attempt in 1..=max_attempts {
            match Self::test_otlp_connection(endpoint).await {
                Ok(()) => {
                    info!("Successfully connected to OTLP endpoint");
                    return Ok(());
                }
                Err(e) if attempt < max_attempts => {
                    warn!(
                        "Failed to connect to OTLP endpoint (attempt {}/{}): {}",
                        attempt, max_attempts, e
                    );
                    tokio::time::sleep(retry_delay).await;
                }
                Err(e) => {
                    error!(
                        "Failed to connect to OTLP endpoint after {} attempts: {}",
                        max_attempts, e
                    );
                    return Err(format!(
                        "Cannot connect to OpenTelemetry collector at {}. \
                        Please ensure the collector is running and accessible.",
                        endpoint
                    )
                    .into());
                }
            }
        }
        unreachable!()
    }

    /// Test connection to OTLP endpoint using TCP connection test
    async fn test_otlp_connection(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
        use tokio::net::TcpStream;
        use tokio::time::timeout;

        // Parse the endpoint URL to extract host and port
        let endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");

        // Default OTLP gRPC port is 4317
        let addr = if endpoint.contains(':') {
            endpoint.to_string()
        } else {
            format!("{}:4317", endpoint)
        };

        // Try to establish TCP connection to verify the endpoint is reachable
        match timeout(Duration::from_secs(2), TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => {
                // Connection successful - endpoint is reachable
                Ok(())
            }
            Ok(Err(e)) => Err(format!("Cannot connect to OTLP endpoint at {}: {}", addr, e).into()),
            Err(_) => Err(format!("Connection to OTLP endpoint at {} timed out", addr).into()),
        }
    }
}
