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

use elasticsearch::{Elasticsearch, http::transport::Transport, indices::IndicesCreateParts};
use iggy_connector_elasticsearch_source::{
    ElasticsearchSource, ElasticsearchSourceConfig, StateConfig,
};
use serde_json::json;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::info;

const ES_URL: &str = "http://localhost:9200";
const PERF_TEST_INDEX: &str = "perf_test_logs";

struct PerformanceTestEnvironment {
    client: Elasticsearch,
    temp_dir: TempDir,
}

impl PerformanceTestEnvironment {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let transport = Transport::single_node(ES_URL)?;
        let client = Elasticsearch::new(transport);
        
        // Wait for ES to be available
        for _ in 0..30 {
            if client.ping().send().await.is_ok() {
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        
        let temp_dir = tempfile::tempdir()?;
        
        Ok(Self { client, temp_dir })
    }
    
    async fn setup_performance_test_index(&self) -> Result<(), Box<dyn std::error::Error>> {
        let index_config = json!({
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "@timestamp": { "type": "date" },
                    "message": { "type": "text" },
                    "level": { "type": "keyword" },
                    "service": { "type": "keyword" },
                    "user_id": { "type": "keyword" }
                }
            }
        });
        
        self.client
            .indices()
            .create(IndicesCreateParts::Index(PERF_TEST_INDEX))
            .body(index_config)
            .send()
            .await?;
        
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }
    
    async fn insert_performance_test_data(&self, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        let batch_size = 1000;
        let mut bulk_data = String::new();
        
        for i in 0..count {
            let timestamp = chrono::Utc::now() + chrono::Duration::seconds(i as i64);
            let doc = json!({
                "@timestamp": timestamp.to_rfc3339(),
                "message": format!("Performance test message {}", i),
                "level": if i % 5 == 0 { "ERROR" } else { "INFO" },
                "service": format!("service-{}", i % 10),
                "user_id": format!("user-{}", i % 100)
            });
            
            bulk_data.push_str(&format!("{{ \"index\": {{ \"_index\": \"{}\" }} }}\n", PERF_TEST_INDEX));
            bulk_data.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
            
            if (i + 1) % batch_size == 0 {
                self.client
                    .bulk(elasticsearch::BulkParts::None)
                    .body(bulk_data.clone())
                    .send()
                    .await?;
                bulk_data.clear();
            }
        }
        
        if !bulk_data.is_empty() {
            self.client
                .bulk(elasticsearch::BulkParts::None)
                .body(bulk_data)
                .send()
                .await?;
        }
        
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }
    
    fn create_performance_test_config(&self, state_enabled: bool) -> ElasticsearchSourceConfig {
        let state_config = if state_enabled {
            Some(StateConfig {
                enabled: true,
                storage_type: Some("file".to_string()),
                storage_config: Some(json!({
                    "base_path": self.temp_dir.path().to_string_lossy()
                })),
                state_id: Some("perf_test_connector".to_string()),
                auto_save_interval: Some("5s".to_string()),
                tracked_fields: Some(vec![
                    "last_poll_timestamp".to_string(),
                    "total_documents_fetched".to_string(),
                ]),
            })
        } else {
            None
        };
        
        ElasticsearchSourceConfig {
            url: ES_URL.to_string(),
            index: PERF_TEST_INDEX.to_string(),
            username: None,
            password: None,
            query: Some(json!({ "match_all": {} })),
            polling_interval: Some("1s".to_string()),
            batch_size: Some(100),
            timestamp_field: Some("@timestamp".to_string()),
            scroll_timeout: Some("1m".to_string()),
            state: state_config,
        }
    }
}

#[tokio::test]
async fn test_performance_without_state_management() -> Result<(), Box<dyn std::error::Error>> {
    let env = PerformanceTestEnvironment::new().await?;
    env.setup_performance_test_index().await?;
    env.insert_performance_test_data(1000).await?;
    
    let config = env.create_performance_test_config(false);
    let mut connector = ElasticsearchSource::new(100, config);
    
    connector.open().await?;
    
    let start_time = Instant::now();
    let mut total_messages = 0;
    let mut poll_count = 0;
    
    // Run for 10 seconds
    while start_time.elapsed() < Duration::from_secs(10) {
        let messages = connector.poll().await?;
        total_messages += messages.messages.len();
        poll_count += 1;
    }
    
    let elapsed = start_time.elapsed();
    let messages_per_second = total_messages as f64 / elapsed.as_secs_f64();
    let polls_per_second = poll_count as f64 / elapsed.as_secs_f64();
    
    info!("Performance without state management:");
    info!("  Total messages: {}", total_messages);
    info!("  Total polls: {}", poll_count);
    info!("  Elapsed time: {:?}", elapsed);
    info!("  Messages per second: {:.2}", messages_per_second);
    info!("  Polls per second: {:.2}", polls_per_second);
    
    connector.close().await?;
    
    // Basic performance assertions
    assert!(messages_per_second > 0.0, "Should process some messages");
    assert!(polls_per_second > 0.0, "Should perform some polls");
    
    Ok(())
}

#[tokio::test]
async fn test_performance_with_state_management() -> Result<(), Box<dyn std::error::Error>> {
    let env = PerformanceTestEnvironment::new().await?;
    env.setup_performance_test_index().await?;
    env.insert_performance_test_data(1000).await?;
    
    let config = env.create_performance_test_config(true);
    let mut connector = ElasticsearchSource::new(101, config);
    
    connector.open().await?;
    
    let start_time = Instant::now();
    let mut total_messages = 0;
    let mut poll_count = 0;
    
    // Run for 10 seconds
    while start_time.elapsed() < Duration::from_secs(10) {
        let messages = connector.poll().await?;
        total_messages += messages.messages.len();
        poll_count += 1;
    }
    
    let elapsed = start_time.elapsed();
    let messages_per_second = total_messages as f64 / elapsed.as_secs_f64();
    let polls_per_second = poll_count as f64 / elapsed.as_secs_f64();
    
    info!("Performance with state management:");
    info!("  Total messages: {}", total_messages);
    info!("  Total polls: {}", poll_count);
    info!("  Elapsed time: {:?}", elapsed);
    info!("  Messages per second: {:.2}", messages_per_second);
    info!("  Polls per second: {:.2}", polls_per_second);
    
    connector.close().await?;
    
    // Basic performance assertions
    assert!(messages_per_second > 0.0, "Should process some messages");
    assert!(polls_per_second > 0.0, "Should perform some polls");
    
    Ok(())
}

#[tokio::test]
async fn test_state_save_performance() -> Result<(), Box<dyn std::error::Error>> {
    let env = PerformanceTestEnvironment::new().await?;
    env.setup_performance_test_index().await?;
    env.insert_performance_test_data(100).await?;
    
    let config = env.create_performance_test_config(true);
    let mut connector = ElasticsearchSource::new(102, config);
    
    connector.open().await?;
    
    // Test state save performance
    let start_time = Instant::now();
    let iterations = 100;
    
    for _ in 0..iterations {
        connector.save_state().await?;
    }
    
    let elapsed = start_time.elapsed();
    let saves_per_second = iterations as f64 / elapsed.as_secs_f64();
    let avg_save_time = elapsed.as_millis() as f64 / iterations as f64;
    
    info!("State save performance:");
    info!("  Iterations: {}", iterations);
    info!("  Total time: {:?}", elapsed);
    info!("  Saves per second: {:.2}", saves_per_second);
    info!("  Average save time: {:.2} ms", avg_save_time);
    
    connector.close().await?;
    
    // Performance assertions
    assert!(saves_per_second > 0.0, "Should perform some saves");
    assert!(avg_save_time < 100.0, "Average save time should be reasonable");
    
    Ok(())
}

#[tokio::test]
async fn test_memory_usage_with_state_management() -> Result<(), Box<dyn std::error::Error>> {
    let env = PerformanceTestEnvironment::new().await?;
    env.setup_performance_test_index().await?;
    env.insert_performance_test_data(500).await?;
    
    let config = env.create_performance_test_config(true);
    let mut connector = ElasticsearchSource::new(103, config);
    
    connector.open().await?;
    
    // Monitor memory usage over time
    let start_time = Instant::now();
    let mut memory_samples = Vec::new();
    
    for i in 0..20 {
        connector.poll().await?;
        
        if i % 5 == 0 {
            // Get current memory usage (approximate)
            let memory_info = sysinfo::System::new_all();
            memory_samples.push(memory_info.used_memory());
        }
        
        sleep(Duration::from_millis(500)).await;
    }
    
    let elapsed = start_time.elapsed();
    
    info!("Memory usage with state management:");
    info!("  Test duration: {:?}", elapsed);
    info!("  Memory samples: {}", memory_samples.len());
    info!("  Initial memory: {} MB", memory_samples.first().unwrap_or(&0) / 1024 / 1024);
    info!("  Final memory: {} MB", memory_samples.last().unwrap_or(&0) / 1024 / 1024);
    
    connector.close().await?;
    
    // Basic memory assertions
    assert!(!memory_samples.is_empty(), "Should collect memory samples");
    assert!(memory_samples.len() > 1, "Should have multiple memory samples");
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_state_operations() -> Result<(), Box<dyn std::error::Error>> {
    let env = PerformanceTestEnvironment::new().await?;
    env.setup_performance_test_index().await?;
    env.insert_performance_test_data(200).await?;
    
    let config = env.create_performance_test_config(true);
    let mut connector = ElasticsearchSource::new(104, config);
    
    connector.open().await?;
    
    // Test concurrent state operations
    let start_time = Instant::now();
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let connector_clone = &connector;
        let handle = tokio::spawn(async move {
            for _ in 0..5 {
                let _ = connector_clone.save_state().await;
                sleep(Duration::from_millis(100)).await;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    for handle in handles {
        handle.await?;
    }
    
    let elapsed = start_time.elapsed();
    
    info!("Concurrent state operations:");
    info!("  Concurrent tasks: 10");
    info!("  Operations per task: 5");
    info!("  Total time: {:?}", elapsed);
    
    connector.close().await?;
    
    // Basic concurrency assertions
    assert!(elapsed < Duration::from_secs(10), "Should complete within reasonable time");
    
    Ok(())
} 