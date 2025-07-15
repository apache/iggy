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
    ElasticsearchSource, ElasticsearchSourceConfig, StateConfig, StateManagerExt,
};
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{info, warn};

const ES_URL: &str = "http://localhost:9200";
const TEST_INDEX: &str = "test_logs";
const TEST_STATE_ID: &str = "test_elasticsearch_connector";

struct TestEnvironment {
    client: Elasticsearch,
    temp_dir: TempDir,
}

impl TestEnvironment {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Wait for Elasticsearch to be ready
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
    
    async fn setup_test_index(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create test index
        let index_config = json!({
            "mappings": {
                "properties": {
                    "@timestamp": { "type": "date" },
                    "message": { "type": "text" },
                    "level": { "type": "keyword" },
                    "service": { "type": "keyword" }
                }
            }
        });
        
        self.client
            .indices()
            .create(IndicesCreateParts::Index(TEST_INDEX))
            .body(index_config)
            .send()
            .await?;
        
        // Wait for index to be ready
        sleep(Duration::from_secs(2)).await;
        
        Ok(())
    }
    
    async fn insert_test_data(&self, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        let mut bulk_data = String::new();
        
        for i in 0..count {
            let timestamp = chrono::Utc::now() + chrono::Duration::seconds(i as i64);
            let doc = json!({
                "@timestamp": timestamp.to_rfc3339(),
                "message": format!("Test log message {}", i),
                "level": if i % 3 == 0 { "ERROR" } else { "INFO" },
                "service": "test-service"
            });
            
            bulk_data.push_str(&format!("{{ \"index\": {{ \"_index\": \"{}\" }} }}\n", TEST_INDEX));
            bulk_data.push_str(&format!("{}\n", serde_json::to_string(&doc)?));
        }
        
        self.client
            .bulk(elasticsearch::BulkParts::None)
            .body(bulk_data)
            .send()
            .await?;
        
        // Wait for data to be indexed
        sleep(Duration::from_secs(2)).await;
        
        Ok(())
    }
    
    fn create_connector_config(&self, state_enabled: bool) -> ElasticsearchSourceConfig {
        let state_config = if state_enabled {
            Some(StateConfig {
                enabled: true,
                storage_type: Some("file".to_string()),
                storage_config: Some(json!({
                    "base_path": self.temp_dir.path().to_string_lossy()
                })),
                state_id: Some(TEST_STATE_ID.to_string()),
                auto_save_interval: Some("1s".to_string()),
                tracked_fields: Some(vec![
                    "last_poll_timestamp".to_string(),
                    "last_document_id".to_string(),
                    "total_documents_fetched".to_string(),
                ]),
            })
        } else {
            None
        };
        
        ElasticsearchSourceConfig {
            url: ES_URL.to_string(),
            index: TEST_INDEX.to_string(),
            username: None,
            password: None,
            query: Some(json!({ "match_all": {} })),
            polling_interval: Some("1s".to_string()),
            batch_size: Some(10),
            timestamp_field: Some("@timestamp".to_string()),
            scroll_timeout: Some("1m".to_string()),
            state: state_config,
        }
    }
}

#[tokio::test]
async fn test_basic_connector_functionality() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(50).await?;
    
    let config = env.create_connector_config(false);
    let mut connector = ElasticsearchSource::new(1, config);
    
    // Test basic functionality without state management
    connector.open().await?;
    
    let messages = connector.poll().await?;
    assert!(!messages.messages.is_empty(), "Should fetch some messages");
    assert!(messages.messages.len() <= 10, "Should respect batch size");
    
    connector.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_state_management_basic() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(30).await?;
    
    let config = env.create_connector_config(true);
    let mut connector = ElasticsearchSource::new(2, config);
    
    // Test state management
    connector.open().await?;
    
    // First poll
    let messages1 = connector.poll().await?;
    assert!(!messages1.messages.is_empty());
    
    // Get state after first poll
    let state1 = connector.get_state().await?;
    assert_eq!(state1.id, TEST_STATE_ID);
    assert_eq!(state1.version, 1);
    
    let data1 = state1.data.as_object().unwrap();
    assert!(data1.get("total_documents_fetched").unwrap().as_u64().unwrap() > 0);
    assert!(data1.get("poll_count").unwrap().as_u64().unwrap() > 0);
    
    // Second poll
    let messages2 = connector.poll().await?;
    
    // Get state after second poll
    let state2 = connector.get_state().await?;
    let data2 = state2.data.as_object().unwrap();
    
    let total_docs1 = data1.get("total_documents_fetched").unwrap().as_u64().unwrap();
    let total_docs2 = data2.get("total_documents_fetched").unwrap().as_u64().unwrap();
    
    assert!(total_docs2 >= total_docs1, "Total documents should increase or stay the same");
    
    connector.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_state_persistence_and_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(40).await?;
    
    let config = env.create_connector_config(true);
    
    // First connector instance
    {
        let mut connector1 = ElasticsearchSource::new(3, config.clone());
        connector1.open().await?;
        
        let messages1 = connector1.poll().await?;
        assert!(!messages1.messages.is_empty());
        
        let state1 = connector1.get_state().await?;
        let total_docs1 = state1.data.as_object().unwrap()
            .get("total_documents_fetched").unwrap().as_u64().unwrap();
        
        connector1.close().await?;
        
        // Verify state was saved
        let state_manager = connector1.get_state_manager().unwrap();
        let stats = state_manager.get_state_stats().await?;
        assert_eq!(stats.total_states, 1);
        assert_eq!(stats.states[0].id, TEST_STATE_ID);
    }
    
    // Second connector instance (should recover state)
    {
        let mut connector2 = ElasticsearchSource::new(4, config);
        connector2.open().await?;
        
        let state2 = connector2.get_state().await?;
        let total_docs2 = state2.data.as_object().unwrap()
            .get("total_documents_fetched").unwrap().as_u64().unwrap();
        
        assert!(total_docs2 > 0, "Should recover state from previous instance");
        
        // Poll again and verify incremental processing
        let messages2 = connector2.poll().await?;
        let state3 = connector2.get_state().await?;
        let total_docs3 = state3.data.as_object().unwrap()
            .get("total_documents_fetched").unwrap().as_u64().unwrap();
        
        // Should process new documents (if any) or maintain same count
        assert!(total_docs3 >= total_docs2);
        
        connector2.close().await?;
    }
    
    Ok(())
}

#[tokio::test]
async fn test_state_export_import() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(20).await?;
    
    let config = env.create_connector_config(true);
    let mut connector = ElasticsearchSource::new(5, config);
    
    connector.open().await?;
    connector.poll().await?;
    
    // Export state
    let state_json = connector.export_state().await?;
    assert!(state_json.is_object());
    
    let exported_data = state_json.as_object().unwrap();
    assert!(exported_data.contains_key("id"));
    assert!(exported_data.contains_key("data"));
    assert!(exported_data.contains_key("metadata"));
    
    // Import state to new connector
    let config2 = env.create_connector_config(true);
    let mut connector2 = ElasticsearchSource::new(6, config2);
    
    connector2.open().await?;
    connector2.import_state(state_json).await?;
    
    let imported_state = connector2.get_state().await?;
    assert_eq!(imported_state.id, TEST_STATE_ID);
    
    connector.close().await?;
    connector2.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_state_reset() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(15).await?;
    
    let config = env.create_connector_config(true);
    let mut connector = ElasticsearchSource::new(7, config);
    
    connector.open().await?;
    connector.poll().await?;
    
    let state_before = connector.get_state().await?;
    let total_docs_before = state_before.data.as_object().unwrap()
        .get("total_documents_fetched").unwrap().as_u64().unwrap();
    
    assert!(total_docs_before > 0);
    
    // Reset state
    connector.reset_state().await?;
    
    let state_after = connector.get_state().await?;
    let total_docs_after = state_after.data.as_object().unwrap()
        .get("total_documents_fetched").unwrap().as_u64().unwrap();
    
    assert_eq!(total_docs_after, 0, "State should be reset");
    
    connector.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_error_tracking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    
    // Create connector with invalid index to trigger errors
    let mut config = env.create_connector_config(true);
    config.index = "non_existent_index".to_string();
    
    let mut connector = ElasticsearchSource::new(8, config);
    
    connector.open().await?;
    
    // This should fail but not crash
    let result = connector.poll().await;
    assert!(result.is_err(), "Should fail with non-existent index");
    
    let state = connector.get_state().await?;
    let data = state.data.as_object().unwrap();
    
    let error_count = data.get("error_count").unwrap().as_u64().unwrap();
    assert!(error_count > 0, "Should track error count");
    
    let last_error = data.get("last_error").unwrap().as_str().unwrap();
    assert!(!last_error.is_empty(), "Should track last error");
    
    connector.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_processing_statistics() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(25).await?;
    
    let config = env.create_connector_config(true);
    let mut connector = ElasticsearchSource::new(9, config);
    
    connector.open().await?;
    
    // Multiple polls to gather statistics
    for _ in 0..3 {
        connector.poll().await?;
        sleep(Duration::from_millis(100)).await;
    }
    
    let state = connector.get_state().await?;
    let data = state.data.as_object().unwrap();
    let stats = data.get("processing_stats").unwrap().as_object().unwrap();
    
    let successful_polls = stats.get("successful_polls_count").unwrap().as_u64().unwrap();
    assert!(successful_polls > 0, "Should track successful polls");
    
    let avg_processing_time = stats.get("avg_batch_processing_time_ms").unwrap().as_f64().unwrap();
    assert!(avg_processing_time >= 0.0, "Should track average processing time");
    
    let last_successful = stats.get("last_successful_poll").unwrap().as_str().unwrap();
    assert!(!last_successful.is_empty(), "Should track last successful poll");
    
    connector.close().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_state_manager_utilities() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    env.insert_test_data(10).await?;
    
    let config = env.create_connector_config(true);
    let connector = ElasticsearchSource::new(10, config);
    
    // Test state manager utilities
    let state_manager = connector.get_state_manager().unwrap();
    
    // Get initial stats
    let initial_stats = state_manager.get_state_stats().await?;
    assert_eq!(initial_stats.total_states, 0);
    
    // Create and use connector to generate state
    {
        let mut connector2 = ElasticsearchSource::new(11, env.create_connector_config(true));
        connector2.open().await?;
        connector2.poll().await?;
        connector2.close().await?;
    }
    
    // Get updated stats
    let updated_stats = state_manager.get_state_stats().await?;
    assert!(updated_stats.total_states > 0);
    
    // Test cleanup (should not delete recent states)
    let deleted_count = state_manager.cleanup_old_states(1).await?;
    assert_eq!(deleted_count, 0, "Should not delete recent states");
    
    Ok(())
}

#[tokio::test]
async fn test_incremental_processing() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.setup_test_index().await?;
    
    // Insert initial data
    env.insert_test_data(10).await?;
    
    let config = env.create_connector_config(true);
    let mut connector = ElasticsearchSource::new(12, config);
    
    connector.open().await?;
    
    // First poll
    let messages1 = connector.poll().await?;
    let initial_count = messages1.messages.len();
    
    let state1 = connector.get_state().await?;
    let last_timestamp1 = state1.data.as_object().unwrap()
        .get("last_poll_timestamp").unwrap().as_str().unwrap();
    
    // Insert more data
    sleep(Duration::from_secs(2)).await;
    env.insert_test_data(5).await?;
    
    // Second poll
    let messages2 = connector.poll().await?;
    let state2 = connector.get_state().await?;
    let last_timestamp2 = state2.data.as_object().unwrap()
        .get("last_poll_timestamp").unwrap().as_str().unwrap();
    
    // Verify timestamps are different (indicating incremental processing)
    assert_ne!(last_timestamp1, last_timestamp2);
    
    connector.close().await?;
    
    Ok(())
} 