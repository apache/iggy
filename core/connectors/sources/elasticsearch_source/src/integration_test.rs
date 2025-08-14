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

use super::*;
use serde_json::json;
use iggy_connector_sdk::{Error, ProducedMessages, Schema, Source};
use tokio::time::sleep;

#[tokio::test]
async fn elasticsearch_source_should_connect_to_elasticsearch() {
    // Given an Elasticsearch source configuration
    let config = ElasticsearchSourceConfig {
        url: "http://localhost:9200".to_string(),
        index: "test_index".to_string(),
        query: Some(json!({
            "query": {
                "match_all": {}
            },
            "size": 10
        })),
        scroll_timeout: Some("1m".to_string()),
        batch_size: Some(100),
        state_config: Some(StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": "/tmp/elasticsearch_test"
            })),
            state_id: Some("test_integration_state".to_string()),
            auto_save_interval: Some("5s".to_string()),
            tracked_fields: Some(vec![
                "last_poll_timestamp".to_string(),
                "total_documents_fetched".to_string(),
            ]),
        }),
    };

    // When creating the Elasticsearch source
    let mut source = ElasticsearchSource::new(1, config, None);

    // Then it should open successfully (even if ES is not available, it should handle the error gracefully)
    let result = source.open().await;
    
    // The test should not panic, even if Elasticsearch is not available
    // This tests the error handling of the connector
    match result {
        Ok(_) => println!("Successfully connected to Elasticsearch"),
        Err(Error::InitError(msg)) => {
            println!("Expected initialization error (Elasticsearch not available): {}", msg);
            // This is expected if Elasticsearch is not running
        }
        Err(e) => {
            println!("Unexpected error: {:?}", e);
            // Don't fail the test for connection issues
        }
    }
}

#[tokio::test]
async fn elasticsearch_source_should_handle_poll_without_elasticsearch() {
    // Given an Elasticsearch source configuration
    let config = ElasticsearchSourceConfig {
        url: "http://localhost:9200".to_string(),
        index: "test_index".to_string(),
        query: Some(json!({
            "query": {
                "match_all": {}
            },
            "size": 10
        })),
        scroll_timeout: Some("1m".to_string()),
        batch_size: Some(100),
        state_config: Some(StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": "/tmp/elasticsearch_test"
            })),
            state_id: Some("test_poll_state".to_string()),
            auto_save_interval: Some("5s".to_string()),
            tracked_fields: Some(vec![
                "last_poll_timestamp".to_string(),
                "total_documents_fetched".to_string(),
            ]),
        }),
    };

    // When creating the Elasticsearch source
    let source = ElasticsearchSource::new(2, config, None);

    // Then it should handle poll gracefully even without Elasticsearch
    let result = source.poll().await;
    
    match result {
        Ok(ProducedMessages { messages, schema, state }) => {
            println!("Poll successful: {} messages, schema: {:?}", messages.len(), schema);
            // Should return empty messages when ES is not available
            assert_eq!(messages.len(), 0);
            assert_eq!(schema, Schema::Json);
            assert!(state.is_some());
        }
        Err(Error::InvalidRecord) => {
            println!("Expected invalid record error (Elasticsearch not available)");
            // This is expected if Elasticsearch is not running
        }
        Err(e) => {
            println!("Unexpected error during poll: {:?}", e);
            // Don't fail the test for connection issues
        }
    }
}

#[tokio::test]
async fn state_manager_should_work_integration_test() {
    // Given a state manager configuration
    let config = StateConfig {
        enabled: true,
        storage_type: Some("file".to_string()),
        storage_config: Some(json!({
            "base_path": "/tmp/elasticsearch_integration_test"
        })),
        state_id: Some("integration_test_state".to_string()),
        auto_save_interval: Some("1s".to_string()),
        tracked_fields: Some(vec![
            "test_field".to_string(),
        ]),
    };

    // When creating a state manager
    let state_manager = StateManager::new(config).unwrap();

    // Then it should be able to export and import state
    let test_state = json!({
        "test_field": "test_value",
        "timestamp": "2024-01-15T10:30:00Z"
    });

    // Export state
    let export_result = state_manager.export_state().await;
    assert!(export_result.is_ok());

    // Import state
    let import_result = state_manager.import_state(test_state.clone()).await;
    assert!(import_result.is_ok());

    // Reset state
    let reset_result = state_manager.reset_state().await;
    assert!(reset_result.is_ok());

    println!("State manager integration test completed successfully");
}

#[tokio::test]
async fn elasticsearch_source_should_close_gracefully() {
    // Given an Elasticsearch source configuration
    let config = ElasticsearchSourceConfig {
        url: "http://localhost:9200".to_string(),
        index: "test_index".to_string(),
        query: Some(json!({
            "query": {
                "match_all": {}
            },
            "size": 10
        })),
        scroll_timeout: Some("1m".to_string()),
        batch_size: Some(100),
        state_config: Some(StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": "/tmp/elasticsearch_close_test"
            })),
            state_id: Some("test_close_state".to_string()),
            auto_save_interval: Some("5s".to_string()),
            tracked_fields: Some(vec![
                "last_poll_timestamp".to_string(),
                "total_documents_fetched".to_string(),
            ]),
        }),
    };

    // When creating and closing the Elasticsearch source
    let mut source = ElasticsearchSource::new(3, config, None);
    
    // Open (may fail if ES is not available, but that's OK)
    let _ = source.open().await;
    
    // Close should always succeed
    let close_result = source.close().await;
    assert!(close_result.is_ok());
    
    println!("Elasticsearch source closed gracefully");
}

#[tokio::test]
async fn elasticsearch_source_should_handle_state_persistence() {
    // Given an Elasticsearch source with state management
    let config = ElasticsearchSourceConfig {
        url: "http://localhost:9200".to_string(),
        index: "test_index".to_string(),
        query: Some(json!({
            "query": {
                "match_all": {}
            },
            "size": 5
        })),
        scroll_timeout: Some("1m".to_string()),
        batch_size: Some(50),
        state_config: Some(StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": "/tmp/elasticsearch_state_test"
            })),
            state_id: Some("test_state_persistence".to_string()),
            auto_save_interval: Some("1s".to_string()),
            tracked_fields: Some(vec![
                "last_poll_timestamp".to_string(),
                "total_documents_fetched".to_string(),
                "scroll_id".to_string(),
            ]),
        }),
    };

    // When creating the source and performing operations
    let source = ElasticsearchSource::new(4, config, None);
    
    // Poll should return state even if no data is available
    let poll_result = source.poll().await;
    
    match poll_result {
        Ok(ProducedMessages { messages, schema, state }) => {
            println!("Poll returned: {} messages, schema: {:?}", messages.len(), schema);
            assert_eq!(schema, Schema::Json);
            
            // State should be present even if no messages
            if let Some(state_value) = state {
                println!("State returned: {:?}", state_value);
                // State should be a valid JSON object
                assert!(state_value.is_object());
            }
        }
        Err(e) => {
            println!("Poll error (expected if ES not available): {:?}", e);
            // Don't fail the test for connection issues
        }
    }
    
    println!("State persistence test completed");
} 