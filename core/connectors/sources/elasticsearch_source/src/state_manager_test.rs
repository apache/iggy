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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StateManager, StateConfig, StateStats, StateInfo};
    use iggy_connector_sdk::{SourceState, FileStateStorage};
    use serde_json::json;
    use tempfile::TempDir;
    use chrono::Utc;

    fn create_test_state_config(temp_dir: &TempDir) -> StateConfig {
        StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": temp_dir.path().to_string_lossy()
            })),
            state_id: Some("test_state".to_string()),
            auto_save_interval: Some("1s".to_string()),
            tracked_fields: Some(vec![
                "last_poll_timestamp".to_string(),
                "total_documents_fetched".to_string(),
            ]),
        }
    }

    fn create_test_source_state() -> SourceState {
        SourceState {
            id: "test_state".to_string(),
            last_updated: Utc::now(),
            version: 1,
            data: json!({
                "last_poll_timestamp": "2024-01-15T10:30:00Z",
                "total_documents_fetched": 100,
                "poll_count": 10,
                "error_count": 0,
                "processing_stats": {
                    "total_bytes_processed": 1024,
                    "avg_batch_processing_time_ms": 50.0,
                    "last_successful_poll": "2024-01-15T10:30:00Z",
                    "empty_polls_count": 2,
                    "successful_polls_count": 8
                }
            }),
            metadata: Some(json!({
                "connector_type": "elasticsearch_source",
                "connector_id": 1
            })),
        }
    }

    #[tokio::test]
    async fn test_state_manager_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = create_test_state_config(&temp_dir);
        
        let state_manager = StateManager::new(config).await.unwrap();
        assert!(state_manager.auto_save_interval.is_some());
    }

    #[tokio::test]
    async fn test_file_storage_operations() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = FileStateStorage::new(temp_dir.path());
        
        let state = create_test_source_state();
        
        // Test save
        storage.save_state(&state).await.unwrap();
        
        // Test load
        let loaded_state = storage.load_state(&state.id).await.unwrap();
        assert!(loaded_state.is_some());
        
        let loaded = loaded_state.unwrap();
        assert_eq!(loaded.id, state.id);
        assert_eq!(loaded.version, state.version);
        
        // Test list
        let states = storage.list_states().await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0], state.id);
        
        // Test delete
        storage.delete_state(&state.id).await.unwrap();
        
        let deleted_state = storage.load_state(&state.id).await.unwrap();
        assert!(deleted_state.is_none());
    }

    #[tokio::test]
    async fn test_state_manager_stats() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = create_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).await.unwrap();
        
        // Initially no states
        let stats = state_manager.get_state_stats().await.unwrap();
        assert_eq!(stats.total_states, 0);
        assert!(stats.states.is_empty());
        
        // Create a state
        let storage = FileStateStorage::new(temp_dir.path());
        let state = create_test_source_state();
        storage.save_state(&state).await.unwrap();
        
        // Check stats again
        let stats = state_manager.get_state_stats().await.unwrap();
        assert_eq!(stats.total_states, 1);
        assert_eq!(stats.states.len(), 1);
        assert_eq!(stats.states[0].id, state.id);
        assert_eq!(stats.states[0].version, state.version);
        assert_eq!(stats.states[0].connector_type, "elasticsearch_source");
    }

    #[tokio::test]
    async fn test_state_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = create_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).await.unwrap();
        
        // Create a state
        let storage = FileStateStorage::new(temp_dir.path());
        let state = create_test_source_state();
        storage.save_state(&state).await.unwrap();
        
        // Cleanup should not delete recent states
        let deleted_count = state_manager.cleanup_old_states(1).await.unwrap();
        assert_eq!(deleted_count, 0);
        
        // Verify state still exists
        let stats = state_manager.get_state_stats().await.unwrap();
        assert_eq!(stats.total_states, 1);
    }

    #[tokio::test]
    async fn test_invalid_storage_type() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = create_test_state_config(&temp_dir);
        config.storage_type = Some("invalid_storage".to_string());
        
        // Should fall back to file storage
        let state_manager = StateManager::new(config).await.unwrap();
        assert!(state_manager.auto_save_interval.is_some());
    }

    #[tokio::test]
    async fn test_state_config_serialization() {
        let config = StateConfig {
            enabled: true,
            storage_type: Some("file".to_string()),
            storage_config: Some(json!({
                "base_path": "/tmp/test"
            })),
            state_id: Some("test_id".to_string()),
            auto_save_interval: Some("5m".to_string()),
            tracked_fields: Some(vec!["field1".to_string(), "field2".to_string()]),
        };
        
        // Test serialization
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StateConfig = serde_json::from_str(&json).unwrap();
        
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.storage_type, deserialized.storage_type);
        assert_eq!(config.state_id, deserialized.state_id);
        assert_eq!(config.auto_save_interval, deserialized.auto_save_interval);
    }

    #[tokio::test]
    async fn test_source_state_serialization() {
        let state = create_test_source_state();
        
        // Test serialization
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SourceState = serde_json::from_str(&json).unwrap();
        
        assert_eq!(state.id, deserialized.id);
        assert_eq!(state.version, deserialized.version);
        assert_eq!(state.data, deserialized.data);
    }
} 