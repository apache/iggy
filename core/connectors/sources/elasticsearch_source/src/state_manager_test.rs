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
    use crate::{StateManager, StateConfig};
    use iggy_connector_sdk::{SourceState, FileStateStorage, StateStorage};
    use serde_json::json;
    use tempfile::TempDir;
    use chrono::Utc;

    // Test fixtures
    fn given_test_state_config(temp_dir: &TempDir) -> StateConfig {
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

    fn given_test_source_state() -> SourceState {
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

    // StateManager creation tests
    #[tokio::test]
    async fn state_manager_should_be_created_with_valid_config() {
        // Given a valid state configuration
        let temp_dir = tempfile::tempdir().unwrap();
        let config = given_test_state_config(&temp_dir);
        
        // When creating a state manager
        let state_manager = StateManager::new(config).unwrap();
        
        // Then it should be created successfully with auto-save interval
        assert!(state_manager.auto_save_interval().is_some());
    }

    #[tokio::test]
    async fn state_manager_should_fallback_to_file_storage_when_invalid_storage_type() {
        // Given a configuration with invalid storage type
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = given_test_state_config(&temp_dir);
        config.storage_type = Some("invalid_storage".to_string());
        
        // When creating a state manager
        let state_manager = StateManager::new(config).unwrap();
        
        // Then it should fall back to file storage
        assert!(state_manager.auto_save_interval().is_some());
    }

    // File storage operations tests
    #[tokio::test]
    async fn file_storage_should_save_and_load_state_correctly() {
        // Given a file storage and a test state
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = FileStateStorage::new(temp_dir.path());
        let state = given_test_source_state();
        
        // When saving the state
        storage.save_source_state(&state).await.unwrap();
        
        // Then it should be loadable
        let loaded_state = storage.load_source_state(&state.id).await.unwrap();
        assert!(loaded_state.is_some());
        
        let loaded = loaded_state.unwrap();
        assert_eq!(loaded.id, state.id);
        assert_eq!(loaded.version, state.version);
    }

    #[tokio::test]
    async fn file_storage_should_list_all_states() {
        // Given a file storage with a saved state
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = FileStateStorage::new(temp_dir.path());
        let state = given_test_source_state();
        storage.save_source_state(&state).await.unwrap();
        
        // When listing states
        let states = storage.list_states().await.unwrap();
        
        // Then it should contain the saved state
        assert_eq!(states.len(), 1);
        assert_eq!(states[0], state.id);
    }

    #[tokio::test]
    async fn file_storage_should_delete_state_when_requested() {
        // Given a file storage with a saved state
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = FileStateStorage::new(temp_dir.path());
        let state = given_test_source_state();
        storage.save_source_state(&state).await.unwrap();
        
        // When deleting the state
        storage.delete_state(&state.id).await.unwrap();
        
        // Then it should no longer exist
        let deleted_state = storage.load_source_state(&state.id).await.unwrap();
        assert!(deleted_state.is_none());
    }

    // State manager statistics tests
    #[tokio::test]
    async fn state_manager_should_report_empty_stats_when_no_states_exist() {
        // Given a state manager with no states
        let temp_dir = tempfile::tempdir().unwrap();
        let config = given_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).unwrap();
        
        // When getting state statistics
        let stats = state_manager.get_state_stats().await.unwrap();
        
        // Then it should report empty stats
        assert_eq!(stats.total_states, 0);
        assert!(stats.states.is_empty());
    }

    #[tokio::test]
    async fn state_manager_should_report_correct_stats_when_states_exist() {
        // Given a state manager and a saved state
        let temp_dir = tempfile::tempdir().unwrap();
        let config = given_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).unwrap();
        
        let storage = FileStateStorage::new(temp_dir.path());
        let state = given_test_source_state();
        storage.save_source_state(&state).await.unwrap();
        
        // When getting state statistics
        let stats = state_manager.get_state_stats().await.unwrap();
        
        // Then it should report correct stats
        assert_eq!(stats.total_states, 1);
        assert_eq!(stats.states.len(), 1);
        assert_eq!(stats.states[0].id, state.id);
        assert_eq!(stats.states[0].version, state.version);
        assert_eq!(stats.states[0].connector_type, "elasticsearch_source");
    }

    // State cleanup tests
    #[tokio::test]
    async fn state_manager_should_not_delete_recent_states_during_cleanup() {
        // Given a state manager with a recent state
        let temp_dir = tempfile::tempdir().unwrap();
        let config = given_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).unwrap();
        
        let storage = FileStateStorage::new(temp_dir.path());
        let state = given_test_source_state();
        storage.save_source_state(&state).await.unwrap();
        
        // When cleaning up old states
        let deleted_count = state_manager.cleanup_old_states(1).await.unwrap();
        
        // Then no states should be deleted
        assert_eq!(deleted_count, 0);
        
        // And the state should still exist
        let stats = state_manager.get_state_stats().await.unwrap();
        assert_eq!(stats.total_states, 1);
    }

    // Configuration serialization tests
    #[tokio::test]
    async fn state_config_should_serialize_and_deserialize_correctly() {
        // Given a state configuration
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
        
        // When serializing and deserializing
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StateConfig = serde_json::from_str(&json).unwrap();
        
        // Then all fields should match
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.storage_type, deserialized.storage_type);
        assert_eq!(config.state_id, deserialized.state_id);
        assert_eq!(config.auto_save_interval, deserialized.auto_save_interval);
    }

    // Source state serialization tests
    #[tokio::test]
    async fn source_state_should_serialize_and_deserialize_correctly() {
        // Given a source state
        let state = given_test_source_state();
        
        // When serializing and deserializing
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: SourceState = serde_json::from_str(&json).unwrap();
        
        // Then all fields should match
        assert_eq!(state.id, deserialized.id);
        assert_eq!(state.version, deserialized.version);
        assert_eq!(state.data, deserialized.data);
    }

    // Integration tests
    #[tokio::test]
    async fn state_manager_should_handle_complete_workflow() {
        // Given a state manager and storage
        let temp_dir = tempfile::tempdir().unwrap();
        let config = given_test_state_config(&temp_dir);
        let state_manager = StateManager::new(config).unwrap();
        let storage = FileStateStorage::new(temp_dir.path());
        
        // When performing a complete workflow: save -> load -> update -> save
        let mut state = given_test_source_state();
        storage.save_source_state(&state).await.unwrap();
        
        let loaded_state = storage.load_source_state(&state.id).await.unwrap().unwrap();
        assert_eq!(loaded_state.id, state.id);
        
        // Update state
        state.version = 2;
        state.data = json!({
            "last_poll_timestamp": "2024-01-15T11:00:00Z",
            "total_documents_fetched": 200,
        });
        storage.save_source_state(&state).await.unwrap();
        
        // Then the updated state should be persisted
        let updated_state = storage.load_source_state(&state.id).await.unwrap().unwrap();
        assert_eq!(updated_state.version, 2);
        assert_eq!(updated_state.data["total_documents_fetched"], 200);
    }
} 