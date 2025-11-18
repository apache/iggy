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

use integration::file::{file_exists, get_root_path};
use serial_test::serial;
use server::configs::config_provider::{ConfigProvider, FileConfigProvider};
use std::env;

async fn scenario_parsing_from_file(extension: &str) {
    let mut config_path = get_root_path().join("../configs/server");
    assert!(config_path.set_extension(extension), "Cannot set extension");
    let config_path = config_path.as_path().display().to_string();
    let config_provider = FileConfigProvider::new(config_path.clone());
    assert!(
        file_exists(&config_path),
        "Config file not found: {config_path}"
    );
    assert!(
        config_provider.load_config().await.is_ok(),
        "ConfigProvider failed to parse config from {config_path}"
    );
}

#[compio::test]
async fn validate_server_config_toml_from_repository() {
    scenario_parsing_from_file("toml").await;
}

// This test needs to be run in serial because it modifies the environment variables
// which are shared, since all tests run in parallel by default.
#[serial]
#[compio::test]
async fn validate_custom_env_provider() {
    let expected_datagram_send_buffer_size = "1.00 KB";
    let expected_quic_certificate_self_signed = false;
    let expected_http_enabled = false;
    let expected_tcp_enabled = "false";
    let expected_message_saver_enabled = false;
    let expected_message_expiry = "10s";

    unsafe {
        env::set_var(
            "IGGY_QUIC_DATAGRAM_SEND_BUFFER_SIZE",
            expected_datagram_send_buffer_size,
        );
        env::set_var(
            "IGGY_QUIC_CERTIFICATE_SELF_SIGNED",
            expected_quic_certificate_self_signed.to_string(),
        );
        env::set_var("IGGY_HTTP_ENABLED", expected_http_enabled.to_string());
        env::set_var("IGGY_TCP_ENABLED", expected_tcp_enabled);
        env::set_var(
            "IGGY_MESSAGE_SAVER_ENABLED",
            expected_message_saver_enabled.to_string(),
        );
        env::set_var("IGGY_SYSTEM_SEGMENT_MESSAGE_EXPIRY", "10s");
    }

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .await
        .expect("Failed to load default server.toml config");

    assert_eq!(
        config.quic.datagram_send_buffer_size.to_string(),
        expected_datagram_send_buffer_size
    );
    assert_eq!(
        config.quic.certificate.self_signed,
        expected_quic_certificate_self_signed
    );
    assert_eq!(config.http.enabled, expected_http_enabled);
    assert_eq!(config.tcp.enabled.to_string(), expected_tcp_enabled);
    assert_eq!(config.message_saver.enabled, expected_message_saver_enabled);
    assert_eq!(
        config.system.segment.message_expiry.to_string(),
        expected_message_expiry
    );

    unsafe {
        env::remove_var("IGGY_QUIC_DATAGRAM_SEND_BUFFER_SIZE");
        env::remove_var("IGGY_QUIC_CERTIFICATE_SELF_SIGNED");
        env::remove_var("IGGY_HTTP_ENABLED");
        env::remove_var("IGGY_TCP_ENABLED");
        env::remove_var("IGGY_MESSAGE_SAVER_ENABLED");
        env::remove_var("IGGY_SYSTEM_RETENTION_POLICY_MESSAGE_EXPIRY");
    }
}

// Test for cluster configuration with environment variable overrides
#[serial]
#[tokio::test]
async fn validate_cluster_config_env_override() {
    // Test data for cluster configuration
    let expected_cluster_enabled = true;
    let expected_cluster_id = 42;
    let expected_cluster_name = "test-cluster";
    let expected_node_id = 5;

    // Test data for cluster nodes array
    let expected_node_0_id = 10;
    let expected_node_0_name = "test-node-1";
    let expected_node_0_address = "192.168.1.100:9090";

    let expected_node_1_id = 20;
    let expected_node_1_name = "test-node-2";
    let expected_node_1_address = "192.168.1.101:9091";

    let expected_node_2_id = 30;
    let expected_node_2_name = "test-node-3";
    let expected_node_2_address = "192.168.1.102:9092";

    unsafe {
        // Set cluster configuration environment variables
        env::set_var("IGGY_CLUSTER_ENABLED", expected_cluster_enabled.to_string());
        env::set_var("IGGY_CLUSTER_ID", expected_cluster_id.to_string());
        env::set_var("IGGY_CLUSTER_NAME", expected_cluster_name);
        env::set_var("IGGY_CLUSTER_NODE_ID", expected_node_id.to_string());

        // Set cluster nodes array environment variables
        env::set_var("IGGY_CLUSTER_NODES_0_ID", expected_node_0_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_0_NAME", expected_node_0_name);
        env::set_var("IGGY_CLUSTER_NODES_0_ADDRESS", expected_node_0_address);

        env::set_var("IGGY_CLUSTER_NODES_1_ID", expected_node_1_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_1_NAME", expected_node_1_name);
        env::set_var("IGGY_CLUSTER_NODES_1_ADDRESS", expected_node_1_address);

        env::set_var("IGGY_CLUSTER_NODES_2_ID", expected_node_2_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_2_NAME", expected_node_2_name);
        env::set_var("IGGY_CLUSTER_NODES_2_ADDRESS", expected_node_2_address);
    }

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .await
        .expect("Failed to load server.toml config with cluster env overrides");

    // Verify cluster configuration
    assert_eq!(config.cluster.enabled, expected_cluster_enabled);
    assert_eq!(config.cluster.id, expected_cluster_id);
    assert_eq!(config.cluster.name, expected_cluster_name);
    assert_eq!(config.cluster.node.id, expected_node_id);

    // Verify cluster nodes array - should have 3 nodes instead of the default 2
    assert_eq!(
        config.cluster.nodes.len(),
        3,
        "Should have 3 nodes from environment variables"
    );

    // Verify first node
    assert_eq!(config.cluster.nodes[0].id, expected_node_0_id);
    assert_eq!(config.cluster.nodes[0].name, expected_node_0_name);
    assert_eq!(config.cluster.nodes[0].address, expected_node_0_address);

    // Verify second node
    assert_eq!(config.cluster.nodes[1].id, expected_node_1_id);
    assert_eq!(config.cluster.nodes[1].name, expected_node_1_name);
    assert_eq!(config.cluster.nodes[1].address, expected_node_1_address);

    // Verify third node (added via env vars)
    assert_eq!(config.cluster.nodes[2].id, expected_node_2_id);
    assert_eq!(config.cluster.nodes[2].name, expected_node_2_name);
    assert_eq!(config.cluster.nodes[2].address, expected_node_2_address);

    unsafe {
        // Clean up environment variables
        env::remove_var("IGGY_CLUSTER_ENABLED");
        env::remove_var("IGGY_CLUSTER_ID");
        env::remove_var("IGGY_CLUSTER_NAME");
        env::remove_var("IGGY_CLUSTER_NODE_ID");

        env::remove_var("IGGY_CLUSTER_NODES_0_ID");
        env::remove_var("IGGY_CLUSTER_NODES_0_NAME");
        env::remove_var("IGGY_CLUSTER_NODES_0_ADDRESS");

        env::remove_var("IGGY_CLUSTER_NODES_1_ID");
        env::remove_var("IGGY_CLUSTER_NODES_1_NAME");
        env::remove_var("IGGY_CLUSTER_NODES_1_ADDRESS");

        env::remove_var("IGGY_CLUSTER_NODES_2_ID");
        env::remove_var("IGGY_CLUSTER_NODES_2_NAME");
        env::remove_var("IGGY_CLUSTER_NODES_2_ADDRESS");
    }
}

// Test partial override - only override specific fields
#[serial]
#[tokio::test]
async fn validate_cluster_partial_env_override() {
    // Only override the cluster ID and one node's address
    let expected_cluster_id = 99;
    let expected_node_1_address = "10.0.0.1:8888";

    unsafe {
        env::set_var("IGGY_CLUSTER_ID", expected_cluster_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_1_ADDRESS", expected_node_1_address);
    }

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .await
        .expect("Failed to load server.toml config with partial cluster env overrides");

    // Verify overridden values
    assert_eq!(config.cluster.id, expected_cluster_id);
    assert_eq!(config.cluster.nodes[1].address, expected_node_1_address);

    // Verify non-overridden values remain default
    assert_eq!(config.cluster.name, "iggy-cluster"); // default from server.toml
    assert_eq!(config.cluster.nodes[0].id, 1); // default from server.toml
    assert_eq!(config.cluster.nodes[0].name, "iggy-node-1"); // default from server.toml
    assert_eq!(config.cluster.nodes[1].id, 2); // default from server.toml
    assert_eq!(config.cluster.nodes[1].name, "iggy-node-2"); // default from server.toml

    unsafe {
        env::remove_var("IGGY_CLUSTER_ID");
        env::remove_var("IGGY_CLUSTER_NODES_1_ADDRESS");
    }
}

// Test sparse array override - setting index 5 when only 2 nodes exist in TOML
// This test verifies that sparse arrays will fail because intermediate elements
// won't have required fields, which is the expected safety behavior
#[serial]
#[tokio::test]
async fn validate_cluster_sparse_array_fails_with_missing_fields() {
    // Set node at index 5 (when TOML only has nodes 0 and 1)
    // This should fail because nodes 2-4 will be created as empty dicts
    // without required fields
    let expected_node_5_id = 100;
    let expected_node_5_name = "sparse-node";
    let expected_node_5_address = "10.0.0.100:9999";

    unsafe {
        env::set_var("IGGY_CLUSTER_NODES_5_ID", expected_node_5_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_5_NAME", expected_node_5_name);
        env::set_var("IGGY_CLUSTER_NODES_5_ADDRESS", expected_node_5_address);
    }

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());

    // This should fail because nodes 2-4 will be missing required fields
    let result = file_config_provider.load_config().await;
    assert!(
        result.is_err(),
        "Should fail to load config with sparse array due to missing required fields in intermediate elements"
    );

    unsafe {
        env::remove_var("IGGY_CLUSTER_NODES_5_ID");
        env::remove_var("IGGY_CLUSTER_NODES_5_NAME");
        env::remove_var("IGGY_CLUSTER_NODES_5_ADDRESS");
    }
}

// Test that we can add node 2 successfully (contiguous array)
#[serial]
#[tokio::test]
async fn validate_cluster_contiguous_array_override() {
    // Add node at index 2 (TOML has nodes 0 and 1, so this is contiguous)
    let expected_node_2_id = 50;
    let expected_node_2_name = "iggy-node-3";
    let expected_node_2_address = "10.0.0.50:8092";

    unsafe {
        env::set_var("IGGY_CLUSTER_NODES_2_ID", expected_node_2_id.to_string());
        env::set_var("IGGY_CLUSTER_NODES_2_NAME", expected_node_2_name);
        env::set_var("IGGY_CLUSTER_NODES_2_ADDRESS", expected_node_2_address);
    }

    let config_path = get_root_path().join("../configs/server.toml");
    let file_config_provider = FileConfigProvider::new(config_path.as_path().display().to_string());
    let config = file_config_provider
        .load_config()
        .await
        .expect("Failed to load server.toml config with contiguous array override");

    // Should have 3 nodes total
    assert_eq!(
        config.cluster.nodes.len(),
        3,
        "Should have 3 nodes when adding index 2"
    );

    // Check original nodes are preserved
    assert_eq!(config.cluster.nodes[0].id, 1); // from TOML
    assert_eq!(config.cluster.nodes[0].name, "iggy-node-1"); // from TOML
    assert_eq!(config.cluster.nodes[1].id, 2); // from TOML
    assert_eq!(config.cluster.nodes[1].name, "iggy-node-2"); // from TOML

    // Check the node we added at index 2
    assert_eq!(config.cluster.nodes[2].id, expected_node_2_id);
    assert_eq!(config.cluster.nodes[2].name, expected_node_2_name);
    assert_eq!(config.cluster.nodes[2].address, expected_node_2_address);

    unsafe {
        env::remove_var("IGGY_CLUSTER_NODES_2_ID");
        env::remove_var("IGGY_CLUSTER_NODES_2_NAME");
        env::remove_var("IGGY_CLUSTER_NODES_2_ADDRESS");
    }
}
