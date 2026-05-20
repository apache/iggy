/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use configs::{ConfigEnvMappings, ConfigProvider, TypedEnvProvider};
use configs_derive::ConfigEnv;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Figment, Provider};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use server::configs::server::ServerConfig;
use std::env;
use std::path::PathBuf;

#[serial]
#[tokio::test]
async fn validate_config_env_override() {
    let expected_http = true;
    let expected_tcp = true;
    let expected_message_saver = true;
    let expected_message_expiry = "1s";

    unsafe {
        env::set_var("IGGY_HTTP_ENABLED", expected_http.to_string());
        env::set_var("IGGY_TCP_ENABLED", expected_tcp.to_string());
        env::set_var(
            "IGGY_MESSAGE_SAVER_ENABLED",
            expected_message_saver.to_string(),
        );
        env::set_var("IGGY_SYSTEM_TOPIC_MESSAGE_EXPIRY", expected_message_expiry);
    }

    let config_path = get_root_path().join("../server/config.toml");
    let file_config_provider =
        ServerConfig::config_provider(&config_path.as_path().display().to_string());
    let config: ServerConfig = file_config_provider
        .load_config()
        .await
        .expect("Failed to load config.toml config");

    assert_eq!(config.http.enabled, expected_http);
    assert_eq!(config.tcp.enabled, expected_tcp);
    assert_eq!(config.message_saver.enabled, expected_message_saver);
    assert_eq!(
        config.system.topic.message_expiry.to_string(),
        expected_message_expiry
    );

    unsafe {
        env::remove_var("IGGY_HTTP_ENABLED");
        env::remove_var("IGGY_TCP_ENABLED");
        env::remove_var("IGGY_MESSAGE_SAVER_ENABLED");
        env::remove_var("IGGY_SYSTEM_TOPIC_MESSAGE_EXPIRY");
    }
}

#[serial]
#[tokio::test]
async fn validate_socket_override() {
    // Environment variables are set as raw byte counts
    let send_buffer_bytes = 666666_u64;
    let recv_buffer_bytes = 777777_u64;
    unsafe {
        env::set_var("IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS", "true");
        env::set_var(
            "IGGY_TCP_SOCKET_SEND_BUFFER_SIZE",
            send_buffer_bytes.to_string(),
        );
        env::set_var(
            "IGGY_TCP_SOCKET_RECV_BUFFER_SIZE",
            recv_buffer_bytes.to_string(),
        );
    }

    let config_path = get_root_path().join("../server/config.toml");
    let file_config_provider =
        ServerConfig::config_provider(&config_path.as_path().display().to_string());
    let config: ServerConfig = file_config_provider
        .load_config()
        .await
        .expect("Failed to load config.toml config with socket override");

    assert!(config.tcp.socket.override_defaults);
    // Verify the buffer sizes match the expected byte counts
    assert_eq!(
        config.tcp.socket.send_buffer_size.as_bytes_u64(),
        send_buffer_bytes
    );
    assert_eq!(
        config.tcp.socket.recv_buffer_size.as_bytes_u64(),
        recv_buffer_bytes
    );

    unsafe {
        env::remove_var("IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS");
        env::remove_var("IGGY_TCP_SOCKET_SEND_BUFFER_SIZE");
        env::remove_var("IGGY_TCP_SOCKET_RECV_BUFFER_SIZE");
    }
}

#[serial]
#[tokio::test]
async fn validate_socket_no_override() {
    let config_path = get_root_path().join("../server/config.toml");
    let file_config_provider =
        ServerConfig::config_provider(&config_path.as_path().display().to_string());
    let config: ServerConfig = file_config_provider
        .load_config()
        .await
        .expect("Failed to load config.toml config without socket override");

    assert!(!config.tcp.socket.override_defaults);
}

#[serial]
#[tokio::test]
async fn validate_cluster_config_env_override() {
    // Test data for cluster configuration
    let expected_cluster_enabled = true;
    let expected_cluster_name = "test-cluster";

    let expected_node_0_name = "test-node-1";
    let expected_node_0_ip = "192.168.1.100";
    let expected_node_0_replica_id = 0_u8;
    let expected_node_0_tcp = 9090_u16;

    let expected_node_1_name = "test-node-2";
    let expected_node_1_ip = "192.168.1.101";
    let expected_node_1_replica_id = 1_u8;
    let expected_node_1_tcp = 9091_u16;
    let expected_node_1_quic = 9081_u16;
    let expected_node_1_http = 4001_u16;
    let expected_node_1_websocket = 9093_u16;

    let expected_node_2_name = "test-node-3";
    let expected_node_2_ip = "192.168.1.102";
    let expected_node_2_replica_id = 2_u8;
    let expected_node_2_tcp = 9092_u16;
    let expected_node_2_quic = 9082_u16;
    let expected_node_2_http = 4002_u16;
    let expected_node_2_websocket = 9094_u16;

    unsafe {
        env::set_var("IGGY_CLUSTER_ENABLED", expected_cluster_enabled.to_string());
        env::set_var("IGGY_CLUSTER_NAME", expected_cluster_name);

        env::set_var("IGGY_CLUSTER_NODES_0_NAME", expected_node_0_name);
        env::set_var("IGGY_CLUSTER_NODES_0_IP", expected_node_0_ip);
        env::set_var(
            "IGGY_CLUSTER_NODES_0_REPLICA_ID",
            expected_node_0_replica_id.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_0_PORTS_TCP",
            expected_node_0_tcp.to_string(),
        );

        env::set_var("IGGY_CLUSTER_NODES_1_NAME", expected_node_1_name);
        env::set_var("IGGY_CLUSTER_NODES_1_IP", expected_node_1_ip);
        env::set_var(
            "IGGY_CLUSTER_NODES_1_REPLICA_ID",
            expected_node_1_replica_id.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_1_PORTS_TCP",
            expected_node_1_tcp.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_1_PORTS_QUIC",
            expected_node_1_quic.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_1_PORTS_HTTP",
            expected_node_1_http.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_1_PORTS_WEBSOCKET",
            expected_node_1_websocket.to_string(),
        );

        env::set_var("IGGY_CLUSTER_NODES_2_NAME", expected_node_2_name);
        env::set_var("IGGY_CLUSTER_NODES_2_IP", expected_node_2_ip);
        env::set_var(
            "IGGY_CLUSTER_NODES_2_REPLICA_ID",
            expected_node_2_replica_id.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_2_PORTS_TCP",
            expected_node_2_tcp.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_2_PORTS_QUIC",
            expected_node_2_quic.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_2_PORTS_HTTP",
            expected_node_2_http.to_string(),
        );
        env::set_var(
            "IGGY_CLUSTER_NODES_2_PORTS_WEBSOCKET",
            expected_node_2_websocket.to_string(),
        );
    }

    let config_path = get_root_path().join("../server/config.toml");
    let file_config_provider =
        ServerConfig::config_provider(&config_path.as_path().display().to_string());
    let config: ServerConfig = file_config_provider
        .load_config()
        .await
        .expect("Failed to load config.toml config with cluster env overrides");

    assert_eq!(config.cluster.enabled, expected_cluster_enabled);
    assert_eq!(config.cluster.name, expected_cluster_name);
    assert_eq!(
        config.cluster.nodes.len(),
        3,
        "Should have 3 cluster nodes from environment variables"
    );

    assert_eq!(config.cluster.nodes[0].name, expected_node_0_name);
    assert_eq!(config.cluster.nodes[0].ip, expected_node_0_ip);
    assert_eq!(
        config.cluster.nodes[0].replica_id,
        expected_node_0_replica_id
    );
    assert_eq!(config.cluster.nodes[0].ports.tcp, Some(expected_node_0_tcp));

    assert_eq!(config.cluster.nodes[1].name, expected_node_1_name);
    assert_eq!(config.cluster.nodes[1].ip, expected_node_1_ip);
    assert_eq!(
        config.cluster.nodes[1].replica_id,
        expected_node_1_replica_id
    );
    assert_eq!(config.cluster.nodes[1].ports.tcp, Some(expected_node_1_tcp));
    assert_eq!(
        config.cluster.nodes[1].ports.quic,
        Some(expected_node_1_quic)
    );
    assert_eq!(
        config.cluster.nodes[1].ports.http,
        Some(expected_node_1_http)
    );
    assert_eq!(
        config.cluster.nodes[1].ports.websocket,
        Some(expected_node_1_websocket)
    );

    assert_eq!(config.cluster.nodes[2].name, expected_node_2_name);
    assert_eq!(config.cluster.nodes[2].ip, expected_node_2_ip);
    assert_eq!(
        config.cluster.nodes[2].replica_id,
        expected_node_2_replica_id
    );
    assert_eq!(config.cluster.nodes[2].ports.tcp, Some(expected_node_2_tcp));
    assert_eq!(
        config.cluster.nodes[2].ports.quic,
        Some(expected_node_2_quic)
    );
    assert_eq!(
        config.cluster.nodes[2].ports.http,
        Some(expected_node_2_http)
    );
    assert_eq!(
        config.cluster.nodes[2].ports.websocket,
        Some(expected_node_2_websocket)
    );

    unsafe {
        env::remove_var("IGGY_CLUSTER_ENABLED");
        env::remove_var("IGGY_CLUSTER_NAME");
        for idx in 0..=2 {
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_NAME"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_IP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_REPLICA_ID"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_TCP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_QUIC"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_HTTP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_WEBSOCKET"));
        }
    }
}

#[serial]
#[tokio::test]
async fn validate_four_node_cluster_config_env_override() {
    let expected_cluster_enabled = true;
    let expected_cluster_name = "test-4node-cluster";

    let nodes = [
        (
            "node-1", "10.0.0.1", 0_u8, 8090_u16, 8080_u16, 3000_u16, 8091_u16,
        ),
        (
            "node-2", "10.0.0.2", 1_u8, 8092_u16, 8082_u16, 3002_u16, 8093_u16,
        ),
        (
            "node-3", "10.0.0.3", 2_u8, 8094_u16, 8084_u16, 3004_u16, 8095_u16,
        ),
    ];
    let sparse_name = "node-4";
    let sparse_ip = "10.0.0.4";
    let sparse_replica_id = 3_u8;
    let sparse_tcp = 8096_u16;
    let sparse_http = 3006_u16;

    unsafe {
        env::set_var("IGGY_CLUSTER_ENABLED", expected_cluster_enabled.to_string());
        env::set_var("IGGY_CLUSTER_NAME", expected_cluster_name);

        for (idx, (name, ip, replica_id, tcp, quic, http, websocket)) in nodes.iter().enumerate() {
            env::set_var(format!("IGGY_CLUSTER_NODES_{idx}_NAME"), name);
            env::set_var(format!("IGGY_CLUSTER_NODES_{idx}_IP"), ip);
            env::set_var(
                format!("IGGY_CLUSTER_NODES_{idx}_REPLICA_ID"),
                replica_id.to_string(),
            );
            env::set_var(
                format!("IGGY_CLUSTER_NODES_{idx}_PORTS_TCP"),
                tcp.to_string(),
            );
            env::set_var(
                format!("IGGY_CLUSTER_NODES_{idx}_PORTS_QUIC"),
                quic.to_string(),
            );
            env::set_var(
                format!("IGGY_CLUSTER_NODES_{idx}_PORTS_HTTP"),
                http.to_string(),
            );
            env::set_var(
                format!("IGGY_CLUSTER_NODES_{idx}_PORTS_WEBSOCKET"),
                websocket.to_string(),
            );
        }

        // 4th node intentionally omits QUIC and WebSocket to exercise the
        // defaults-applied-at-runtime path.
        env::set_var("IGGY_CLUSTER_NODES_3_NAME", sparse_name);
        env::set_var("IGGY_CLUSTER_NODES_3_IP", sparse_ip);
        env::set_var(
            "IGGY_CLUSTER_NODES_3_REPLICA_ID",
            sparse_replica_id.to_string(),
        );
        env::set_var("IGGY_CLUSTER_NODES_3_PORTS_TCP", sparse_tcp.to_string());
        env::set_var("IGGY_CLUSTER_NODES_3_PORTS_HTTP", sparse_http.to_string());
    }

    let config_path = get_root_path().join("../server/config.toml");
    let file_config_provider =
        ServerConfig::config_provider(&config_path.as_path().display().to_string());
    let config: ServerConfig = file_config_provider
        .load_config()
        .await
        .expect("Failed to load config.toml config with 4-node cluster env overrides");

    assert_eq!(config.cluster.enabled, expected_cluster_enabled);
    assert_eq!(config.cluster.name, expected_cluster_name);
    assert_eq!(
        config.cluster.nodes.len(),
        4,
        "Should have 4 cluster nodes from environment variables"
    );

    for (idx, (name, ip, replica_id, tcp, quic, http, websocket)) in nodes.iter().enumerate() {
        assert_eq!(config.cluster.nodes[idx].name, *name);
        assert_eq!(config.cluster.nodes[idx].ip, *ip);
        assert_eq!(config.cluster.nodes[idx].replica_id, *replica_id);
        assert_eq!(config.cluster.nodes[idx].ports.tcp, Some(*tcp));
        assert_eq!(config.cluster.nodes[idx].ports.quic, Some(*quic));
        assert_eq!(config.cluster.nodes[idx].ports.http, Some(*http));
        assert_eq!(config.cluster.nodes[idx].ports.websocket, Some(*websocket));
    }

    // Sparse node: TCP and HTTP set, QUIC and WebSocket unset.
    assert_eq!(config.cluster.nodes[3].name, sparse_name);
    assert_eq!(config.cluster.nodes[3].ip, sparse_ip);
    assert_eq!(config.cluster.nodes[3].replica_id, sparse_replica_id);
    assert_eq!(config.cluster.nodes[3].ports.tcp, Some(sparse_tcp));
    assert_eq!(
        config.cluster.nodes[3].ports.quic, None,
        "QUIC port should be None (default applied at runtime)"
    );
    assert_eq!(config.cluster.nodes[3].ports.http, Some(sparse_http));
    assert_eq!(
        config.cluster.nodes[3].ports.websocket, None,
        "WebSocket port should be None (default applied at runtime)"
    );

    unsafe {
        env::remove_var("IGGY_CLUSTER_ENABLED");
        env::remove_var("IGGY_CLUSTER_NAME");
        for idx in 0..=3 {
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_NAME"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_IP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_REPLICA_ID"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_TCP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_QUIC"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_HTTP"));
            env::remove_var(format!("IGGY_CLUSTER_NODES_{idx}_PORTS_WEBSOCKET"));
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
#[config_env(tag = "config_type")]
#[serde(tag = "config_type", rename_all = "lowercase")]
enum TestTaggedEnum {
    Local(TestLocalConfig),
    Http(TestHttpConfig),
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
struct TestLocalConfig {
    pub config_dir: String,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
struct TestHttpConfig {
    pub base_url: String,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[config_env(prefix = "TEST_")]
#[serde(default)]
struct TestRootConfig {
    pub name: String,
    pub nested: TestTaggedEnum,
}

impl Default for TestTaggedEnum {
    fn default() -> Self {
        Self::Local(TestLocalConfig::default())
    }
}

#[test]
fn validate_tagged_enum_generates_tag_mapping() {
    let mappings = TestTaggedEnum::env_mappings();

    let has_config_type_mapping = mappings
        .iter()
        .any(|m| m.config_path == "config_type" && m.env_name == "CONFIG_TYPE");

    assert!(
        has_config_type_mapping,
        "Expected env mapping for 'config_type' tag field, but found: {:?}",
        mappings
            .iter()
            .map(|m| format!("{}={}", m.env_name, m.config_path))
            .collect::<Vec<_>>()
    );
}

#[test]
fn validate_nested_tagged_enum_has_prefixed_tag_mapping() {
    let mappings = TestRootConfig::env_mappings();

    println!("All mappings for TestRootConfig:");
    for m in mappings {
        println!("  {} -> {}", m.env_name, m.config_path);
    }

    let has_nested_config_type = mappings
        .iter()
        .any(|m| m.config_path == "nested.config_type" && m.env_name == "TEST_NESTED_CONFIG_TYPE");

    assert!(
        has_nested_config_type,
        "Expected nested tag mapping 'TEST_NESTED_CONFIG_TYPE' -> 'nested.config_type', but found: {:?}",
        mappings
            .iter()
            .map(|m| format!("{}={}", m.env_name, m.config_path))
            .collect::<Vec<_>>()
    );
}

#[serial]
#[tokio::test]
async fn validate_tagged_enum_deserialization_with_figment() {
    // TOML with "local" variant (mirrors connectors/runtime/config.toml)
    let toml_content = r#"
        [nested]
        config_type = "local"
        config_dir = "/some/path"
    "#;

    unsafe {
        env::set_var("TEST_NESTED_CONFIG_TYPE", "http");
        env::set_var("TEST_NESTED_BASE_URL", "http://example.com");
    }

    struct TestEnvProvider;
    impl Provider for TestEnvProvider {
        fn metadata(&self) -> figment::Metadata {
            figment::Metadata::named("test-env")
        }
        fn data(&self) -> Result<figment::value::Map<figment::Profile, Dict>, figment::Error> {
            let provider: TypedEnvProvider<TestRootConfig> = TypedEnvProvider::from_config("TEST_");
            provider.data()
        }
    }

    let config_result: Result<TestRootConfig, figment::Error> = Figment::new()
        .merge(Toml::string(toml_content))
        .merge(TestEnvProvider)
        .extract();

    unsafe {
        env::remove_var("TEST_NESTED_CONFIG_TYPE");
        env::remove_var("TEST_NESTED_BASE_URL");
    }

    match config_result {
        Ok(config) => {
            println!("Config loaded successfully: {:?}", config);
            match config.nested {
                TestTaggedEnum::Http(http) => {
                    assert_eq!(http.base_url, "http://example.com");
                }
                TestTaggedEnum::Local(_) => {
                    panic!("Expected Http variant but got Local");
                }
            }
        }
        Err(e) => {
            panic!("Failed to load config: {}", e);
        }
    }
}

#[test]
fn debug_print_test_root_config_mappings() {
    println!("\n=== TestRootConfig env_mappings() ===");
    for m in TestRootConfig::env_mappings() {
        println!("  {} -> {}", m.env_name, m.config_path);
    }

    println!("\n=== TestTaggedEnum env_mappings() ===");
    for m in TestTaggedEnum::env_mappings() {
        println!("  {} -> {}", m.env_name, m.config_path);
    }

    let has_tag = TestRootConfig::env_mappings()
        .iter()
        .any(|m| m.env_name == "TEST_NESTED_CONFIG_TYPE" && m.config_path == "nested.config_type");
    assert!(
        has_tag,
        "Missing TEST_NESTED_CONFIG_TYPE -> nested.config_type mapping"
    );
}

fn get_root_path() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set!");
    PathBuf::from(manifest_dir)
}
