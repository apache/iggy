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

use crate::configs::connectors::{ConnectorsConfigProvider, create_connectors_config_provider};
use ::configs::ConfigProvider;
use clap::Parser;
use configs::connectors::ConfigFormat;
use configs::runtime::ConnectorsRuntimeConfig;
use dlopen2::wrapper::{Container, WrapperApi};
use dotenvy::dotenv;
use error::RuntimeError;
use figlet_rs::FIGfont;
use iggy::prelude::{Client, IggyConsumer, IggyProducer};
use iggy_connector_sdk::{
    StreamDecoder, StreamEncoder,
    sink::ConsumeCallback,
    source::{HandleCallback, SendCallback},
    transforms::Transform,
};
use mimalloc::MiMalloc;
use state::StateStorage;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, atomic::AtomicU32},
};
use tracing::info;

mod api;
pub(crate) mod configs;
pub(crate) mod context;
pub(crate) mod error;
mod log;
mod manager;
pub(crate) mod metrics;
mod sink;
mod source;
mod state;
pub(crate) mod stats;
mod stream;
mod transform;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(author = "Apache Iggy (Incubating)", version)]
struct Args {}

static PLUGIN_ID: AtomicU32 = AtomicU32::new(1);
const ALLOWED_PLUGIN_EXTENSIONS: [&str; 3] = ["so", "dylib", "dll"];
const DEFAULT_CONFIG_PATH: &str = "core/connectors/runtime/config.toml";

#[derive(WrapperApi)]
struct SourceApi {
    open: extern "C" fn(
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        state_ptr: *const u8,
        state_len: usize,
        log_callback: iggy_connector_sdk::LogCallback,
    ) -> i32,
    handle: extern "C" fn(id: u32, callback: SendCallback) -> i32,
    close: extern "C" fn(id: u32) -> i32,
    version: extern "C" fn() -> *const std::ffi::c_char,
}

#[derive(WrapperApi)]
struct SinkApi {
    open: extern "C" fn(
        id: u32,
        config_ptr: *const u8,
        config_len: usize,
        log_callback: iggy_connector_sdk::LogCallback,
    ) -> i32,
    #[allow(clippy::too_many_arguments)]
    consume: extern "C" fn(
        id: u32,
        topic_meta_ptr: *const u8,
        topic_meta_len: usize,
        messages_meta_ptr: *const u8,
        messages_meta_len: usize,
        messages_ptr: *const u8,
        messages_len: usize,
    ) -> i32,
    close: extern "C" fn(id: u32) -> i32,
    version: extern "C" fn() -> *const std::ffi::c_char,
}

fn print_ascii_art(text: &str) {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert(text);
    println!("{}", figure.unwrap());
}

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    Args::parse();
    print_ascii_art("Iggy Connectors");

    if let Ok(env_path) = std::env::var("IGGY_CONNECTORS_ENV_PATH") {
        if dotenvy::from_path(&env_path).is_ok() {
            println!("Loaded environment variables from path: {env_path}");
        }
    } else if let Ok(path) = dotenv() {
        println!(
            "Loaded environment variables from .env file at path: {}",
            path.display()
        );
    }

    let config_path =
        env::var("IGGY_CONNECTORS_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
    println!("Starting Iggy Connectors Runtime, loading configuration from: {config_path}...");

    let config: ConnectorsRuntimeConfig = ConnectorsRuntimeConfig::config_provider(config_path)
        .load_config()
        .await
        .expect("Failed to load configuration");

    log::init_logging(&config.telemetry, VERSION);

    std::fs::create_dir_all(&config.state.path).expect("Failed to create state directory");

    info!("State will be stored in: {}", config.state.path);

    let iggy_clients = stream::init(config.iggy.clone()).await?;

    let connectors_config_provider: Box<dyn ConnectorsConfigProvider> =
        create_connectors_config_provider(&config.connectors).await?;

    let connectors_config = connectors_config_provider.get_active_configs().await?;
    info!(
        "Found {} source and {} sink configurations.",
        connectors_config.sources().len(),
        connectors_config.sinks().len()
    );
    let sources_config = connectors_config.sources();
    let sources = source::init(
        sources_config.clone(),
        &iggy_clients.producer,
        &config.state.path,
    )
    .await?;

    let sinks_config = connectors_config.sinks();
    let sinks = sink::init(sinks_config.clone(), &iggy_clients.consumer).await?;

    let mut sink_wrappers = vec![];
    let mut sink_with_plugins = HashMap::new();
    for (key, sink) in sinks {
        let plugin_ids = sink
            .plugins
            .iter()
            .filter(|plugin| plugin.error.is_none())
            .map(|plugin| plugin.id)
            .collect();
        sink_wrappers.push(SinkConnectorWrapper {
            callback: sink.container.consume,
            plugins: sink.plugins,
        });
        sink_with_plugins.insert(
            key,
            SinkWithPlugins {
                container: sink.container,
                plugin_ids,
            },
        );
    }

    let mut source_wrappers = vec![];
    let mut source_with_plugins = HashMap::new();
    for (key, source) in sources {
        let plugin_ids = source
            .plugins
            .iter()
            .filter(|plugin| plugin.error.is_none())
            .map(|plugin| plugin.id)
            .collect();
        source_wrappers.push(SourceConnectorWrapper {
            callback: source.container.handle,
            plugins: source.plugins,
        });
        source_with_plugins.insert(
            key,
            SourceWithPlugins {
                container: source.container,
                plugin_ids,
            },
        );
    }

    let context = context::init(
        &config,
        sinks_config,
        sources_config,
        &sink_wrappers,
        &source_wrappers,
        connectors_config_provider,
    );
    let context = Arc::new(context);
    api::init(&config.http, context.clone()).await;

    let source_handler_tasks = source::handle(source_wrappers, context.clone());
    sink::consume(sink_wrappers, context.clone());
    info!("All sources and sinks spawned.");

    #[cfg(unix)]
    let (mut ctrl_c, mut sigterm) = {
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::interrupt()).expect("Failed to create SIGINT signal"),
            signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal"),
        )
    };

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c.recv() => {
            info!("Received SIGINT. Shutting down connectors runtime...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down connectors runtime...");
        }
    }

    for (key, source) in source_with_plugins {
        for id in source.plugin_ids {
            info!("Closing source connector with ID: {id} for plugin: {key}");
            source.container.close(id);
            source::cleanup_sender(id);
            info!("Closed source connector with ID: {id} for plugin: {key}");
        }
    }

    // Wait for source handler tasks to drain remaining messages and persist state
    // before shutting down the Iggy clients they depend on.
    for handle in source_handler_tasks {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    }

    for (key, sink) in sink_with_plugins {
        for id in sink.plugin_ids {
            info!("Closing sink connector with ID: {id} for plugin: {key}");
            sink.container.close(id);
            info!("Closed sink connector with ID: {id} for plugin: {key}");
        }
    }

    iggy_clients.producer.shutdown().await?;
    iggy_clients.consumer.shutdown().await?;
    info!("All connectors closed. Runtime shutdown complete.");
    Ok(())
}

/// Resolves a plugin shared library path from the connector config `path` field.
///
/// Accepts both `plugin.so` and `plugin` (OS-specific extension appended if missing).
/// For absolute paths, checks existence at the literal location.
/// For relative paths, searches in order:
///   1. Literal path (relative to working directory)
///   2. Directory of the runtime binary
///   3. Current working directory (filename only)
///   4. /usr/lib
///   5. /usr/lib64
///   6. /lib
///   7. /lib64
///   8. /usr/local/lib
///   9. /usr/local/lib64
pub(crate) fn resolve_plugin_path(path: &str) -> Result<String, RuntimeError> {
    let extension = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    let with_extension = if ALLOWED_PLUGIN_EXTENSIONS.contains(&extension) {
        path.to_string()
    } else {
        let os_extension = match std::env::consts::OS {
            "macos" => "dylib",
            "windows" => "dll",
            _ => "so",
        };
        format!("{path}.{os_extension}")
    };

    let candidate = std::path::Path::new(&with_extension);

    if candidate.exists() {
        info!("Resolved plugin path: {with_extension}");
        return Ok(with_extension);
    }

    if candidate.is_relative() {
        let Some(file_name) = candidate.file_name() else {
            return Err(RuntimeError::InvalidConfiguration(format!(
                "Invalid plugin path: '{with_extension}'"
            )));
        };

        let search_dirs: Vec<std::path::PathBuf> = [
            std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.to_path_buf())),
            std::env::current_dir().ok(),
            Some(std::path::PathBuf::from("/usr/lib")),
            Some(std::path::PathBuf::from("/usr/lib64")),
            Some(std::path::PathBuf::from("/lib")),
            Some(std::path::PathBuf::from("/lib64")),
            Some(std::path::PathBuf::from("/usr/local/lib")),
            Some(std::path::PathBuf::from("/usr/local/lib64")),
        ]
        .into_iter()
        .flatten()
        .collect();

        for dir in &search_dirs {
            let full = dir.join(file_name);
            if full.exists() {
                let resolved = match full.to_str() {
                    Some(s) => s.to_owned(),
                    None => continue,
                };
                info!(
                    "Resolved plugin path: {resolved} (found in {})",
                    dir.display()
                );
                return Ok(resolved);
            }
        }

        let searched: Vec<String> = std::iter::once(with_extension.clone())
            .chain(search_dirs.iter().filter_map(|d| {
                let full = d.join(file_name);
                full.to_str().map(|s| s.to_owned())
            }))
            .collect();

        return Err(RuntimeError::InvalidConfiguration(format!(
            "Plugin library not found. Searched paths:\n{}\n\
             Ensure the shared library (.so/.dylib/.dll) is built and placed in one of these locations.",
            searched
                .iter()
                .map(|p| format!("  - {p}"))
                .collect::<Vec<_>>()
                .join("\n")
        )));
    }

    Err(RuntimeError::InvalidConfiguration(format!(
        "Plugin library not found at '{with_extension}'. \
         Ensure the shared library file exists at this path."
    )))
}

struct SinkConnector {
    container: Container<SinkApi>,
    plugins: Vec<SinkConnectorPlugin>,
}

struct SinkConnectorPlugin {
    id: u32,
    key: String,
    name: String,
    path: String,
    version: String,
    config_format: Option<ConfigFormat>,
    consumers: Vec<SinkConnectorConsumer>,
    error: Option<String>,
    verbose: bool,
}

struct SinkConnectorConsumer {
    batch_size: u32,
    consumer: IggyConsumer,
    decoder: Arc<dyn StreamDecoder>,
    transforms: Vec<Arc<dyn Transform>>,
}

struct SinkConnectorWrapper {
    callback: ConsumeCallback,
    plugins: Vec<SinkConnectorPlugin>,
}

struct SinkWithPlugins {
    container: Container<SinkApi>,
    plugin_ids: Vec<u32>,
}

struct SourceConnector {
    container: Container<SourceApi>,
    plugins: Vec<SourceConnectorPlugin>,
}

struct SourceConnectorPlugin {
    id: u32,
    key: String,
    name: String,
    path: String,
    version: String,
    config_format: Option<ConfigFormat>,
    transforms: Vec<Arc<dyn Transform>>,
    producer: Option<SourceConnectorProducer>,
    state_storage: StateStorage,
    error: Option<String>,
    verbose: bool,
}

struct SourceConnectorProducer {
    encoder: Arc<dyn StreamEncoder>,
    producer: IggyProducer,
}

struct SourceWithPlugins {
    container: Container<SourceApi>,
    plugin_ids: Vec<u32>,
}

struct SourceConnectorWrapper {
    callback: HandleCallback,
    plugins: Vec<SourceConnectorPlugin>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn path_with_known_extension_is_preserved() {
        let result = resolve_plugin_path("/tmp/nonexistent_test_plugin.so");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent_test_plugin.so"),
            "Error should mention the .so path, got: {err}"
        );
    }

    #[test]
    fn path_without_extension_gets_os_suffix() {
        let result = resolve_plugin_path("/tmp/nonexistent_test_plugin");
        let err = result.unwrap_err().to_string();
        let expected_ext = match std::env::consts::OS {
            "macos" => "dylib",
            _ => "so",
        };
        assert!(
            err.contains(&format!("nonexistent_test_plugin.{expected_ext}")),
            "Error should mention OS-specific extension, got: {err}"
        );
    }

    #[test]
    fn nonexistent_relative_path_lists_searched_locations() {
        let result = resolve_plugin_path("nonexistent_test_plugin.so");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Searched paths:"),
            "Should list searched paths, got: {err}"
        );
    }

    #[test]
    fn nonexistent_absolute_path_returns_specific_error() {
        let result = resolve_plugin_path("/no/such/dir/plugin.so");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("/no/such/dir/plugin.so"),
            "Should mention the exact path, got: {err}"
        );
    }

    #[test]
    fn existing_file_resolves_directly() {
        let dir = TempDir::new().unwrap();
        let plugin_path = dir.path().join("test_plugin.so");
        fs::write(&plugin_path, b"fake-plugin").unwrap();

        let result = resolve_plugin_path(plugin_path.to_str().unwrap())
            .expect("should resolve existing file");
        assert_eq!(result, plugin_path.to_str().unwrap());
    }
}
