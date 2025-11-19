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
use configs::connectors::ConfigFormat;
use configs::runtime::ConnectorsRuntimeConfig;
use dlopen2::wrapper::{Container, WrapperApi};
use dotenvy::dotenv;
use error::RuntimeError;
use figlet_rs::FIGfont;
use iggy::prelude::{Client, IggyConsumer, IggyProducer};
use iggy_common::ConfigProvider;
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
use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

mod api;
pub(crate) mod configs;
pub(crate) mod context;
pub(crate) mod error;
mod manager;
mod sink;
mod source;
mod state;
mod stream;
mod transform;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    ) -> i32,
    handle: extern "C" fn(id: u32, callback: SendCallback) -> i32,
    close: extern "C" fn(id: u32) -> i32,
}

#[derive(WrapperApi)]
struct SinkApi {
    open: extern "C" fn(id: u32, config_ptr: *const u8, config_len: usize) -> i32,
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
}

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy Connectors");
    println!("{}", figure.unwrap());

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

    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();

    let config_path =
        env::var("IGGY_CONNECTORS_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
    info!("Starting Iggy Connectors Runtime, loading configuration from: {config_path}...");

    let config: ConnectorsRuntimeConfig = ConnectorsRuntimeConfig::config_provider(config_path)
        .load_config()
        .await
        .expect("Failed to load configuration");

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
        let plugin_ids = sink.plugins.iter().map(|plugin| plugin.id).collect();
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
        let plugin_ids = source.plugins.iter().map(|plugin| plugin.id).collect();
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
    api::init(&config.http, context).await;

    source::handle(source_wrappers);
    sink::consume(sink_wrappers);
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
            info!("Closed source connector with ID: {id} for plugin: {key}");
        }
    }

    for (key, sink) in sink_with_plugins {
        for id in sink.plugin_ids {
            info!("Closing sink connector with ID: {id} for plugin: {key}",);
            sink.container.close(id);
            info!("Closed sink connector with ID: {id} for plugin: {key}");
        }
    }

    iggy_clients.producer.shutdown().await?;
    iggy_clients.consumer.shutdown().await?;
    info!("All connectors closed. Runtime shutdown complete.");
    Ok(())
}

pub(crate) fn resolve_plugin_path(path: &str) -> String {
    let extension = path.split('.').next_back().unwrap_or_default();
    if ALLOWED_PLUGIN_EXTENSIONS.contains(&extension) {
        path.to_string()
    } else {
        let os = std::env::consts::OS;
        let os_extension = match os {
            "windows" => "dll",
            "macos" => "dylib",
            _ => "so",
        };

        debug!("Resolved plugin path: {path}.{os_extension} for detected OS: {os}");
        format!("{path}.{os_extension}")
    }
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
    config_format: Option<ConfigFormat>,
    consumers: Vec<SinkConnectorConsumer>,
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
    config_format: Option<ConfigFormat>,
    transforms: Vec<Arc<dyn Transform>>,
    producer: Option<SourceConnectorProducer>,
    state_storage: StateStorage,
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
