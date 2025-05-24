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

use config::{Config, Environment, File};
use configs::RuntimeConfig;
use dlopen2::wrapper::{Container, WrapperApi};
use error::RuntimeError;
use iggy::prelude::{Client, IggyClient, IggyClientBuilder, IggyConsumer, IggyProducer};
use iggy_connector_sdk::{
    StreamDecoder, StreamEncoder,
    sink::ConsumeCallback,
    source::{HandleCallback, SendCallback},
    transforms::Transform,
};
use mimalloc::MiMalloc;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, atomic::AtomicU32},
};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) mod configs;
pub(crate) mod error;
mod sink;
mod source;
mod transform;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static PLUGIN_ID: AtomicU32 = AtomicU32::new(1);

const ALLOWED_PLUGIN_EXTENSIONS: [&str; 3] = ["so", "dylib", "dll"];

#[derive(WrapperApi)]
struct SourceApi {
    open: extern "C" fn(id: u32, config_ptr: *const u8, config_len: usize) -> i32,
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
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    let config_path =
        env::var("IGGY_CONNECTORS_RUNTIME_CONFIG_PATH").unwrap_or_else(|_| "config".to_string());
    info!("Starting Iggy Connector Runtime, loading configuration from: {config_path}...");
    let builder = Config::builder()
        .add_source(File::with_name(&config_path))
        .add_source(Environment::with_prefix("IGGY").separator("_"));

    let config: RuntimeConfig = builder
        .build()
        .expect("Failed to build runtime config")
        .try_deserialize()
        .expect("Failed to deserialize runtime config");

    let iggy_address = config.iggy.address;
    let iggy_username = config.iggy.username;
    let iggy_password = config.iggy.password;
    let iggy_token = config.iggy.token;
    let consumer_client = create_iggy_client(
        &iggy_address,
        iggy_username.as_deref(),
        iggy_password.as_deref(),
        iggy_token.as_deref(),
    )
    .await?;
    consumer_client.connect().await?;
    let producer_client = create_iggy_client(
        &iggy_address,
        iggy_username.as_deref(),
        iggy_password.as_deref(),
        iggy_token.as_deref(),
    )
    .await?;
    producer_client.connect().await?;

    let sources = source::init(config.sources, &producer_client).await?;
    let sinks = sink::init(config.sinks, &consumer_client).await?;

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

    producer_client.shutdown().await?;
    consumer_client.shutdown().await?;

    info!("All connectors closed. Runtime shutdown complete.");
    Ok(())
}

pub fn resolve_plugin_path(path: &str) -> String {
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

async fn create_iggy_client(
    address: &str,
    username: Option<&str>,
    password: Option<&str>,
    token: Option<&str>,
) -> Result<IggyClient, RuntimeError> {
    let connection_string = if let Some(token) = token {
        if token.is_empty() {
            error!("Iggy token cannot be empty (if username and password are not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let redacted_token = token.chars().take(3).collect::<String>();
        info!("Using token: {redacted_token}*** for Iggy authentication");
        format!("iggy://{token}@{address}")
    } else {
        info!("Using username and password for Iggy authentication");
        let username = username.ok_or(RuntimeError::MissingIggyCredentials)?;
        if username.is_empty() {
            error!("Iggy password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let password = password.ok_or(RuntimeError::MissingIggyCredentials)?;
        if password.is_empty() {
            error!("Iggy password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingIggyCredentials);
        }

        let redacted_username = username.chars().take(3).collect::<String>();
        let redacted_password = password.chars().take(3).collect::<String>();
        info!(
            "Using username: {redacted_username}***, password: {redacted_password}*** for Iggy authentication"
        );
        format!("iggy://{username}:{password}@{address}")
    };

    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}

struct SinkConnector {
    container: Container<SinkApi>,
    plugins: Vec<SinkConnectorPlugin>,
}

struct SinkConnectorPlugin {
    id: u32,
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
    transforms: Vec<Arc<dyn Transform>>,
    producer: Option<SourceConnectorProducer>,
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
