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

use axum::routing::get;
use config::{Config, Environment, File};
use configs::{McpServerConfig, McpTransport};
use dotenvy::dotenv;
use error::McpRuntimeError;
use figlet_rs::FIGfont;
use iggy::prelude::Identifier;
use rmcp::{
    ServiceExt,
    model::ErrorData,
    transport::{
        StreamableHttpService, stdio, streamable_http_server::session::local::LocalSessionManager,
    },
};
use service::IggyService;
use std::{env, sync::Arc};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

mod configs;
mod error;
mod service;
mod stream;

#[tokio::main]
async fn main() -> Result<(), McpRuntimeError> {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Iggy MCP Server");
    eprintln!("{}", figure.unwrap());

    if let Ok(env_path) = std::env::var("IGGY_MCP_ENV_PATH") {
        if dotenvy::from_path(&env_path).is_ok() {
            eprintln!("Loaded environment variables from path: {env_path}");
        }
    } else if let Ok(path) = dotenv() {
        eprintln!(
            "Loaded environment variables from .env file at path: {}",
            path.display()
        );
    }

    let config_path = env::var("IGGY_MCP_CONFIG_PATH").unwrap_or_else(|_| "config".to_string());

    eprintln!("Loading configuration from: {config_path}");

    let builder = Config::builder()
        .add_source(File::with_name(&config_path))
        .add_source(Environment::with_prefix("IGGY_MCP").separator("_"));

    let config: McpServerConfig = builder
        .build()
        .expect("Failed to build runtime config")
        .try_deserialize()
        .expect("Failed to deserialize runtime config");

    let transport = config.transport;
    if transport == McpTransport::Stdio {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("DEBUG")))
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .init();
    } else {
        Registry::default()
            .with(tracing_subscriber::fmt::layer())
            .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
            .init();
    }

    info!("Starting Iggy MCP Server, transport: {transport}...");

    let consumer_id =
        Identifier::from_str_value(config.iggy.consumer_name.as_deref().unwrap_or("iggy-mcp"))
            .map_err(|error| {
                error!("Failed to create Iggy consumer ID: {:?}", error);
                McpRuntimeError::FailedToCreateConsumerId
            })?;
    let iggy_consumer = Arc::new(iggy::prelude::Consumer::new(consumer_id));
    let iggy_client = Arc::new(stream::init(config.iggy).await?);
    let permissions = Permissions {
        create: config.permissions.create,
        read: config.permissions.read,
        update: config.permissions.update,
        delete: config.permissions.delete,
    };

    if transport == McpTransport::Stdio {
        let Ok(service) = IggyService::new(iggy_client, iggy_consumer, permissions)
            .serve(stdio())
            .await
            .inspect_err(|e| {
                error!("Serving error: {:?}", e);
            })
        else {
            error!("Failed to create service");
            return Err(McpRuntimeError::FailedToCreateService);
        };

        if let Err(error) = service.waiting().await {
            error!("waiting error: {:?}", error);
        }
    } else {
        let Some(http_config) = config.http_api else {
            error!("HTTP API configuration not found");
            return Err(McpRuntimeError::MissingConfig);
        };

        let service = StreamableHttpService::new(
            move || {
                Ok(IggyService::new(
                    iggy_client.clone(),
                    iggy_consumer.clone(),
                    permissions,
                ))
            },
            LocalSessionManager::default().into(),
            Default::default(),
        );

        if !http_config.path.starts_with("/") {
            error!("HTTP API path must start with '/'");
            return Err(McpRuntimeError::InvalidApiPath);
        }

        if http_config.path == "/" {
            error!("HTTP API path cannot be '/'");
            return Err(McpRuntimeError::InvalidApiPath);
        }

        let router = axum::Router::new()
            .route("/", get(|| async { "Iggy MCP Server" }))
            .route("/ping", get(|| async { "pong" }))
            .route("/health", get(|| async { "healthy" }))
            .nest_service(&http_config.path, service);
        let tcp_listener = tokio::net::TcpListener::bind(&http_config.address)
            .await
            .map_err(|error| {
                error!("Failed to bind TCP listener: {:?}", error);
                McpRuntimeError::FailedToStartHttpServer
            })?;
        info!(
            "HTTP API listening on: {}, MCP path: {}",
            http_config.address, http_config.path
        );
        let _ = axum::serve(tcp_listener, router)
            .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.unwrap() })
            .await;
    }

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
            info!("Received SIGINT. Shutting down Iggy MCP Server...");
        },
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Shutting down Iggy MCP Server...");
        }
    }

    info!("Iggy MCP Server stopped successfully");
    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub struct Permissions {
    create: bool,
    read: bool,
    update: bool,
    delete: bool,
}

impl Permissions {
    pub fn ensure_read(&self) -> Result<(), ErrorData> {
        if self.read {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'read' permissions",
                None,
            ))
        }
    }

    pub fn ensure_create(&self) -> Result<(), ErrorData> {
        if self.create {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'create' permissions",
                None,
            ))
        }
    }

    pub fn ensure_update(&self) -> Result<(), ErrorData> {
        if self.update {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'update' permissions",
                None,
            ))
        }
    }

    pub fn ensure_delete(&self) -> Result<(), ErrorData> {
        if self.delete {
            Ok(())
        } else {
            Err(ErrorData::invalid_request(
                "Insufficient 'delete' permissions",
                None,
            ))
        }
    }
}
