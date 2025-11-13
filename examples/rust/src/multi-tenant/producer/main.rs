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

use ahash::AHashMap;
use futures_util::future::join_all;
use iggy::prelude::*;
use iggy_examples::shared::args::Args;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

const TOPICS: &[&str] = &["events", "logs", "notifications"];
const PASSWORD: &str = "secret";

struct Tenant {
    id: u32,
    stream: String,
    user: String,
    client: IggyClient,
    producers: Vec<TenantProducer>,
}

impl Tenant {
    pub fn new(id: u32, stream: String, user: String, client: IggyClient) -> Self {
        Self {
            id,
            stream,
            user,
            client,
            producers: Vec::new(),
        }
    }

    pub fn add_producers(&mut self, producers: Vec<TenantProducer>) {
        self.producers.extend(producers);
    }
}

struct TenantProducer {
    id: u32,
    stream: String,
    topic: String,
    producer: IggyProducer,
}

impl TenantProducer {
    pub fn new(id: u32, stream: String, topic: String, producer: IggyProducer) -> Self {
        Self {
            id,
            stream,
            topic,
            producer,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse_with_defaults("multi-tenant-producer");
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    let tenants_count = env::var("TENANTS_COUNT")
        .unwrap_or_else(|_| 3.to_string())
        .parse::<u32>()
        .expect("Invalid tenants count");

    let producers_count = env::var("PRODUCERS_COUNT")
        .unwrap_or_else(|_| 3.to_string())
        .parse::<u32>()
        .expect("Invalid producers count");

    let partitions_count = env::var("PARTITIONS_COUNT")
        .unwrap_or_else(|_| 3.to_string())
        .parse::<u32>()
        .expect("Invalid partitions count");

    let ensure_access = env::var("ENSURE_ACCESS")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .expect("Invalid ensure stream access");

    print_info(&format!(
        "Multi-tenant producer has started, tenants: {tenants_count}, producers: {producers_count}, partitions: {partitions_count}"
    ));
    let address = args.tcp_server_address;

    print_info("Creating root client to manage streams and users");
    let root_client = create_client(&address, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await?;

    print_info("Creating streams and users with permissions for each tenant");
    let mut streams_with_users = HashMap::new();
    for i in 1..=tenants_count {
        let name = format!("tenant_{i}");
        let stream = format!("{name}_stream");
        let user = format!("{name}_producer");
        create_stream_and_user(&stream, &user, &root_client).await?;
        streams_with_users.insert(stream, user);
    }

    print_info("Disconnecting root client");
    root_client.shutdown().await?;

    print_info("Creating clients for each tenant");
    let mut tenants = Vec::new();
    let mut tenant_id = 1;
    for (stream, user) in streams_with_users.into_iter() {
        let client = create_client(&address, &user, PASSWORD).await?;
        tenants.push(Tenant::new(tenant_id, stream, user, client));
        tenant_id += 1;
    }

    if ensure_access {
        print_info("Ensuring access to streams for each tenant");
        for tenant in tenants.iter() {
            let unavailable_streams = tenants
                .iter()
                .filter(|t| t.stream != tenant.stream)
                .map(|t| t.stream.as_str())
                .collect::<Vec<_>>();
            ensure_stream_access(&tenant.client, &tenant.stream, &unavailable_streams).await?;
        }
    }

    print_info(&format!(
        "Creating {producers_count} producer(s) for each tenant"
    ));
    for tenant in tenants.iter_mut() {
        let producers = create_producers(
            &tenant.client,
            producers_count,
            partitions_count,
            &tenant.stream,
            TOPICS,
            args.messages_per_batch,
            &args.interval,
        )
        .await?;
        tenant.add_producers(producers);
        info!(
            "Created {producers_count} producer(s) for tenant stream: {}, username: {}",
            tenant.stream, tenant.user
        );
    }

    print_info(&format!(
        "Starting {producers_count} producer(s) for each tenant"
    ));
    let mut tasks = Vec::new();
    for tenant in tenants.into_iter() {
        let producers_tasks = start_producers(
            tenant.id,
            tenant.producers,
            args.message_batches_limit,
            args.messages_per_batch,
        );
        tasks.extend(producers_tasks);
    }

    join_all(tasks).await;
    print_info("Disconnecting clients");
    Ok(())
}

fn start_producers(
    tenant_id: u32,
    producers: Vec<TenantProducer>,
    batches_count: u64,
    batch_length: u32,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    let topics_count = producers
        .iter()
        .map(|p| p.topic.as_str())
        .collect::<Vec<_>>()
        .len() as u64;
    for producer in producers {
        let producer_id = producer.id;
        let task = tokio::spawn(async move {
            let mut counter = 1;
            let mut events_id = 1;
            let mut logs_id = 1;
            let mut notifications_id = 1;
            while counter <= topics_count * batches_count {
                let (message_id, message) = match producer.topic.as_str() {
                    "events" => {
                        events_id += 1;
                        (events_id, "event")
                    }
                    "logs" => {
                        logs_id += 1;
                        (logs_id, "log")
                    }
                    "notifications" => {
                        notifications_id += 1;
                        (notifications_id, "notification")
                    }
                    _ => panic!("Invalid topic"),
                };

                let mut messages = Vec::with_capacity(batch_length as usize);
                for _ in 1..=batch_length {
                    let payload = format!("{message}-{producer_id}-{message_id}");
                    let message = IggyMessage::from_str(&payload).expect("Invalid message");
                    messages.push(message);
                }

                if let Err(error) = producer.producer.send(messages).await {
                    error!(
                        "Failed to send: {batch_length} message(s) to: {} -> {} by tenant: {tenant_id}, producer: {producer_id} with error: {error}",
                        producer.stream, producer.topic,
                    );
                    continue;
                }

                counter += 1;
                info!(
                    "Sent: {batch_length} message(s) by tenant: {tenant_id}, producer: {producer_id}, to: {} -> {}",
                    producer.stream, producer.topic
                );
            }
        });
        tasks.push(task);
    }
    tasks
}

async fn create_producers(
    client: &IggyClient,
    producers_count: u32,
    partitions_count: u32,
    stream: &str,
    topics: &[&str],
    batch_length: u32,
    interval: &str,
) -> Result<Vec<TenantProducer>, IggyError> {
    let mut producers = Vec::new();
    for topic in topics {
        for id in 1..=producers_count {
            let producer = client
                .producer(stream, topic)?
                .direct(
                    DirectConfig::builder()
                        .batch_length(batch_length)
                        .linger_time(IggyDuration::from_str(interval).expect("Invalid duration"))
                        .build(),
                )
                .partitioning(Partitioning::balanced())
                .create_topic_if_not_exists(
                    partitions_count,
                    None,
                    IggyExpiry::ServerDefault,
                    MaxTopicSize::ServerDefault,
                )
                .build();
            producer.init().await?;
            producers.push(TenantProducer::new(
                id,
                stream.to_owned(),
                topic.to_string(),
                producer,
            ));
        }
    }
    Ok(producers)
}

async fn ensure_stream_access(
    client: &IggyClient,
    available_stream: &str,
    unavailable_streams: &[&str],
) -> Result<(), IggyError> {
    client
        .get_stream(&available_stream.try_into()?)
        .await?
        .unwrap_or_else(|| panic!("No access to stream: {available_stream}"));
    info!("Ensured access to stream: {available_stream}");
    for stream in unavailable_streams {
        if client
            .get_stream(&Identifier::named(stream)?)
            .await?
            .is_none()
        {
            info!("Ensured no access to stream: {stream}");
        } else {
            panic!("Access to stream: {stream} should not be allowed");
        }
    }
    Ok(())
}

async fn create_client(
    address: &str,
    username: &str,
    password: &str,
) -> Result<IggyClient, IggyError> {
    let connection_string = format!("iggy://{username}:{password}@{address}");
    let client = IggyClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}

async fn create_stream_and_user(
    stream_name: &str,
    username: &str,
    client: &IggyClient,
) -> Result<(), IggyError> {
    let stream = client.create_stream(stream_name).await?;
    info!("Created stream: {stream_name} with ID: {}", stream.id);
    let mut streams_permissions = AHashMap::new();
    streams_permissions.insert(
        stream.id as usize,
        StreamPermissions {
            read_stream: true,
            manage_topics: true,
            ..Default::default()
        },
    );
    let permissions = Permissions {
        streams: Some(streams_permissions),
        ..Default::default()
    };
    let user = client
        .create_user(username, PASSWORD, UserStatus::Active, Some(permissions))
        .await?;
    info!(
        "Created user: {username} with ID: {}, with permissions for stream: {stream_name}",
        user.id
    );
    Ok(())
}

fn print_info(message: &str) {
    info!("\n\n--- {message} ---\n");
}
