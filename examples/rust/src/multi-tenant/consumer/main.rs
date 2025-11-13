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
use futures_util::StreamExt;
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
const CONSUMER_GROUP: &str = "multi-tenant";
const PASSWORD: &str = "secret";

struct Tenant {
    id: u32,
    stream: String,
    user: String,
    client: IggyClient,
    consumers: Vec<TenantConsumer>,
}

impl Tenant {
    pub fn new(id: u32, stream: String, user: String, client: IggyClient) -> Self {
        Self {
            id,
            stream,
            user,
            client,
            consumers: Vec::new(),
        }
    }

    pub fn add_consumers(&mut self, consumers: Vec<TenantConsumer>) {
        self.consumers.extend(consumers);
    }
}

struct TenantConsumer {
    id: u32,
    stream: String,
    topic: String,
    consumer: IggyConsumer,
}

impl TenantConsumer {
    pub fn new(id: u32, stream: String, topic: String, consumer: IggyConsumer) -> Self {
        Self {
            id,
            stream,
            topic,
            consumer,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse_with_defaults("multi-tenant-consumer");
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    let tenants_count = env::var("TENANTS_COUNT")
        .unwrap_or_else(|_| 3.to_string())
        .parse::<u32>()
        .expect("Invalid tenants count");

    let consumers_count = env::var("CONSUMERS_COUNT")
        .unwrap_or_else(|_| 1.to_string())
        .parse::<u32>()
        .expect("Invalid consumers count");

    let ensure_access = env::var("ENSURE_ACCESS")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .expect("Invalid ensure stream access");

    print_info(&format!(
        "Multi-tenant consumers has started, tenants: {tenants_count}, consumers: {consumers_count}"
    ));
    let address = args.tcp_server_address;

    print_info("Creating root client to manage streams and users");
    let root_client = create_client(&address, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await?;

    print_info("Creating users with stream permissions for each tenant");
    let mut streams_with_users = HashMap::new();
    for i in 1..=tenants_count {
        let name = format!("tenant_{i}");
        let stream = format!("{name}_stream");
        let user = format!("{name}_consumer");
        create_user(&stream, TOPICS, &user, &root_client).await?;
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
        print_info("Ensuring access to stream topics for each tenant");
        for tenant in tenants.iter() {
            let unavailable_streams = tenants
                .iter()
                .filter(|t| t.stream != tenant.stream)
                .map(|t| t.stream.as_str())
                .collect::<Vec<_>>();
            ensure_stream_topics_access(
                &tenant.client,
                TOPICS,
                &tenant.stream,
                &unavailable_streams,
            )
            .await?;
        }
    }

    print_info(&format!(
        "Creating {consumers_count} consumer(s) for each tenant"
    ));
    for tenant in tenants.iter_mut() {
        let consumers = create_consumers(
            &tenant.client,
            consumers_count,
            &tenant.stream,
            TOPICS,
            args.messages_per_batch,
            &args.interval,
        )
        .await?;
        tenant.add_consumers(consumers);
        info!(
            "Created {consumers_count} consumer(s) for tenant stream: {}, username: {}",
            tenant.stream, tenant.user
        );
    }

    print_info(&format!(
        "Starting {consumers_count} consumers(s) for each tenant"
    ));
    let mut tasks = Vec::new();
    for tenant in tenants.into_iter() {
        let consumer_tasks =
            start_consumers(tenant.id, tenant.consumers, args.message_batches_limit);
        tasks.extend(consumer_tasks);
    }

    join_all(tasks).await;
    print_info("Disconnecting clients");
    Ok(())
}

async fn create_user(
    stream_name: &str,
    topics: &[&str],
    username: &str,
    client: &IggyClient,
) -> Result<(), IggyError> {
    let stream = client
        .get_stream(&stream_name.try_into()?)
        .await?
        .expect("Stream does not exist");
    let mut topic_permissions = AHashMap::new();
    for topic in topics {
        let topic_id = Identifier::named(topic)?;
        let topic = client
            .get_topic(&stream_name.try_into()?, &topic_id)
            .await?
            .expect("Topic does not exist");

        topic_permissions.insert(
            topic.id as usize,
            TopicPermissions {
                read_topic: true,
                poll_messages: true,
                ..Default::default()
            },
        );
    }

    let mut streams_permissions = AHashMap::new();
    streams_permissions.insert(
        stream.id as usize,
        StreamPermissions {
            read_stream: true,
            topics: Some(topic_permissions),
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
        "Created user: {username} with ID: {}, with permissions for topics: {:?} in stream: {stream_name}",
        user.id, topics
    );
    Ok(())
}

fn start_consumers(
    tenant_id: u32,
    consumers: Vec<TenantConsumer>,
    message_batches_limit: u64,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    for mut consumer in consumers {
        let task = tokio::spawn(async move {
            let consumer_id = consumer.id;
            let stream = consumer.stream;
            let topic = consumer.topic;
            let mut batches_processed = 0u64;
            while let Some(message) = consumer.consumer.next().await {
                if batches_processed >= message_batches_limit {
                    info!(
                        "Tenant: {tenant_id} consumer: {consumer_id} reached message batches limit: {message_batches_limit}, stopping."
                    );
                    break;
                }
                if let Ok(message) = message {
                    let current_offset = message.current_offset;
                    let partition_id = message.partition_id;
                    let offset = message.message.header.offset;
                    let payload = std::str::from_utf8(&message.message.payload);
                    if let Err(error) = payload {
                        error!(
                            "Error while decoding the message payload at offset: {offset}, partition ID: {partition_id}, perhaps it's encrypted? {error}"
                        );
                        continue;
                    }

                    let payload = payload.unwrap();
                    info!(
                        "Tenant: {tenant_id} consumer: {consumer_id} received: {payload} from partition: {partition_id}, topic: {topic}, stream: {stream}, at offset: {offset}, current offset: {current_offset}"
                    );
                    batches_processed += 1;
                } else if let Err(error) = message {
                    error!(
                        "Error while handling message: {error} by tenant: {tenant_id} consumer: {consumer_id}, topic: {topic}, stream: {stream}"
                    );
                    continue;
                }
            }
        });
        tasks.push(task);
    }
    tasks
}

async fn create_consumers(
    client: &IggyClient,
    consumers_count: u32,
    stream: &str,
    topics: &[&str],
    batch_length: u32,
    interval: &str,
) -> Result<Vec<TenantConsumer>, IggyError> {
    let mut consumers = Vec::new();
    for topic in topics {
        for id in 1..=consumers_count {
            let mut consumer = client
                .consumer_group(CONSUMER_GROUP, stream, topic)?
                .batch_length(batch_length)
                .poll_interval(IggyDuration::from_str(interval).expect("Invalid duration"))
                .polling_strategy(PollingStrategy::next())
                .auto_join_consumer_group()
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .build();
            consumer.init().await?;
            consumers.push(TenantConsumer::new(
                id,
                stream.to_owned(),
                topic.to_string(),
                consumer,
            ));
        }
    }
    Ok(consumers)
}

async fn ensure_stream_topics_access(
    client: &IggyClient,
    topics: &[&str],
    available_stream: &str,
    unavailable_streams: &[&str],
) -> Result<(), IggyError> {
    for topic in topics {
        let topic_id = Identifier::named(topic)?;
        client
            .get_topic(&available_stream.try_into()?, &topic_id)
            .await?
            .unwrap_or_else(|| panic!("No access to topic: {topic} in stream: {available_stream}"));
        info!("Ensured access to topic: {topic} in stream: {available_stream}");
        for stream in unavailable_streams {
            if client
                .get_topic(&Identifier::named(stream)?, &topic_id)
                .await?
                .is_none()
            {
                info!("Ensured no access to topic: {topic} in stream: {stream}");
            } else {
                panic!("Access to topic: {topic} in stream: {stream} should not be allowed");
            }
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
    let client = IggyClient::builder_from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}

fn print_info(message: &str) {
    info!("\n\n--- {message} ---\n");
}
