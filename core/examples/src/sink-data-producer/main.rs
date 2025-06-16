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

use std::{env, str::FromStr, time::Duration};

use chrono::{DateTime, Days, Utc};
use iggy::prelude::{
    Client, DirectConfig, IggyClient, IggyClientBuilder, IggyDuration, IggyError, IggyMessage,
    Partitioning,
};
use rand::{
    Rng,
    distr::{Alphanumeric, Uniform},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

const SOURCES: [&str; 6] = ["browser", "mobile", "desktop", "email", "network", "other"];
const STATES: [&str; 5] = ["active", "inactive", "blocked", "deleted", "unknown"];
const DOMAINS: [&str; 5] = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
];

#[tokio::main]
async fn main() -> Result<(), DataProducerError> {
    Registry::default()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .init();
    info!("Starting data producer...");
    let address = env::var("IGGY_ADDRESS").unwrap_or("localhost:8090".to_owned());
    let username = env::var("IGGY_USERNAME").unwrap_or("iggy".to_owned());
    let password = env::var("IGGY_PASSWORD").unwrap_or("iggy".to_owned());
    let stream = env::var("IGGY_STREAM").unwrap_or("qw".to_owned());
    let topic = env::var("IGGY_TOPIC").unwrap_or("records".to_owned());
    let client = create_client(&address, &username, &password).await?;
    let producer = client
        .producer(&stream, &topic)?
        .direct(
            DirectConfig::builder()
                .batch_length(1000)
                .linger_time(IggyDuration::from_str("5ms").unwrap())
                .build(),
        )
        .partitioning(Partitioning::balanced())
        .build();
    producer.init().await?;

    let mut rng = rand::rng();
    let mut batches_count = 0;
    while batches_count < 100000 {
        let records_count = rng.sample(Uniform::new(500u32, 1000).unwrap());
        let messages = (0..records_count)
            .map(|_| random_record())
            .flat_map(|record| serde_json::to_string(&record).ok())
            .flat_map(|payload| IggyMessage::from_str(&payload).ok())
            .collect::<Vec<_>>();
        producer.send(messages).await?;
        info!("Sent {records_count} messages");
        sleep(Duration::from_millis(10)).await;
        batches_count += 1;
    }

    info!("Reached maximum batches count");
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

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    user_id: String,
    user_type: u8,
    email: String,
    source: String,
    state: String,
    created_at: DateTime<Utc>,
    message: String,
}

fn random_record() -> Record {
    let mut rng = rand::rng();
    let source =
        SOURCES[rng.sample(Uniform::new(0u8, SOURCES.len() as u8).unwrap()) as usize].to_owned();
    let state =
        STATES[rng.sample(Uniform::new(0u8, STATES.len() as u8).unwrap()) as usize].to_owned();
    let email = format!(
        "{}@{}",
        random_string(rng.sample(Uniform::new(3u32, 20).unwrap()) as usize),
        DOMAINS[rng.sample(Uniform::new(0u8, DOMAINS.len() as u8).unwrap()) as usize]
    );
    let created_at = Utc::now()
        .checked_sub_days(Days::new(rng.sample(Uniform::new(0u64, 1000).unwrap())))
        .unwrap();
    Record {
        user_id: format!("user_{}", rng.sample(Uniform::new(1u32, 100).unwrap())),
        user_type: rng.sample(Uniform::new(1u8, 5).unwrap()),
        email,
        source,
        state,
        message: random_string(rng.sample(Uniform::new(10u32, 100).unwrap()) as usize),
        created_at,
    }
}

fn random_string(size: usize) -> String {
    let mut rng = rand::rng();
    let text: String = (0..size)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    text
}

#[derive(Debug, Error)]
enum DataProducerError {
    #[error("Iggy client error")]
    IggyClient(#[from] iggy::prelude::ClientError),
    #[error("Iggy error")]
    IggyError(#[from] iggy::prelude::IggyError),
}
