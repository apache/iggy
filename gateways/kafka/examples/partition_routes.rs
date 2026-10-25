// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Prints the metadata primary and the primary of each partition of one topic, as one JSON line
//! a second, until stopped. `scripts/cluster-failover.sh` reads it.
//!
//! ```bash
//! cargo run --release -p iggy-gateway-kafka --example partition_routes -- \
//!     127.0.0.1:28000 kafka orders 6
//! ```
//!
//! `IGGY_USERNAME` and `IGGY_PASSWORD` default to `iggy`.

use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iggy::prelude::{
    AutoLogin, Client, ClusterClient, ClusterNodeRole, Credentials, IggyClient, IggyClientBuilder,
};
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
use secrecy::SecretString;

const INTERVAL: Duration = Duration::from_secs(1);
/// A poll that runs past this prints an error line, so the series keeps its pace.
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [address, stream, topic, partitions] = args.as_slice() else {
        return Err("usage: partition_routes <iggy address> <stream> <topic> <partitions>".into());
    };
    let partitions: u32 = partitions.parse()?;
    let username = std::env::var("IGGY_USERNAME").unwrap_or_else(|_| "iggy".to_string());
    let password = std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| "iggy".to_string());
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(address.clone())
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            username,
            SecretString::from(password),
        )))
        .build()?;
    client.connect().await?;
    let requests = (0..partitions)
        .map(|partition_id| {
            Ok(GetPartitionRouteRequest {
                stream_id: WireIdentifier::named(stream.as_str())?,
                topic_id: WireIdentifier::named(topic.as_str())?,
                partition_id,
            }
            .to_bytes())
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

    let mut ticks = tokio::time::interval(INTERVAL);
    loop {
        ticks.tick().await;
        let at_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let line = match tokio::time::timeout(POLL_TIMEOUT, routes(&client, &requests)).await {
            Ok(Ok((metadata_primary, routes))) => {
                let primaries: Vec<u8> = routes.iter().map(|route| route.primary_replica).collect();
                let views: Vec<u32> = routes.iter().map(|route| route.view).collect();
                format!(
                    r#"{{"at_ms":{at_ms},"metadata_primary":{metadata_primary},"primaries":{primaries:?},"views":{views:?}}}"#
                )
            }
            Ok(Err(error)) => {
                let error = error.to_string().replace(['"', '\\'], "'");
                format!(r#"{{"at_ms":{at_ms},"error":"{error}"}}"#)
            }
            Err(_) => format!(r#"{{"at_ms":{at_ms},"error":"no answer within {POLL_TIMEOUT:?}"}}"#),
        };
        println!("{line}");
    }
}

/// The metadata primary, then the route of each partition, as the node that answers sees them.
async fn routes(
    client: &IggyClient,
    requests: &[bytes::Bytes],
) -> Result<(u8, Vec<PartitionRouteResponse>), Box<dyn Error>> {
    let metadata = client.get_cluster_metadata().await?;
    let metadata_primary = metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .ok_or("no node is the metadata primary")?
        .replica_id;
    let mut routes = Vec::with_capacity(requests.len());
    for request in requests {
        let body = client
            .send_binary_request(GET_PARTITION_ROUTE_CODE, request.clone())
            .await?;
        routes.push(PartitionRouteResponse::decode_from(&body)?);
    }
    Ok((metadata_primary, routes))
}
