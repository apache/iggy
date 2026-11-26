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

//! A geo topic on one node with the `s3` backend, against Floci. Docker must
//! run.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::thread::{self, JoinHandle};

use integration::harness::floci::{self, ACCESS_KEY_ID, FlociBucket, REGION, SECRET_ACCESS_KEY};
use integration::harness::{TestHarness, TestServerConfig};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};

use super::http_client::HttpClient;
use crate::server::scenarios::geo_mode_faults_scenario::{
    Answer, Bucket, Ledger, RawSender, StoreFault, Topics, assert_settled,
    produce_through_store_fault,
};

const MESSAGES: u64 = 20;
const MESSAGE_BYTES: usize = 1024;
/// About 12 MiB in all, more than twice the smallest part size, so a
/// long-term segment of them goes up in parts.
const LARGE_MESSAGES: u64 = 24;
const LARGE_MESSAGE_BYTES: usize = 512 * 1024;

#[tokio::test]
async fn given_a_geo_topic_on_s3_when_producing_should_store_the_sends_and_read_each_once_from_long_term_segments()
 {
    let mut harness = server_with_s3_store(&[]);
    harness.start().await.expect("the server boots");
    let bucket = Bucket::S3(
        harness
            .tiered_storage_bucket()
            .expect("the harness makes a bucket for the s3 backend")
            .clone(),
    );
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let topics = Topics::create(&client, "geo-s3").await;

    let mut geo = Ledger::default();
    let mut normal = Ledger::default();
    for _ in 0..MESSAGES {
        assert_eq!(
            geo.send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
        assert_eq!(
            normal
                .send(&client, &topics.stream, &topics.normal, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }

    assert_settled(&client, &topics, &geo, &bucket).await;
}

/// Floci takes S3 multipart uploads with a full object CRC32C and a
/// conditional completion, so a long-term segment above twice the part size
/// goes up in parts and reads back whole.
#[tokio::test]
async fn given_a_geo_topic_on_s3_when_a_long_term_segment_is_large_should_store_it_in_parts_and_read_it_back()
 {
    let mut harness = server_with_s3_store(&[("IGGY_TIERED_STORAGE_PART_SIZE", "5 MiB")]);
    harness.start().await.expect("the server boots");
    let bucket = Bucket::S3(
        harness
            .tiered_storage_bucket()
            .expect("the harness makes a bucket for the s3 backend")
            .clone(),
    );
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let topics = Topics::create(&client, "geo-s3-parts").await;

    // One batch, so no rewrite can split the messages into long-term
    // segments below the threshold of an upload in parts.
    let mut geo = Ledger::default();
    assert_eq!(
        geo.send_batch(
            &client,
            &topics.stream,
            &topics.geo,
            LARGE_MESSAGES,
            LARGE_MESSAGE_BYTES
        )
        .await,
        Answer::Acked
    );

    assert_settled(&client, &topics, &geo, &bucket).await;
    let http = HttpClient::login_root(&harness).await;
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("the metrics text");
    let completed = summed(&exposition, "object_store_parts_uploads_completed_total");
    assert!(
        completed >= 1,
        "no upload in parts completed:\n{exposition}"
    );
    assert_eq!(
        summed(&exposition, "object_store_parts_fallbacks_total"),
        0,
        "the store took the uploads in parts"
    );
}

/// The sum of the samples of `name` over every shard.
fn summed(exposition: &str, name: &str) -> u64 {
    exposition
        .lines()
        .filter_map(|line| line.strip_prefix(name))
        .filter_map(|rest| rest.rsplit_once(' '))
        .filter_map(|(_, value)| value.parse::<u64>().ok())
        .sum()
}

/// The relay between the server and Floci holds every connection while the
/// store stalls, so an upload waits until the server gives it up.
#[tokio::test]
async fn given_a_geo_topic_on_s3_when_its_store_stalls_should_refuse_geo_sends_and_keep_the_normal_topic_serving()
 {
    let bucket = floci::new_bucket()
        .await
        .expect("a Floci bucket, which needs Docker");
    let mut relay = StallingRelay::start(floci_address(&bucket));
    let endpoint = format!("http://{}", relay.address);
    let mut harness = server_with_s3_store(&[
        ("IGGY_TIERED_STORAGE_ENDPOINT", endpoint.as_str()),
        ("IGGY_TIERED_STORAGE_BUCKET", bucket.bucket.as_str()),
        ("IGGY_TIERED_STORAGE_REGION", REGION),
        ("IGGY_TIERED_STORAGE_ACCESS_KEY_ID", ACCESS_KEY_ID),
        ("IGGY_TIERED_STORAGE_SECRET_ACCESS_KEY", SECRET_ACCESS_KEY),
    ]);
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let topics = Topics::create(&client, "geo-s3-stall").await;
    let node = harness
        .server()
        .tcp_addr()
        .expect("the server's tcp address");
    let mut raw = RawSender::connect(node, &client, &topics).await;

    let ledger = produce_through_store_fault(&client, &mut raw, &topics, &mut relay).await;

    assert_settled(&client, &topics, &ledger, &Bucket::S3(bucket)).await;
}

/// A node on the `s3` backend, with `envs` over the harness's own bucket.
fn server_with_s3_store(envs: &[(&str, &str)]) -> TestHarness {
    let mut extra_envs = HashMap::from([
        // A geo topic takes messages once the metadata primary stored its
        // object, and a node without an origin stores none.
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "s3".to_owned()),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    extra_envs.extend(
        envs.iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Floci on the loopback address, as the relay dials it.
fn floci_address(bucket: &FlociBucket) -> SocketAddr {
    let port = bucket
        .endpoint
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or_else(|| panic!("no port in {}", bucket.endpoint));
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// A TCP relay in front of Floci, on a thread of its own so that it serves
/// while the test blocks. While it stalls, it holds each new connection
/// open without a byte through, and it drops the ones that were open.
struct StallingRelay {
    address: SocketAddr,
    stalled: watch::Sender<bool>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl StallingRelay {
    fn start(target: SocketAddr) -> Self {
        let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind the relay");
        listener
            .set_nonblocking(true)
            .expect("a non-blocking listener");
        let address = listener.local_addr().expect("the relay's address");
        let (stalled, state) = watch::channel(false);
        let (shutdown, stop) = oneshot::channel();
        let thread = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the relay's runtime")
                .block_on(async move {
                    let listener = TcpListener::from_std(listener).expect("the relay's listener");
                    let accept = async {
                        loop {
                            if let Ok((inbound, _)) = listener.accept().await {
                                tokio::spawn(relay(inbound, target, state.clone()));
                            }
                        }
                    };
                    tokio::select! {
                        () = accept => {}
                        _ = stop => {}
                    }
                });
        });
        Self {
            address,
            stalled,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }
}

impl StoreFault for StallingRelay {
    fn begin(&mut self) {
        self.stalled.send_replace(true);
    }

    fn end(&mut self) {
        self.stalled.send_replace(false);
    }
}

impl Drop for StallingRelay {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn relay(mut inbound: TcpStream, target: SocketAddr, mut state: watch::Receiver<bool>) {
    if *state.borrow_and_update() {
        let _ = state.wait_for(|stalled| !*stalled).await;
        return;
    }
    let Ok(mut upstream) = TcpStream::connect(target).await else {
        return;
    };
    tokio::select! {
        _ = copy_bidirectional(&mut inbound, &mut upstream) => {}
        _ = state.wait_for(|stalled| *stalled) => {}
    }
}
