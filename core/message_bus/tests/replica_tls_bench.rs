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

//! Informative replica-plane throughput comparison: plaintext vs TLS.
//!
//! Ignored by default; run manually with
//!
//! ```sh
//! cargo test --release -p message_bus --test replica_tls_bench -- --ignored --nocapture
//! ```
//!
//! Measures one-directional dialer -> acceptor frame throughput through
//! the full install path (delegated handshake + transport pump). TLS
//! numbers quantify the rustls copy cost on the replica hot path (the
//! plaintext plane preserves `Frozen` ownership end to end; rustls
//! structurally cannot, see `transports/tcp_tls.rs`). Informative only:
//! `cluster.tls` stays off by default regardless of these numbers.

mod common;

use async_channel::Receiver;
use common::{
    install_dialed_replicas_locally, install_replicas_locally, loopback,
    self_signed_replica_tls_ctx, set_replica_ctx, set_replica_ctx_with_tls,
};
use iggy_binary_protocol::{Command2, GenericHeader};
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica::listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus, SendError};
use server_common::Message;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0xBE7C;
const FRAME_SIZE: usize = 64 * 1024;
const FRAME_COUNT: usize = 4096;

#[compio::test]
#[ignore = "informative benchmark; run manually with --ignored --nocapture"]
#[allow(clippy::cast_precision_loss)]
async fn replica_throughput_plaintext_vs_tls() {
    let plaintext = run_one(false).await;
    let tls = run_one(true).await;
    let total_mib = (FRAME_SIZE * FRAME_COUNT) as f64 / (1024.0 * 1024.0);
    println!(
        "replica throughput, {FRAME_COUNT} frames x {FRAME_SIZE} B ({total_mib:.0} MiB):\n\
         plaintext: {:>8.1} MiB/s ({:?})\n\
         tls:       {:>8.1} MiB/s ({:?})",
        total_mib / plaintext.as_secs_f64(),
        plaintext,
        total_mib / tls.as_secs_f64(),
        tls,
    );
}

#[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
async fn run_one(tls: bool) -> Duration {
    // Acceptor bus (replica 1) counts received frames.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    let (done_tx, done_rx) = async_channel::bounded::<()>(1);
    let received = Rc::new(std::cell::Cell::new(0usize));
    let received_for_handler = Rc::clone(&received);
    let on_message1: MessageHandler = Rc::new(move |_, _| {
        let count = received_for_handler.get() + 1;
        received_for_handler.set(count);
        if count == FRAME_COUNT {
            done_tx.try_send(()).ok();
        }
    });
    if tls {
        set_replica_ctx_with_tls(&bus1, CLUSTER, 1, 2, None, self_signed_replica_tls_ctx(2));
    } else {
        set_replica_ctx(&bus1, CLUSTER, 1, 2, None);
    }
    let (l1, addr1) = bind(loopback()).await.expect("bind acceptor");
    let token_for_l1 = bus1.token();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1);
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate_1).await;
    });
    bus1.track_background(l1_handle);

    // Dialer bus (replica 0).
    let bus0 = Rc::new(IggyMessageBus::new(0));
    if tls {
        set_replica_ctx_with_tls(&bus0, CLUSTER, 0, 2, None, self_signed_replica_tls_ctx(2));
    } else {
        set_replica_ctx(&bus0, CLUSTER, 0, 2, None);
    }
    let on_message0: MessageHandler = Rc::new(|_, _| {});
    let dial_delegate_0 = install_dialed_replicas_locally(bus0.clone(), on_message0);
    start_connector(
        &bus0,
        0,
        vec![(1, addr1)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;
    wait_until(|| bus0.replicas().contains(1), Duration::from_secs(5)).await;

    let start = Instant::now();
    for _ in 0..FRAME_COUNT {
        // send_to_replica try_sends and surfaces a full peer queue as
        // Backpressure (VSR retransmit covers it in production); the
        // bench just rebuilds the frame and retries after a yield.
        loop {
            let frame = Message::<GenericHeader>::new(FRAME_SIZE)
                .transmute_header(|_, h: &mut GenericHeader| {
                    h.command = Command2::Prepare;
                    h.cluster = CLUSTER;
                    h.size = FRAME_SIZE as u32;
                })
                .into_frozen();
            match bus0.send_to_replica(1, frame).await {
                Ok(()) => break,
                Err(SendError::Backpressure) => {
                    compio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(other) => panic!("send frame: {other}"),
            }
        }
    }
    expect_recv(&done_rx, Duration::from_mins(2)).await;
    let elapsed = start.elapsed();

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
    elapsed
}

#[allow(clippy::future_not_send)]
async fn wait_until<F: Fn() -> bool>(cond: F, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("wait_until: condition not met within {timeout:?}");
}

#[allow(clippy::future_not_send)]
async fn expect_recv(rx: &Receiver<()>, timeout: Duration) {
    compio::time::timeout(timeout, rx.recv())
        .await
        .expect("expect_recv: timeout")
        .expect("expect_recv: channel closed");
}
