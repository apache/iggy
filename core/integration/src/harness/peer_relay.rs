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

//! TCP relays on the replica-to-replica links of a cluster, so a test can cut
//! one node off from its peers and restore it without root or Docker.
//!
//! Node `from` dials peer `to` at the relay for `(from, to)` instead of the
//! peer's real replica port, and the relay forwards to that port. A link is
//! cut while either of its ends is isolated: it drops its live connections
//! and closes new ones, so both sides see the connection end at once (a
//! reset partition, not a silent drop).

use std::net::{IpAddr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::{Notify, oneshot};
use tracing::{debug, info, warn};

use crate::harness::error::TestBinaryError;

const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(50);

/// One relay per ordered pair of nodes, serving on its own thread.
pub(crate) struct PeerRelays {
    node_count: usize,
    links: Vec<Link>,
    /// The nodes cut off from their peers. A link is cut iff one of its ends
    /// is in here, so healing one node keeps its links to another isolated
    /// node cut.
    isolated: Mutex<Vec<bool>>,
    /// Dropping it ends the relay thread.
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

struct Link {
    from: usize,
    to: usize,
    address: SocketAddr,
    state: Arc<LinkState>,
}

#[derive(Default)]
struct LinkState {
    cut: AtomicBool,
    notify: Notify,
}

impl PeerRelays {
    /// Binds a relay on `loopback` for every ordered pair of nodes.
    /// `replica_addrs[j]` is the real replica port of node `j`.
    pub(crate) fn bind(
        loopback: IpAddr,
        replica_addrs: &[SocketAddr],
    ) -> Result<Self, TestBinaryError> {
        let node_count = replica_addrs.len();
        let pair_count = node_count * node_count.saturating_sub(1);
        let mut links = Vec::with_capacity(pair_count);
        let mut relays = Vec::with_capacity(pair_count);
        for from in 0..node_count {
            for (to, &target) in replica_addrs.iter().enumerate() {
                if from == to {
                    continue;
                }
                let listener = StdTcpListener::bind(SocketAddr::new(loopback, 0))
                    .map_err(TestBinaryError::Io)?;
                listener
                    .set_nonblocking(true)
                    .map_err(TestBinaryError::Io)?;
                let address = listener.local_addr().map_err(TestBinaryError::Io)?;
                let state = Arc::new(LinkState::default());
                relays.push((listener, target, Arc::clone(&state)));
                links.push(Link {
                    from,
                    to,
                    address,
                    state,
                });
            }
        }

        let (shutdown, shutdown_rx) = oneshot::channel();
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(TestBinaryError::Io)?;
        // A runtime of its own: the test's runtime may be blocked for seconds
        // by a kill or a restart, and the harness must relay meanwhile.
        let thread = thread::Builder::new()
            .name("peer-relays".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    for (listener, target, state) in relays {
                        match TcpListener::from_std(listener) {
                            Ok(listener) => {
                                tokio::spawn(relay(listener, target, state));
                            }
                            Err(error) => warn!(%target, %error, "peer relay listener lost"),
                        }
                    }
                    let _ = shutdown_rx.await;
                });
            })
            .map_err(TestBinaryError::Io)?;

        Ok(Self {
            node_count,
            links,
            isolated: Mutex::new(vec![false; node_count]),
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }

    /// The address node `from` dials for peer `to`.
    pub(crate) fn address(&self, from: usize, to: usize) -> Option<SocketAddr> {
        self.links
            .iter()
            .find(|link| link.from == from && link.to == to)
            .map(|link| link.address)
    }

    /// Cuts every link to and from `node`.
    pub(crate) fn cut_node(&self, node: usize) -> Result<(), TestBinaryError> {
        self.set_node_links(node, true)
    }

    /// Restores every link to and from `node`.
    pub(crate) fn heal_node(&self, node: usize) -> Result<(), TestBinaryError> {
        self.set_node_links(node, false)
    }

    fn set_node_links(&self, node: usize, cut: bool) -> Result<(), TestBinaryError> {
        if node >= self.node_count {
            return Err(TestBinaryError::MissingServer);
        }
        // Held across the flag updates, so two concurrent calls cannot leave
        // a link's flag out of step with the set.
        let mut isolated = self
            .isolated
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        isolated[node] = cut;
        for link in self
            .links
            .iter()
            .filter(|link| link.from == node || link.to == node)
        {
            let link_cut = isolated[link.from] || isolated[link.to];
            let was_cut = link.state.cut.swap(link_cut, Ordering::SeqCst);
            if link_cut && !was_cut {
                link.state.notify.notify_waiters();
            }
        }
        if cut {
            info!(node, "cut every peer link of the node");
        } else {
            info!(node, "healed every peer link of the node");
        }
        Ok(())
    }
}

impl Drop for PeerRelays {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Accepts on one relay and forwards each connection to `target`. A cut link
/// closes what it accepts, so the dialer fails fast and keeps redialing.
async fn relay(listener: TcpListener, target: SocketAddr, state: Arc<LinkState>) {
    loop {
        let (inbound, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                warn!(%target, %error, "peer relay accept failed");
                tokio::time::sleep(ACCEPT_RETRY_PAUSE).await;
                continue;
            }
        };
        if state.cut.load(Ordering::SeqCst) {
            continue;
        }
        let outbound = match TcpStream::connect(target).await {
            Ok(outbound) => outbound,
            Err(error) => {
                debug!(%target, %error, "peer relay cannot reach the target");
                continue;
            }
        };
        let _ = inbound.set_nodelay(true);
        let _ = outbound.set_nodelay(true);
        tokio::spawn(forward(inbound, outbound, Arc::clone(&state)));
    }
}

/// Copies both ways until one side closes or the link is cut; the drop on
/// return closes both sockets.
async fn forward(mut inbound: TcpStream, mut outbound: TcpStream, state: Arc<LinkState>) {
    let cut = state.notify.notified();
    tokio::pin!(cut);
    // Register before reading the flag, so a cut between accept and here is
    // seen either as the flag or as the notification.
    cut.as_mut().enable();
    if state.cut.load(Ordering::SeqCst) {
        return;
    }
    tokio::select! {
        _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
        _ = &mut cut => {}
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    use super::PeerRelays;

    const PROMPTLY: Duration = Duration::from_secs(5);
    const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    async fn echo_server() -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::new(LOOPBACK, 0))
            .await
            .expect("bind the echo server");
        let address = listener.local_addr().expect("echo server address");
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept an echo client");
                tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.split();
                    let _ = tokio::io::copy(&mut reader, &mut writer).await;
                });
            }
        });
        address
    }

    async fn connect(relays: &PeerRelays, from: usize, to: usize) -> TcpStream {
        let address = relays.address(from, to).expect("a relay for the pair");
        timeout(PROMPTLY, TcpStream::connect(address))
            .await
            .expect("connect within the timeout")
            .expect("connect through the relay")
    }

    async fn echo_round_trip(stream: &mut TcpStream, payload: &[u8]) {
        stream.write_all(payload).await.expect("write the payload");
        let mut echoed = vec![0; payload.len()];
        timeout(PROMPTLY, stream.read_exact(&mut echoed))
            .await
            .expect("read within the timeout")
            .expect("read the echo");
        assert_eq!(echoed, payload);
    }

    /// Ok(0) is an orderly close, Err a reset: both end the connection.
    async fn ends_promptly(stream: &mut TcpStream) {
        let mut buffer = [0; 16];
        let read = timeout(PROMPTLY, stream.read(&mut buffer))
            .await
            .expect("the connection ends within the timeout");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "the connection stayed open: {read:?}"
        );
    }

    #[tokio::test]
    async fn given_relay_when_client_writes_should_forward_both_ways() {
        let echo = echo_server().await;
        let relays = PeerRelays::bind(LOOPBACK, &[echo, echo]).expect("bind the relays");
        assert!(relays.address(0, 0).is_none());

        let mut stream = connect(&relays, 0, 1).await;
        echo_round_trip(&mut stream, b"ping").await;
        let mut reverse = connect(&relays, 1, 0).await;
        echo_round_trip(&mut reverse, b"pong").await;
    }

    #[tokio::test]
    async fn given_cut_node_when_links_are_used_should_drop_and_refuse_until_healed() {
        let echo = echo_server().await;
        let relays = PeerRelays::bind(LOOPBACK, &[echo, echo, echo]).expect("bind the relays");

        let mut live = connect(&relays, 0, 1).await;
        echo_round_trip(&mut live, b"before").await;
        let mut untouched = connect(&relays, 0, 2).await;

        relays.cut_node(1).expect("cut node 1");
        relays.cut_node(1).expect("cutting twice is harmless");
        ends_promptly(&mut live).await;
        let mut refused = connect(&relays, 2, 1).await;
        ends_promptly(&mut refused).await;
        echo_round_trip(&mut untouched, b"other link").await;

        relays.heal_node(1).expect("heal node 1");
        let mut healed = connect(&relays, 0, 1).await;
        echo_round_trip(&mut healed, b"after").await;

        assert!(relays.cut_node(3).is_err());
    }

    #[tokio::test]
    async fn given_two_isolated_nodes_when_one_is_healed_should_keep_the_other_cut() {
        let echo = echo_server().await;
        let relays = PeerRelays::bind(LOOPBACK, &[echo, echo, echo]).expect("bind the relays");

        relays.cut_node(1).expect("cut node 1");
        relays.cut_node(2).expect("cut node 2");
        relays.heal_node(1).expect("heal node 1");

        for (from, to) in [(1, 2), (2, 1), (0, 2), (2, 0)] {
            let mut refused = connect(&relays, from, to).await;
            ends_promptly(&mut refused).await;
        }
        let mut healed = connect(&relays, 0, 1).await;
        echo_round_trip(&mut healed, b"node 1 is back").await;
        let mut reverse = connect(&relays, 1, 0).await;
        echo_round_trip(&mut reverse, b"both ways").await;

        relays.heal_node(2).expect("heal node 2");
        let mut restored = connect(&relays, 1, 2).await;
        echo_round_trip(&mut restored, b"all healed").await;
    }
}
