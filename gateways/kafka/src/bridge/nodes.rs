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

//! The Iggy nodes that a new client of the gateway can dial.
//!
//! A new SDK client knows only the address it is given, until it signs in and reads the roster.
//! The gateway opens new clients all the time: for each producer session, each SASL login, and
//! each read or offset client that a timeout dropped. Beside a stopped node, the configured
//! address is down, so a new client also tries the nodes of the last roster that the bridge read.
//! That roster comes from the cluster itself, so a new client never reaches another cluster.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use iggy::prelude::{ClusterMetadata, IggyError};

/// How long an address that a dial could not reach stays at the back of the dial order.
const UNREACHABLE_FOR: Duration = Duration::from_secs(30);

/// The nodes of the last roster that the bridge read. Clones share one list.
#[derive(Debug, Clone, Default)]
pub struct KnownNodes(Arc<Mutex<Known>>);

#[derive(Debug, Default)]
struct Known {
    /// The TCP address of each node, in roster order.
    roster: Vec<String>,
    /// Until when each address that a dial could not reach stays at the back.
    unreachable: HashMap<String, Instant>,
}

impl KnownNodes {
    /// Replaces the roster with the TCP address of each node in `metadata`. A node without a TCP
    /// port, or one that advertises a wildcard address, cannot be dialed, so it is left out.
    pub fn learn(&self, metadata: &ClusterMetadata) {
        let roster = metadata
            .nodes
            .iter()
            .filter_map(|node| tcp_address(&node.ip, node.endpoints.tcp))
            .collect();
        self.known().roster = roster;
    }

    /// The addresses a new client tries in turn: `configured` first, then each node of the
    /// roster. An address that a dial could not reach in the last 30 s goes to the back.
    #[must_use]
    pub fn dial_order(&self, configured: &str) -> Vec<String> {
        let known = self.known();
        let mut order = vec![configured.to_owned()];
        for address in &known.roster {
            if !order.iter().any(|listed| same_address(listed, address)) {
                order.push(address.clone());
            }
        }
        let now = Instant::now();
        let (mut reachable, unreachable): (Vec<String>, Vec<String>) =
            order.into_iter().partition(|address| {
                known
                    .unreachable
                    .get(address)
                    .is_none_or(|until| *until <= now)
            });
        drop(known);
        reachable.extend(unreachable);
        reachable
    }

    /// Moves `address` to the back of the dial order for a while.
    pub fn mark_unreachable(&self, address: &str) {
        self.known()
            .unreachable
            .insert(address.to_owned(), Instant::now() + UNREACHABLE_FOR);
    }

    /// Puts `address` back in its place in the dial order.
    pub fn mark_reachable(&self, address: &str) {
        self.known().unreachable.remove(address);
    }

    fn known(&self) -> MutexGuard<'_, Known> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether `error` says that a client could not reach its node, so that another node can still
/// answer.
#[must_use]
pub const fn is_unreachable(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::CannotEstablishConnection
            | IggyError::Disconnected
            | IggyError::NotConnected
            | IggyError::TcpError
    )
}

/// `ip:port`, with a literal IPv6 address in brackets. `None` for port 0 or a wildcard address.
fn tcp_address(ip: &str, port: u16) -> Option<String> {
    let host = ip.trim_start_matches('[').trim_end_matches(']');
    if port == 0 || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_unspecified()) {
        return None;
    }
    if host.contains(':') {
        Some(format!("[{host}]:{port}"))
    } else {
        Some(format!("{host}:{port}"))
    }
}

/// Whether two addresses name the same socket, comparing literal addresses as addresses.
fn same_address(first: &str, second: &str) -> bool {
    match (first.parse::<SocketAddr>(), second.parse::<SocketAddr>()) {
        (Ok(first), Ok(second)) => first == second,
        _ => first == second,
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::{
        ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, TransportEndpoints,
    };

    use super::*;

    fn node(replica_id: u8, ip: &str, tcp: u16) -> ClusterNode {
        ClusterNode {
            replica_id,
            name: format!("iggy-node-{replica_id}"),
            ip: ip.to_string(),
            endpoints: TransportEndpoints {
                tcp,
                quic: 8080,
                http: 3000,
                websocket: 8092,
            },
            role: ClusterNodeRole::Follower,
            status: ClusterNodeStatus::Healthy,
        }
    }

    fn roster(nodes: Vec<ClusterNode>) -> ClusterMetadata {
        ClusterMetadata {
            name: "iggy-cluster".to_string(),
            nodes,
        }
    }

    #[test]
    fn given_a_roster_when_dialing_should_try_the_configured_address_then_each_other_node() {
        let known = KnownNodes::default();
        known.learn(&roster(vec![
            node(0, "10.0.0.1", 8090),
            node(1, "10.0.0.2", 8090),
            node(2, "10.0.0.3", 8090),
        ]));

        assert_eq!(
            known.dial_order("10.0.0.2:8090"),
            ["10.0.0.2:8090", "10.0.0.1:8090", "10.0.0.3:8090"]
        );
    }

    #[test]
    fn given_nodes_that_cannot_be_dialed_when_learning_should_leave_them_out() {
        let known = KnownNodes::default();
        known.learn(&roster(vec![
            node(0, "0.0.0.0", 8090),
            node(1, "::", 8090),
            node(2, "10.0.0.3", 0),
            node(3, "fd00::4", 8090),
        ]));

        assert_eq!(
            known.dial_order("iggy:8090"),
            ["iggy:8090", "[fd00::4]:8090"]
        );
    }

    #[test]
    fn given_a_node_that_a_dial_could_not_reach_should_try_it_last_until_it_answers() {
        let known = KnownNodes::default();
        known.learn(&roster(vec![
            node(0, "10.0.0.1", 8090),
            node(1, "10.0.0.2", 8090),
        ]));

        known.mark_unreachable("10.0.0.1:8090");
        assert_eq!(
            known.dial_order("10.0.0.1:8090"),
            ["10.0.0.2:8090", "10.0.0.1:8090"]
        );

        known.mark_reachable("10.0.0.1:8090");
        assert_eq!(
            known.dial_order("10.0.0.1:8090"),
            ["10.0.0.1:8090", "10.0.0.2:8090"]
        );
    }

    #[test]
    fn given_no_roster_when_dialing_should_try_the_configured_address_alone() {
        assert_eq!(
            KnownNodes::default().dial_order("127.0.0.1:8090"),
            ["127.0.0.1:8090"]
        );
    }

    #[test]
    fn given_errors_when_classifying_should_move_on_only_from_a_node_out_of_reach() {
        assert!(is_unreachable(&IggyError::CannotEstablishConnection));
        assert!(is_unreachable(&IggyError::Disconnected));
        assert!(!is_unreachable(&IggyError::InvalidCredentials));
        assert!(!is_unreachable(&IggyError::Unauthenticated));
    }
}
