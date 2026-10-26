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

use iggy_common::{ClientState, ClusterNode, IggyError, TransportProtocol};
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex as StdMutex, PoisonError};
use tokio::sync::Notify;

/// Bound on one name lookup made to compare two addresses.
const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// One node's `host:port`, bracketing a literal IPv6 address. Appending a
/// port to a bare `::1` yields a spelling no dial can parse, so an IPv6
/// cluster would hand out a roster of undialable entries that still count as
/// endpoints to fail over to.
pub(crate) fn node_address(node: &ClusterNode, port: u16) -> String {
    if node.ip.contains(':') && !node.ip.starts_with('[') {
        format!("[{}]:{port}", node.ip)
    } else {
        format!("{}:{port}", node.ip)
    }
}

pub(crate) fn transport_port(node: &ClusterNode, transport: TransportProtocol) -> u16 {
    match transport {
        TransportProtocol::Tcp => node.endpoints.tcp,
        TransportProtocol::Quic => node.endpoints.quic,
        TransportProtocol::Http => node.endpoints.http,
        TransportProtocol::WebSocket => node.endpoints.websocket,
    }
}

/// Whether two addresses are written the same way, up to canonicalization
/// (`localhost` and `[::]` spellings, and a literal address compared as an
/// address rather than as text).
///
/// Cheap and non-blocking, which is the whole point: the resolving comparison
/// below is a `getaddrinfo`, and every caller reaches this first.
pub(crate) fn is_same_spelling(addr1: &str, addr2: &str) -> bool {
    match (parse_address(addr1), parse_address(addr2)) {
        (Some(sock1), Some(sock2)) => sock1.ip() == sock2.ip() && sock1.port() == sock2.port(),
        _ => normalize_address(addr1) == normalize_address(addr2),
    }
}

/// Check if two addresses refer to the same endpoint
/// Handles various formats like 127.0.0.1:8090 vs localhost:8090
///
/// A host name and the address it resolves to are one endpoint too: a client
/// configured as `iggy-server:8090` whose roster advertises `10.0.0.5:8090`
/// would otherwise dial that node twice per failover sweep, and a single-node
/// deployment would be treated as a cluster.
///
/// Resolution is the last resort, only when the spellings differ and at least
/// one side is not a literal address. It runs through the runtime's resolver
/// rather than `ToSocketAddrs`: name lookup is a blocking `getaddrinfo`, and
/// this is called from the connect and redirect paths, where stalling a
/// runtime worker on a slow resolver would stall every task sharing it.
pub(crate) async fn is_same_address(addr1: &str, addr2: &str) -> bool {
    is_same_address_with(addr1, addr2, resolve_all).await
}

/// [`is_same_address`] against a caller-provided resolver, so the fallback can
/// be exercised without depending on what the machine's resolver answers.
async fn is_same_address_with<R, F>(addr1: &str, addr2: &str, resolve: R) -> bool
where
    R: Fn(String) -> F,
    F: Future<Output = Option<Vec<SocketAddr>>>,
{
    if is_same_spelling(addr1, addr2) {
        return true;
    }

    // Two literal addresses that did not compare equal are different
    // endpoints; resolving them would only hand back what they already say.
    if parse_address(addr1).is_some() && parse_address(addr2).is_some() {
        return false;
    }

    let (Some(first), Some(second)) = (
        resolve(addr1.to_owned()).await,
        resolve(addr2.to_owned()).await,
    ) else {
        return false;
    };
    first.iter().any(|resolved| second.contains(resolved))
}

/// Every socket address a host:port spelling resolves to, `None` when the
/// resolver does not know the name or does not answer in time (which then
/// compares unequal, at worst costing one extra dial).
async fn resolve_all(addr: String) -> Option<Vec<SocketAddr>> {
    // A resolver that never answers must not own the request budget: this
    // comparison runs on the connect and redirect paths, the redirect one
    // inside the caller's request deadline, and `lookup_host` has no deadline
    // of its own.
    let lookup = tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(addr))
        .await
        .ok()?;
    let resolved: Vec<SocketAddr> = lookup.ok()?.collect();
    (!resolved.is_empty()).then_some(resolved)
}

/// Parse address string to SocketAddr, handling various formats
fn parse_address(addr: &str) -> Option<SocketAddr> {
    if let Ok(socket_addr) = SocketAddr::from_str(addr) {
        return Some(socket_addr);
    }

    let normalized = addr
        .replace("localhost", "127.0.0.1")
        .replace("[::]", "[::1]");

    SocketAddr::from_str(&normalized).ok()
}

/// Normalize address string for comparison
fn normalize_address(addr: &str) -> String {
    addr.to_lowercase()
        .replace("localhost", "127.0.0.1")
        .replace("[::]", "[::1]")
}

/// Whether a transport in state `current` takes a write of `next`.
///
/// Shutdown is final: a later write, such as a disconnect or a lost
/// connection, must not make the client usable again. After an explicit
/// disconnect, a reply that was in flight across it, such as a sign-in, must
/// not bring the client back up either. A sweep that owns `Connecting` moves
/// on from it as before, and its own checks of the disconnect end it.
pub(crate) fn accepts_state(
    current: ClientState,
    next: ClientState,
    caller_disconnected: bool,
) -> bool {
    match current {
        ClientState::Shutdown => false,
        ClientState::Disconnected if caller_disconnected => !matches!(
            next,
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated
        ),
        _ => true,
    }
}

/// Coordinates callers of one client's complete connect and authentication
/// sequence. Only the owner runs connection work. Concurrent callers receive
/// that exact result instead of treating `Connecting` as success.
#[derive(Debug)]
pub(crate) struct ConnectCoordinator {
    active: AtomicBool,
    abandoned: AtomicBool,
    generation: AtomicU64,
    result: StdMutex<Option<(u64, Result<(), IggyError>)>>,
    changed: Notify,
}

impl ConnectCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            abandoned: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            result: StdMutex::new(None),
            changed: Notify::new(),
        }
    }

    pub(crate) async fn run<F, Fut>(&self, operation: F) -> Result<(), IggyError>
    where
        F: FnOnce(bool) -> Fut,
        Fut: Future<Output = Result<(), IggyError>>,
    {
        let observed_generation = self.generation.load(Ordering::SeqCst);
        if self
            .active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let abandoned = self.abandoned.swap(false, Ordering::SeqCst);
            let mut owner = ConnectOwner {
                coordinator: self,
                completed: false,
            };
            let result = operation(abandoned).await;
            owner.complete(result.clone());
            return result;
        }

        loop {
            let changed = self.changed.notified();
            let generation = self.generation.load(Ordering::SeqCst);
            if generation > observed_generation {
                return self.result_for(generation);
            }
            changed.await;
        }
    }

    pub(crate) async fn wait_until_inactive(&self) {
        loop {
            let changed = self.changed.notified();
            if !self.active.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }

    fn result_for(&self, generation: u64) -> Result<(), IggyError> {
        self.result
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|(completed_generation, _)| *completed_generation >= generation)
            .map(|(_, result)| result.clone())
            .unwrap_or(Err(IggyError::Disconnected))
    }

    fn finish(&self, result: Result<(), IggyError>, abandoned: bool) {
        if abandoned {
            self.abandoned.store(true, Ordering::SeqCst);
        }
        let generation = self.generation.load(Ordering::SeqCst) + 1;
        self.result
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace((generation, result));
        self.generation.store(generation, Ordering::SeqCst);
        self.active.store(false, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

impl Default for ConnectCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

struct ConnectOwner<'a> {
    coordinator: &'a ConnectCoordinator,
    completed: bool,
}

impl ConnectOwner<'_> {
    fn complete(&mut self, result: Result<(), IggyError>) {
        self.coordinator.finish(result, false);
        self.completed = true;
    }
}

impl Drop for ConnectOwner<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.coordinator.finish(Err(IggyError::Disconnected), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn test_is_same_address() {
        assert!(is_same_address("127.0.0.1:8090", "127.0.0.1:8090").await);
        assert!(is_same_address("localhost:8090", "127.0.0.1:8090").await);
        assert!(!is_same_address("127.0.0.1:8090", "127.0.0.1:8091").await);
        assert!(!is_same_address("192.168.1.1:8090", "127.0.0.1:8090").await);
    }

    /// A stand-in resolver: the BDD cluster's spelling of one node, which no
    /// canonicalization rewrites, so only the resolving comparison can equate
    /// the two. `None` for anything else, like a name the resolver does not
    /// know.
    async fn resolve_bdd_leader(addr: String) -> Option<Vec<SocketAddr>> {
        match addr.as_str() {
            "iggy-leader:8091" | "172.28.0.101:8091" => {
                Some(vec![SocketAddr::from(([172, 28, 0, 101], 8091))])
            }
            _ => None,
        }
    }

    // A host name and the address it resolves to name one endpoint. Exactly
    // the case the BDD cluster hits: the client dials `iggy-leader:8091` and
    // the roster advertises `172.28.0.101:8091`.
    #[tokio::test]
    async fn a_host_name_matches_the_address_it_resolves_to() {
        assert!(
            is_same_address_with("iggy-leader:8091", "172.28.0.101:8091", resolve_bdd_leader).await
        );
    }

    // A name the resolver does not know compares unequal rather than
    // erroring, and a resolvable name never matches another port.
    #[tokio::test]
    async fn an_unresolvable_name_or_another_port_is_a_different_endpoint() {
        assert!(
            !is_same_address_with(
                "iggy-follower:8092",
                "172.28.0.101:8091",
                resolve_bdd_leader
            )
            .await
        );
        assert!(
            !is_same_address_with("iggy-leader:8091", "172.28.0.101:8092", resolve_bdd_leader)
                .await
        );
        assert!(!is_same_address("no-such-host.invalid:8090", "127.0.0.1:8090").await);
    }

    #[tokio::test]
    async fn concurrent_connect_callers_share_the_owners_result() {
        let coordinator = Arc::new(ConnectCoordinator::new());
        let operations = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let owner = {
            let coordinator = Arc::clone(&coordinator);
            let operations = Arc::clone(&operations);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                coordinator
                    .run(|abandoned| async move {
                        assert!(!abandoned);
                        operations.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        release.notified().await;
                        Err(IggyError::InvalidCredentials)
                    })
                    .await
            })
        };
        started.notified().await;
        let waiter = {
            let coordinator = Arc::clone(&coordinator);
            let operations = Arc::clone(&operations);
            tokio::spawn(async move {
                coordinator
                    .run(|_| async move {
                        operations.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        release.notify_one();

        assert!(matches!(
            owner.await.unwrap(),
            Err(IggyError::InvalidCredentials)
        ));
        assert!(matches!(
            waiter.await.unwrap(),
            Err(IggyError::InvalidCredentials)
        ));
        assert_eq!(operations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_cancelled_connect_releases_waiters_and_marks_cleanup_needed() {
        let coordinator = Arc::new(ConnectCoordinator::new());
        let started = Arc::new(Notify::new());
        let owner = {
            let coordinator = Arc::clone(&coordinator);
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                coordinator
                    .run(|_| async move {
                        started.notify_one();
                        std::future::pending::<Result<(), IggyError>>().await
                    })
                    .await
            })
        };
        started.notified().await;
        let waiter = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move { coordinator.run(|_| async { Ok(()) }).await })
        };
        tokio::task::yield_now().await;
        owner.abort();

        assert!(matches!(
            waiter.await.unwrap(),
            Err(IggyError::Disconnected)
        ));
        let observed_abandoned = Arc::new(AtomicBool::new(false));
        let marker = Arc::clone(&observed_abandoned);
        coordinator
            .run(|abandoned| async move {
                marker.store(abandoned, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
        assert!(observed_abandoned.load(Ordering::SeqCst));
    }

    #[test]
    fn test_normalize_address() {
        assert_eq!(normalize_address("localhost:8090"), "127.0.0.1:8090");
        assert_eq!(normalize_address("LOCALHOST:8090"), "127.0.0.1:8090");
        assert_eq!(normalize_address("[::]:8090"), "[::1]:8090");
    }
}
