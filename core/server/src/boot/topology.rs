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

//! Listener topology and the cluster roster, resolved from config.

use crate::cluster_meta::{
    BoundPorts, ClusterRoster, METADATA_VIEW_UNKNOWN, resolved_roster_nodes,
    self_advertised_address,
};
use crate::server_error::ServerError;
use configs::server::ServerConfig;
use message_bus::replica::auth;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tracing::warn;

const SHARD_REPLICA_ID: u8 = 0;

pub(in crate::boot) struct TcpTopology {
    /// Domain-separation cluster id derived from `cluster.name`; threaded to
    /// every consensus instance and the replica handshake so frames agree.
    pub(in crate::boot) cluster_id: u128,
    pub(in crate::boot) self_replica_id: u8,
    pub(in crate::boot) replica_count: u8,
    pub(in crate::boot) client_listen_addr: SocketAddr,
    pub(in crate::boot) replica_listen_addr: Option<SocketAddr>,
    pub(in crate::boot) ws_listen_addr: Option<SocketAddr>,
    pub(in crate::boot) quic_listen_addr: Option<SocketAddr>,
    pub(in crate::boot) http_listen_addr: Option<SocketAddr>,
    pub(in crate::boot) tcp_tls_listen_addr: Option<SocketAddr>,
    pub(in crate::boot) peers: Vec<(u8, SocketAddr)>,
}

/// Process-wide cells behind every shard's [`ClusterRoster`], written by
/// shard 0 and read by each shard's cluster-metadata reply: the metadata-group
/// view its consensus publishes, so leader marking works off-shard, and the
/// client ports its listeners bound, so a configured `:0` port reports the
/// one the OS picked on every transport.
#[derive(Clone)]
pub(in crate::boot) struct RosterCells {
    pub(in crate::boot) metadata_view: Arc<AtomicU64>,
    pub(in crate::boot) bound_ports: Arc<BoundPorts>,
}

/// Unpublished: an unknown view and no bound ports.
impl Default for RosterCells {
    fn default() -> Self {
        Self {
            metadata_view: Arc::new(AtomicU64::new(METADATA_VIEW_UNKNOWN)),
            bound_ports: Arc::default(),
        }
    }
}

/// Copy the configured cluster roster plus this node's own client ports into
/// the shared [`ClusterRoster`] so the binary `GetClusterMetadata` read serves
/// the real topology. `self_*` back only the cluster-disabled self-synthesis
/// and carry the configured listener ports from the resolved topology until
/// shard 0 publishes the bound ones through `bound_ports` (a `:0` wildcard
/// resolves once its listener is up). The self address resolves through
/// [`self_advertised_address`], which boot validation has already guaranteed
/// names somewhere a client can dial.
pub(in crate::boot) fn build_cluster_roster(
    shard_id: u16,
    config: &ServerConfig,
    topology: &TcpTopology,
    cells: &RosterCells,
) -> Result<ClusterRoster, ServerError> {
    let declared = config.node.advertised_address.as_deref();
    let self_advertised = self_advertised_address(declared, derived_bind_ip(topology, config));
    // The roster answers this per node, so a value here would be read by
    // nobody. Silence would leave the operator believing it took effect.
    // Every shard builds its own roster off the same config, so keep the
    // operator-facing explanation to one line per process.
    if declared.is_some() && config.cluster.enabled && shard_id == 0 {
        warn!(
            "node.advertised_address is set but cluster.enabled is true, so it is ignored; \
             the client-facing address of each node comes from its cluster.nodes entry"
        );
    }
    Ok(ClusterRoster {
        enabled: config.cluster.enabled,
        name: config.cluster.name.clone(),
        nodes: resolved_roster_nodes(&config.cluster).map_err(ServerError::Config)?,
        self_advertised,
        configured_ports: configs::cluster::TransportPorts {
            tcp: config
                .tcp
                .enabled
                .then(|| topology.client_listen_addr.port()),
            quic: topology.quic_listen_addr.map(|addr| addr.port()),
            http: topology.http_listen_addr.map(|addr| addr.port()),
            websocket: topology.ws_listen_addr.map(|addr| addr.port()),
            tcp_replica: None,
        },
        bound_ports: Arc::clone(&cells.bound_ports),
        metadata_view: Arc::clone(&cells.metadata_view),
    })
}

pub(in crate::boot) fn resolve_tcp_topology(
    config: &ServerConfig,
    current_replica_id: Option<u8>,
) -> Result<TcpTopology, ServerError> {
    let default_client_addr = parse_socket_addr("tcp.address", &config.tcp.address)?;
    let default_ws_addr = resolve_optional_listener_addr(
        config.websocket.enabled,
        "websocket.address",
        &config.websocket.address,
    )?;
    let default_quic_addr =
        resolve_optional_listener_addr(config.quic.enabled, "quic.address", &config.quic.address)?;
    let default_http_addr =
        resolve_optional_listener_addr(config.http.enabled, "http.address", &config.http.address)?;
    if !config.cluster.enabled {
        if let Some(replica_id) = current_replica_id
            && replica_id != SHARD_REPLICA_ID
        {
            return Err(ServerError::ReplicaIdRequiresCluster {
                supplied: replica_id,
                default: SHARD_REPLICA_ID,
            });
        }
        return Ok(TcpTopology {
            cluster_id: auth::cluster_domain_id(&config.cluster.name),
            // Keep parity with the current server binary and the integration
            // harness: `--replica-id 0` may be passed unconditionally in
            // single-node mode; any other id is rejected above so the WAL
            // cannot commit under an identity that will later disagree with
            // a cluster.nodes[] entry.
            self_replica_id: SHARD_REPLICA_ID,
            replica_count: 1,
            client_listen_addr: default_client_addr,
            replica_listen_addr: Some(SocketAddr::new(default_client_addr.ip(), 0)),
            ws_listen_addr: default_ws_addr,
            quic_listen_addr: default_quic_addr,
            http_listen_addr: default_http_addr,
            tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(default_client_addr),
            peers: Vec::new(),
        });
    }

    let self_replica_id = current_replica_id.ok_or(ServerError::MissingReplicaId)?;

    let self_node = config
        .cluster
        .nodes
        .iter()
        .find(|node| node.replica_id == self_replica_id)
        .ok_or(ServerError::ClusterNodeNotFound {
            replica_id: self_replica_id,
        })?;
    let replica_count = u8::try_from(config.cluster.nodes.len()).map_err(|_| {
        ServerError::ClusterReplicaCountTooLarge {
            count: config.cluster.nodes.len(),
        }
    })?;
    let ClusterClientAddrs {
        client: client_listen_addr,
        ws: ws_listen_addr,
        quic: quic_listen_addr,
        http: http_listen_addr,
    } = resolve_cluster_client_addrs(
        self_node,
        default_client_addr,
        default_ws_addr,
        default_quic_addr,
        default_http_addr,
    )?;
    let replica_port = self_node
        .ports
        .tcp_replica
        .ok_or(ServerError::ClusterPortMissing {
            transport: "tcp_replica",
            replica_id: self_node.replica_id,
        })?;
    let replica_listen_addr = Some(socket_addr_from_parts(
        "cluster.nodes[*].ports.tcp_replica",
        &self_node.ip,
        replica_port,
    )?);
    let peers = resolve_cluster_replica_peers(&config.cluster.nodes, self_replica_id)?;

    Ok(TcpTopology {
        cluster_id: auth::cluster_domain_id(&config.cluster.name),
        self_replica_id,
        replica_count,
        client_listen_addr,
        replica_listen_addr,
        ws_listen_addr,
        quic_listen_addr,
        http_listen_addr,
        tcp_tls_listen_addr: config.tcp.tls.enabled.then_some(client_listen_addr),
        peers,
    })
}

fn resolve_optional_listener_addr(
    enabled: bool,
    context: &'static str,
    address: &str,
) -> Result<Option<SocketAddr>, ServerError> {
    if enabled {
        return Ok(Some(parse_socket_addr(context, address)?));
    }
    Ok(None)
}

/// Client-facing listener addresses resolved for this cluster node. Each port
/// comes from the node's roster entry; there is no fallback to the top-level
/// listener port, an enabled transport without a roster port refuses to boot.
/// Every transport keeps the bind interface from its own `address` config: the
/// roster ip is advertised, not bound.
struct ClusterClientAddrs {
    client: SocketAddr,
    ws: Option<SocketAddr>,
    quic: Option<SocketAddr>,
    http: Option<SocketAddr>,
}

fn resolve_cluster_client_addrs(
    self_node: &configs::cluster::ClusterNodeConfig,
    default_tcp_addr: SocketAddr,
    default_ws_addr: Option<SocketAddr>,
    default_quic_addr: Option<SocketAddr>,
    default_http_addr: Option<SocketAddr>,
) -> Result<ClusterClientAddrs, ServerError> {
    let client_port = self_node.ports.tcp.ok_or(ServerError::ClusterPortMissing {
        transport: "tcp",
        replica_id: self_node.replica_id,
    })?;
    let client =
        merge_roster_port_with_bind_ip("tcp", &self_node.ip, default_tcp_addr, client_port);
    let ws = resolve_cluster_optional_addr(self_node, "websocket", default_ws_addr, |ports| {
        ports.websocket
    })?;
    let quic =
        resolve_cluster_optional_addr(self_node, "quic", default_quic_addr, |ports| ports.quic)?;
    let http =
        resolve_cluster_optional_addr(self_node, "http", default_http_addr, |ports| ports.http)?;
    Ok(ClusterClientAddrs {
        client,
        ws,
        quic,
        http,
    })
}

fn resolve_cluster_optional_addr(
    self_node: &configs::cluster::ClusterNodeConfig,
    transport: &'static str,
    default_addr: Option<SocketAddr>,
    port_selector: impl Fn(&configs::cluster::TransportPorts) -> Option<u16>,
) -> Result<Option<SocketAddr>, ServerError> {
    let Some(default_addr) = default_addr else {
        return Ok(None);
    };
    // No fallback to the top-level port: two same-host nodes leaving the same
    // transport port unset would race for one socket. Either the roster is
    // explicit or the server refuses to boot.
    let port = port_selector(&self_node.ports).ok_or(ServerError::ClusterPortMissing {
        transport,
        replica_id: self_node.replica_id,
    })?;
    Ok(Some(merge_roster_port_with_bind_ip(
        transport,
        &self_node.ip,
        default_addr,
        port,
    )))
}

/// Combine the roster-supplied `port` with the bind interface the transport's
/// own `address` config asked for.
///
/// The roster ip is what the cluster advertises (metadata, follower-to-primary
/// HTTP forwarding targets); the transport's own `address` decides the bind
/// interface. Merging keeps a loopback-only `127.0.0.1` private and a
/// `0.0.0.0` wide in cluster mode instead of silently rebinding to the roster
/// interface, which would strand every co-located dialer (sidecars, health
/// probes, on-host consumers) on `ECONNREFUSED`.
fn merge_roster_port_with_bind_ip(
    transport: &'static str,
    roster_ip: &str,
    bind_addr: SocketAddr,
    port: u16,
) -> SocketAddr {
    let listen_addr = SocketAddr::new(bind_addr.ip(), port);
    if roster_ip_unreachable_from_bind_addr(roster_ip, listen_addr) {
        warn!(
            "{transport} listener binds {listen_addr} but the roster advertises {roster_ip}:{port}; \
             peers and clients dialing the advertised endpoint may not reach this node"
        );
    }
    listen_addr
}

/// The client-facing listeners paired with the config key naming their bind
/// address, in the order [`ServerConfig::client_listeners`] derives the
/// published client-facing address from them. `None` marks a listener that is
/// switched off and therefore binds nothing.
pub(in crate::boot) fn client_listeners(
    topology: &TcpTopology,
    config: &ServerConfig,
) -> [(&'static str, Option<SocketAddr>); 4] {
    [
        (
            "tcp.address",
            config.tcp.enabled.then_some(topology.client_listen_addr),
        ),
        ("websocket.address", topology.ws_listen_addr),
        ("quic.address", topology.quic_listen_addr),
        ("http.address", topology.http_listen_addr),
    ]
}

/// The bind interface the published client-facing address names when none is
/// declared: the first enabled listener's, the same one boot validation gated
/// its wildcard refusal on. With every client listener off nothing dials this
/// node, so the tcp bind address stands in for an answer no client reads.
pub(in crate::boot) fn derived_bind_ip(topology: &TcpTopology, config: &ServerConfig) -> IpAddr {
    client_listeners(topology, config)
        .into_iter()
        .find_map(|(_, listen_addr)| listen_addr)
        .unwrap_or(topology.client_listen_addr)
        .ip()
}

/// Whether the address cluster metadata publishes for this node misses one of
/// its own listeners. Only a derived address is judged: it names one
/// listener's bind interface, so a listener on a different one is unreachable
/// at the published address. A declared `node.advertised_address` is
/// deliberate (NAT, a public name) and says nothing about which local
/// interface serves a transport, so it stays quiet.
pub(in crate::boot) fn derived_address_misses_listener(
    declared: Option<&str>,
    self_advertised: &str,
    listen_addr: SocketAddr,
) -> bool {
    declared.is_none() && roster_ip_unreachable_from_bind_addr(self_advertised, listen_addr)
}

/// Whether a dialer aiming at the advertised roster ip misses `listen_addr`. An
/// unspecified bind covers every interface, and a roster ip that parses as
/// neither IPv4 nor IPv6 (a DNS name, say) can resolve to the bound interface,
/// so both cases stay quiet. Both sides reduce to the canonical form first, so
/// the v4-mapped wildcard (`[::ffff:0.0.0.0]`, which a dual-stack host binds as
/// `0.0.0.0`) stays quiet as well and `10.0.0.5` matches `::ffff:10.0.0.5`.
fn roster_ip_unreachable_from_bind_addr(roster_ip: &str, listen_addr: SocketAddr) -> bool {
    let bind_ip = listen_addr.ip().to_canonical();
    !bind_ip.is_unspecified()
        && roster_ip
            .parse::<IpAddr>()
            .is_ok_and(|parsed| parsed.to_canonical() != bind_ip)
}

pub(in crate::boot) fn wildcard_listener_under_loopback_address(
    declared: Option<&str>,
    self_advertised: &str,
    listen_addr: SocketAddr,
) -> bool {
    declared.is_none()
        && listen_addr.ip().to_canonical().is_unspecified()
        && self_advertised
            .parse::<IpAddr>()
            .is_ok_and(|address| address.to_canonical().is_loopback())
}

fn resolve_cluster_replica_peers(
    nodes: &[configs::cluster::ClusterNodeConfig],
    self_replica_id: u8,
) -> Result<Vec<(u8, SocketAddr)>, ServerError> {
    let mut peers = Vec::with_capacity(nodes.len().saturating_sub(1));
    for node in nodes {
        if node.replica_id == self_replica_id {
            continue;
        }
        let replica_port = node
            .ports
            .tcp_replica
            .ok_or(ServerError::ClusterPortMissing {
                transport: "tcp_replica",
                replica_id: node.replica_id,
            })?;
        peers.push((
            node.replica_id,
            socket_addr_from_parts("cluster.nodes[*].ports.tcp_replica", &node.ip, replica_port)?,
        ));
    }
    Ok(peers)
}

fn parse_socket_addr(context: &'static str, address: &str) -> Result<SocketAddr, ServerError> {
    address
        .parse()
        .map_err(|source| ServerError::SocketAddressParse {
            context,
            address: address.to_string(),
            source,
        })
}

fn socket_addr_from_parts(
    context: &'static str,
    host: &str,
    port: u16,
) -> Result<SocketAddr, ServerError> {
    let ip = host
        .parse::<IpAddr>()
        .map_err(|source| ServerError::SocketAddressParse {
            context,
            address: format!("{host}:{port}"),
            source,
        })?;
    Ok(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_node(ip: &str, http: Option<u16>) -> configs::cluster::ClusterNodeConfig {
        cluster_node_with_ports(ip, Some(18070), http)
    }

    fn cluster_node_with_ports(
        ip: &str,
        tcp: Option<u16>,
        http: Option<u16>,
    ) -> configs::cluster::ClusterNodeConfig {
        configs::cluster::ClusterNodeConfig {
            name: "node".to_owned(),
            ip: ip.to_owned(),
            advertised_address: None,
            advertised_addresses: Vec::new(),
            replica_id: 0,
            ports: configs::cluster::TransportPorts {
                tcp,
                http,
                ..Default::default()
            },
        }
    }

    fn addr(value: &str) -> SocketAddr {
        value.parse().expect("valid socket address literal")
    }

    #[test]
    fn cluster_http_addr_takes_port_from_roster() {
        // A byte-identical top-level [http].address is shared across nodes on
        // one host; the per-node roster port is the only port source so each
        // node binds a distinct HTTP socket.
        let node = cluster_node("127.0.0.1", Some(18090));
        let addrs = resolve_cluster_client_addrs(
            &node,
            addr("127.0.0.1:8090"),
            None,
            None,
            Some(addr("127.0.0.1:3000")),
        )
        .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, Some(addr("127.0.0.1:18090")));
    }

    #[test]
    fn cluster_http_addr_merges_config_ip_with_roster_port() {
        // Docker/Helm bind `0.0.0.0` and probe loopback; the roster ip is
        // only the advertised address. Cluster mode must keep the configured
        // interface and take just the port from the roster.
        let node = cluster_node("10.0.0.5", Some(18090));
        let addrs = resolve_cluster_client_addrs(
            &node,
            addr("0.0.0.0:8090"),
            None,
            None,
            Some(addr("0.0.0.0:3000")),
        )
        .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, Some(addr("0.0.0.0:18090")));
    }

    #[test]
    fn cluster_http_addr_requires_roster_port_for_enabled_transport() {
        // No fallback to the top-level port: a silent default could collide
        // with another same-host node, so a missing roster port for an
        // enabled transport must refuse to boot.
        let node = cluster_node("10.0.0.5", None);
        let result = resolve_cluster_client_addrs(
            &node,
            addr("127.0.0.1:8090"),
            None,
            None,
            Some(addr("127.0.0.1:3000")),
        );
        assert!(matches!(
            result,
            Err(ServerError::ClusterPortMissing {
                transport: "http",
                replica_id: 0,
            })
        ));
    }

    #[test]
    fn cluster_http_addr_is_none_when_http_disabled() {
        // http.enabled = false collapses default_http_addr to None; no roster
        // port can revive a listener the operator turned off.
        let node = cluster_node("127.0.0.1", Some(18090));
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.http, None);
    }

    #[test]
    fn cluster_tcp_addr_takes_port_from_roster() {
        // Same rule as the other transports: the roster owns the port so
        // same-host nodes sharing one [tcp].address still bind distinct
        // sockets.
        let node = cluster_node("127.0.0.1", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("127.0.0.1:18070"));
    }

    #[test]
    fn cluster_tcp_addr_merges_config_ip_with_roster_port() {
        // The roster ip is advertised, not bound. Binding it directly would
        // strand every co-located dialer (sidecars, health probes, on-host
        // consumers) that reaches this node over loopback.
        let node = cluster_node("10.0.0.5", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("0.0.0.0:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("0.0.0.0:18070"));
    }

    #[test]
    fn cluster_tcp_addr_requires_roster_port() {
        // tcp is always enabled in cluster mode, so a roster entry without a
        // tcp port refuses to boot rather than falling back to [tcp].address.
        let node = cluster_node_with_ports("10.0.0.5", None, None);
        let result = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None);
        assert!(matches!(
            result,
            Err(ServerError::ClusterPortMissing {
                transport: "tcp",
                replica_id: 0,
            })
        ));
    }

    #[test]
    fn cluster_tcp_addr_keeps_loopback_bind_and_warns_on_roster_mismatch() {
        // A loopback [tcp].address under a routable roster ip is honoured
        // as configured; remote peers cannot reach it, so the mismatch is
        // warned about instead of silently rebinding.
        let node = cluster_node("10.0.0.5", None);
        let addrs = resolve_cluster_client_addrs(&node, addr("127.0.0.1:8090"), None, None, None)
            .expect("cluster address resolution must succeed");
        assert_eq!(addrs.client, addr("127.0.0.1:18070"));
        assert!(roster_ip_unreachable_from_bind_addr(&node.ip, addrs.client));
    }

    #[test]
    fn roster_mismatch_warning_is_silent_for_wildcard_and_hostname_rosters() {
        // A wildcard bind covers the roster interface, and a DNS roster entry
        // can resolve to the bound one; neither is a misconfiguration.
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("0.0.0.0:18070")
        ));
        assert!(!roster_ip_unreachable_from_bind_addr(
            "node-1.example.com",
            addr("127.0.0.1:18070")
        ));
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("10.0.0.5:18070")
        ));
    }

    #[test]
    fn derived_address_warns_only_when_it_misses_a_listener() {
        // Derived from a loopback tcp.address while another transport serves
        // an external interface: metadata would publish an address no client
        // reaches. Every non-TCP listener carries the same exposure, since one
        // host is published for all four.
        for listener in ["10.0.0.5:3000", "10.0.0.5:8080", "10.0.0.5:8092"] {
            assert!(
                derived_address_misses_listener(None, "127.0.0.1", addr(listener)),
                "{listener} is not reachable at 127.0.0.1"
            );
        }
        // Same interface, and a wildcard bind that covers any of them.
        assert!(!derived_address_misses_listener(
            None,
            "10.0.0.5",
            addr("10.0.0.5:3000")
        ));
        assert!(!derived_address_misses_listener(
            None,
            "127.0.0.1",
            addr("0.0.0.0:3000")
        ));
        // A declared address is deliberate and unrelated to local interfaces.
        assert!(!derived_address_misses_listener(
            Some("broker-1.example.com"),
            "broker-1.example.com",
            addr("10.0.0.5:3000")
        ));
    }

    #[test]
    fn wildcard_listener_warns_only_under_a_derived_loopback_address() {
        // Metadata says 127.0.0.1 while this listener takes connections from
        // anywhere: whoever arrives from another host is told to dial itself.
        assert!(wildcard_listener_under_loopback_address(
            None,
            "127.0.0.1",
            addr("0.0.0.0:3000")
        ));
        assert!(wildcard_listener_under_loopback_address(
            None,
            "127.0.0.1",
            addr("[::]:3000")
        ));
        // A published address that is reachable from elsewhere is what the
        // wildcard listener wants, so there is nothing to say.
        assert!(!wildcard_listener_under_loopback_address(
            None,
            "10.0.0.5",
            addr("0.0.0.0:3000")
        ));
        // A concrete bind is the other warning's business, not this one's.
        assert!(!wildcard_listener_under_loopback_address(
            None,
            "127.0.0.1",
            addr("10.0.0.5:3000")
        ));
        // A declared address is deliberate; a loopback one is a local setup.
        assert!(!wildcard_listener_under_loopback_address(
            Some("127.0.0.1"),
            "127.0.0.1",
            addr("0.0.0.0:3000")
        ));
    }

    #[test]
    fn derived_bind_ip_follows_the_first_enabled_listener() {
        let mut config: ServerConfig =
            toml::from_str(include_str!("../../config.toml")).expect("shipped config deserializes");
        let topology = |ws: Option<&str>, http: Option<&str>| TcpTopology {
            cluster_id: 0,
            self_replica_id: 0,
            replica_count: 1,
            client_listen_addr: addr("127.0.0.1:8090"),
            replica_listen_addr: None,
            ws_listen_addr: ws.map(addr),
            quic_listen_addr: None,
            http_listen_addr: http.map(addr),
            tcp_tls_listen_addr: None,
            peers: Vec::new(),
        };
        let expected = |ip: &str| ip.parse::<IpAddr>().unwrap();

        assert_eq!(
            derived_bind_ip(
                &topology(Some("10.0.0.5:8092"), Some("10.0.0.6:3000")),
                &config
            ),
            expected("127.0.0.1")
        );

        config.tcp.enabled = false;
        assert_eq!(
            derived_bind_ip(
                &topology(Some("10.0.0.5:8092"), Some("10.0.0.6:3000")),
                &config
            ),
            expected("10.0.0.5"),
            "websocket is next in line once tcp is off"
        );
        assert_eq!(
            derived_bind_ip(&topology(None, Some("10.0.0.6:3000")), &config),
            expected("10.0.0.6"),
            "with websocket and quic off too, http answers"
        );
        assert_eq!(
            derived_bind_ip(&topology(None, None), &config),
            expected("127.0.0.1"),
            "with every client listener off the value reaches no client anyway"
        );
    }

    #[test]
    fn roster_mismatch_warning_is_silent_for_v4_mapped_binds() {
        // `[::ffff:0.0.0.0]` is the v4 wildcard and `::ffff:10.0.0.5` is
        // `10.0.0.5`, so neither reaches the dialer any differently than the
        // plain spelling the case above covers.
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("[::ffff:0.0.0.0]:18070")
        ));
        assert!(!roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("[::ffff:10.0.0.5]:18070")
        ));
        // A genuine mismatch still warns through the mapped spelling.
        assert!(roster_ip_unreachable_from_bind_addr(
            "10.0.0.5",
            addr("[::ffff:127.0.0.1]:18070")
        ));
    }
}
