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

//! Listeners, one per Kafka security protocol.
//!
//! The shape is Kafka's own `listeners` and `advertised.listeners`: `PROTOCOL://host:port`
//! entries separated by commas, with the protocol doubling as the listener name. A client is
//! told the address of the listener it arrived through and no other, so a client on the TLS port
//! is never pointed at the plaintext one by Metadata.

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::BrokerAdvertise;

/// Kafka's four listener security protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub const ALL: [Self; 4] = [
        Self::Plaintext,
        Self::Ssl,
        Self::SaslPlaintext,
        Self::SaslSsl,
    ];

    #[must_use]
    pub const fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    #[must_use]
    pub const fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }

    /// Whether a client certificate names the principal on this listener.
    ///
    /// Only on a listener without SASL: with SASL the exchange names the principal and the
    /// certificate, when one is demanded, only gates the transport, which is how Kafka treats a
    /// `SASL_SSL` listener with `ssl.client.auth=required`.
    #[must_use]
    pub const fn authenticates_by_certificate(self) -> bool {
        matches!(self, Self::Ssl)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }
}

impl FromStr for SecurityProtocol {
    type Err = KafkaProtocolError;

    /// Exact, upper-case spellings only, like every switch this gateway reads: a lower-case or
    /// misspelled protocol must stop the process rather than quietly pick a weaker listener.
    fn from_str(raw: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|protocol| protocol.as_str() == raw)
            .ok_or_else(|| {
                KafkaProtocolError::InvalidConfig(format!(
                    "unknown security protocol '{raw}'; expected one of PLAINTEXT, SSL, \
                     SASL_PLAINTEXT, SASL_SSL"
                ))
            })
    }
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One listener: where it binds and what it tells clients to connect back to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerConfig {
    pub protocol: SecurityProtocol,
    /// `host:port` to bind. Must parse as a socket address.
    pub bind_addr: String,
    /// Hostname or IP advertised in Metadata. Required when `bind_addr` is a wildcard.
    pub advertised_host: Option<String>,
    /// Port advertised in Metadata. Defaults to the bound port.
    pub advertised_port: Option<u16>,
}

impl ListenerConfig {
    #[must_use]
    pub fn new(protocol: SecurityProtocol, bind_addr: impl Into<String>) -> Self {
        Self {
            protocol,
            bind_addr: bind_addr.into(),
            advertised_host: None,
            advertised_port: None,
        }
    }

    #[must_use]
    pub fn with_advertised(mut self, host: impl Into<String>, port: u16) -> Self {
        self.advertised_host = Some(host.into());
        self.advertised_port = Some(port);
        self
    }
}

/// Parses `IGGY_KAFKA_LISTENERS`: `PROTOCOL://host:port`, comma separated.
///
/// Each protocol may appear once, since it is the listener's name and the key
/// `IGGY_KAFKA_ADVERTISED_LISTENERS` addresses it by. The host must be an IP address: the bind
/// goes through `socket2`, which takes no hostname.
///
/// # Errors
///
/// Returns `InvalidConfig` naming the variable for an empty list, a malformed entry, an unknown
/// protocol or a repeated one.
pub fn parse_listeners(raw: &str) -> Result<Vec<ListenerConfig>> {
    let mut listeners: Vec<ListenerConfig> = Vec::new();
    for entry in raw.split(',').map(str::trim) {
        if entry.is_empty() {
            return Err(invalid(format!(
                "IGGY_KAFKA_LISTENERS has an empty entry in '{raw}'"
            )));
        }
        let (protocol, address) = split_entry("IGGY_KAFKA_LISTENERS", entry)?;
        let bind_addr: SocketAddr = address.parse().map_err(|error| {
            invalid(format!(
                "IGGY_KAFKA_LISTENERS entry '{entry}' must bind an IP address and port: {error}"
            ))
        })?;
        if listeners
            .iter()
            .any(|listener| listener.protocol == protocol)
        {
            return Err(invalid(format!(
                "IGGY_KAFKA_LISTENERS names {protocol} twice; one listener per security protocol"
            )));
        }
        listeners.push(ListenerConfig::new(protocol, bind_addr.to_string()));
    }
    Ok(listeners)
}

/// Applies `IGGY_KAFKA_ADVERTISED_LISTENERS` to `listeners`: `PROTOCOL://host:port` per entry.
///
/// The host may be a name, since it is handed to clients rather than bound.
///
/// # Errors
///
/// Returns `InvalidConfig` naming the variable for a malformed entry, a protocol that has no
/// listener, or one advertised twice.
pub fn apply_advertised_listeners(listeners: &mut [ListenerConfig], raw: &str) -> Result<()> {
    let mut seen: Vec<SecurityProtocol> = Vec::new();
    for entry in raw.split(',').map(str::trim) {
        if entry.is_empty() {
            return Err(invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS has an empty entry in '{raw}'"
            )));
        }
        let (protocol, address) = split_entry("IGGY_KAFKA_ADVERTISED_LISTENERS", entry)?;
        let (host, port) = split_host_port(address).ok_or_else(|| {
            invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS entry '{entry}' must be PROTOCOL://host:port"
            ))
        })?;
        let port: u16 = port.parse().map_err(|error| {
            invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS entry '{entry}' has an invalid port: {error}"
            ))
        })?;
        if port == 0 {
            return Err(invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS entry '{entry}' advertises port 0"
            )));
        }
        if seen.contains(&protocol) {
            return Err(invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS names {protocol} twice"
            )));
        }
        let Some(listener) = listeners
            .iter_mut()
            .find(|listener| listener.protocol == protocol)
        else {
            return Err(invalid(format!(
                "IGGY_KAFKA_ADVERTISED_LISTENERS advertises {protocol}, which IGGY_KAFKA_LISTENERS \
                 does not bind"
            )));
        };
        listener.advertised_host = Some(host.to_string());
        listener.advertised_port = Some(port);
        seen.push(protocol);
    }
    Ok(())
}

/// One entry of `IGGY_KAFKA_BROKERS`: the node id of a gateway and the address one of its
/// listeners advertises. The `id@host:port` form names no protocol, and fits a gateway with one
/// listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerEntry {
    pub node_id: u16,
    pub protocol: Option<SecurityProtocol>,
    pub host: String,
    pub port: u16,
}

impl BrokerEntry {
    /// Whether this entry is the address of a `protocol` listener.
    #[must_use]
    pub fn serves(&self, protocol: SecurityProtocol) -> bool {
        self.protocol.is_none_or(|own| own == protocol)
    }
}

/// Parses `IGGY_KAFKA_BROKERS`: every gateway in front of one Iggy cluster, as `id@host:port` or
/// `id@PROTOCOL://host:port`, comma separated.
///
/// # Errors
///
/// Returns `InvalidConfig` naming the variable for an empty list, a malformed entry, a node id
/// outside 0 to 65535, or port 0.
pub fn parse_brokers(raw: &str) -> Result<Vec<BrokerEntry>> {
    raw.split(',')
        .map(str::trim)
        .map(|entry| parse_broker(raw, entry))
        .collect()
}

fn parse_broker(raw: &str, entry: &str) -> Result<BrokerEntry> {
    if entry.is_empty() {
        return Err(invalid(format!(
            "IGGY_KAFKA_BROKERS has an empty entry in '{raw}'"
        )));
    }
    let malformed = || {
        invalid(format!(
            "IGGY_KAFKA_BROKERS entry '{entry}' must be id@host:port or id@PROTOCOL://host:port"
        ))
    };
    let (node_id, address) = entry.split_once('@').ok_or_else(malformed)?;
    let node_id: u16 = node_id.parse().map_err(|error| {
        invalid(format!(
            "IGGY_KAFKA_BROKERS entry '{entry}' has an invalid node id: {error}"
        ))
    })?;
    let (protocol, address) = if address.contains("://") {
        let (protocol, address) = split_entry("IGGY_KAFKA_BROKERS", address)?;
        (Some(protocol), address)
    } else {
        (None, address)
    };
    let (host, port) = split_host_port(address).ok_or_else(malformed)?;
    let port: u16 = port.parse().map_err(|error| {
        invalid(format!(
            "IGGY_KAFKA_BROKERS entry '{entry}' has an invalid port: {error}"
        ))
    })?;
    if port == 0 {
        return Err(invalid(format!(
            "IGGY_KAFKA_BROKERS entry '{entry}' advertises port 0"
        )));
    }
    if host.len() > i16::MAX as usize {
        return Err(invalid(format!(
            "IGGY_KAFKA_BROKERS entry for node {node_id} has a host over Kafka's string limit \
             (32767 bytes)"
        )));
    }
    Ok(BrokerEntry {
        node_id,
        protocol,
        host: host.to_string(),
        port,
    })
}

/// Checks `brokers` against the listeners of this gateway, whose node id is `node_id`.
///
/// Each gateway needs one entry per listener, so that every listener lists the same gateways.
///
/// # Errors
///
/// Returns `InvalidConfig` naming the variable when an entry names a protocol that no listener
/// here binds, an entry without a protocol meets several listeners, a gateway has no entry or two
/// for a listener, or `node_id` has no entry.
pub fn check_brokers(
    brokers: &[BrokerEntry],
    listeners: &[ListenerConfig],
    node_id: u16,
) -> Result<()> {
    for entry in brokers {
        match entry.protocol {
            None if listeners.len() > 1 => {
                return Err(invalid(format!(
                    "IGGY_KAFKA_BROKERS entry for node {} names no protocol, but this gateway has \
                     {} listeners; write it as id@PROTOCOL://host:port",
                    entry.node_id,
                    listeners.len()
                )));
            }
            Some(protocol)
                if !listeners
                    .iter()
                    .any(|listener| listener.protocol == protocol) =>
            {
                return Err(invalid(format!(
                    "IGGY_KAFKA_BROKERS names {protocol} for node {}, which no listener here binds",
                    entry.node_id
                )));
            }
            _ => {}
        }
    }
    let node_ids: BTreeSet<u16> = brokers.iter().map(|entry| entry.node_id).collect();
    if !node_ids.contains(&node_id) {
        return Err(invalid(format!(
            "IGGY_KAFKA_BROKERS has no entry for this gateway, node {node_id} \
             (IGGY_KAFKA_NODE_ID)"
        )));
    }
    for listener in listeners {
        for node in &node_ids {
            let entries = brokers
                .iter()
                .filter(|entry| entry.node_id == *node && entry.serves(listener.protocol))
                .count();
            if entries != 1 {
                return Err(invalid(format!(
                    "IGGY_KAFKA_BROKERS has {entries} {} entries for node {node}; each gateway \
                     needs one per listener",
                    listener.protocol
                )));
            }
        }
    }
    Ok(())
}

/// The gateways other than `node_id`, as their `protocol` listeners advertise them, in node id
/// order. Expects `brokers` to pass [`check_brokers`].
#[must_use]
pub fn peers_of(
    brokers: &[BrokerEntry],
    protocol: SecurityProtocol,
    node_id: u16,
) -> Vec<(i32, BrokerAdvertise)> {
    let mut peers: Vec<(i32, BrokerAdvertise)> = brokers
        .iter()
        .filter(|entry| entry.node_id != node_id && entry.serves(protocol))
        .map(|entry| {
            let endpoint = BrokerAdvertise {
                host: entry.host.clone(),
                port: i32::from(entry.port),
            };
            (i32::from(entry.node_id), endpoint)
        })
        .collect();
    peers.sort_by_key(|(node, _)| *node);
    peers
}

impl BrokerAdvertise {
    /// Resolves the broker endpoint one listener advertises in Metadata and `FindCoordinator`.
    ///
    /// `local_addr` is the address the listener is actually bound to.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the advertised host is empty or too long for a Kafka string,
    /// or when the listener binds a wildcard without an explicit advertised host.
    pub fn for_listener(listener: &ListenerConfig, local_addr: SocketAddr) -> Result<Self> {
        let port = listener
            .advertised_port
            .map_or_else(|| i32::from(local_addr.port()), i32::from);
        let protocol = listener.protocol;

        let host = if let Some(ref advertised) = listener.advertised_host {
            let trimmed = advertised.trim();
            if trimmed.is_empty() {
                return Err(invalid(format!(
                    "the advertised host of the {protocol} listener must not be empty \
                     (IGGY_KAFKA_ADVERTISED_LISTENERS, or IGGY_KAFKA_ADVERTISED_HOST for the \
                     single-listener form)"
                )));
            }
            if trimmed.len() > i16::MAX as usize {
                return Err(invalid(format!(
                    "the advertised host of the {protocol} listener exceeds Kafka's nullable \
                     string limit (32767 bytes) (IGGY_KAFKA_ADVERTISED_LISTENERS, or \
                     IGGY_KAFKA_ADVERTISED_HOST for the single-listener form)"
                )));
            }
            trimmed.to_string()
        } else if local_addr.ip().is_unspecified() {
            return Err(invalid(format!(
                "the {protocol} listener binds a wildcard address (0.0.0.0 or ::), so clients \
                 need a reachable hostname or IP to be pointed back at: set \
                 IGGY_KAFKA_ADVERTISED_LISTENERS={protocol}://<host>:<port> (or \
                 IGGY_KAFKA_ADVERTISED_HOST for the single-listener form)"
            )));
        } else {
            local_addr.ip().to_string()
        };

        Ok(Self { host, port })
    }
}

/// Splits `PROTOCOL://rest` into the protocol and the rest.
fn split_entry<'a>(var: &str, entry: &'a str) -> Result<(SecurityProtocol, &'a str)> {
    let Some((protocol, address)) = entry.split_once("://") else {
        return Err(invalid(format!(
            "{var} entry '{entry}' must be PROTOCOL://host:port"
        )));
    };
    let protocol: SecurityProtocol = protocol
        .parse()
        .map_err(|error| invalid(format!("{var} entry '{entry}': {error}")))?;
    if address.is_empty() {
        return Err(invalid(format!(
            "{var} entry '{entry}' names no address after {protocol}://"
        )));
    }
    Ok((protocol, address))
}

/// Splits `host:port`, with `[v6]:port` for an IPv6 literal, into a bracket-free host and the
/// port text. An unbracketed host containing `:` is refused, since its port cannot be told apart.
fn split_host_port(address: &str) -> Option<(&str, &str)> {
    let (host, port) = if let Some(bracketed) = address.strip_prefix('[') {
        let (host, rest) = bracketed.split_once(']')?;
        (host, rest.strip_prefix(':')?)
    } else {
        let (host, port) = address.rsplit_once(':')?;
        if host.contains(':') {
            return None;
        }
        (host, port)
    };
    if host.is_empty() {
        return None;
    }
    Some((host, port))
}

const fn invalid(message: String) -> KafkaProtocolError {
    KafkaProtocolError::InvalidConfig(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_kafka_style_listeners_should_parse_one_per_protocol() {
        let listeners =
            parse_listeners("PLAINTEXT://127.0.0.1:9093, SSL://0.0.0.0:9094,SASL_SSL://[::]:9095")
                .expect("valid listeners");
        assert_eq!(
            listeners,
            vec![
                ListenerConfig::new(SecurityProtocol::Plaintext, "127.0.0.1:9093"),
                ListenerConfig::new(SecurityProtocol::Ssl, "0.0.0.0:9094"),
                ListenerConfig::new(SecurityProtocol::SaslSsl, "[::]:9095"),
            ]
        );
    }

    #[test]
    fn given_a_malformed_listener_should_name_the_variable_and_the_entry() {
        for raw in [
            "",
            "PLAINTEXT://127.0.0.1:9093,",
            "127.0.0.1:9093",
            "plaintext://127.0.0.1:9093",
            "SSL://",
            "SSL://localhost:9093",
            "SSL://127.0.0.1",
            "SSL://127.0.0.1:9093,SSL://127.0.0.1:9094",
        ] {
            let error = parse_listeners(raw)
                .err()
                .unwrap_or_else(|| panic!("'{raw}' must be rejected"))
                .to_string();
            assert!(
                error.contains("IGGY_KAFKA_LISTENERS"),
                "'{raw}' rejection must name the variable, got: {error}"
            );
        }
    }

    #[test]
    fn given_advertised_listeners_should_apply_to_the_matching_protocol_only() {
        let mut listeners =
            parse_listeners("PLAINTEXT://0.0.0.0:9093,SSL://0.0.0.0:9094").expect("valid");
        apply_advertised_listeners(
            &mut listeners,
            "SSL://kafka.example.com:19094, PLAINTEXT://[2001:db8::1]:19093",
        )
        .expect("valid advertisement");
        assert_eq!(
            listeners[0].advertised_host.as_deref(),
            Some("2001:db8::1"),
            "an IPv6 literal is advertised without its brackets"
        );
        assert_eq!(listeners[0].advertised_port, Some(19093));
        assert_eq!(
            listeners[1].advertised_host.as_deref(),
            Some("kafka.example.com")
        );
        assert_eq!(listeners[1].advertised_port, Some(19094));
    }

    #[test]
    fn given_an_advertisement_for_an_unbound_protocol_should_refuse() {
        let mut listeners = parse_listeners("PLAINTEXT://127.0.0.1:9093").expect("valid");
        for raw in [
            "SSL://kafka.example.com:9094",
            "PLAINTEXT://:9093",
            "PLAINTEXT://kafka.example.com",
            "PLAINTEXT://kafka.example.com:0",
            "PLAINTEXT://kafka.example.com:9093,PLAINTEXT://other:9093",
        ] {
            let error = apply_advertised_listeners(&mut listeners, raw)
                .err()
                .unwrap_or_else(|| panic!("'{raw}' must be rejected"))
                .to_string();
            assert!(
                error.contains("IGGY_KAFKA_ADVERTISED_LISTENERS"),
                "'{raw}' rejection must name the variable, got: {error}"
            );
        }
    }

    #[test]
    fn given_an_unbracketed_ipv6_advertised_host_should_refuse() {
        let mut listeners = parse_listeners("SSL://0.0.0.0:9094").expect("valid");
        for raw in ["SSL://fe80::1", "SSL://fe80::1:9094", "SSL://[fe80::1]9094"] {
            let error = apply_advertised_listeners(&mut listeners, raw)
                .err()
                .unwrap_or_else(|| panic!("'{raw}' must be rejected"))
                .to_string();
            assert!(
                error.contains("IGGY_KAFKA_ADVERTISED_LISTENERS"),
                "'{raw}' rejection must name the variable, got: {error}"
            );
        }
        apply_advertised_listeners(&mut listeners, "SSL://[fe80::1]:9094")
            .expect("a bracketed IPv6 literal is valid");
        assert_eq!(listeners[0].advertised_host.as_deref(), Some("fe80::1"));
    }

    #[test]
    fn given_a_wildcard_bind_without_an_advertised_host_should_refuse_and_name_the_listener() {
        let listener = ListenerConfig::new(SecurityProtocol::SaslSsl, "0.0.0.0:9095");
        let error = BrokerAdvertise::for_listener(&listener, "0.0.0.0:9095".parse().unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("SASL_SSL"), "got: {error}");
        assert!(
            error.contains("IGGY_KAFKA_ADVERTISED_LISTENERS"),
            "got: {error}"
        );
        assert!(error.contains("IGGY_KAFKA_ADVERTISED_HOST"), "got: {error}");
    }

    #[test]
    fn given_an_advertised_port_should_override_the_bound_one() {
        let listener = ListenerConfig::new(SecurityProtocol::Ssl, "127.0.0.1:0")
            .with_advertised("broker.example.com", 19094);
        let broker = BrokerAdvertise::for_listener(&listener, "127.0.0.1:41234".parse().unwrap())
            .expect("valid");
        assert_eq!(broker.host, "broker.example.com");
        assert_eq!(broker.port, 19094);
    }

    #[test]
    fn given_a_concrete_bind_should_advertise_the_bound_address() {
        let listener = ListenerConfig::new(SecurityProtocol::Plaintext, "192.168.1.10:0");
        let broker =
            BrokerAdvertise::for_listener(&listener, "192.168.1.10:19092".parse().unwrap())
                .expect("valid");
        assert_eq!(broker.host, "192.168.1.10");
        assert_eq!(broker.port, 19092);
    }

    #[test]
    fn security_protocol_round_trips_through_its_name() {
        for protocol in SecurityProtocol::ALL {
            assert_eq!(
                protocol.as_str().parse::<SecurityProtocol>().unwrap(),
                protocol
            );
        }
        assert!("ssl".parse::<SecurityProtocol>().is_err());
        assert!(SecurityProtocol::SaslSsl.uses_tls() && SecurityProtocol::SaslSsl.uses_sasl());
        assert!(SecurityProtocol::Ssl.authenticates_by_certificate());
        assert!(!SecurityProtocol::SaslSsl.authenticates_by_certificate());
    }

    #[test]
    fn given_both_entry_forms_when_parsing_brokers_should_keep_the_node_protocol_and_address() {
        let brokers =
            parse_brokers("1@gw-1:9093, 2@SASL_SSL://[fd00::2]:9094").expect("valid brokers");
        assert_eq!(
            brokers,
            vec![
                BrokerEntry {
                    node_id: 1,
                    protocol: None,
                    host: "gw-1".to_string(),
                    port: 9093,
                },
                BrokerEntry {
                    node_id: 2,
                    protocol: Some(SecurityProtocol::SaslSsl),
                    host: "fd00::2".to_string(),
                    port: 9094,
                },
            ]
        );
    }

    #[test]
    fn given_malformed_brokers_when_parsing_should_name_the_variable() {
        for raw in [
            "",
            "gw-1:9093",
            "x@gw-1:9093",
            "70000@gw-1:9093",
            "1@gw-1",
            "1@gw-1:0",
            "1@TLS://gw-1:9093",
        ] {
            let error = parse_brokers(raw).expect_err(raw).to_string();
            assert!(error.contains("IGGY_KAFKA_BROKERS"), "'{raw}': {error}");
        }
    }

    #[test]
    fn given_one_listener_when_checking_brokers_should_need_this_gateway_once() {
        let listeners = [ListenerConfig::new(
            SecurityProtocol::Plaintext,
            "0.0.0.0:9092",
        )];
        let brokers = parse_brokers("1@gw-1:9092,2@gw-2:9092").expect("valid brokers");
        assert!(check_brokers(&brokers, &listeners, 2).is_ok());

        let missing = check_brokers(&brokers, &listeners, 3).expect_err("node 3 is not listed");
        assert!(missing.to_string().contains("node 3"), "{missing}");
        let twice = parse_brokers("1@gw-1:9092,1@gw-9:9092").expect("valid brokers");
        assert!(check_brokers(&twice, &listeners, 1).is_err());
    }

    #[test]
    fn given_several_listeners_when_checking_brokers_should_need_one_entry_per_listener() {
        let listeners = [
            ListenerConfig::new(SecurityProtocol::Plaintext, "0.0.0.0:9092"),
            ListenerConfig::new(SecurityProtocol::SaslSsl, "0.0.0.0:9094"),
        ];
        let complete = parse_brokers(
            "1@PLAINTEXT://gw-1:9092,1@SASL_SSL://gw-1:9094,\
             2@PLAINTEXT://gw-2:9092,2@SASL_SSL://gw-2:9094",
        )
        .expect("valid brokers");
        assert!(check_brokers(&complete, &listeners, 1).is_ok());

        for raw in [
            "1@PLAINTEXT://gw-1:9092,1@SASL_SSL://gw-1:9094,2@PLAINTEXT://gw-2:9092",
            "1@gw-1:9092",
            "1@PLAINTEXT://gw-1:9092,1@SASL_SSL://gw-1:9094,1@SSL://gw-1:9093",
        ] {
            let brokers = parse_brokers(raw).expect("valid brokers");
            assert!(check_brokers(&brokers, &listeners, 1).is_err(), "{raw}");
        }
    }

    #[test]
    fn given_several_listeners_when_listing_peers_should_give_each_listener_its_own_endpoints() {
        let brokers = parse_brokers(
            "2@PLAINTEXT://gw-2:9092,2@SASL_SSL://gw-2:9094,1@PLAINTEXT://gw-1:9092,\
             1@SASL_SSL://gw-1:9094,3@PLAINTEXT://gw-3:9092,3@SASL_SSL://gw-3:9094",
        )
        .expect("valid brokers");
        let listed: Vec<(i32, String, i32)> = peers_of(&brokers, SecurityProtocol::SaslSsl, 2)
            .into_iter()
            .map(|(node, endpoint)| (node, endpoint.host, endpoint.port))
            .collect();
        assert_eq!(
            listed,
            [(1, "gw-1".to_string(), 9094), (3, "gw-3".to_string(), 9094)]
        );
    }
}
