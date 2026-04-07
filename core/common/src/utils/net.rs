/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use crate::IggyError;
use std::net::{Ipv4Addr, Ipv6Addr};

/// Validates that `addr` is syntactically a valid `host:port` string.
/// Does NOT perform DNS resolution.
///
/// Accepted formats:
/// - `hostname:port` (e.g. `iggy-server:8090`, `localhost:8090`)
/// - `ipv4:port` (e.g. `127.0.0.1:8090`)
/// - `[ipv6]:port` (e.g. `[::1]:8090`)
///
/// Rejected formats:
/// - Bare IPv6 without brackets (e.g. `::1:8080`) — ambiguous due to colons
/// - Missing port (e.g. `localhost`)
/// - Invalid port (e.g. `localhost:abc`, `localhost:65536`)
pub fn validate_server_address(addr: &str) -> Result<(), IggyError> {
    if addr.starts_with('[') {
        // Bracketed IPv6: "[::1]:port"
        let close = addr.find(']').ok_or(IggyError::InvalidIpAddress(
            addr.to_string(),
            "<missing>".to_string(),
        ))?;
        let ipv6_str = &addr[1..close];
        let port_str = addr[close + 1..]
            .strip_prefix(':')
            .ok_or(IggyError::InvalidIpAddress(
                addr.to_string(),
                "<missing>".to_string(),
            ))?;

        // Validate IPv6 address
        ipv6_str
            .parse::<Ipv6Addr>()
            .map_err(|_| IggyError::InvalidIpAddress(ipv6_str.to_string(), port_str.to_string()))?;

        if !port_str.parse::<u16>().is_ok_and(|port| port != 0) {
            return Err(IggyError::InvalidIpAddress(
                ipv6_str.to_string(),
                port_str.to_string(),
            ));
        }

        return Ok(());
    }
    // hostname:port or IPv4:port — rsplit_once to split at last colon
    let (host, port_str) = addr.rsplit_once(':').ok_or(IggyError::InvalidIpAddress(
        addr.to_string(),
        "<missing>".to_string(),
    ))?;

    // Validate host (IPv4 or hostname with RFC 1123 compliance)
    if !is_valid_host(host) {
        return Err(IggyError::InvalidIpAddress(
            host.to_string(),
            port_str.to_string(),
        ));
    }

    if !port_str.parse::<u16>().is_ok_and(|port| port != 0) {
        return Err(IggyError::InvalidIpAddress(
            host.to_string(),
            port_str.to_string(),
        ));
    }

    Ok(())
}

fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 || host.contains(':') {
        return false;
    }

    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            && label
                .chars()
                .last()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            && label
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    })
}

fn is_valid_host(host: &str) -> bool {
    // Try to parse as IP first
    if host.parse::<Ipv4Addr>().is_ok() {
        return true;
    }

    // If it looks like an IP (all digits and dots), reject it
    if host.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return false;
    }

    // Otherwise, validate as hostname
    is_valid_hostname(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ipv4_with_port() {
        assert!(validate_server_address("127.0.0.1:8090").is_ok());
        assert!(validate_server_address("192.168.1.1:65535").is_ok());
    }

    #[test]
    fn valid_ipv6_with_brackets() {
        assert!(validate_server_address("[::1]:8090").is_ok());
        assert!(validate_server_address("[2001:db8::1]:65535").is_ok());
    }

    #[test]
    fn valid_hostname_with_port() {
        assert!(validate_server_address("localhost:8090").is_ok());
        assert!(validate_server_address("iggy-server:8090").is_ok());
        assert!(validate_server_address("iggy.default.svc.cluster.local:8090").is_ok());
        assert!(validate_server_address("example.com:80").is_ok());
    }

    #[test]
    fn bare_ipv6_without_brackets_should_fail() {
        // Ambiguous format, not supported
        assert!(validate_server_address("::1:8080").is_err());
    }

    #[test]
    fn unresolvable_hostname_should_succeed() {
        // Format is valid, DNS is not attempted
        assert!(validate_server_address("invalid.ip:8080").is_ok());
    }

    #[test]
    fn missing_port_should_fail() {
        assert!(validate_server_address("localhost").is_err());
        assert!(validate_server_address("127.0.0.1").is_err());
    }

    #[test]
    fn invalid_port_should_fail() {
        assert!(validate_server_address("localhost:abc").is_err());
        assert!(validate_server_address("127.0.0.1:invalid").is_err());
    }

    #[test]
    fn port_out_of_range_should_fail() {
        assert!(validate_server_address("localhost:65536").is_err());
        assert!(validate_server_address("127.0.0.1:70000").is_err());
    }

    #[test]
    fn port_65535_should_succeed() {
        assert!(validate_server_address("localhost:65535").is_ok());
    }

    #[test]
    fn port_0_should_fail() {
        assert!(validate_server_address("localhost:0").is_err());
        assert!(validate_server_address("127.0.0.1:0").is_err());
        assert!(validate_server_address("[::1]:0").is_err());
    }

    #[test]
    fn ipv6_missing_closing_bracket_should_fail() {
        assert!(validate_server_address("[::1:8090").is_err());
    }

    #[test]
    fn empty_host_should_fail() {
        assert!(validate_server_address(":8090").is_err());
    }

    #[test]
    fn empty_string_should_fail() {
        assert!(validate_server_address("").is_err());
    }

    #[test]
    fn valid_hostname_labels() {
        assert!(is_valid_hostname("localhost"));
        assert!(is_valid_hostname("example"));
        assert!(is_valid_hostname("example-server"));
        assert!(is_valid_hostname("my-server-01"));
        assert!(is_valid_hostname("a"));
    }

    #[test]
    fn valid_fqdn() {
        assert!(is_valid_hostname("example.com"));
        assert!(is_valid_hostname("sub.example.com"));
        assert!(is_valid_hostname("my-server.prod.example.com"));
        assert!(is_valid_hostname("iggy.default.svc.cluster.local"));
    }

    #[test]
    fn valid_hostname_with_underscores() {
        // Docker Compose style names
        assert!(is_valid_hostname("my_project_redis"));
        assert!(is_valid_hostname("docker_compose_service"));
        // SRV records
        assert!(is_valid_hostname("_svc._tcp.example.com"));
        // Mixed
        assert!(is_valid_hostname("my_server-01.prod.example.com"));
    }

    #[test]
    fn valid_underscore_hostname_with_port() {
        assert!(validate_server_address("my_project_redis:6379").is_ok());
        assert!(validate_server_address("_svc._tcp.example.com:8090").is_ok());
    }

    #[test]
    fn invalid_hostname_empty() {
        assert!(!is_valid_hostname(""));
    }

    #[test]
    fn invalid_hostname_too_long() {
        let long = "a".repeat(254);
        assert!(!is_valid_hostname(&long));
    }

    #[test]
    fn invalid_hostname_label_too_long() {
        let long_label = format!("{}.com", "a".repeat(64));
        assert!(!is_valid_hostname(&long_label));
    }

    #[test]
    fn invalid_hostname_empty_label() {
        assert!(!is_valid_hostname("example..com"));
        assert!(!is_valid_hostname(".example.com"));
        assert!(!is_valid_hostname("example.com."));
    }

    #[test]
    fn invalid_hostname_start_with_hyphen() {
        assert!(!is_valid_hostname("-example"));
        assert!(!is_valid_hostname("example.-com"));
    }

    #[test]
    fn invalid_hostname_end_with_hyphen() {
        assert!(!is_valid_hostname("example-"));
        assert!(!is_valid_hostname("example.com-"));
    }

    #[test]
    fn invalid_hostname_invalid_characters() {
        assert!(is_valid_hostname("example_com"));
        assert!(!is_valid_hostname("example@com"));
        assert!(!is_valid_hostname("example com"));
        assert!(!is_valid_hostname("example.c0m!"));
    }

    #[test]
    fn validate_server_address_rejects_invalid_hostname() {
        assert!(validate_server_address("example..com:8090").is_err());
        assert!(validate_server_address("-invalid:8090").is_err());
        assert!(validate_server_address("invalid-:8090").is_err());
        assert!(validate_server_address("example_invalid:8090").is_ok());
    }

    #[test]
    fn invalid_ipv6_in_brackets_should_fail() {
        assert!(validate_server_address("[invalid]:8090").is_err());
        assert!(validate_server_address("[::gggg]:8090").is_err());
        assert!(validate_server_address("[192.168.1.1]:8090").is_err());
    }

    #[test]
    fn valid_ipv4_address_should_succeed() {
        assert!(validate_server_address("192.168.1.1:8090").is_ok());
        assert!(validate_server_address("10.0.0.1:8090").is_ok());
    }

    #[test]
    fn invalid_ipv4_address_should_fail() {
        assert!(validate_server_address("256.1.1.1:8090").is_err());
        assert!(validate_server_address("192.168.1:8090").is_err());
    }
}
