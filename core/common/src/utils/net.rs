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
pub fn parse_server_address(addr: &str) -> Result<(), IggyError> {
    if let Some(rest) = addr.strip_prefix('[') {
        // Bracketed IPv6: "[::1]:port"
        let close = rest.find(']').ok_or(IggyError::InvalidIpAddress(
            addr.to_string(),
            "".to_string(),
        ))?;
        let port_str = rest[close + 1..]
            .strip_prefix(':')
            .ok_or(IggyError::InvalidIpAddress(
                addr.to_string(),
                "".to_string(),
            ))?;

        port_str.parse::<u16>().map_err(|_| {
            IggyError::InvalidIpAddress(rest[0..close].to_string(), port_str.to_string())
        })?;

        return Ok(());
    }
    // hostname:port or IPv4:port — rsplit_once to split at last colon
    let (host, port_str) = addr.rsplit_once(':').ok_or(IggyError::InvalidIpAddress(
        addr.to_string(),
        "".to_string(),
    ))?;

    if host.is_empty() {
        return Err(IggyError::InvalidIpAddress(
            host.to_string(),
            port_str.to_string(),
        ));
    }

    // Reject bare IPv6 addresses
    if host.contains(':') {
        return Err(IggyError::InvalidIpAddress(
            host.to_string(),
            port_str.to_string(),
        ));
    }

    port_str
        .parse::<u16>()
        .map_err(|_| IggyError::InvalidIpAddress(host.to_string(), port_str.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ipv4_with_port() {
        assert!(parse_server_address("127.0.0.1:8090").is_ok());
        assert!(parse_server_address("192.168.1.1:65535").is_ok());
    }

    #[test]
    fn valid_ipv6_with_brackets() {
        assert!(parse_server_address("[::1]:8090").is_ok());
        assert!(parse_server_address("[2001:db8::1]:65535").is_ok());
    }

    #[test]
    fn valid_hostname_with_port() {
        assert!(parse_server_address("localhost:8090").is_ok());
        assert!(parse_server_address("iggy-server:8090").is_ok());
        assert!(parse_server_address("iggy.default.svc.cluster.local:8090").is_ok());
        assert!(parse_server_address("example.com:80").is_ok());
    }

    #[test]
    fn bare_ipv6_without_brackets_should_fail() {
        // Ambiguous format, not supported
        assert!(parse_server_address("::1:8080").is_err());
    }

    #[test]
    fn unresolvable_hostname_should_succeed() {
        // Format is valid, DNS is not attempted
        assert!(parse_server_address("invalid.ip:8080").is_ok());
    }

    #[test]
    fn missing_port_should_fail() {
        assert!(parse_server_address("localhost").is_err());
        assert!(parse_server_address("127.0.0.1").is_err());
    }

    #[test]
    fn invalid_port_should_fail() {
        assert!(parse_server_address("localhost:abc").is_err());
        assert!(parse_server_address("127.0.0.1:invalid").is_err());
    }

    #[test]
    fn port_out_of_range_should_fail() {
        assert!(parse_server_address("localhost:65536").is_err());
        assert!(parse_server_address("127.0.0.1:70000").is_err());
    }

    #[test]
    fn port_65535_should_succeed() {
        assert!(parse_server_address("localhost:65535").is_ok());
    }

    #[test]
    fn ipv6_missing_closing_bracket_should_fail() {
        assert!(parse_server_address("[::1:8090").is_err());
    }

    #[test]
    fn empty_host_should_fail() {
        assert!(parse_server_address(":8090").is_err());
    }

    #[test]
    fn empty_string_should_fail() {
        assert!(parse_server_address("").is_err());
    }
}
