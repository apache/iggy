/* Licensed to the Apache Software Foundation (ASF) under one
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

/// Extracts IP from an address string like "127.0.0.1:8090" or "[::1]:8090"
pub fn extract_ip(address: &str) -> String {
    if let Some(colon_pos) = address.rfind(':') {
        // Handle IPv6 addresses like [::1]:8090
        if address.starts_with('[')
            && let Some(bracket_pos) = address.rfind(']')
        {
            return address[1..bracket_pos].to_string();
        }
        // Handle IPv4 addresses like 127.0.0.1:8090
        return address[..colon_pos].to_string();
    }
    address.to_string()
}

/// Extracts port from an address string like "127.0.0.1:8090"
pub fn extract_port(address: &str) -> u16 {
    if let Some(colon_pos) = address.rfind(':')
        && let Ok(port) = address[colon_pos + 1..].parse::<u16>()
    {
        return port;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_ip_ipv4() {
        assert_eq!(extract_ip("127.0.0.1:8090"), "127.0.0.1");
        assert_eq!(extract_ip("192.168.1.100:3000"), "192.168.1.100");
    }

    #[test]
    fn test_extract_ip_ipv6() {
        assert_eq!(extract_ip("[::1]:8090"), "::1");
        assert_eq!(extract_ip("[2001:db8::1]:443"), "2001:db8::1");
    }

    #[test]
    fn test_extract_ip_no_port() {
        assert_eq!(extract_ip("127.0.0.1"), "127.0.0.1");
    }

    #[test]
    fn test_extract_port() {
        assert_eq!(extract_port("127.0.0.1:8090"), 8090);
        assert_eq!(extract_port("192.168.1.100:3000"), 3000);
        assert_eq!(extract_port("[::1]:8090"), 8090);
    }

    #[test]
    fn test_extract_port_no_port() {
        assert_eq!(extract_port("127.0.0.1"), 0);
    }
}
