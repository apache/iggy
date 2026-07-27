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

package org.apache.iggy.connector.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpEndpointTest {

    @Test
    void shouldUseDefaultPortForHost() {
        assertThat(TcpEndpoint.parse("localhost")).isEqualTo(new TcpEndpoint("localhost", TcpEndpoint.DEFAULT_PORT));
    }

    @Test
    void shouldUseDefaultPortForTcpUri() {
        assertThat(TcpEndpoint.parse("tcp://iggy.example.com"))
                .isEqualTo(new TcpEndpoint("iggy.example.com", TcpEndpoint.DEFAULT_PORT));
    }

    @Test
    void shouldParseHostAndPort() {
        assertThat(TcpEndpoint.parse("iggy:8091")).isEqualTo(new TcpEndpoint("iggy", 8091));
    }

    @Test
    void shouldParseTcpUri() {
        assertThat(TcpEndpoint.parse("tcp://iggy.example.com:8092"))
                .isEqualTo(new TcpEndpoint("iggy.example.com", 8092));
    }

    @Test
    void shouldParseIpv6Address() {
        assertThat(TcpEndpoint.parse("tcp://[::1]:8093")).isEqualTo(new TcpEndpoint("[::1]", 8093));
    }

    @Test
    void shouldParseIpv6AddressWithoutScheme() {
        assertThat(TcpEndpoint.parse("[2001:db8::1]:8094")).isEqualTo(new TcpEndpoint("[2001:db8::1]", 8094));
    }

    @Test
    void shouldRejectUnsupportedScheme() {
        assertThatThrownBy(() -> TcpEndpoint.parse("http://localhost:3000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported TCP server address scheme");
    }

    @Test
    void shouldRejectBlankAddress() {
        assertThatThrownBy(() -> TcpEndpoint.parse(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or blank");
    }

    @Test
    void shouldRejectNullAddress() {
        assertThatThrownBy(() -> TcpEndpoint.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or blank");
    }

    @Test
    void shouldRejectMissingHost() {
        assertThatThrownBy(() -> TcpEndpoint.parse("tcp://:8090"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot extract host");
    }

    @Test
    void shouldRejectZeroPort() {
        assertThatThrownBy(() -> TcpEndpoint.parse("localhost:0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP port");
    }

    @Test
    void shouldRejectPortAboveMaximum() {
        assertThatThrownBy(() -> TcpEndpoint.parse("localhost:65536"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP port");
    }

    @Test
    void shouldRejectUserInfo() {
        assertThatThrownBy(() -> TcpEndpoint.parse("tcp://user@localhost:8090"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP server address");
    }

    @Test
    void shouldRejectPath() {
        assertThatThrownBy(() -> TcpEndpoint.parse("tcp://localhost:8090/messages"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP server address");
    }

    @Test
    void shouldRejectQuery() {
        assertThatThrownBy(() -> TcpEndpoint.parse("tcp://localhost:8090?tls=true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP server address");
    }

    @Test
    void shouldRejectFragment() {
        assertThatThrownBy(() -> TcpEndpoint.parse("tcp://localhost:8090#server"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid TCP server address");
    }
}
