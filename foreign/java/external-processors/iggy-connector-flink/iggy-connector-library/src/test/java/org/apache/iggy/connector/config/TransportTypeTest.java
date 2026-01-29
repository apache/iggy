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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransportTypeTest {

    @Test
    void shouldHaveCorrectDefaultPorts() {
        assertThat(TransportType.HTTP.getDefaultPort()).isEqualTo(3000);
        assertThat(TransportType.TCP.getDefaultPort()).isEqualTo(8090);
    }

    @Test
    void shouldHaveCorrectProtocolNames() {
        assertThat(TransportType.HTTP.getProtocolName()).isEqualTo("http");
        assertThat(TransportType.TCP.getProtocolName()).isEqualTo("tcp");
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "HTTP", "Http", "HtTp", "  http  ", "  HTTP  "})
    void shouldParseHttpFromString(String value) {
        assertThat(TransportType.fromString(value)).isEqualTo(TransportType.HTTP);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tcp", "TCP", "Tcp", "TcP", "  tcp  ", "  TCP  "})
    void shouldParseTcpFromString(String value) {
        assertThat(TransportType.fromString(value)).isEqualTo(TransportType.TCP);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t", "\n"})
    void shouldDefaultToHttpForNullOrEmptyString(String value) {
        assertThat(TransportType.fromString(value)).isEqualTo(TransportType.HTTP);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "httpp", "tcpp", "udp", "grpc"})
    void shouldThrowExceptionForInvalidTransportType(String value) {
        assertThatThrownBy(() -> TransportType.fromString(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid transport type")
                .hasMessageContaining(value)
                .hasMessageContaining("Valid values are: HTTP, TCP");
    }

    @Test
    void shouldHaveTwoValues() {
        assertThat(TransportType.values()).hasSize(2);
        assertThat(TransportType.values()).containsExactly(TransportType.HTTP, TransportType.TCP);
    }
}
