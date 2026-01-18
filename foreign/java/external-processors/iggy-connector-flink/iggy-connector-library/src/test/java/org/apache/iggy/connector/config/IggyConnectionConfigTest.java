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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IggyConnectionConfigTest {

    @Test
    void shouldBuildValidConfig() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .username("iggy")
                .password("iggy")
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(20))
                .maxRetries(5)
                .retryBackoff(Duration.ofMillis(200))
                .enableTls(true)
                .build();

        assertThat(config.getServerAddress()).isEqualTo("localhost:8080");
        assertThat(config.getUsername()).isEqualTo("iggy");
        assertThat(config.getPassword()).isEqualTo("iggy");
        assertThat(config.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(20));
        assertThat(config.getMaxRetries()).isEqualTo(5);
        assertThat(config.getRetryBackoff()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.isEnableTls()).isTrue();
    }

    @Test
    void shouldUseDefaultValues() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config.getConnectionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.getMaxRetries()).isEqualTo(3);
        assertThat(config.getRetryBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.isEnableTls()).isFalse();
        // Transport defaults
        assertThat(config.getTransportType()).isEqualTo(TransportType.HTTP);
        assertThat(config.getHttpPort()).isEqualTo(3000);
        assertThat(config.getTcpPort()).isEqualTo(8090);
    }

    @Test
    void shouldBuildConfigWithTcpTransport() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost")
                .transportType(TransportType.TCP)
                .tcpPort(9090)
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config.getTransportType()).isEqualTo(TransportType.TCP);
        assertThat(config.getTcpPort()).isEqualTo(9090);
        assertThat(config.getEffectivePort()).isEqualTo(9090);
    }

    @Test
    void shouldBuildConfigWithHttpTransport() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost")
                .transportType(TransportType.HTTP)
                .httpPort(8000)
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config.getTransportType()).isEqualTo(TransportType.HTTP);
        assertThat(config.getHttpPort()).isEqualTo(8000);
        assertThat(config.getEffectivePort()).isEqualTo(8000);
    }

    @Test
    void shouldReturnCorrectEffectivePort() {
        IggyConnectionConfig httpConfig = IggyConnectionConfig.builder()
                .serverAddress("localhost")
                .transportType(TransportType.HTTP)
                .httpPort(3001)
                .tcpPort(8091)
                .username("iggy")
                .password("iggy")
                .build();

        IggyConnectionConfig tcpConfig = IggyConnectionConfig.builder()
                .serverAddress("localhost")
                .transportType(TransportType.TCP)
                .httpPort(3001)
                .tcpPort(8091)
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(httpConfig.getEffectivePort()).isEqualTo(3001);
        assertThat(tcpConfig.getEffectivePort()).isEqualTo(8091);
    }

    @Test
    void shouldThrowExceptionForInvalidHttpPort() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost")
                        .httpPort(0)
                        .username("iggy")
                        .password("iggy")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("httpPort must be between 1 and 65535");

        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost")
                        .httpPort(65536)
                        .username("iggy")
                        .password("iggy")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("httpPort must be between 1 and 65535");
    }

    @Test
    void shouldThrowExceptionForInvalidTcpPort() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost")
                        .tcpPort(0)
                        .username("iggy")
                        .password("iggy")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tcpPort must be between 1 and 65535");

        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost")
                        .tcpPort(-1)
                        .username("iggy")
                        .password("iggy")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tcpPort must be between 1 and 65535");
    }

    @Test
    void shouldThrowExceptionWhenServerAddressIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .username("iggy")
                        .password("iggy")
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serverAddress must not be null");
    }

    @Test
    void shouldThrowExceptionWhenUsernameIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .password("iggy")
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("username must not be null");
    }

    @Test
    void shouldThrowExceptionWhenPasswordIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .username("iggy")
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("password must not be null");
    }

    @Test
    void shouldThrowExceptionWhenConnectionTimeoutIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .username("iggy")
                        .password("iggy")
                        .connectionTimeout(null)
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("connectionTimeout must not be null");
    }

    @Test
    void shouldThrowExceptionWhenRequestTimeoutIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .username("iggy")
                        .password("iggy")
                        .requestTimeout(null)
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("requestTimeout must not be null");
    }

    @Test
    void shouldThrowExceptionWhenRetryBackoffIsNull() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .username("iggy")
                        .password("iggy")
                        .retryBackoff(null)
                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("retryBackoff must not be null");
    }

    @Test
    void shouldThrowExceptionWhenMaxRetriesIsNegative() {
        assertThatThrownBy(() -> IggyConnectionConfig.builder()
                        .serverAddress("localhost:8080")
                        .username("iggy")
                        .password("iggy")
                        .maxRetries(-1)
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries must be non-negative");
    }

    @Test
    void shouldAcceptZeroMaxRetries() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .username("iggy")
                .password("iggy")
                .maxRetries(0)
                .build();

        assertThat(config.getMaxRetries()).isZero();
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        IggyConnectionConfig config1 = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .transportType(TransportType.TCP)
                .httpPort(3000)
                .tcpPort(8090)
                .username("iggy")
                .password("iggy")
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(20))
                .maxRetries(3)
                .retryBackoff(Duration.ofMillis(100))
                .enableTls(true)
                .build();

        IggyConnectionConfig config2 = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .transportType(TransportType.TCP)
                .httpPort(3000)
                .tcpPort(8090)
                .username("iggy")
                .password("iggy")
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(20))
                .maxRetries(3)
                .retryBackoff(Duration.ofMillis(100))
                .enableTls(true)
                .build();

        IggyConnectionConfig config3 = IggyConnectionConfig.builder()
                .serverAddress("different:8080")
                .username("iggy")
                .password("iggy")
                .build();

        IggyConnectionConfig config4 = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .transportType(TransportType.HTTP) // Different transport type
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config1).isEqualTo(config2);
        assertThat(config1).hasSameHashCodeAs(config2);
        assertThat(config1).isNotEqualTo(config3);
        assertThat(config1).isNotEqualTo(config4);
        assertThat(config1).isEqualTo(config1);
        assertThat(config1).isNotEqualTo(null);
        assertThat(config1).isNotEqualTo(new Object());
    }

    @Test
    void shouldImplementToStringCorrectly() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .transportType(TransportType.TCP)
                .username("iggy")
                .password("iggy")
                .build();

        String toString = config.toString();
        assertThat(toString).contains("IggyConnectionConfig");
        assertThat(toString).contains("localhost:8080");
        assertThat(toString).contains("iggy");
        assertThat(toString).contains("transportType=TCP");
        assertThat(toString).contains("httpPort=");
        assertThat(toString).contains("tcpPort=");
        assertThat(toString).doesNotContain("password=iggy"); // Password should not be in toString
    }

    @Test
    void shouldBeSerializable() {
        IggyConnectionConfig config = IggyConnectionConfig.builder()
                .serverAddress("localhost:8080")
                .username("iggy")
                .password("iggy")
                .build();

        assertThat(config).isInstanceOf(java.io.Serializable.class);
    }
}
