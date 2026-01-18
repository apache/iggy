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

package org.apache.iggy.connector.flink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IggyClientFactoryTest {

    @Test
    void shouldExtractHostFromPlainHostname() {
        assertThat(IggyClientFactory.extractHost("localhost")).isEqualTo("localhost");
        assertThat(IggyClientFactory.extractHost("iggy-server")).isEqualTo("iggy-server");
        assertThat(IggyClientFactory.extractHost("192.168.1.100")).isEqualTo("192.168.1.100");
    }

    @Test
    void shouldExtractHostFromHostWithPort() {
        assertThat(IggyClientFactory.extractHost("localhost:8090")).isEqualTo("localhost");
        assertThat(IggyClientFactory.extractHost("iggy-server:3000")).isEqualTo("iggy-server");
        assertThat(IggyClientFactory.extractHost("192.168.1.100:9090")).isEqualTo("192.168.1.100");
    }

    @ParameterizedTest
    @CsvSource({
            "http://localhost:3000, localhost",
            "https://iggy-server:3000, iggy-server",
            "tcp://192.168.1.100:8090, 192.168.1.100",
            "http://example.com, example.com",
            "https://secure.iggy.io:443, secure.iggy.io"
    })
    void shouldExtractHostFromFullUrl(String url, String expectedHost) {
        assertThat(IggyClientFactory.extractHost(url)).isEqualTo(expectedHost);
    }

    @Test
    void shouldTrimWhitespaceFromServerAddress() {
        assertThat(IggyClientFactory.extractHost("  localhost  ")).isEqualTo("localhost");
        assertThat(IggyClientFactory.extractHost("  localhost:8090  ")).isEqualTo("localhost");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t", "\n"})
    void shouldThrowExceptionForNullOrEmptyServerAddress(String address) {
        assertThatThrownBy(() -> IggyClientFactory.extractHost(address))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("serverAddress cannot be null or blank");
    }

    @Test
    void shouldThrowExceptionForInvalidUri() {
        assertThatThrownBy(() -> IggyClientFactory.extractHost("http://"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("http://");
    }
}
