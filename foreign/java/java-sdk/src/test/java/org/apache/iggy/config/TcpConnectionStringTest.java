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

package org.apache.iggy.config;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpConnectionStringTest {

    @Test
    void shouldParseDefaultScheme() {
        TcpConnectionString parsed = TcpConnectionString.parse("iggy://iggy:secret@localhost:8090");

        assertParsed(parsed, "localhost", 8090, "iggy", "secret");
    }

    @Test
    void shouldParseTcpScheme() {
        TcpConnectionString parsed = TcpConnectionString.parse("iggy+tcp://user:pass@10.0.0.5:3000");

        assertParsed(parsed, "10.0.0.5", 3000, "user", "pass");
    }

    @Test
    void shouldParseTlsOption() {
        assertThat(TcpConnectionString.parse("iggy://iggy:iggy@localhost:8090?tls=true")
                        .tls())
                .contains(true);
        assertThat(TcpConnectionString.parse("iggy://iggy:iggy@localhost:8090?tls=false")
                        .tls())
                .contains(false);
    }

    @Test
    void shouldParseTlsCaFileOption() {
        TcpConnectionString parsed =
                TcpConnectionString.parse("iggy://iggy:iggy@localhost:8090?tls=true&tls_ca_file=/certs/ca.pem");

        assertThat(parsed.tls()).contains(true);
        assertThat(parsed.tlsCaFile()).contains("/certs/ca.pem");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tls_ca_file=/certs/ca.pem", "tls=false&tls_ca_file=/certs/ca.pem"})
    void shouldRejectTlsCaFileWithoutTls(String options) {
        assertInvalid("iggy://iggy:iggy@localhost:8090?" + options, "option 'tls_ca_file' requires tls=true");
    }

    @Test
    void shouldParseBracketedIpv6Host() {
        TcpConnectionString parsed = TcpConnectionString.parse("iggy://iggy:iggy@[::1]:8090");

        assertThat(parsed.host()).isEqualTo("::1");
        assertThat(parsed.port()).isEqualTo(8090);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldRejectMissingConnectionString(String connectionString) {
        assertInvalid(connectionString, "must not be empty");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "localhost:8090",
                "http://iggy:iggy@localhost:8090",
                "iggy://iggy:iggy@localhost:8090://",
            })
    void shouldRejectInvalidScheme(String connectionString) {
        assertInvalid(connectionString, "Invalid connection string");
    }

    @ParameterizedTest
    @ValueSource(strings = {"quic", "http", "ws"})
    void shouldRejectOtherTransports(String transport) {
        assertInvalid("iggy+" + transport + "://iggy:iggy@localhost:8090", "Unsupported transport '" + transport + "'");
    }

    @Test
    void shouldNotEchoMalformedTransport() {
        assertThatThrownBy(() -> TcpConnectionString.parse("iggy+user:s3cret@localhost:8090?x=a://b"))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("Invalid connection string")
                .message()
                .doesNotContain("s3cret");
    }

    @Test
    void shouldRejectPersonalAccessToken() {
        assertInvalid("iggy://some-token@localhost:8090", "personal access tokens are not supported");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "iggy://:iggy@localhost:8090",
                "iggy://iggy:@localhost:8090",
                "iggy://iggy:ig:gy@localhost:8090",
                "iggy://iggy:iggy@extra@localhost:8090",
                "iggy://localhost:8090",
            })
    void shouldRejectInvalidCredentials(String connectionString) {
        assertInvalid(connectionString, "Invalid connection string");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "iggy://iggy:iggy@localhost",
                "iggy://iggy:iggy@localhost:",
                "iggy://iggy:iggy@:8090",
                "iggy://iggy:iggy@localhost:port",
                "iggy://iggy:iggy@localhost:65536",
                "iggy://iggy:iggy@localhost:123456",
                "iggy://iggy:iggy@2001:db8::1:8090",
                "iggy://iggy:iggy@localhost:8090:9090",
                "iggy://iggy:iggy@localhost:8090?tls=true?tls=false",
                "iggy://iggy:iggy@local host:8090",
                "iggy://iggy:iggy@local\thost:8090",
                "iggy://iggy:iggy@[::1 ]:8090",
            })
    void shouldRejectInvalidServerAddress(String connectionString) {
        assertInvalid(connectionString, "Invalid connection string");
    }

    @ParameterizedTest
    @ValueSource(strings = {"nodelay=true", "tls_domain=localhost", "reconnection_retries=5"})
    void shouldRejectUnsupportedOptions(String option) {
        String name = option.substring(0, option.indexOf('='));

        assertInvalid("iggy://iggy:iggy@localhost:8090?" + option, "Unsupported option '" + name + "'");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "tls",
                "tls=",
                "tls=yes",
                "tls=true&",
                "tls=true=false",
                "tls=true&tls_ca_file=",
                "tls=true&tls_ca_file= "
            })
    void shouldRejectInvalidOptionValue(String options) {
        assertInvalid("iggy://iggy:iggy@localhost:8090?" + options, "Invalid connection string");
    }

    @Test
    void shouldRejectEmptyOptions() {
        assertInvalid("iggy://iggy:iggy@localhost:8090?", "Invalid connection string");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tls=true&tls=false", "tls_ca_file=a.pem&tls_ca_file=b.pem"})
    void shouldRejectRepeatedOptions(String options) {
        assertInvalid("iggy://iggy:iggy@localhost:8090?" + options, "must not be set more than once");
    }

    @Test
    void shouldNotExposePassword() {
        assertThatThrownBy(() -> TcpConnectionString.parse("iggy://iggy:s3cret@localhost"))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .message()
                .doesNotContain("s3cret");
        assertThat(TcpConnectionString.parse("iggy://iggy:s3cret@localhost:8090")
                        .toString())
                .doesNotContain("s3cret");
    }

    private static void assertParsed(
            TcpConnectionString parsed, String host, int port, String username, String password) {
        assertThat(parsed.host()).isEqualTo(host);
        assertThat(parsed.port()).isEqualTo(port);
        assertThat(parsed.username()).isEqualTo(username);
        assertThat(parsed.password()).isEqualTo(password);
        assertThat(parsed.tls()).isEmpty();
        assertThat(parsed.tlsCaFile()).isEmpty();
    }

    private static void assertInvalid(String connectionString, String expectedMessage) {
        assertThatThrownBy(() -> TcpConnectionString.parse(connectionString))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining(expectedMessage);
    }
}
