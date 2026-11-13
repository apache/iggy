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

package org.apache.iggy.client.blocking.http;

import com.sun.net.httpserver.HttpServer;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PersonalAccessTokensHttpClientPathTest {

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "my-token | /personal-access-tokens/my-token",
                "../users/42 | /personal-access-tokens/..%2Fusers%2F42",
                "a/b | /personal-access-tokens/a%2Fb",
                "a?b=c | /personal-access-tokens/a%3Fb%3Dc",
                "a#b | /personal-access-tokens/a%23b",
                "my token | /personal-access-tokens/my%20token",
                "a+b | /personal-access-tokens/a%2Bb",
                "a%2Fb | /personal-access-tokens/a%252Fb",
            })
    void shouldSendTheNameAsOneEscapedPathSegment(String name, String expectedRawPath) throws IOException {
        try (var server = new RecordingServer();
                var client = new IggyHttpClient(server.url())) {
            client.personalAccessTokens().deletePersonalAccessToken(name);

            assertThat(server.rawPaths).containsExactly("DELETE " + expectedRawPath);
        }
    }

    @ParameterizedTest
    @CsvSource({".", ".."})
    void shouldRejectDotSegmentNamesWithoutSendingARequest(String name) throws IOException {
        try (var server = new RecordingServer();
                var client = new IggyHttpClient(server.url())) {
            assertThatThrownBy(() -> client.personalAccessTokens().deletePersonalAccessToken(name))
                    .isInstanceOf(IggyInvalidArgumentException.class);

            assertThat(server.rawPaths).isEmpty();
        }
    }

    private static final class RecordingServer implements AutoCloseable {
        private final HttpServer server;
        private final List<String> rawPaths = new CopyOnWriteArrayList<>();

        RecordingServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", exchange -> {
                rawPaths.add(exchange.getRequestMethod() + " "
                        + exchange.getRequestURI().getRawPath());
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            });
            server.start();
        }

        String url() {
            return "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":"
                    + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
