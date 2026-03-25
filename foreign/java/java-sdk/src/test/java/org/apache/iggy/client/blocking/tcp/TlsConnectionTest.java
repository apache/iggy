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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for TCP/TLS connections.
 * Requires an externally started Iggy server with TLS enabled (like other SDKs).
 *
 * Enabled only when IGGY_TCP_TLS_ENABLED is set, indicating a TLS server is available.
 */
@EnabledIfEnvironmentVariable(named = "IGGY_TCP_TLS_ENABLED", matches = ".+")
class TlsConnectionTest extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TlsConnectionTest.class);
    private static final Path CERTS_DIR = findCertsDir();
    // The server certificate SAN is DNS:localhost, so TLS clients must connect
    // via "localhost" to pass hostname verification (not 127.0.0.1).
    private static final String TLS_HOST = "localhost";

    @Test
    void connectWithTlsWithCaCertShouldSucceed() {
        var client = IggyTcpClient.builder()
                .host(TLS_HOST)
                .port(serverTcpPort())
                .enableTls()
                .tlsCertificate(CERTS_DIR.resolve("iggy_ca_cert.pem").toFile())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        var stats = client.system().getStats();
        assertThat(stats).isNotNull();
    }

    @Test
    void connectWithTlsWithServerCertShouldSucceed() {
        var client = IggyTcpClient.builder()
                .host(TLS_HOST)
                .port(serverTcpPort())
                .enableTls()
                .tlsCertificate(CERTS_DIR.resolve("iggy_cert.pem").toFile())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        var stats = client.system().getStats();
        assertThat(stats).isNotNull();
    }

    @Test
    void connectWithoutTlsShouldFailWhenTlsRequired() {
        // The blocking TCP client hangs on responses.take() when the server drops
        // a non-TLS connection, so we run in a separate thread with a timeout.
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> {
                var client = IggyTcpClient.builder()
                        .host(serverHost())
                        .port(serverTcpPort())
                        .credentials("iggy", "iggy")
                        .buildAndLogin();
                client.system().getStats();
            });
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .isInstanceOfAny(ExecutionException.class, TimeoutException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void sendAndReceiveOverTlsShouldWork() {
        var client = IggyTcpClient.builder()
                .host(TLS_HOST)
                .port(serverTcpPort())
                .enableTls()
                .tlsCertificate(CERTS_DIR.resolve("iggy_ca_cert.pem").toFile())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        String streamName = "tls-test-stream";
        StreamId streamId = StreamId.of(streamName);
        String topicName = "tls-test-topic";
        TopicId topicId = TopicId.of(topicName);

        try {
            StreamDetails stream = client.streams().createStream(streamName);
            assertThat(stream).isNotNull();

            client.topics()
                    .createTopic(
                            streamId,
                            1L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            empty(),
                            topicName);

            List<Message> messages =
                    List.of(Message.of("tls-message-1"), Message.of("tls-message-2"), Message.of("tls-message-3"));

            client.messages().sendMessages(streamId, topicId, Partitioning.partitionId(0L), messages);

            PolledMessages polled = client.messages()
                    .pollMessages(
                            streamId,
                            topicId,
                            Optional.of(0L),
                            org.apache.iggy.consumergroup.Consumer.of(0L),
                            PollingStrategy.offset(BigInteger.ZERO),
                            3L,
                            false);

            assertThat(polled.messages()).hasSize(3);
        } finally {
            try {
                client.streams().deleteStream(streamId);
            } catch (RuntimeException e) {
                log.debug("Cleanup failed: {}", e.getMessage());
            }
        }
    }

    private static Path findCertsDir() {
        File dir = new File(System.getProperty("user.dir"));
        while (dir != null) {
            File certs = new File(dir, "core/certs");
            if (certs.isDirectory()
                    && new File(certs, "iggy_cert.pem").exists()
                    && new File(certs, "iggy_key.pem").exists()
                    && new File(certs, "iggy_ca_cert.pem").exists()) {
                return certs.toPath();
            }
            dir = dir.getParentFile();
        }
        throw new IllegalStateException(
                "Could not find core/certs/ directory with TLS certificates. Run tests from the repository root.");
    }
}
