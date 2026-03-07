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

import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Ulimit;
import io.netty.util.ResourceLeakDetector;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
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
 * Starts an Iggy server container with TLS enabled and custom certificates.
 *
 * Skipped when USE_EXTERNAL_SERVER is set (CI) because the external server
 * does not have TLS enabled. Run locally with Docker to exercise these tests.
 */
@Testcontainers
@DisabledIfEnvironmentVariable(named = "USE_EXTERNAL_SERVER", matches = ".+")
class TlsConnectionTest {

    private static final int TCP_PORT = 8090;
    private static final Logger log = LoggerFactory.getLogger(TlsConnectionTest.class);
    private static GenericContainer<?> iggyServer;

    private static Path certsDir;

    @BeforeAll
    static void setupContainer() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        certsDir = findCertsDir();

        log.info("Starting Iggy Server Container with TLS enabled...");
        iggyServer = new GenericContainer<>(DockerImageName.parse("apache/iggy:edge"))
                .withExposedPorts(TCP_PORT)
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
                .withEnv("IGGY_TCP_TLS_ENABLED", "true")
                .withEnv("IGGY_TCP_TLS_CERT_FILE", "/app/certs/iggy_cert.pem")
                .withEnv("IGGY_TCP_TLS_KEY_FILE", "/app/certs/iggy_key.pem")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(certsDir.resolve("iggy_cert.pem")), "/app/certs/iggy_cert.pem")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(certsDir.resolve("iggy_key.pem")), "/app/certs/iggy_key.pem")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(certsDir.resolve("iggy_ca_cert.pem")), "/app/certs/iggy_ca_cert.pem")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withCapAdd(Capability.SYS_NICE)
                        .withSecurityOpts(List.of("seccomp:unconfined"))
                        .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                .waitingFor(Wait.forLogMessage(".*Iggy TCP TLS server has started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))
                .withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
        iggyServer.start();
    }

    @AfterAll
    static void stopContainer() {
        if (iggyServer != null && iggyServer.isRunning()) {
            System.out.println("=== Iggy TLS Server Container Logs ===");
            System.out.println(iggyServer.getLogs());
            System.out.println("=======================================");
            iggyServer.stop();
        }
    }

    @Test
    void connectWithTlsWithCaCertShouldSucceed() {
        var client = IggyTcpClient.builder()
                .host(iggyServer.getHost())
                .port(iggyServer.getMappedPort(TCP_PORT))
                .enableTls()
                .tlsCertificate(certsDir.resolve("iggy_ca_cert.pem").toFile())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        var stats = client.system().getStats();
        assertThat(stats).isNotNull();
    }

    @Test
    void connectWithTlsWithServerCertShouldSucceed() {
        var client = IggyTcpClient.builder()
                .host(iggyServer.getHost())
                .port(iggyServer.getMappedPort(TCP_PORT))
                .enableTls()
                .tlsCertificate(certsDir.resolve("iggy_cert.pem").toFile())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        var stats = client.system().getStats();
        assertThat(stats).isNotNull();
    }

    @Test
    void connectWithoutTlsShouldFailWhenTlsRequired() {
        // The blocking TCP client hangs on responses.take() when the server drops
        // a non-TLS connection, so we run in a separate thread with a timeout.
        // The connection should either fail with an exception or hang (both mean failure).
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> {
                var client = IggyTcpClient.builder()
                        .host(iggyServer.getHost())
                        .port(iggyServer.getMappedPort(TCP_PORT))
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
                .host(iggyServer.getHost())
                .port(iggyServer.getMappedPort(TCP_PORT))
                .enableTls()
                .tlsCertificate(certsDir.resolve("iggy_ca_cert.pem").toFile())
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
        // Walk up from the current working directory to find core/certs/
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
