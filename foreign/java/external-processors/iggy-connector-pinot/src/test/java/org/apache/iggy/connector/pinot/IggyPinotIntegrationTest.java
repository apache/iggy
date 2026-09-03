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

package org.apache.iggy.connector.pinot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Ulimit;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class IggyPinotIntegrationTest {

    // The Java SDK speaks VSR, so use the same VSR-capable image as its integration tests.
    private static final DockerImageName IGGY_IMAGE = DockerImageName.parse("apache/iggy:edge");
    private static final DockerImageName PINOT_IMAGE = DockerImageName.parse(
            Objects.requireNonNull(System.getProperty("iggy.pinot.image"), "Missing iggy.pinot.image system property"));
    private static final DockerImageName ZOOKEEPER_IMAGE = DockerImageName.parse("zookeeper:3.9");

    private static final int IGGY_HTTP_PORT = 3000;
    private static final int IGGY_TCP_PORT = 8090;
    private static final int PINOT_CONTROLLER_PORT = 9000;
    private static final int PINOT_BROKER_PORT = 8099;
    private static final int PINOT_SERVER_ADMIN_PORT = 8097;
    private static final String EXTERNAL_SERVER_HOST = "127.0.0.1";
    private static final String TESTCONTAINERS_HOST = "host.testcontainers.internal";
    private static final boolean USE_EXTERNAL_SERVER = System.getenv("USE_EXTERNAL_SERVER") != null;

    private static final String STREAM_NAME = "pinot-test-stream-" + UUID.randomUUID();
    private static final String TOPIC_NAME = "test-events";
    private static final String CONSUMER_GROUP_NAME = "pinot-integration-test";
    private static final String TABLE_NAME = "test_events_REALTIME";

    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration QUERY_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(HTTP_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    private static Network network;
    private static GenericContainer<?> iggy;
    private static GenericContainer<?> zookeeper;
    private static GenericContainer<?> pinotController;
    private static GenericContainer<?> pinotBroker;
    private static GenericContainer<?> pinotServer;
    private static IggyTcpClient iggyClient;

    @BeforeAll
    static void startEnvironment() {
        Path pluginDirectory = requiredDirectory("iggy.pinot.plugin.dir");
        Path deploymentDirectory = requiredDirectory("iggy.pinot.deployment.dir");

        if (USE_EXTERNAL_SERVER) {
            Testcontainers.exposeHostPorts(externalTcpPort());
        }
        network = Network.newNetwork();
        try {
            startZookeeper();
            if (!USE_EXTERNAL_SERVER) {
                startIggy();
            }
            startPinotController(pluginDirectory);
            startPinotBroker();
            startPinotServer(pluginDirectory);

            iggyClient = IggyTcpClient.builder()
                    .host(iggyHost())
                    .port(iggyPort())
                    .credentials("iggy", "iggy")
                    .connectionTimeout(Duration.ofSeconds(10))
                    .requestTimeout(Duration.ofSeconds(10))
                    .buildAndLogin();
            createIggyResources();

            postControllerResource("/schemas", Files.readString(deploymentDirectory.resolve("schema.json")));
            postControllerResource("/tables", tableConfiguration(deploymentDirectory.resolve("table.json")));
            awaitQuery("SELECT COUNT(*) FROM " + TABLE_NAME, IggyPinotIntegrationTest::hasResultRow);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("Failed to start the Iggy-Pinot test environment\n" + diagnostics(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting the Iggy-Pinot test environment", e);
        }
    }

    @AfterAll
    static void stopEnvironment() {
        stop(pinotServer);
        stop(pinotBroker);
        stop(pinotController);

        if (iggyClient != null) {
            try {
                iggyClient.streams().deleteStream(StreamId.of(STREAM_NAME));
            } catch (RuntimeException ignored) {
                // Startup may have failed before the stream was created.
            }
            try {
                iggyClient.close();
            } catch (RuntimeException ignored) {
                // Containers are still stopped below.
            }
        }

        stop(iggy);
        stop(zookeeper);
        if (network != null) {
            network.close();
        }
    }

    @Test
    void shouldIngestAndMapJsonMessage() throws Exception {
        String marker = "mapping-" + UUID.randomUUID();
        long timestamp = Instant.now().toEpochMilli();
        String payload = jsonMessage(marker, "account-updated", "mobile", 750L, timestamp);

        sendMessages(List.of(Message.of(payload)));

        JsonNode result = awaitQuery(
                "SELECT * FROM " + TABLE_NAME + " WHERE userId = '" + marker + "' LIMIT 1",
                IggyPinotIntegrationTest::hasResultRow);

        assertThat(value(result, "userId").asText()).isEqualTo(marker);
        assertThat(value(result, "eventType").asText()).isEqualTo("account-updated");
        assertThat(value(result, "deviceType").asText()).isEqualTo("mobile");
        assertThat(value(result, "duration").asLong()).isEqualTo(750L);
        assertThat(value(result, "timestamp").asLong()).isEqualTo(timestamp);
    }

    @Test
    void shouldIngestMessageBatch() throws Exception {
        String marker = "batch-" + UUID.randomUUID();
        int batchSize = 10;
        List<Message> messages = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            messages.add(Message.of(jsonMessage(
                    marker + "-" + i,
                    marker,
                    i % 2 == 0 ? "desktop" : "mobile",
                    i * 100L,
                    Instant.now().toEpochMilli() + i)));
        }

        sendMessages(messages);

        JsonNode result = awaitQuery(
                "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE eventType = '" + marker + "'",
                response -> firstValue(response).asInt() == batchSize);

        assertThat(firstValue(result).asInt()).isEqualTo(batchSize);
    }

    private static void startZookeeper() {
        zookeeper = new GenericContainer<>(ZOOKEEPER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withExposedPorts(2181)
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        zookeeper.start();
    }

    private static void startIggy() {
        iggy = new GenericContainer<>(IGGY_IMAGE)
                .withImagePullPolicy(PullPolicy.alwaysPull())
                .withNetwork(network)
                .withNetworkAliases("iggy")
                .withExposedPorts(IGGY_HTTP_PORT, IGGY_TCP_PORT)
                .withEnv("IGGY_SYSTEM_LOGGING_LEVEL", "info")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
                .withEnv("IGGY_HTTP_ENABLED", "true")
                .withEnv("IGGY_HTTP_ADDRESS", "0.0.0.0:3000")
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "1")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withCapAdd(Capability.SYS_NICE)
                        .withSecurityOpts(List.of("seccomp:unconfined"))
                        .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                .waitingFor(Wait.forHttp("/")
                        .forPort(IGGY_HTTP_PORT)
                        .forStatusCodeMatching(status -> status >= 200 && status < 500)
                        .withStartupTimeout(STARTUP_TIMEOUT));
        iggy.start();
    }

    private static String iggyHost() {
        return USE_EXTERNAL_SERVER ? EXTERNAL_SERVER_HOST : iggy.getHost();
    }

    private static int iggyPort() {
        return USE_EXTERNAL_SERVER ? externalTcpPort() : iggy.getMappedPort(IGGY_TCP_PORT);
    }

    private static int externalTcpPort() {
        String configured = System.getenv("EXTERNAL_TCP_PORT");
        return configured != null ? Integer.parseInt(configured) : IGGY_TCP_PORT;
    }

    private static void startPinotController(Path pluginDirectory) {
        pinotController = pinotContainer(pluginDirectory)
                .withNetworkAliases("pinot-controller")
                .withExposedPorts(PINOT_CONTROLLER_PORT)
                .withCommand("StartController", "-zkAddress", "zookeeper:2181")
                .withEnv("JAVA_OPTS", "-Xms512M -Xmx1G -XX:+UseG1GC -Dplugins.include=iggy-connector")
                .waitingFor(
                        Wait.forHttp("/health").forPort(PINOT_CONTROLLER_PORT).withStartupTimeout(STARTUP_TIMEOUT));
        pinotController.start();
    }

    private static void startPinotBroker() {
        pinotBroker = new GenericContainer<>(PINOT_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("pinot-broker")
                .withExposedPorts(PINOT_BROKER_PORT)
                .withCommand("StartBroker", "-zkAddress", "zookeeper:2181")
                .withEnv("JAVA_OPTS", "-Xms512M -Xmx1G -XX:+UseG1GC")
                .waitingFor(Wait.forHttp("/health").forPort(PINOT_BROKER_PORT).withStartupTimeout(STARTUP_TIMEOUT));
        pinotBroker.start();
    }

    private static void startPinotServer(Path pluginDirectory) {
        pinotServer = pinotContainer(pluginDirectory)
                .withNetworkAliases("pinot-server")
                .withExposedPorts(PINOT_SERVER_ADMIN_PORT)
                .withCommand("StartServer", "-zkAddress", "zookeeper:2181")
                .withEnv("JAVA_OPTS", "-Xms512M -Xmx1G -XX:+UseG1GC -Dplugins.include=iggy-connector")
                .waitingFor(
                        Wait.forHttp("/health").forPort(PINOT_SERVER_ADMIN_PORT).withStartupTimeout(STARTUP_TIMEOUT));
        pinotServer.start();
    }

    private static GenericContainer<?> pinotContainer(Path pluginDirectory) {
        return new GenericContainer<>(PINOT_IMAGE)
                .withNetwork(network)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(pluginDirectory),
                        "/opt/pinot/plugins/pinot-stream-ingestion/iggy-connector");
    }

    private static void createIggyResources() {
        iggyClient.streams().createStream(STREAM_NAME);
        StreamId streamId = StreamId.of(STREAM_NAME);
        iggyClient
                .topics()
                .createTopic(streamId, 2L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, TOPIC_NAME);
        iggyClient.consumerGroups().createConsumerGroup(streamId, TopicId.of(TOPIC_NAME), CONSUMER_GROUP_NAME);
    }

    private static void sendMessages(List<Message> messages) {
        iggyClient
                .messages()
                .sendMessages(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME), Partitioning.partitionId(0L), messages);
    }

    private static String jsonMessage(String userId, String eventType, String deviceType, long duration, long timestamp)
            throws IOException {
        return OBJECT_MAPPER.writeValueAsString(OBJECT_MAPPER
                .createObjectNode()
                .put("userId", userId)
                .put("eventType", eventType)
                .put("deviceType", deviceType)
                .put("duration", duration)
                .put("timestamp", timestamp));
    }

    private static String tableConfiguration(Path tableConfigurationPath) throws IOException {
        JsonNode tableConfiguration = OBJECT_MAPPER.readTree(Files.readString(tableConfigurationPath));
        ObjectNode streamConfigs =
                (ObjectNode) tableConfiguration.required("tableIndexConfig").required("streamConfigs");
        streamConfigs.put("stream.iggy.stream.id", STREAM_NAME);
        if (USE_EXTERNAL_SERVER) {
            streamConfigs.put("stream.iggy.host", TESTCONTAINERS_HOST);
            streamConfigs.put("stream.iggy.port", Integer.toString(externalTcpPort()));
        }
        return OBJECT_MAPPER.writeValueAsString(tableConfiguration);
    }

    private static void postControllerResource(String path, String body) throws IOException, InterruptedException {
        HttpResponse<String> response = post(pinotController, PINOT_CONTROLLER_PORT, path, body);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Pinot controller request to %s failed with status %d: %s"
                    .formatted(path, response.statusCode(), response.body()));
        }
    }

    private static JsonNode awaitQuery(String sql, Predicate<JsonNode> success) {
        long deadline = System.nanoTime() + QUERY_TIMEOUT.toNanos();
        String lastResponse = "No response received";

        while (System.nanoTime() < deadline) {
            try {
                HttpResponse<String> response = post(
                        pinotBroker,
                        PINOT_BROKER_PORT,
                        "/query/sql",
                        OBJECT_MAPPER.createObjectNode().put("sql", sql).toString());
                lastResponse = "HTTP " + response.statusCode() + ": " + response.body();
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    JsonNode json = OBJECT_MAPPER.readTree(response.body());
                    if (hasExceptionCode(json, QueryErrorCode.SQL_PARSING.getId())) {
                        return fail("Pinot rejected SQL query: %s%nResponse: %s".formatted(sql, response.body()));
                    }
                    if (json.path("exceptions").isEmpty() && success.test(json)) {
                        return json;
                    }
                }
            } catch (IOException e) {
                lastResponse = e.toString();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Pinot query", e);
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Pinot query", e);
            }
        }

        return fail("Pinot query did not reach the expected result within %s.%nSQL: %s%nLast response: %s%n%s"
                .formatted(QUERY_TIMEOUT, sql, lastResponse, diagnostics()));
    }

    private static HttpResponse<String> post(GenericContainer<?> container, int port, String path, String body)
            throws IOException, InterruptedException {
        URI uri = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(port) + path);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(HTTP_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static boolean hasResultRow(JsonNode response) {
        return response.path("resultTable").path("rows").size() > 0;
    }

    private static JsonNode firstValue(JsonNode response) {
        return response.path("resultTable").path("rows").path(0).path(0);
    }

    private static JsonNode value(JsonNode response, String columnName) {
        JsonNode columnNames = response.path("resultTable").path("dataSchema").path("columnNames");
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnName.equals(columnNames.get(i).asText())) {
                return response.path("resultTable").path("rows").path(0).path(i);
            }
        }
        return fail("Pinot result did not contain column '%s': %s".formatted(columnName, response));
    }

    private static boolean hasExceptionCode(JsonNode response, int errorCode) {
        for (JsonNode exception : response.path("exceptions")) {
            if (exception.path("errorCode").asInt() == errorCode) {
                return true;
            }
        }
        return false;
    }

    private static Path requiredDirectory(String property) {
        String value = System.getProperty(property);
        if (value == null) {
            throw new IllegalStateException("Missing required system property: " + property);
        }
        Path directory = Path.of(value);
        if (!Files.isDirectory(directory)) {
            throw new IllegalStateException("Required directory does not exist: " + directory);
        }
        return directory;
    }

    private static void stop(GenericContainer<?> container) {
        if (container != null) {
            container.stop();
        }
    }

    private static String diagnostics() {
        return String.join(
                "\n",
                iggyDiagnostics(),
                logs("Pinot controller", pinotController),
                logs("Pinot broker", pinotBroker),
                logs("Pinot server", pinotServer));
    }

    private static String iggyDiagnostics() {
        if (USE_EXTERNAL_SERVER) {
            return "Iggy uses external server at " + iggyHost() + ":" + iggyPort() + ".";
        }
        return logs("Iggy", iggy);
    }

    private static String logs(String name, GenericContainer<?> container) {
        if (container == null || !container.isCreated()) {
            return name + " was not created.";
        }
        String logs = container.getLogs();
        int start = Math.max(0, logs.length() - 4_000);
        return "=== " + name + " logs ===\n" + logs.substring(start);
    }
}
