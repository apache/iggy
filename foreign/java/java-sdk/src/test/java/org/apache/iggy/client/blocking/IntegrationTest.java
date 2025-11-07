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

package org.apache.iggy.client.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.apache.iggy.topic.CompressionAlgorithm;
import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import static java.util.Optional.empty;
import static java.util.Optional.of;

@Testcontainers
public abstract class IntegrationTest {

    public static final int HTTP_PORT = 3000;
    public static final int TCP_PORT = 8090;

    private static final boolean USE_EXTERNAL_SERVER = System.getenv("USE_EXTERNAL_SERVER") != null;

    protected static GenericContainer<?> iggyServer;

    // Track created resources for cleanup
    protected List<Long> createdStreamIds = new ArrayList<>();
    protected List<Long> createdUserIds = new ArrayList<>();

    @BeforeAll
    static void setupContainer() {
        if (!USE_EXTERNAL_SERVER) {
            iggyServer = new GenericContainer<>(DockerImageName.parse("apache/iggy:edge"))
                .withExposedPorts(HTTP_PORT, TCP_PORT)
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
                .withEnv("IGGY_HTTP_ADDRESS", "0.0.0.0:3000")
                .withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
            iggyServer.start();
        }
    }

    @AfterAll
    static void stopContainer() {
        if (iggyServer != null && iggyServer.isRunning()) {
            // Print last logs before stopping
            System.out.println("=== Iggy Server Container Logs ===");
            System.out.println(iggyServer.getLogs());
            System.out.println("=================================");
            iggyServer.stop();
        }
    }

    protected IggyBaseClient client;

    @BeforeEach
    void beforeEachIntegrationTest() {
        client = getClient();
        // Clear tracking lists for new test
        createdStreamIds.clear();
        createdUserIds.clear();
    }

    @AfterEach
    void cleanupTestResources() {
        if (client == null) {
            return;
        }

        // Login as root to ensure we have permissions for cleanup
        try {
            login();
        } catch (Exception e) {
            // Already logged in or login failed - continue with cleanup anyway
        }

        // Delete all created streams (which also deletes topics within them)
        for (Long streamId : createdStreamIds) {
            try {
                client.streams().deleteStream(streamId);
            } catch (Exception e) {
                // Stream might already be deleted or doesn't exist - ignore
            }
        }

        // Delete all created non-root users
        for (Long userId : createdUserIds) {
            try {
                if (userId != 1) { // Don't try to delete root user
                    client.users().deleteUser(userId);
                }
            } catch (Exception e) {
                // User might already be deleted or doesn't exist - ignore
            }
        }
    }

    abstract protected IggyBaseClient getClient();

    protected void setUpStream() {
        client.streams().createStream(of(42L), "test-stream");
        createdStreamIds.add(42L);
    }

    protected void setUpStreamAndTopic() {
        setUpStream();
        client.topics()
                .createTopic(42L,
                        of(42L),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        "test-topic");
    }

    protected void login() {
        client.users().login("iggy", "iggy");
    }

    // Helper method for tests that need to track streams created directly
    protected void trackStream(Long streamId) {
        if (!createdStreamIds.contains(streamId)) {
            createdStreamIds.add(streamId);
        }
    }

    // Helper method for tests that need to track users created
    protected void trackUser(Long userId) {
        if (!createdUserIds.contains(userId)) {
            createdUserIds.add(userId);
        }
    }

}
