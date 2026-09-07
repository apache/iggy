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

package org.apache.iggy.client;

import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Ulimit;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

/**
 * Base for integration tests. By default the server under test is the
 * {@code apache/iggy:edge} image started via testcontainers. The tag tracks
 * master, so refresh the local copy with {@code docker pull apache/iggy:edge}
 * whenever the SDK's wire protocol moves ahead of it.
 *
 * <p>With {@code USE_EXTERNAL_SERVER} set, tests instead target an externally
 * started server on localhost, running standalone (single-node) mode. Start
 * it from the repo root:
 * <pre>{@code
 * IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy cargo run --bin iggy-server
 * }</pre>
 */
@Testcontainers
public abstract class BaseIntegrationTest {

    protected static GenericContainer<?> iggyServer;
    private static final DockerImageName IGGY_IMAGE = DockerImageName.parse("apache/iggy:edge");
    private static final String LOCALHOST_IP = "127.0.0.1";
    private static final int HTTP_PORT = 3000;
    private static final int TCP_PORT = 8090;
    private static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);
    private static final boolean USE_EXTERNAL_SERVER = System.getenv("USE_EXTERNAL_SERVER") != null;

    public static int serverTcpPort() {
        return USE_EXTERNAL_SERVER ? externalPort("EXTERNAL_TCP_PORT", TCP_PORT) : iggyServer.getMappedPort(TCP_PORT);
    }

    public static int serverHttpPort() {
        return USE_EXTERNAL_SERVER
                ? externalPort("EXTERNAL_HTTP_PORT", HTTP_PORT)
                : iggyServer.getMappedPort(HTTP_PORT);
    }

    private static int externalPort(String variable, int defaultPort) {
        var configured = System.getenv(variable);
        return configured != null ? Integer.parseInt(configured) : defaultPort;
    }

    public static String serverHost() {
        return USE_EXTERNAL_SERVER ? LOCALHOST_IP : iggyServer.getHost();
    }

    @BeforeAll
    static void setupContainer() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        if (!USE_EXTERNAL_SERVER) {
            log.info("Starting Iggy Server Container...");
            iggyServer = new GenericContainer<>(IGGY_IMAGE)
                    .withExposedPorts(HTTP_PORT, TCP_PORT)
                    .withEnv("IGGY_ROOT_USERNAME", "iggy")
                    .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                    .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:" + TCP_PORT)
                    .withEnv("IGGY_HTTP_ADDRESS", "0.0.0.0:" + HTTP_PORT)
                    .withEnv("IGGY_NODE_ADVERTISED_ADDRESS", LOCALHOST_IP)
                    .withEnv("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "all")
                    .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                            .withCapAdd(Capability.SYS_NICE)
                            .withSecurityOpts(List.of("seccomp:unconfined"))
                            .withUlimits(List.of(new Ulimit("memlock", -1L, -1L))))
                    .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT))
                    .withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
            iggyServer.start();
        } else {
            log.info("Using external Iggy Server");
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
}
