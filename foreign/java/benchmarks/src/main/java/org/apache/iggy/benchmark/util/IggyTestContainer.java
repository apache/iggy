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

package org.apache.iggy.benchmark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public final class IggyTestContainer {

    public static final int HTTP_PORT = 3000;
    public static final int TCP_PORT = 8090;

    private static final Logger log = LoggerFactory.getLogger(IggyTestContainer.class);

    private final GenericContainer<?> container;

    private IggyTestContainer(GenericContainer<?> container) {
        this.container = container;
    }

    public static IggyTestContainer start() {
        log.info("Starting Iggy Server Container...");

        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("apache/iggy:edge"))
                .withExposedPorts(HTTP_PORT, TCP_PORT)
                .withEnv("IGGY_ROOT_USERNAME", "iggy")
                .withEnv("IGGY_ROOT_PASSWORD", "iggy")
                .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:" + TCP_PORT)
                .withEnv("IGGY_HTTP_ADDRESS", "0.0.0.0:" + HTTP_PORT)
                .withCreateContainerCmdModifier(
                        cmd -> cmd.getHostConfig().withSecurityOpts(List.of("seccomp=unconfined")));

        if (log.isDebugEnabled()) {
            // Show container logs
            container = container.withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
        }

        try {
            container.start();
        } catch (Exception e) {
            log.error("Failed to start Iggy container. Container logs:");
            log.error(container.getLogs());
            throw e;
        }

        IggyTestContainer wrapper = new IggyTestContainer(container);
        log.info("Iggy Server started - HTTP port: {}, TCP port: {}", wrapper.getHttpPort(), wrapper.getTcpPort());

        return wrapper;
    }

    public int getHttpPort() {
        return container.getMappedPort(HTTP_PORT);
    }

    public int getTcpPort() {
        return container.getMappedPort(TCP_PORT);
    }

    public void stop() {
        if (container != null && container.isRunning()) {
            log.info("Stopping Iggy Server Container...");
            try {
                container.stop();
            } catch (Exception e) {
                log.error("Failed to stop Iggy container. Container logs:");
                log.error(container.getLogs());
                throw e;
            }
        }
    }

    public boolean isRunning() {
        return container != null && container.isRunning();
    }
}
