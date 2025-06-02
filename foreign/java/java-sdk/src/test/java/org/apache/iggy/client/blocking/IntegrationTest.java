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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.apache.iggy.topic.CompressionAlgorithm;
import java.math.BigInteger;
import static java.util.Optional.empty;
import static java.util.Optional.of;

@Testcontainers
public abstract class IntegrationTest {

    public static final int HTTP_PORT = 3000;
    public static final int TCP_PORT = 8090;

    @Container
    protected final GenericContainer<?> iggyServer = new GenericContainer<>(DockerImageName.parse("apache/iggy:edge"))
            .withExposedPorts(HTTP_PORT, TCP_PORT);

    protected IggyBaseClient client;

    @BeforeEach
    void beforeEachIntegrationTest() {
        client = getClient();
    }

    abstract protected IggyBaseClient getClient();

    protected void setUpStream() {
        client.streams().createStream(of(42L), "test-stream");
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

}
