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

package org.apache.iggy.examples;

import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.examples.multitenant.consumer.MultiTenantConsumer;
import org.apache.iggy.examples.multitenant.producer.MultiTenantProducer;
import org.apache.iggy.exception.IggyAuthorizationException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiTenantAccessTest {
    @ParameterizedTest(name = "{0}, foreign resource {1}")
    @MethodSource("accessResults")
    void shouldDistinguishDeniedAccessFromFailedChecks(Class<?> example, LookupResult result) throws Exception {
        try (var client = new IggyTcpClient("localhost", 8090) {
            @Override
            public StreamsClient streams() {
                return lookupService(StreamsClient.class, result);
            }

            @Override
            public TopicsClient topics() {
                return lookupService(TopicsClient.class, result);
            }
        }) {
            String method = example == MultiTenantProducer.class ? "ensureStreamAccess" : "ensureStreamTopicsAccess";
            Method check = example.getDeclaredMethod(method, IggyTcpClient.class, String.class, List.class);
            check.setAccessible(true);
            switch (result) {
                case VISIBLE ->
                    assertThatThrownBy(() -> check.invoke(null, client, "own", List.of("foreign")))
                            .hasCauseInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage(
                                    example == MultiTenantProducer.class
                                            ? "Access to stream: foreign should not be allowed"
                                            : "Access to topic: events in stream: foreign should not be allowed");
                case DISCONNECTED ->
                    assertThatThrownBy(() -> check.invoke(null, client, "own", List.of("foreign")))
                            .hasCauseInstanceOf(IggyConnectionException.class);
                case DENIED, MISSING ->
                    assertThatCode(() -> check.invoke(null, client, "own", List.of("foreign")))
                            .doesNotThrowAnyException();
            }
        }
    }

    private static Stream<Arguments> accessResults() {
        return Stream.of(MultiTenantProducer.class, MultiTenantConsumer.class)
                .flatMap(example -> Stream.of(LookupResult.values()).map(result -> Arguments.of(example, result)));
    }

    private static <T> T lookupService(Class<T> service, LookupResult foreignResult) {
        return service.cast(
                Proxy.newProxyInstance(service.getClassLoader(), new Class<?>[] {service}, (proxy, method, args) -> {
                    if (!method.getName().equals("getStream")
                            && !method.getName().equals("getTopic")) {
                        throw new AssertionError("Unexpected operation: " + method.getName());
                    }
                    String stream = ((StreamId) args[0]).getName();
                    if (stream.equals("foreign")) {
                        switch (foreignResult) {
                            case DENIED ->
                                throw new IggyAuthorizationException(
                                        IggyErrorCode.UNAUTHORIZED,
                                        IggyErrorCode.UNAUTHORIZED.getCode(),
                                        "Access denied",
                                        Optional.empty(),
                                        Optional.empty());
                            case DISCONNECTED ->
                                throw new IggyConnectionException("Connection lost during access check");
                            case MISSING -> {
                                return Optional.empty();
                            }
                            case VISIBLE -> {}
                        }
                    }
                    if (service == StreamsClient.class) {
                        return Optional.of(
                                new StreamDetails(0L, BigInteger.ZERO, stream, "0 B", BigInteger.ZERO, 0L, List.of()));
                    }
                    return Optional.of(new TopicDetails(
                            0L,
                            BigInteger.ZERO,
                            "events",
                            "0 B",
                            BigInteger.ZERO,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            0L,
                            List.of(),
                            Map.of(),
                            Map.of()));
                }));
    }

    private enum LookupResult {
        VISIBLE,
        DENIED,
        MISSING,
        DISCONNECTED
    }
}
