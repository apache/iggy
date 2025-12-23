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

package org.apache.iggy.examples.sinkdataproducer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public final class SinkDataProducer {

    private static final String[] SOURCES = {"browser", "mobile", "desktop", "email", "network", "other"};
    private static final String[] STATES = {"active", "inactive", "blocked", "deleted", "unknown"};
    private static final String[] DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"};

    private static final int MAX_BATCHES = 100;
    private static final Logger log = LoggerFactory.getLogger(SinkDataProducer.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private SinkDataProducer() {}

    public static void main(String[] args) {
        String address = "localhost:8090";
        String username = "iggy";
        String password = "iggy";
        String stream = "qw";
        String topic = "records";

        HostAndPort hostAndPort = parseAddress(address);
        var client = IggyTcpClient.builder()
                .host(hostAndPort.host())
                .port(hostAndPort.port())
                .credentials(username, password)
                .build();

        StreamId streamId = StreamId.of(stream);
        TopicId topicId = TopicId.of(topic);
        Partitioning partitioning = Partitioning.balanced();

        Random random = new Random();
        int batchesCount = 0;
        log.info("Starting data producer...");

        createStreamIfMissing(client, streamId);
        createTopicIfMissing(client, streamId, topicId);

        while (batchesCount < MAX_BATCHES) {
            int recordsCount = random.nextInt(100) + 1000;
            List<Message> messages = new ArrayList<>(recordsCount);

            for (int i = 0; i < recordsCount; i++) {
                UserRecord record = randomRecord(random);
                try {
                    messages.add(Message.of(record.toJson(MAPPER)));
                } catch (JacksonException e) {
                    log.warn("Failed to serialize record, skipping.", e);
                }
            }

            client.messages().sendMessages(streamId, topicId, partitioning, messages);
            log.info("Sent {} messages", recordsCount);
            batchesCount++;
        }

        log.info("Reached maximum batches count");
    }

    private static void createStreamIfMissing(IggyTcpClient client, StreamId streamId) {
        client.streams()
                .getStream(streamId)
                .ifPresentOrElse(existing -> log.info("Stream {} already exists.", streamId.getName()), () -> {
                    client.streams().createStream(streamId.getName());
                    log.info("Created stream {}.", streamId.getName());
                });
    }

    private static void createTopicIfMissing(IggyTcpClient client, StreamId streamId, TopicId topicId) {
        client.topics()
                .getTopic(streamId, topicId)
                .ifPresentOrElse(existing -> log.info("Topic {} already exists.", topicId.getName()), () -> {
                    client.topics()
                            .createTopic(
                                    streamId,
                                    1L,
                                    CompressionAlgorithm.None,
                                    java.math.BigInteger.ZERO,
                                    java.math.BigInteger.ZERO,
                                    java.util.Optional.empty(),
                                    topicId.getName());
                    log.info("Created topic {}.", topicId.getName());
                });
    }

    private static UserRecord randomRecord(Random random) {
        String source = SOURCES[random.nextInt(SOURCES.length)];
        String state = STATES[random.nextInt(STATES.length)];
        String email = randomEmail(random);
        Instant createdAt = Instant.now().minus(random.nextLong(0, 1000), ChronoUnit.DAYS);

        String userId = "user_" + (random.nextInt(99) + 1);
        int userType = random.nextInt(4) + 1;
        String message = randomString(random, random.nextInt(91) + 10);

        return new UserRecord(userId, userType, email, source, state, createdAt, message);
    }

    private static String randomEmail(Random random) {
        int nameLength = random.nextInt(17) + 3;
        String name = randomString(random, nameLength);
        String domain = DOMAINS[random.nextInt(DOMAINS.length)];
        return name + "@" + domain;
    }

    private static String randomString(Random random, int size) {
        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            int choice = random.nextInt(36);
            if (choice < 10) {
                builder.append((char) ('0' + choice));
            } else {
                char base = random.nextBoolean() ? 'A' : 'a';
                builder.append((char) (base + (choice - 10)));
            }
        }
        return builder.toString();
    }

    private static HostAndPort parseAddress(String address) {
        String normalized = address.toLowerCase(Locale.ROOT).replace("iggy://", "");
        String[] parts = normalized.split(":", 2);
        String host = parts.length > 0 && !parts[0].isBlank() ? parts[0] : "localhost";
        int port = 8090;
        if (parts.length == 2) {
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException ignored) {
                log.warn("Invalid port in IGGY_ADDRESS, defaulting to 8090");
            }
        }
        return new HostAndPort(host, port);
    }

    private record HostAndPort(String host, int port) {}

    private record UserRecord(
            String userId, int userType, String email, String source, String state, Instant createdAt, String message) {
        String toJson(ObjectMapper mapper) {
            return mapper.writeValueAsString(this);
        }
    }
}
