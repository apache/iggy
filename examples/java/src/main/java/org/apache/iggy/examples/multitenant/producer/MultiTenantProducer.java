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

package org.apache.iggy.examples.multitenant.producer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class MultiTenantProducer {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantProducer.class);

    private static final String[] TOPICS = {"events", "logs", "notifications"};
    private static final String PASSWORD = "secret";

    private MultiTenantProducer() {}

    public static void main(String[] args) {
        int tenantsCount = 3;
        int producersCount = 3;
        int partitionsCount = 3;
        boolean ensureAccess = true;
        long batchesLimit = 10L;
        int messagesPerBatch = 1;
        long intervalMs = 1L;

        String address = "127.0.0.1:8090";
        String rootUsername = "iggy";
        String rootPassword = "iggy";

        log.info(
                "Multi-tenant producer has started, tenants: {}, producers: {}, partitions: {}",
                tenantsCount,
                producersCount,
                partitionsCount);

        HostAndPort hostAndPort = parseAddress(address);
        IggyTcpClient rootClient = IggyTcpClient.builder()
                .host(hostAndPort.host())
                .port(hostAndPort.port())
                .credentials(rootUsername, rootPassword)
                .buildAndLogin();

        log.info("Creating streams and users with permissions for each tenant");
        Map<String, String> streamsWithUsers = new HashMap<>();
        for (int i = 1; i <= tenantsCount; i++) {
            String tenantPrefix = "tenant_" + i;
            String stream = tenantPrefix + "_stream";
            String user = tenantPrefix + "_producer";
            createStreamAndUser(rootClient, stream, user);
            streamsWithUsers.put(stream, user);
        }

        log.info("Creating clients for each tenant");
        List<Tenant> tenants = new ArrayList<>();
        int tenantId = 1;
        for (Map.Entry<String, String> entry : streamsWithUsers.entrySet()) {
            IggyTcpClient client = IggyTcpClient.builder()
                    .host(hostAndPort.host())
                    .port(hostAndPort.port())
                    .credentials(entry.getValue(), PASSWORD)
                    .buildAndLogin();
            tenants.add(Tenant.newTenant(tenantId, entry.getKey(), entry.getValue(), client));
            tenantId++;
        }

        if (ensureAccess) {
            log.info("Ensuring access to streams for each tenant");
            for (Tenant tenant : tenants) {
                List<String> unavailable = new ArrayList<>();
                for (Tenant other : tenants) {
                    if (!other.stream().equals(tenant.stream())) {
                        unavailable.add(other.stream());
                    }
                }
                ensureStreamAccess(tenant.client(), tenant.stream(), unavailable);
            }
        }

        log.info("Creating {} producer(s) for each tenant", producersCount);
        for (Tenant tenant : tenants) {
            List<TenantProducer> producers =
                    createProducers(tenant.client(), producersCount, partitionsCount, tenant.stream(), TOPICS);
            tenant.addProducers(producers);
            log.info(
                    "Created {} producer(s) for tenant stream: {}, username: {}",
                    producersCount,
                    tenant.stream(),
                    tenant.user());
        }

        log.info("Starting {} producer(s) for each tenant", producersCount);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> tasks = new ArrayList<>();
        for (Tenant tenant : tenants) {
            for (TenantProducer producer : tenant.producers()) {
                tasks.add(executor.submit(() ->
                        sendBatches(tenant.id(), producer, batchesLimit, messagesPerBatch, intervalMs, TOPICS.length)));
            }
        }

        waitFor(tasks);
        executor.shutdown();
        log.info("Disconnecting clients");
    }

    private static void sendBatches(
            int tenantId,
            TenantProducer producer,
            long batchesCount,
            int batchLength,
            long intervalMs,
            int topicsCount) {
        long counter = 1;
        long eventsId = 1;
        long logsId = 1;
        long notificationsId = 1;

        while (counter <= (long) topicsCount * batchesCount) {
            long messageId;
            String messagePrefix;
            switch (producer.topic()) {
                case "events" -> {
                    eventsId += 1;
                    messageId = eventsId;
                    messagePrefix = "event";
                }
                case "logs" -> {
                    logsId += 1;
                    messageId = logsId;
                    messagePrefix = "log";
                }
                case "notifications" -> {
                    notificationsId += 1;
                    messageId = notificationsId;
                    messagePrefix = "notification";
                }
                default -> throw new IllegalStateException("Invalid topic");
            }

            List<Message> messages = new ArrayList<>(batchLength);
            List<String> payloads = new ArrayList<>(batchLength);
            for (int i = 0; i < batchLength; i++) {
                String payload = messagePrefix + "-" + producer.id() + "-" + messageId;
                messages.add(Message.of(payload));
                payloads.add(payload);
            }

            try {
                producer.client()
                        .messages()
                        .sendMessages(producer.streamId(), producer.topicId(), producer.partitioning(), messages);
                log.info(
                        "Sent: {} message(s) by tenant: {}, producer: {}, to: {} -> {}",
                        batchLength,
                        tenantId,
                        producer.id(),
                        producer.stream(),
                        producer.topic());
            } catch (Exception error) {
                log.error(
                        "Failed to send: {} message(s) to: {} -> {} by tenant: {}, producer: {} with error: {}",
                        batchLength,
                        producer.stream(),
                        producer.topic(),
                        tenantId,
                        producer.id(),
                        error.getMessage());
            }

            counter += 1;

            if (intervalMs > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static List<TenantProducer> createProducers(
            IggyTcpClient client, int producersCount, int partitionsCount, String stream, String[] topics) {
        List<TenantProducer> producers = new ArrayList<>();
        StreamId streamId = StreamId.of(stream);

        for (String topic : topics) {
            TopicId topicId = TopicId.of(topic);
            ensureTopic(client, streamId, topicId, partitionsCount);

            for (int id = 1; id <= producersCount; id++) {
                Partitioning partitioning = Partitioning.balanced();
                producers.add(TenantProducer.newProducer(id, stream, topic, streamId, topicId, client, partitioning));
            }
        }
        return producers;
    }

    private static void ensureTopic(IggyTcpClient client, StreamId streamId, TopicId topicId, int partitionsCount) {
        Optional<TopicDetails> topic = tryGetTopic(client, streamId, topicId);
        if (topic.isPresent()) {
            log.info("Topic {} already exists for stream {}", topicId.getName(), streamId.getName());
            return;
        }

        client.topics()
                .createTopic(
                        streamId,
                        (long) partitionsCount,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        topicId.getName());
        log.info("Created topic {} for stream {}", topicId.getName(), streamId.getName());
    }

    private static void ensureStreamAccess(
            IggyTcpClient client, String availableStream, List<String> unavailableStreams) {
        StreamId availableStreamId = StreamId.of(availableStream);
        Optional<StreamDetails> stream = tryGetStream(client, availableStreamId);
        if (stream.isEmpty()) {
            throw new IllegalStateException("No access to stream: " + availableStream);
        }
        log.info("Ensured access to stream: {}", availableStream);

        for (String otherStream : unavailableStreams) {
            Optional<StreamDetails> forbidden = tryGetStream(client, StreamId.of(otherStream));
            if (forbidden.isEmpty()) {
                log.info("Ensured no access to stream: {}", otherStream);
            } else {
                throw new IllegalStateException("Access to stream: " + otherStream + " should not be allowed");
            }
        }
    }

    private static Optional<StreamDetails> tryGetStream(IggyTcpClient client, StreamId streamId) {
        try {
            return client.streams().getStream(streamId);
        } catch (Exception e) {
            log.debug("Unable to get stream {}: {}", streamId.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    private static Optional<TopicDetails> tryGetTopic(IggyTcpClient client, StreamId streamId, TopicId topicId) {
        try {
            return client.topics().getTopic(streamId, topicId);
        } catch (Exception e) {
            log.debug("Unable to get topic {} in stream {}: {}", topicId.getName(), streamId.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    private static void createStreamAndUser(IggyTcpClient client, String streamName, String username) {
        StreamDetails stream = client.streams()
                .getStream(StreamId.of(streamName))
                .orElseGet(() -> client.streams().createStream(streamName));
        log.info("Created stream: {} with ID: {}", streamName, stream.id());

        Map<Long, StreamPermissions> streamPermissions = new HashMap<>();
        streamPermissions.put(stream.id(), new StreamPermissions(false, true, true, true, false, true, Map.of()));

        Permissions permissions = new Permissions(
                new GlobalPermissions(false, false, false, false, false, false, false, false, false, false),
                streamPermissions);

        if (userExists(client, username)) {
            log.info("User: {} already exists, skipping creation", username);
            return;
        }

        UserInfoDetails user =
                client.users().createUser(username, PASSWORD, UserStatus.Active, Optional.of(permissions));
        log.info("Created user: {} with ID: {}, with permissions for stream: {}", username, user.id(), streamName);
    }

    private static boolean userExists(IggyTcpClient client, String username) {
        try {
            for (UserInfo user : client.users().getUsers()) {
                if (user.username().equals(username)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.debug("Unable to check if user {} exists: {}", username, e.getMessage());
        }
        return false;
    }

    private static void waitFor(List<Future<?>> tasks) {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Producer task failed: {}", e.getMessage());
            }
        }
    }

    private static HostAndPort parseAddress(String address) {
        String[] parts = address.split(":", 2);
        String host = parts.length > 0 && !parts[0].isBlank() ? parts[0] : "localhost";
        int port = 8090;
        if (parts.length == 2) {
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException ignored) {
                log.warn("Invalid port in given address, defaulting to 8090");
            }
        }
        return new HostAndPort(host, port);
    }

    private record HostAndPort(String host, int port) {}

    private record Tenant(int id, String stream, String user, IggyTcpClient client, List<TenantProducer> producers) {
        static Tenant newTenant(int id, String stream, String user, IggyTcpClient client) {
            return new Tenant(id, stream, user, client, new ArrayList<>());
        }

        void addProducers(List<TenantProducer> producers) {
            this.producers.addAll(producers);
        }
    }

    private record TenantProducer(
            int id,
            String stream,
            String topic,
            StreamId streamId,
            TopicId topicId,
            IggyTcpClient client,
            Partitioning partitioning) {
        static TenantProducer newProducer(
                int id,
                String stream,
                String topic,
                StreamId streamId,
                TopicId topicId,
                IggyTcpClient client,
                Partitioning partitioning) {
            return new TenantProducer(id, stream, topic, streamId, topicId, client, partitioning);
        }
    }
}
