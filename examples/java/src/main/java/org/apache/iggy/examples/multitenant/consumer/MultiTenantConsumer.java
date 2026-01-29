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

package org.apache.iggy.examples.multitenant.consumer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class MultiTenantConsumer {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantConsumer.class);

    private static final String[] TOPICS = {"events", "logs", "notifications"};
    private static final String CONSUMER_GROUP = "multi-tenant";
    private static final String PASSWORD = "secret";

    private static final int TENANTS_COUNT = 3;
    private static final int CONSUMERS_COUNT = 1;
    private static final boolean ENSURE_ACCESS = true;
    private static final long MESSAGE_BATCHES_LIMIT = 10L;
    private static final long MESSAGES_PER_BATCH = 1L;
    private static final long POLL_INTERVAL_MS = 1L;

    private static final String ADDRESS = "127.0.0.1:8090";
    private static final String ROOT_USERNAME = "iggy";
    private static final String ROOT_PASSWORD = "iggy";

    private MultiTenantConsumer() {}

    public static void main(String[] args) {
        log.info("Multi-tenant consumer has started, tenants: {}, consumers: {}", TENANTS_COUNT, CONSUMERS_COUNT);

        HostAndPort hostAndPort = parseAddress(ADDRESS);
        IggyTcpClient rootClient = IggyTcpClient.builder()
                .host(hostAndPort.host())
                .port(hostAndPort.port())
                .credentials(ROOT_USERNAME, ROOT_PASSWORD)
                .buildAndLogin();

        log.info("Creating users with topic permissions for each tenant");
        Map<String, String> streamsWithUsers = new HashMap<>();
        for (int i = 1; i <= TENANTS_COUNT; i++) {
            String name = "tenant_" + i;
            String stream = name + "_stream";
            String user = name + "_consumer";
            createUser(rootClient, stream, user);
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

        if (ENSURE_ACCESS) {
            log.info("Ensuring access to topics for each tenant");
            for (Tenant tenant : tenants) {
                List<String> unavailable = new ArrayList<>();
                for (Tenant other : tenants) {
                    if (!other.stream().equals(tenant.stream())) {
                        unavailable.add(other.stream());
                    }
                }
                ensureStreamTopicsAccess(tenant.client(), tenant.stream(), unavailable);
            }
        }

        log.info("Creating {} consumer(s) for each tenant", CONSUMERS_COUNT);
        for (Tenant tenant : tenants) {
            List<TenantConsumer> consumers = createConsumers(tenant.client(), CONSUMERS_COUNT, tenant.stream());
            tenant.addConsumers(consumers);
            log.info(
                    "Created {} consumer(s) for tenant stream: {}, username: {}",
                    CONSUMERS_COUNT,
                    tenant.stream(),
                    tenant.user());
        }

        log.info("Starting {} consumer(s) for each tenant", CONSUMERS_COUNT);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> tasks = new ArrayList<>();
        for (Tenant tenant : tenants) {
            for (TenantConsumer consumer : tenant.consumers()) {
                tasks.add(executor.submit(() -> consume(tenant.id(), consumer)));
            }
        }

        waitFor(tasks);
        executor.shutdown();
        log.info("Finished consuming messages for all tenants");
    }

    private static void consume(int tenantId, TenantConsumer tenantConsumer) {
        long batchesProcessed = 0;
        try {
            tenantConsumer
                    .client()
                    .consumerGroups()
                    .joinConsumerGroup(
                            tenantConsumer.streamId(),
                            tenantConsumer.topicId(),
                            tenantConsumer.consumer().id());

            while (batchesProcessed < MESSAGE_BATCHES_LIMIT) {
                PolledMessages polledMessages = tenantConsumer
                        .client()
                        .messages()
                        .pollMessages(
                                tenantConsumer.streamId(),
                                tenantConsumer.topicId(),
                                Optional.empty(),
                                tenantConsumer.consumer(),
                                PollingStrategy.next(),
                                MESSAGES_PER_BATCH,
                                true);

                if (polledMessages.messages().isEmpty()) {
                    TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                for (Message message : polledMessages.messages()) {
                    String payload = new String(message.payload(), StandardCharsets.UTF_8);
                    log.info(
                            "Tenant: {} consumer: {} received: {} from partition: {}, topic: {}, stream: {}, at offset: {}, current offset: {}",
                            tenantId,
                            tenantConsumer.id(),
                            payload,
                            polledMessages.partitionId(),
                            tenantConsumer.topic(),
                            tenantConsumer.stream(),
                            message.header().offset(),
                            polledMessages.currentOffset());
                }
                batchesProcessed++;
                TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL_MS);
            }

            log.info(
                    "Tenant: {} consumer: {} reached message batches limit: {}, stopping.",
                    tenantId,
                    tenantConsumer.id(),
                    MESSAGE_BATCHES_LIMIT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception error) {
            log.error(
                    "Error while consuming messages by tenant: {}, consumer: {}, topic: {}, stream: {}",
                    tenantId,
                    tenantConsumer.id(),
                    tenantConsumer.topic(),
                    tenantConsumer.stream(),
                    error);
        }
    }

    private static List<TenantConsumer> createConsumers(IggyTcpClient client, int consumersCount, String stream) {
        List<TenantConsumer> consumers = new ArrayList<>();
        StreamId streamId = StreamId.of(stream);

        for (String topic : TOPICS) {
            TopicId topicId = TopicId.of(topic);
            ensureConsumerGroup(client, streamId, topicId);

            for (int id = 1; id <= consumersCount; id++) {
                Consumer consumer = Consumer.group(ConsumerId.of(CONSUMER_GROUP));
                consumers.add(TenantConsumer.newConsumer(id, stream, topic, streamId, topicId, consumer, client));
            }
        }

        return consumers;
    }

    private static void ensureConsumerGroup(IggyTcpClient client, StreamId streamId, TopicId topicId) {
        Optional<ConsumerGroupDetails> consumerGroup =
                client.consumerGroups().getConsumerGroup(streamId, topicId, ConsumerId.of(CONSUMER_GROUP));
        if (consumerGroup.isPresent()) {
            return;
        }
        client.consumerGroups().createConsumerGroup(streamId, topicId, CONSUMER_GROUP);
        log.info(
                "Created consumer group: {} for stream: {}, topic: {}",
                CONSUMER_GROUP,
                streamId.getName(),
                topicId.getName());
    }

    private static void createUser(IggyTcpClient client, String streamName, String username) {
        if (userExists(client, username)) {
            log.info("User: {} already exists, skipping creation", username);
            return;
        }

        StreamDetails stream = client.streams()
                .getStream(StreamId.of(streamName))
                .orElseThrow(() -> new IllegalStateException("Stream does not exist: " + streamName));

        Map<Long, TopicPermissions> topicPermissions = new HashMap<>();
        for (String topic : TOPICS) {
            TopicDetails topicDetails = client.topics()
                    .getTopic(StreamId.of(streamName), TopicId.of(topic))
                    .orElseThrow(() -> new IllegalStateException("Topic does not exist: " + topic));
            topicPermissions.put(topicDetails.id(), new TopicPermissions(false, true, true, false));
        }

        Map<Long, StreamPermissions> streamPermissions = new HashMap<>();
        streamPermissions.put(
                stream.id(), new StreamPermissions(false, true, false, true, true, false, topicPermissions));

        Permissions permissions = new Permissions(
                new GlobalPermissions(false, false, false, false, false, false, false, false, false, false),
                streamPermissions);

        UserInfoDetails user =
                client.users().createUser(username, PASSWORD, UserStatus.Active, Optional.of(permissions));
        log.info(
                "Created user: {} with ID: {}, with permissions for topics: {} in stream: {}",
                username,
                user.id(),
                List.of(TOPICS),
                streamName);
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

    private static void ensureStreamTopicsAccess(
            IggyTcpClient client, String availableStream, List<String> unavailableStreams) {
        StreamId availableStreamId = StreamId.of(availableStream);
        for (String topic : TOPICS) {
            TopicId topicId = TopicId.of(topic);
            Optional<TopicDetails> topicDetails = client.topics().getTopic(availableStreamId, topicId);
            if (topicDetails.isEmpty()) {
                throw new IllegalStateException("No access to topic: " + topic + " in stream: " + availableStream);
            }
            log.info("Ensured access to topic: {} in stream: {}", topic, availableStream);
        }

        for (String otherStream : unavailableStreams) {
            StreamId forbiddenStreamId = StreamId.of(otherStream);
            for (String topic : TOPICS) {
                TopicId topicId = TopicId.of(topic);
                try {
                    Optional<TopicDetails> forbidden = client.topics().getTopic(forbiddenStreamId, topicId);
                    if (forbidden.isPresent()) {
                        throw new IllegalStateException(
                                "Access to topic: " + topic + " in stream: " + otherStream + " should not be allowed");
                    }
                    log.info("Ensured no access to topic: {} in stream: {}", topic, otherStream);
                } catch (Exception e) {
                    log.info("Ensured no access to topic: {} in stream: {} ({})", topic, otherStream, e.getMessage());
                }
            }
        }
    }

    private static void waitFor(List<Future<?>> tasks) {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Consumer task failed: {}", e.getMessage());
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
                log.warn("Invalid port in address, defaulting to 8090");
            }
        }
        return new HostAndPort(host, port);
    }

    private record HostAndPort(String host, int port) {}

    private record Tenant(int id, String stream, String user, IggyTcpClient client, List<TenantConsumer> consumers) {
        static Tenant newTenant(int id, String stream, String user, IggyTcpClient client) {
            return new Tenant(id, stream, user, client, new ArrayList<>());
        }

        void addConsumers(List<TenantConsumer> newConsumers) {
            this.consumers.addAll(newConsumers);
        }
    }

    private record TenantConsumer(
            int id,
            String stream,
            String topic,
            StreamId streamId,
            TopicId topicId,
            Consumer consumer,
            IggyTcpClient client) {
        static TenantConsumer newConsumer(
                int id,
                String stream,
                String topic,
                StreamId streamId,
                TopicId topicId,
                Consumer consumer,
                IggyTcpClient client) {
            return new TenantConsumer(id, stream, topic, streamId, topicId, consumer, client);
        }
    }
}
