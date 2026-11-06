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

package org.apache.iggy.conformance;

import org.apache.iggy.client.async.reader.IggyConsumer;
import org.apache.iggy.client.async.reader.PartitionReader;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/** The clients, readers and consumers of the agent, by entity id. */
final class Entities {

    /** A close can wait for the reply of a committing poll, which ends at its request timeout. */
    private static final long CLOSE_TIMEOUT_SECONDS = 60;

    private static final Logger log = LoggerFactory.getLogger(Entities.class);

    private final Map<String, AsyncIggyTcpClient> clients = new ConcurrentHashMap<>();
    private final Map<String, Guarded<PartitionReader>> readers = new ConcurrentHashMap<>();
    private final Map<String, Guarded<IggyConsumer>> consumers = new ConcurrentHashMap<>();

    AsyncIggyTcpClient client(String entity) {
        return clients.get(entity);
    }

    Guarded<PartitionReader> reader(String entity) {
        return readers.get(entity);
    }

    Guarded<IggyConsumer> consumer(String entity) {
        return consumers.get(entity);
    }

    void keepClient(String entity, AsyncIggyTcpClient client) {
        AsyncIggyTcpClient replaced = clients.put(entity, client);
        if (replaced != null) {
            await(replaced.close(), entity);
        }
    }

    void keepReader(String entity, PartitionReader reader) {
        Guarded<PartitionReader> replaced = readers.put(entity, new Guarded<>(reader));
        if (replaced != null) {
            await(replaced.value().closeAsync(), entity);
        }
    }

    void keepConsumer(String entity, IggyConsumer consumer) {
        Guarded<IggyConsumer> replaced = consumers.put(entity, new Guarded<>(consumer));
        if (replaced != null) {
            await(replaced.value().shutdown(), entity);
        }
    }

    void close(String entity) {
        Guarded<IggyConsumer> consumer = consumers.remove(entity);
        if (consumer != null) {
            await(consumer.value().shutdown(), entity);
        }
        Guarded<PartitionReader> reader = readers.remove(entity);
        if (reader != null) {
            await(reader.value().closeAsync(), entity);
        }
        AsyncIggyTcpClient client = clients.remove(entity);
        if (client != null) {
            await(client.close(), entity);
        }
    }

    /** Closes the readers and the consumers first, because they store offsets and leave groups through a client. */
    void closeAll() {
        List.copyOf(consumers.keySet()).forEach(this::close);
        List.copyOf(readers.keySet()).forEach(this::close);
        List.copyOf(clients.keySet()).forEach(this::close);
    }

    private static void await(CompletableFuture<Void> closing, String entity) {
        try {
            closing.get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException error) {
            log.warn("Cannot close {}: {}", entity, error.toString());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** A reader or a consumer, with the lock that lets one call at a time take its next batch or message. */
    record Guarded<T>(T value, ReentrantLock lock) {
        Guarded(T value) {
            this(value, new ReentrantLock());
        }
    }
}
