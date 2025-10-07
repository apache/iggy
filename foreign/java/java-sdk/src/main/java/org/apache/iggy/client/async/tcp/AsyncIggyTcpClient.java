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

package org.apache.iggy.client.async.tcp;

import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.StreamsClient;
import org.apache.iggy.client.async.TopicsClient;
import org.apache.iggy.client.async.UsersClient;

import java.util.concurrent.CompletableFuture;

/**
 * Async TCP client for Apache Iggy using Netty.
 * This is a true async implementation with non-blocking I/O.
 */
public class AsyncIggyTcpClient {

    private final String host;
    private final int port;
    private AsyncTcpConnection connection;
    private MessagesClient messagesClient;
    private ConsumerGroupsClient consumerGroupsClient;
    private StreamsClient streamsClient;
    private TopicsClient topicsClient;
    private UsersClient usersClient;

    public AsyncIggyTcpClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the Iggy server asynchronously.
     */
    public CompletableFuture<Void> connect() {
        connection = new AsyncTcpConnection(host, port);
        return connection.connect()
            .thenRun(() -> {
                messagesClient = new MessagesTcpClient(connection);
                consumerGroupsClient = new ConsumerGroupsTcpClient(connection);
                streamsClient = new StreamsTcpClient(connection);
                topicsClient = new TopicsTcpClient(connection);
                usersClient = new UsersTcpClient(connection);
            });
    }

    /**
     * Gets the async users client.
     */
    public UsersClient users() {
        if (usersClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return usersClient;
    }

    /**
     * Gets the async messages client.
     */
    public MessagesClient messages() {
        if (messagesClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return messagesClient;
    }

    /**
     * Gets the async consumer groups client.
     */
    public ConsumerGroupsClient consumerGroups() {
        if (consumerGroupsClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return consumerGroupsClient;
    }

    /**
     * Gets the async streams client.
     */
    public StreamsClient streams() {
        if (streamsClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return streamsClient;
    }

    /**
     * Gets the async topics client.
     */
    public TopicsClient topics() {
        if (topicsClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return topicsClient;
    }

    /**
     * Closes the connection and releases resources.
     */
    public CompletableFuture<Void> close() {
        if (connection != null) {
            return connection.close();
        }
        return CompletableFuture.completedFuture(null);
    }
}
