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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.AsyncConsumerGroupsClient;
import org.apache.iggy.client.async.AsyncMessagesClient;
import org.apache.iggy.client.async.AsyncStreamsClient;
import org.apache.iggy.client.async.AsyncTopicsClient;
import org.apache.iggy.client.blocking.tcp.CommandCode;

import java.util.concurrent.CompletableFuture;

/**
 * Async TCP client for Apache Iggy using Netty.
 * This is a true async implementation with non-blocking I/O.
 */
public class AsyncIggyTcpClient {

    private final String host;
    private final int port;
    private AsyncTcpConnection connection;
    private AsyncMessagesClient messagesClient;
    private AsyncConsumerGroupsClient consumerGroupsClient;
    private AsyncStreamsClient streamsClient;
    private AsyncTopicsClient topicsClient;

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
                messagesClient = new AsyncMessagesTcpClient(connection);
                consumerGroupsClient = new AsyncConsumerGroupsTcpClient(connection);
                streamsClient = new AsyncStreamsTcpClient(connection);
                topicsClient = new AsyncTopicsTcpClient(connection);
            });
    }

    /**
     * Logs in to the server asynchronously.
     */
    public CompletableFuture<Void> login(String username, String password) {
        String version = "0.6.30";
        String context = "java-sdk";

        var payload = Unpooled.buffer();
        var usernameBytes = AsyncBytesSerializer.toBytes(username);
        var passwordBytes = AsyncBytesSerializer.toBytes(password);

        payload.writeBytes(usernameBytes);
        payload.writeBytes(passwordBytes);
        payload.writeIntLE(version.length());
        payload.writeBytes(version.getBytes());
        payload.writeIntLE(context.length());
        payload.writeBytes(context.getBytes());

        return connection.sendAsync(CommandCode.User.LOGIN.getValue(), payload)
            .thenAccept(response -> {
                // Login successful, response contains user info
                // For now, just release the buffer
                response.release();
            });
    }

    /**
     * Gets the async messages client.
     */
    public AsyncMessagesClient messages() {
        if (messagesClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return messagesClient;
    }

    /**
     * Gets the async consumer groups client.
     */
    public AsyncConsumerGroupsClient consumerGroups() {
        if (consumerGroupsClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return consumerGroupsClient;
    }

    /**
     * Gets the async streams client.
     */
    public AsyncStreamsClient streams() {
        if (streamsClient == null) {
            throw new IllegalStateException("Client not connected. Call connect() first.");
        }
        return streamsClient;
    }

    /**
     * Gets the async topics client.
     */
    public AsyncTopicsClient topics() {
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
