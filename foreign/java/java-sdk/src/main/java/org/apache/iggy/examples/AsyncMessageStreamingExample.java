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

import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.IggyAsyncClient;
import org.apache.iggy.client.async.MessagesAsyncClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Example demonstrating the usage of the asynchronous client for message streaming.
 */
public class AsyncMessageStreamingExample {

    private static final String HOST = "localhost";
    private static final int PORT = 8090;
    private static final long STREAM_ID = 1;
    private static final long TOPIC_ID = 1;
    private static final long CONSUMER_ID = 1;

    public static void main(String[] args) {
        try {
            // Create and connect the async client
            IggyAsyncClient client = Iggy.asyncClientBuilder()
                    .withTcpClient(HOST, PORT)
                    .buildAndConnect()
                    .join(); // Join only for the example, in a real application you would use CompletableFuture chaining

            // Run the example and wait for it to complete
            runExample(client).join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static CompletableFuture<Void> runExample(IggyAsyncClient client) {
        MessagesAsyncClient messagesClient = client.getBaseClient().messages();

        // Create a message to send
        Message message = Message.of("Hello, Async Iggy!");

        // Send the message
        return messagesClient.sendMessages(
                STREAM_ID,
                TOPIC_ID,
                Partitioning.balanced(),
                List.of(message)
        ).thenCompose(v -> 
                // Poll the message
                messagesClient.pollMessages(
                        STREAM_ID,
                        TOPIC_ID,
                        Optional.empty(), // No specific partition
                        CONSUMER_ID,
                        PollingStrategy.offset(BigInteger.ZERO), // Start from the beginning
                        1L, // Poll one message
                        true // Auto-commit
                )
        ).thenAccept(polledMessages -> {
            System.out.println("Received " + polledMessages.count() + " messages:");
            polledMessages.messages().forEach(msg -> {
                String content = new String(msg.payload(), StandardCharsets.UTF_8);
                System.out.println("Message: " + content);
            });
        });
    }
}
