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

package org.apache.iggy.flink.example;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test to send sample text data to text-input stream for WordCountJob testing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SendTextDataTest {

    private static final Logger log = LoggerFactory.getLogger(SendTextDataTest.class);

    private static final String IGGY_SERVER_HOST = "localhost";
    private static final int IGGY_SERVER_TCP_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private AsyncIggyTcpClient client;

    @BeforeAll
    void setUp() {
        log.info("Setting up Async TCP Client...");

        client = AsyncIggyTcpClient.builder()
                .host(IGGY_SERVER_HOST)
                .port(IGGY_SERVER_TCP_PORT)
                .credentials(USERNAME, PASSWORD)
                .build();

        client.connect().join();
        log.info("Connected to Iggy server at {}:{}", IGGY_SERVER_HOST, IGGY_SERVER_TCP_PORT);
    }

    @AfterAll
    void tearDown() {
        if (client != null) {
            try {
                client.users().logoutAsync().join();
                log.info("Logged out successfully");
            } catch (RuntimeException e) {
                log.warn("Error during logout: {}", e.getMessage());
            }

            try {
                client.close().join();
                log.info("Disconnected from Iggy server");
            } catch (RuntimeException e) {
                log.warn("Error during disconnect: {}", e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("Send sample text lines for word count")
    void testSendSampleTextLines() throws Exception {
        log.info("Sending sample text lines for WordCountJob testing...");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        List<String> sampleTexts = List.of(
                "hello world hello flink",
                "apache flink connector for iggy",
                "flink streaming with iggy broker",
                "hello flink hello world",
                "real time stream processing",
                "apache flink word count example",
                "iggy message broker stream",
                "flink apache iggy connector test",
                "hello world stream processing",
                "apache iggy flink integration");

        List<Message> messages = new ArrayList<>();
        for (String text : sampleTexts) {
            messages.add(Message.of(text));
        }

        client.messages()
                .sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages)
                .get();

        log.info("Successfully sent {} text lines to stream 'text-input', topic 'lines'", messages.size());

        // Send a few more batches to have continuous data
        for (int batch = 1; batch <= 3; batch++) {
            Thread.sleep(1000);

            List<Message> batchMessages = new ArrayList<>();
            for (String text : sampleTexts) {
                batchMessages.add(Message.of(text + " batch" + batch));
            }

            client.messages()
                    .sendMessagesAsync(streamId, topicId, Partitioning.balanced(), batchMessages)
                    .get();

            log.info("Sent batch {} with {} messages", batch, batchMessages.size());
        }

        log.info("Total messages sent: {}", sampleTexts.size() * 4);
    }
}
