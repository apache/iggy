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

package org.apache.iggy.bench.common.generator;

import org.apache.iggy.bench.models.common.generator.DataBatch;
import org.apache.iggy.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public final class BenchmarkBatchGenerator {

    private static final byte[] ALPHANUMERIC =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.US_ASCII);

    private final int messagesPerBatch;
    private final String payloadTemplate;

    public BenchmarkBatchGenerator(int messageSize, int messagesPerBatch) {
        this.messagesPerBatch = messagesPerBatch;
        this.payloadTemplate = randomPayload(messageSize);
    }

    public DataBatch generateBatch() {
        return buildBatch(messagesPerBatch);
    }

    public DataBatch generateBatch(int messagesInBatch) {
        return buildBatch(messagesInBatch);
    }

    private DataBatch buildBatch(int messagesInBatch) {
        var messages = new ArrayList<Message>(messagesInBatch);
        long userDataBytes = 0L;
        long totalBytes = 0L;

        for (int messageIndex = 0; messageIndex < messagesInBatch; messageIndex++) {
            var message = Message.of(payloadTemplate);
            messages.add(message);
            userDataBytes += message.payload().length;
            totalBytes += message.getSize();
        }

        return new DataBatch(messages, userDataBytes, totalBytes);
    }

    private static String randomPayload(int messageSize) {
        var payload = new byte[messageSize];
        var random = ThreadLocalRandom.current();

        for (int index = 0; index < messageSize; index++) {
            payload[index] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
        }

        return new String(payload, StandardCharsets.US_ASCII);
    }
}
