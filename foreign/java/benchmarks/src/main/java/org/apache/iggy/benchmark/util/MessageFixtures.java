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

package org.apache.iggy.benchmark.util;

import org.apache.iggy.message.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class MessageFixtures {

    private static final byte[] ALPHANUMERIC_CHARS;

    static {
        ALPHANUMERIC_CHARS = new byte[62];
        int idx = 0;
        for (int i = 0; i < 26; i++) {
            ALPHANUMERIC_CHARS[idx++] = (byte) ('A' + i);
        }
        for (int i = 0; i < 26; i++) {
            ALPHANUMERIC_CHARS[idx++] = (byte) ('a' + i);
        }
        for (int i = 0; i < 10; i++) {
            ALPHANUMERIC_CHARS[idx++] = (byte) ('0' + i);
        }
    }

    private MessageFixtures() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static byte[] generatePayload(int size) {
        byte[] payload = new byte[size];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            payload[i] = ALPHANUMERIC_CHARS[random.nextInt(62)];
        }
        return payload;
    }

    public static List<Message> generateMessages(int count, int payloadSize) {
        List<Message> messages = new ArrayList<>(count);
        String payloadStr = new String(generatePayload(payloadSize));

        for (int i = 0; i < count; i++) {
            messages.add(Message.of(payloadStr));
        }

        return messages;
    }
}
