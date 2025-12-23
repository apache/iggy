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

package org.apache.iggy.examples.shared;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

public final class Messages {
    public static final String ORDER_CREATED_TYPE = "order_created";
    public static final String ORDER_CONFIRMED_TYPE = "order_confirmed";
    public static final String ORDER_REJECTED_TYPE = "order_rejected";

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public interface SerializableMessage {
        String getMessageType();

        default String toJson() {
            return toJsonWith(MAPPER);
        }

        default String toJsonEnvelope() {
            try {
                return MAPPER.writeValueAsString(Envelope.of(getMessageType(), this));
            } catch (JacksonException e) {
                throw new IllegalStateException("Failed to serialize envelope", e);
            }
        }

        private String toJsonWith(ObjectMapper mapper) {
            try {
                return mapper.writeValueAsString(this);
            } catch (JacksonException e) {
                throw new IllegalStateException("Failed to serialize message", e);
            }
        }
    }

    public record Envelope(String messageType, String payload) {
        public static Envelope of(String messageType, Object payload) {
            try {
                return new Envelope(messageType, MAPPER.writeValueAsString(payload));
            } catch (JacksonException e) {
                throw new IllegalStateException("Failed to serialize payload", e);
            }
        }

        public String toJson() {
            try {
                return MAPPER.writeValueAsString(this);
            } catch (JacksonException e) {
                throw new IllegalStateException("Failed to serialize envelope", e);
            }
        }
    }

    public record OrderCreated(
            long orderId, String currencyPair, double price, double quantity, String side, Instant timestamp)
            implements SerializableMessage {
        @Override
        public String getMessageType() {
            return ORDER_CREATED_TYPE;
        }
    }

    public record OrderConfirmed(long orderId, double price, Instant timestamp) implements SerializableMessage {
        @Override
        public String getMessageType() {
            return ORDER_CONFIRMED_TYPE;
        }
    }

    public record OrderRejected(long orderId, Instant timestamp, String reason) implements SerializableMessage {
        @Override
        public String getMessageType() {
            return ORDER_REJECTED_TYPE;
        }
    }
}
