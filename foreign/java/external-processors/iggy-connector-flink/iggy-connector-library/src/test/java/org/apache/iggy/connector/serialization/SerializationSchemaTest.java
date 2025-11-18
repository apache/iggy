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

package org.apache.iggy.connector.serialization;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerializationSchemaTest {

    @Test
    void shouldSerializeData() throws IOException {
        SerializationSchema<String> schema = new TestSerializationSchema();

        byte[] result = schema.serialize("test-data");

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("test-data");
    }

    @Test
    void shouldReturnEmptyOptionalForPartitionKeyByDefault() {
        SerializationSchema<String> schema = new TestSerializationSchema();

        Optional<Integer> partitionKey = schema.extractPartitionKey("test-data");

        assertThat(partitionKey).isEmpty();
    }

    @Test
    void shouldAllowOverridingExtractPartitionKey() {
        SerializationSchema<String> schema = new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String element) {
                return element.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<Integer> extractPartitionKey(String element) {
                return Optional.of(element.hashCode());
            }
        };

        Optional<Integer> partitionKey = schema.extractPartitionKey("test-data");

        assertThat(partitionKey).isPresent();
        assertThat(partitionKey.get()).isEqualTo("test-data".hashCode());
    }

    @Test
    void shouldReturnFalseForIsNullableByDefault() {
        SerializationSchema<String> schema = new TestSerializationSchema();

        assertThat(schema.isNullable()).isFalse();
    }

    @Test
    void shouldAllowOverridingIsNullable() {
        SerializationSchema<String> schema = new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String element) {
                return element != null ? element.getBytes(StandardCharsets.UTF_8) : new byte[0];
            }

            @Override
            public boolean isNullable() {
                return true;
            }
        };

        assertThat(schema.isNullable()).isTrue();
    }

    @Test
    void shouldHandleEmptyString() throws IOException {
        SerializationSchema<String> schema = new TestSerializationSchema();

        byte[] result = schema.serialize("");

        assertThat(result).isEmpty();
    }

    @Test
    void shouldPropagateIOException() {
        SerializationSchema<String> schema = new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String element) throws IOException {
                throw new IOException("Serialization failed");
            }
        };

        assertThatThrownBy(() -> schema.serialize("test"))
                .isInstanceOf(IOException.class)
                .hasMessage("Serialization failed");
    }

    @Test
    void shouldSupportDifferentTypes() throws IOException {
        SerializationSchema<Integer> intSchema = new SerializationSchema<Integer>() {
            @Override
            public byte[] serialize(Integer element) {
                return element.toString().getBytes(StandardCharsets.UTF_8);
            }
        };

        byte[] result = intSchema.serialize(42);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("42");
    }

    @Test
    void shouldBeSerializable() {
        SerializationSchema<String> schema = new TestSerializationSchema();
        assertThat(schema).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void shouldSupportCustomPartitionKeyExtraction() {
        SerializationSchema<TestRecord> schema = new SerializationSchema<TestRecord>() {
            @Override
            public byte[] serialize(TestRecord element) {
                return element.data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<Integer> extractPartitionKey(TestRecord element) {
                return Optional.of(element.partitionKey);
            }
        };

        TestRecord record = new TestRecord("test-data", 5);
        Optional<Integer> partitionKey = schema.extractPartitionKey(record);

        assertThat(partitionKey).isPresent();
        assertThat(partitionKey.get()).isEqualTo(5);
    }

    @Test
    void shouldSupportConditionalPartitionKeyExtraction() {
        SerializationSchema<TestRecord> schema = new SerializationSchema<TestRecord>() {
            @Override
            public byte[] serialize(TestRecord element) {
                return element.data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<Integer> extractPartitionKey(TestRecord element) {
                // Only return partition key if it's positive
                return element.partitionKey > 0 ? Optional.of(element.partitionKey) : Optional.empty();
            }
        };

        TestRecord recordWithKey = new TestRecord("test-data", 5);
        TestRecord recordWithoutKey = new TestRecord("test-data", -1);

        assertThat(schema.extractPartitionKey(recordWithKey)).isPresent();
        assertThat(schema.extractPartitionKey(recordWithoutKey)).isEmpty();
    }

    @Test
    void shouldHandleLargeData() throws IOException {
        SerializationSchema<String> schema = new TestSerializationSchema();
        String largeData = "x".repeat(10000);

        byte[] result = schema.serialize(largeData);

        assertThat(result).hasSize(10000);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo(largeData);
    }

    // Helper implementation for testing
    private static final class TestSerializationSchema implements SerializationSchema<String> {
        @Override
        public byte[] serialize(String element) {
            return element.getBytes(StandardCharsets.UTF_8);
        }
    }

    // Helper class for testing custom types
    private static final class TestRecord {
        private final String data;
        private final int partitionKey;

        TestRecord(String data, int partitionKey) {
            this.data = data;
            this.partitionKey = partitionKey;
        }
    }
}
