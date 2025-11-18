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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeserializationSchemaTest {

    @Test
    void shouldDeserializeData() throws IOException {
        DeserializationSchema<String> schema = new TestDeserializationSchema();
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 0, 100L);

        String result = schema.deserialize("test-data".getBytes(StandardCharsets.UTF_8), metadata);

        assertThat(result).isEqualTo("test-data");
    }

    @Test
    void shouldReturnCorrectTypeDescriptor() {
        DeserializationSchema<String> schema = new TestDeserializationSchema();

        TypeDescriptor<String> typeDescriptor = schema.getProducedType();

        assertThat(typeDescriptor.getTypeClass()).isEqualTo(String.class);
    }

    @Test
    void shouldReturnFalseForIsNullableByDefault() {
        DeserializationSchema<String> schema = new TestDeserializationSchema();

        assertThat(schema.isNullable()).isFalse();
    }

    @Test
    void shouldAllowOverridingIsNullable() {
        DeserializationSchema<String> schema = new DeserializationSchema<String>() {
            @Override
            public String deserialize(byte[] data, RecordMetadata metadata) {
                return null;
            }

            @Override
            public TypeDescriptor<String> getProducedType() {
                return TypeDescriptor.of(String.class);
            }

            @Override
            public boolean isNullable() {
                return true;
            }
        };

        assertThat(schema.isNullable()).isTrue();
    }

    @Test
    void shouldHandleEmptyData() throws IOException {
        DeserializationSchema<String> schema = new TestDeserializationSchema();
        RecordMetadata metadata = RecordMetadata.empty();

        String result = schema.deserialize(new byte[0], metadata);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldPropagateIOException() {
        DeserializationSchema<String> schema = new DeserializationSchema<String>() {
            @Override
            public String deserialize(byte[] data, RecordMetadata metadata) throws IOException {
                throw new IOException("Deserialization failed");
            }

            @Override
            public TypeDescriptor<String> getProducedType() {
                return TypeDescriptor.of(String.class);
            }
        };

        assertThatThrownBy(() -> schema.deserialize("test".getBytes(), RecordMetadata.empty()))
                .isInstanceOf(IOException.class)
                .hasMessage("Deserialization failed");
    }

    @Test
    void shouldSupportDifferentTypes() throws IOException {
        DeserializationSchema<Integer> intSchema = new DeserializationSchema<Integer>() {
            @Override
            public Integer deserialize(byte[] data, RecordMetadata metadata) {
                return Integer.parseInt(new String(data, StandardCharsets.UTF_8));
            }

            @Override
            public TypeDescriptor<Integer> getProducedType() {
                return TypeDescriptor.of(Integer.class);
            }
        };

        Integer result = intSchema.deserialize("42".getBytes(StandardCharsets.UTF_8), RecordMetadata.empty());
        assertThat(result).isEqualTo(42);
        assertThat(intSchema.getProducedType().getTypeClass()).isEqualTo(Integer.class);
    }

    @Test
    void shouldBeSerializable() {
        DeserializationSchema<String> schema = new TestDeserializationSchema();
        assertThat(schema).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void shouldAccessMetadataInDeserialization() throws IOException {
        DeserializationSchema<String> schema = new DeserializationSchema<String>() {
            @Override
            public String deserialize(byte[] data, RecordMetadata metadata) {
                String content = new String(data, StandardCharsets.UTF_8);
                return String.format(
                        "%s:%s:%d:%d:%s",
                        metadata.getStreamId(),
                        metadata.getTopicId(),
                        metadata.getPartitionId(),
                        metadata.getOffset(),
                        content);
            }

            @Override
            public TypeDescriptor<String> getProducedType() {
                return TypeDescriptor.of(String.class);
            }
        };

        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 5, 1000L);
        String result = schema.deserialize("data".getBytes(StandardCharsets.UTF_8), metadata);

        assertThat(result).isEqualTo("stream1:topic1:5:1000:data");
    }

    // Helper implementation for testing
    private static final class TestDeserializationSchema implements DeserializationSchema<String> {
        @Override
        public String deserialize(byte[] data, RecordMetadata metadata) {
            return new String(data, StandardCharsets.UTF_8);
        }

        @Override
        public TypeDescriptor<String> getProducedType() {
            return TypeDescriptor.of(String.class);
        }
    }
}
