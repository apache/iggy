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

import static org.assertj.core.api.Assertions.assertThat;

class RecordMetadataTest {

    @Test
    void shouldCreateMetadataWithAllParameters() {
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 5, 1000L);

        assertThat(metadata.getStreamId()).isEqualTo("stream1");
        assertThat(metadata.getTopicId()).isEqualTo("topic1");
        assertThat(metadata.getPartitionId()).isEqualTo(5);
        assertThat(metadata.getOffset()).isEqualTo(1000L);
    }

    @Test
    void shouldCreateEmptyMetadata() {
        RecordMetadata metadata = RecordMetadata.empty();

        assertThat(metadata.getStreamId()).isNull();
        assertThat(metadata.getTopicId()).isNull();
        assertThat(metadata.getPartitionId()).isNull();
        assertThat(metadata.getOffset()).isNull();
    }

    @Test
    void shouldAllowNullValues() {
        RecordMetadata metadata = RecordMetadata.of(null, null, null, null);

        assertThat(metadata.getStreamId()).isNull();
        assertThat(metadata.getTopicId()).isNull();
        assertThat(metadata.getPartitionId()).isNull();
        assertThat(metadata.getOffset()).isNull();
    }

    @Test
    void shouldSupportPartialMetadata() {
        RecordMetadata streamOnly = RecordMetadata.of("stream1", null, null, null);
        assertThat(streamOnly.getStreamId()).isEqualTo("stream1");
        assertThat(streamOnly.getTopicId()).isNull();

        RecordMetadata streamAndTopic = RecordMetadata.of("stream1", "topic1", null, null);
        assertThat(streamAndTopic.getStreamId()).isEqualTo("stream1");
        assertThat(streamAndTopic.getTopicId()).isEqualTo("topic1");
        assertThat(streamAndTopic.getPartitionId()).isNull();
    }

    @Test
    void shouldSupportZeroValues() {
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 0, 0L);

        assertThat(metadata.getPartitionId()).isZero();
        assertThat(metadata.getOffset()).isZero();
    }

    @Test
    void shouldSupportLargeOffsets() {
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 999, Long.MAX_VALUE);

        assertThat(metadata.getPartitionId()).isEqualTo(999);
        assertThat(metadata.getOffset()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        RecordMetadata metadata1 = RecordMetadata.of("stream1", "topic1", 5, 1000L);
        RecordMetadata metadata2 = RecordMetadata.of("stream1", "topic1", 5, 1000L);
        RecordMetadata metadata3 = RecordMetadata.of("stream2", "topic1", 5, 1000L);
        RecordMetadata metadata4 = RecordMetadata.of("stream1", "topic2", 5, 1000L);
        RecordMetadata metadata5 = RecordMetadata.of("stream1", "topic1", 6, 1000L);
        RecordMetadata metadata6 = RecordMetadata.of("stream1", "topic1", 5, 2000L);

        assertThat(metadata1).isEqualTo(metadata2);
        assertThat(metadata1).hasSameHashCodeAs(metadata2);
        assertThat(metadata1).isNotEqualTo(metadata3);
        assertThat(metadata1).isNotEqualTo(metadata4);
        assertThat(metadata1).isNotEqualTo(metadata5);
        assertThat(metadata1).isNotEqualTo(metadata6);
        assertThat(metadata1).isEqualTo(metadata1);
        assertThat(metadata1).isNotEqualTo(null);
        assertThat(metadata1).isNotEqualTo(new Object());
    }

    @Test
    void shouldHandleEqualsWithNullFields() {
        RecordMetadata empty1 = RecordMetadata.empty();
        RecordMetadata empty2 = RecordMetadata.empty();
        RecordMetadata partial = RecordMetadata.of("stream1", null, null, null);

        assertThat(empty1).isEqualTo(empty2);
        assertThat(empty1).hasSameHashCodeAs(empty2);
        assertThat(empty1).isNotEqualTo(partial);
    }

    @Test
    void shouldImplementToStringCorrectly() {
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 5, 1000L);

        String toString = metadata.toString();
        assertThat(toString).contains("RecordMetadata");
        assertThat(toString).contains("stream1");
        assertThat(toString).contains("topic1");
        assertThat(toString).contains("partitionId=5");
        assertThat(toString).contains("offset=1000");
    }

    @Test
    void shouldHandleToStringWithNullFields() {
        RecordMetadata metadata = RecordMetadata.empty();

        String toString = metadata.toString();
        assertThat(toString).contains("RecordMetadata");
        assertThat(toString).contains("null");
    }

    @Test
    void shouldBeSerializable() {
        RecordMetadata metadata = RecordMetadata.of("stream1", "topic1", 5, 1000L);
        assertThat(metadata).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void shouldReuseEmptyInstance() {
        RecordMetadata empty1 = RecordMetadata.empty();
        RecordMetadata empty2 = RecordMetadata.empty();

        assertThat(empty1).isSameAs(empty2);
    }
}
