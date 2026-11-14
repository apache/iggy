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

package org.apache.iggy.topic;

import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TopicDetailsTest {
    @Test
    void constructorWithTopicCreatesTopicDetailsWithExpectedValues() {
        var topic = new Topic(
                123L,
                BigInteger.TEN,
                "foo",
                "size",
                BigInteger.ZERO,
                1L,
                Map.of(
                        "message_expiry",
                        HeaderValue.fromUint64(BigInteger.valueOf(10000L)),
                        "compression_algorithm",
                        HeaderValue.fromString("gzip")),
                Map.of(
                        "max_topic_size",
                        HeaderValue.fromUint64(BigInteger.TWO),
                        "segment_max_age",
                        HeaderValue.fromUint64(BigInteger.valueOf(3_600_000_000L)),
                        "tiered_storage",
                        HeaderValue.fromBool(false),
                        "local_retention_age",
                        HeaderValue.fromUint64(BigInteger.valueOf(86_400_000_000L)),
                        "local_retention_size",
                        HeaderValue.fromUint64(BigInteger.valueOf(1_073_741_824L))));
        var partitions = List.of(new Partition(
                1L,
                BigInteger.TEN,
                2L,
                BigInteger.ZERO,
                "size",
                BigInteger.ONE,
                PartitionContext.EMPTY,
                List.of(0, 1, 2),
                List.of(0)));

        var topicDetails = new TopicDetails(topic, partitions);

        assertThat(topicDetails.id()).isEqualTo(123L);
        assertThat(topicDetails.createdAt()).isEqualTo(BigInteger.TEN);
        assertThat(topicDetails.name()).isEqualTo("foo");
        assertThat(topicDetails.size()).isEqualTo("size");
        assertThat(topicDetails.messageExpiry()).contains(BigInteger.valueOf(10000L));
        assertThat(topicDetails.compressionAlgorithm()).contains(CompressionAlgorithm.Gzip);
        assertThat(topicDetails.maxTopicSize()).contains(BigInteger.TWO);
        assertThat(topicDetails.segmentMaxAge()).contains(BigInteger.valueOf(3_600_000_000L));
        assertThat(topicDetails.tieredStorage()).contains(false);
        assertThat(topicDetails.localRetentionAge()).contains(BigInteger.valueOf(86_400_000_000L));
        assertThat(topicDetails.localRetentionSize()).contains(BigInteger.valueOf(1_073_741_824L));
        assertThat(topicDetails.messagesCount()).isEqualTo(BigInteger.ZERO);
        assertThat(topicDetails.partitionsCount()).isEqualTo(1L);
        assertThat(topicDetails.partitions()).isEqualTo(partitions);
    }
}
