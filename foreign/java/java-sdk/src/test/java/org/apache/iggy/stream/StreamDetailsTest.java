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

package org.apache.iggy.stream;

import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StreamDetailsTest {
    @Test
    void constructorWithStreamBaseCreatesExpectedStreamDetails() {
        var base = new StreamBase(10L, BigInteger.valueOf(500L), "name", "size", BigInteger.ZERO, 1L);
        var topics = List.of(new Topic(
                1L,
                BigInteger.ZERO,
                "name",
                "size",
                BigInteger.TEN,
                CompressionAlgorithm.None,
                BigInteger.ONE,
                (short) 2,
                BigInteger.ZERO,
                2L));

        var streamDetails = new StreamDetails(base, topics);

        assertThat(streamDetails.id()).isEqualTo(10L);
        assertThat(streamDetails.createdAt()).isEqualTo(BigInteger.valueOf(500L));
        assertThat(streamDetails.name()).isEqualTo("name");
        assertThat(streamDetails.size()).isEqualTo("size");
        assertThat(streamDetails.messagesCount()).isEqualTo(BigInteger.ZERO);
        assertThat(streamDetails.topicsCount()).isEqualTo(1L);
        assertThat(streamDetails.topics()).isEqualTo(topics);
    }
}
