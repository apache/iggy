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

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * A topic and its partitions, as the server reports them.
 *
 * <p>See {@link Topic} for what {@code options} and {@code derivedOptions} carry.
 */
public record TopicDetails(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messageExpiry,
        CompressionAlgorithm compressionAlgorithm,
        BigInteger maxTopicSize,
        BigInteger messagesCount,
        Long partitionsCount,
        List<Partition> partitions,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {
    public TopicDetails {
        options = options == null ? Map.of() : options;
        derivedOptions = derivedOptions == null ? Map.of() : derivedOptions;
    }

    public TopicDetails(Topic topic, List<Partition> partitions) {
        this(
                topic.id(),
                topic.createdAt(),
                topic.name(),
                topic.size(),
                topic.messageExpiry(),
                topic.compressionAlgorithm(),
                topic.maxTopicSize(),
                topic.messagesCount(),
                topic.partitionsCount(),
                partitions,
                topic.options(),
                topic.derivedOptions());
    }
}
