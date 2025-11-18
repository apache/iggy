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

import org.apache.iggy.partition.Partition;

import java.math.BigInteger;
import java.util.List;

public record TopicDetails(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messageExpiry,
        CompressionAlgorithm compressionAlgorithm,
        BigInteger maxTopicSize,
        Short replicationFactor,
        BigInteger messagesCount,
        Long partitionsCount,
        List<Partition> partitions) {
    public TopicDetails(Topic topic, List<Partition> partitions) {
        this(
                topic.id(),
                topic.createdAt(),
                topic.name(),
                topic.size(),
                topic.messageExpiry(),
                topic.compressionAlgorithm(),
                topic.maxTopicSize(),
                topic.replicationFactor(),
                topic.messagesCount(),
                topic.partitionsCount(),
                partitions);
    }
}
