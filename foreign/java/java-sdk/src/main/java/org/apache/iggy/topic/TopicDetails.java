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
import java.util.Optional;

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
                topic.messagesCount(),
                topic.partitionsCount(),
                partitions,
                topic.options(),
                topic.derivedOptions());
    }

    /** Message expiry in microseconds, {@code 2^64 - 1} meaning never. */
    public Optional<BigInteger> messageExpiry() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.MESSAGE_EXPIRY);
    }

    /** The stored compression label. */
    public Optional<CompressionAlgorithm> compressionAlgorithm() {
        return TopicSettings.compression(options, derivedOptions);
    }

    /** Topic size cap in bytes, {@code 2^64 - 1} meaning unlimited. */
    public Optional<BigInteger> maxTopicSize() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.MAX_TOPIC_SIZE);
    }

    /** Segment age roll in microseconds, {@code 2^64 - 1} meaning never. */
    public Optional<BigInteger> segmentMaxAge() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.SEGMENT_MAX_AGE);
    }

    /** Whether sealed segments move to the object store. */
    public Optional<Boolean> tieredStorage() {
        return TopicSettings.bool(options, derivedOptions, TopicOptionKey.TIERED_STORAGE);
    }

    /** How long an uploaded segment stays on local disk in microseconds, {@code 2^64 - 1} meaning never removed. */
    public Optional<BigInteger> localRetentionAge() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.LOCAL_RETENTION_AGE);
    }

    /** Cap in bytes on the uploaded segments kept on local disk, {@code 2^64 - 1} meaning unlimited. */
    public Optional<BigInteger> localRetentionSize() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.LOCAL_RETENTION_SIZE);
    }

    /** Whether the topic is normal or geo. */
    public Optional<TopicType> topicType() {
        return TopicSettings.topicType(options, derivedOptions);
    }

    /** How a geo topic writes its messages, empty on a normal topic. */
    public Optional<GeoMode> geoMode() {
        return TopicSettings.geoMode(options, derivedOptions);
    }

    /**
     * The longest time a send waits for its upload to start, in microseconds. This and the five durations below
     * are empty unless a geo topic sets them, and each node then applies its server setting of the same name.
     */
    public Optional<BigInteger> freshUploadInterval() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.FRESH_UPLOAD_INTERVAL);
    }

    /** The shortest time between two uploads of the positions that stored offsets wait for, in microseconds. */
    public Optional<BigInteger> consumerOffsetUploadInterval() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.CONSUMER_OFFSET_UPLOAD_INTERVAL);
    }

    /** How often a node uploads the changed positions of the topic's consumer groups, in microseconds. */
    public Optional<BigInteger> positionsPublishInterval() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.POSITIONS_PUBLISH_INTERVAL);
    }

    /** How long the deleted topic's tombstone and objects stay in the object store, in microseconds. */
    public Optional<BigInteger> tombstoneGrace() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.TOMBSTONE_GRACE);
    }

    /** How long the group polls wait for the positions of the geo peers after a node starts, in microseconds. */
    public Optional<BigInteger> positionsResumeTimeout() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.POSITIONS_RESUME_TIMEOUT);
    }

    /** The longest wait between two long-term rewrites of a partition, in microseconds. */
    public Optional<BigInteger> longTermRewriteMaxInterval() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.LONG_TERM_REWRITE_MAX_INTERVAL);
    }

    /**
     * The memory in bytes that each node keeps for what the topic's reads fetch from the object store, zero
     * keeping nothing. This and {@link #readAhead} are empty unless the topic sets them, and each node then
     * applies its {@code [tiered_storage]} setting of the same name.
     */
    public Optional<BigInteger> readCacheSize() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.READ_CACHE_SIZE);
    }

    /** The most bytes that readahead fetches in front of a sequential reader, zero meaning readahead is off. */
    public Optional<BigInteger> readAhead() {
        return TopicSettings.uint64(options, derivedOptions, TopicOptionKey.READ_AHEAD);
    }
}
