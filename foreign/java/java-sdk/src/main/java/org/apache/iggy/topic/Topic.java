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

import java.math.BigInteger;
import java.util.Map;

/**
 * A topic as the server reports it.
 *
 * <p>{@code options} carries the keys the creating client set explicitly;
 * {@code derivedOptions} carries the values admission resolved for the keys it did not.
 * Both are keyed by option name, and the split is the provenance: a value in
 * {@code derivedOptions} would have resolved differently under another server config.
 * The fixed fields above repeat three of those keys and stay for compatibility.
 */
public record Topic(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messageExpiry,
        CompressionAlgorithm compressionAlgorithm,
        BigInteger maxTopicSize,
        BigInteger messagesCount,
        Long partitionsCount,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {}
