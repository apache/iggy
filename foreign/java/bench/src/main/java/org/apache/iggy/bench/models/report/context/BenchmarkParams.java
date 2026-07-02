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

package org.apache.iggy.bench.models.report.context;

import org.apache.iggy.bench.common.enums.BenchmarkKind;
import org.apache.iggy.bench.common.enums.TransportKind;

import java.util.Objects;
import java.util.Optional;

public record BenchmarkParams(
        BenchmarkKind benchmarkKind,
        TransportKind transport,
        String serverAddress,
        Optional<String> remark,
        Optional<String> extraInfo,
        Optional<String> gitref,
        Optional<String> gitrefDate,
        int messagesPerBatch,
        long messageBatches,
        int messageSize,
        int producers,
        int consumers,
        int streams,
        int partitions,
        int consumerGroups,
        Optional<String> rateLimit,
        String prettyName,
        String benchCommand,
        String paramsIdentifier) {

    public BenchmarkParams {
        benchmarkKind = Objects.requireNonNull(benchmarkKind, "benchmarkKind");
        transport = Objects.requireNonNull(transport, "transport");
        serverAddress = Objects.requireNonNull(serverAddress, "serverAddress");
        remark = Objects.requireNonNull(remark, "remark");
        extraInfo = Objects.requireNonNull(extraInfo, "extraInfo");
        gitref = Objects.requireNonNull(gitref, "gitref");
        gitrefDate = Objects.requireNonNull(gitrefDate, "gitrefDate");
        rateLimit = Objects.requireNonNull(rateLimit, "rateLimit");
        prettyName = Objects.requireNonNull(prettyName, "prettyName");
        benchCommand = Objects.requireNonNull(benchCommand, "benchCommand");
        paramsIdentifier = Objects.requireNonNull(paramsIdentifier, "paramsIdentifier");
    }
}
