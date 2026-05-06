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

package org.apache.iggy.bench.models.cli;

import org.apache.iggy.bench.common.exception.BenchmarkException;

public record GlobalCliArgs(
        int messageSize,
        int messagesPerBatch,
        int messageBatches,
        long totalData,
        long rateLimit,
        long warmupTimeMs,
        long samplingTimeMs,
        int movingAverageWindow,
        String username,
        String password,
        boolean reuseStreams) {

    public void validate() {
        validateCredentials();
        validateMessageSettings();
        validateTimingSettings();
        validateDataSettings();
    }

    private void validateCredentials() {
        if (username == null || username.isBlank()) {
            throw new BenchmarkException("--username cannot be blank.");
        }
        if (password == null || password.isBlank()) {
            throw new BenchmarkException("--password cannot be blank.");
        }
    }

    private void validateMessageSettings() {
        if (messageSize <= 0) {
            throw new BenchmarkException("--message-size must be greater than 0.");
        }
        if (messagesPerBatch <= 0) {
            throw new BenchmarkException("--messages-per-batch must be greater than 0.");
        }
        if (rateLimit < 0) {
            throw new BenchmarkException("--rate-limit must be greater than or equal to 0.");
        }
    }

    private void validateTimingSettings() {
        if (warmupTimeMs < 0) {
            throw new BenchmarkException("--warmup-time must be greater than or equal to 0.");
        }
        if (samplingTimeMs <= 0) {
            throw new BenchmarkException("--sampling-time must be greater than 0.");
        }
        if (movingAverageWindow <= 0) {
            throw new BenchmarkException("--moving-average-window must be greater than 0.");
        }
    }

    private void validateDataSettings() {
        if (totalData < 0) {
            throw new BenchmarkException("--total-data must be greater than or equal to 0.");
        }
        if (totalData == 0 && messageBatches <= 0) {
            throw new BenchmarkException("--message-batches must be greater than 0.");
        }
    }
}
