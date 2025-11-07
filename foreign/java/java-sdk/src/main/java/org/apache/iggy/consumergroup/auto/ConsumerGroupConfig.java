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

package org.apache.iggy.consumergroup.auto;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Configuration for automatic consumer group management.
 */
public class ConsumerGroupConfig {
    private final Duration sessionTimeout;
    private final Duration heartbeatInterval;
    private final RebalanceStrategy rebalanceStrategy;
    private final Consumer<List<Long>> onPartitionsAssigned;
    private final Consumer<List<Long>> onPartitionsRevoked;
    private final boolean autoCommitEnabled;
    private final Duration autoCommitInterval;
    private final boolean createIfNotExists;

    private ConsumerGroupConfig(
            Duration sessionTimeout,
            Duration heartbeatInterval,
            RebalanceStrategy rebalanceStrategy,
            Consumer<List<Long>> onPartitionsAssigned,
            Consumer<List<Long>> onPartitionsRevoked,
            boolean autoCommitEnabled,
            Duration autoCommitInterval,
            boolean createIfNotExists) {
        this.sessionTimeout = sessionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.rebalanceStrategy = rebalanceStrategy;
        this.onPartitionsAssigned = onPartitionsAssigned;
        this.onPartitionsRevoked = onPartitionsRevoked;
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitInterval = autoCommitInterval;
        this.createIfNotExists = createIfNotExists;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public RebalanceStrategy getRebalanceStrategy() {
        return rebalanceStrategy;
    }

    public Consumer<List<Long>> getOnPartitionsAssigned() {
        return onPartitionsAssigned;
    }

    public Consumer<List<Long>> getOnPartitionsRevoked() {
        return onPartitionsRevoked;
    }

    public boolean isAutoCommitEnabled() {
        return autoCommitEnabled;
    }

    public Duration getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public boolean isCreateIfNotExists() {
        return createIfNotExists;
    }

    public static class Builder {
        private Duration sessionTimeout = Duration.ofSeconds(30);
        private Duration heartbeatInterval = Duration.ofSeconds(10);
        private RebalanceStrategy rebalanceStrategy = RebalanceStrategy.ROUND_ROBIN;
        private Consumer<List<Long>> onPartitionsAssigned = partitions -> {};
        private Consumer<List<Long>> onPartitionsRevoked = partitions -> {};
        private boolean autoCommitEnabled = true;
        private Duration autoCommitInterval = Duration.ofSeconds(5);
        private boolean createIfNotExists = true;

        private Builder() {}

        public Builder sessionTimeout(Duration sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder rebalanceStrategy(RebalanceStrategy rebalanceStrategy) {
            this.rebalanceStrategy = rebalanceStrategy;
            return this;
        }

        public Builder onPartitionsAssigned(Consumer<List<Long>> onPartitionsAssigned) {
            this.onPartitionsAssigned = onPartitionsAssigned;
            return this;
        }

        public Builder onPartitionsRevoked(Consumer<List<Long>> onPartitionsRevoked) {
            this.onPartitionsRevoked = onPartitionsRevoked;
            return this;
        }

        public Builder autoCommit(boolean autoCommitEnabled) {
            this.autoCommitEnabled = autoCommitEnabled;
            return this;
        }

        public Builder autoCommitInterval(Duration autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            return this;
        }

        public Builder createIfNotExists(boolean createIfNotExists) {
            this.createIfNotExists = createIfNotExists;
            return this;
        }

        public ConsumerGroupConfig build() {
            return new ConsumerGroupConfig(
                    sessionTimeout,
                    heartbeatInterval,
                    rebalanceStrategy,
                    onPartitionsAssigned,
                    onPartitionsRevoked,
                    autoCommitEnabled,
                    autoCommitInterval,
                    createIfNotExists
            );
        }
    }
}