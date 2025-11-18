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

package org.apache.iggy.connector.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for offset management strategies.
 * Defines how offsets should be initialized, committed, and tracked.
 */
public final class OffsetConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final OffsetResetStrategy resetStrategy;
    private final Duration commitInterval;
    private final boolean autoCommit;
    private final int commitBatchSize;

    private OffsetConfig(
            OffsetResetStrategy resetStrategy, Duration commitInterval, boolean autoCommit, int commitBatchSize) {
        this.resetStrategy = Objects.requireNonNull(resetStrategy, "resetStrategy must not be null");
        this.commitInterval = Objects.requireNonNull(commitInterval, "commitInterval must not be null");
        this.autoCommit = autoCommit;
        this.commitBatchSize = commitBatchSize;

        if (commitBatchSize <= 0) {
            throw new IllegalArgumentException("commitBatchSize must be positive");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public OffsetResetStrategy getResetStrategy() {
        return resetStrategy;
    }

    public Duration getCommitInterval() {
        return commitInterval;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getCommitBatchSize() {
        return commitBatchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetConfig that = (OffsetConfig) o;
        return autoCommit == that.autoCommit
                && commitBatchSize == that.commitBatchSize
                && resetStrategy == that.resetStrategy
                && Objects.equals(commitInterval, that.commitInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resetStrategy, commitInterval, autoCommit, commitBatchSize);
    }

    @Override
    public String toString() {
        return "OffsetConfig{"
                + "resetStrategy=" + resetStrategy
                + ", commitInterval=" + commitInterval
                + ", autoCommit=" + autoCommit
                + ", commitBatchSize=" + commitBatchSize
                + '}';
    }

    /**
     * Strategy for resetting offsets when no valid offset is found.
     */
    public enum OffsetResetStrategy {
        /** Start reading from the earliest available offset. */
        EARLIEST,
        /** Start reading from the latest offset (skip existing messages). */
        LATEST,
        /** Fail if no valid offset is found. */
        NONE
    }

    /**
     * Builder for {@link OffsetConfig}.
     */
    public static final class Builder {
        private OffsetResetStrategy resetStrategy = OffsetResetStrategy.LATEST;
        private Duration commitInterval = Duration.ofSeconds(5);
        private boolean autoCommit = true;
        private int commitBatchSize = 100;

        private Builder() {}

        public Builder resetStrategy(OffsetResetStrategy resetStrategy) {
            this.resetStrategy = resetStrategy;
            return this;
        }

        public Builder commitInterval(Duration commitInterval) {
            this.commitInterval = commitInterval;
            return this;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder commitBatchSize(int commitBatchSize) {
            this.commitBatchSize = commitBatchSize;
            return this;
        }

        public OffsetConfig build() {
            return new OffsetConfig(resetStrategy, commitInterval, autoCommit, commitBatchSize);
        }
    }
}
