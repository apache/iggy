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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetConfigTest {

    @Test
    void shouldBuildValidConfig() {
        OffsetConfig config = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                .commitInterval(Duration.ofSeconds(10))
                .autoCommit(false)
                .commitBatchSize(200)
                .build();

        assertThat(config.getResetStrategy()).isEqualTo(OffsetConfig.OffsetResetStrategy.EARLIEST);
        assertThat(config.getCommitInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.isAutoCommit()).isFalse();
        assertThat(config.getCommitBatchSize()).isEqualTo(200);
    }

    @Test
    void shouldUseDefaultValues() {
        OffsetConfig config = OffsetConfig.builder().build();

        assertThat(config.getResetStrategy()).isEqualTo(OffsetConfig.OffsetResetStrategy.LATEST);
        assertThat(config.getCommitInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.isAutoCommit()).isTrue();
        assertThat(config.getCommitBatchSize()).isEqualTo(100);
    }

    @Test
    void shouldSupportAllResetStrategies() {
        OffsetConfig earliestConfig = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                .build();
        assertThat(earliestConfig.getResetStrategy()).isEqualTo(OffsetConfig.OffsetResetStrategy.EARLIEST);

        OffsetConfig latestConfig = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.LATEST)
                .build();
        assertThat(latestConfig.getResetStrategy()).isEqualTo(OffsetConfig.OffsetResetStrategy.LATEST);

        OffsetConfig noneConfig = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.NONE)
                .build();
        assertThat(noneConfig.getResetStrategy()).isEqualTo(OffsetConfig.OffsetResetStrategy.NONE);
    }

    @Test
    void shouldThrowExceptionWhenResetStrategyIsNull() {
        assertThatThrownBy(() -> OffsetConfig.builder().resetStrategy(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("resetStrategy must not be null");
    }

    @Test
    void shouldThrowExceptionWhenCommitIntervalIsNull() {
        assertThatThrownBy(() -> OffsetConfig.builder().commitInterval(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("commitInterval must not be null");
    }

    @Test
    void shouldThrowExceptionWhenCommitBatchSizeIsZero() {
        assertThatThrownBy(() -> OffsetConfig.builder().commitBatchSize(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commitBatchSize must be positive");
    }

    @Test
    void shouldThrowExceptionWhenCommitBatchSizeIsNegative() {
        assertThatThrownBy(() -> OffsetConfig.builder().commitBatchSize(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commitBatchSize must be positive");
    }

    @Test
    void shouldAcceptMinimalCommitBatchSize() {
        OffsetConfig config = OffsetConfig.builder().commitBatchSize(1).build();

        assertThat(config.getCommitBatchSize()).isEqualTo(1);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        OffsetConfig config1 = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                .commitInterval(Duration.ofSeconds(10))
                .autoCommit(false)
                .commitBatchSize(200)
                .build();

        OffsetConfig config2 = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                .commitInterval(Duration.ofSeconds(10))
                .autoCommit(false)
                .commitBatchSize(200)
                .build();

        OffsetConfig config3 = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.LATEST)
                .build();

        assertThat(config1).isEqualTo(config2);
        assertThat(config1).hasSameHashCodeAs(config2);
        assertThat(config1).isNotEqualTo(config3);
        assertThat(config1).isEqualTo(config1);
        assertThat(config1).isNotEqualTo(null);
        assertThat(config1).isNotEqualTo(new Object());
    }

    @Test
    void shouldImplementToStringCorrectly() {
        OffsetConfig config = OffsetConfig.builder()
                .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                .commitInterval(Duration.ofSeconds(10))
                .autoCommit(false)
                .commitBatchSize(200)
                .build();

        String toString = config.toString();
        assertThat(toString).contains("OffsetConfig");
        assertThat(toString).contains("EARLIEST");
        assertThat(toString).contains("autoCommit=false");
        assertThat(toString).contains("commitBatchSize=200");
    }

    @Test
    void shouldBeSerializable() {
        OffsetConfig config = OffsetConfig.builder().build();
        assertThat(config).isInstanceOf(java.io.Serializable.class);
    }
}
