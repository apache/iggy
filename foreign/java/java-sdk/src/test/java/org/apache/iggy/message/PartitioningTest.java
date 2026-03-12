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

package org.apache.iggy.message;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitioningTest {

    @Test
    void balancedReturnsPartitioningWithBalancedTypeAndEmptyValue() {
        var result = Partitioning.balanced();

        assertThat(result.kind()).isEqualTo(PartitioningKind.Balanced);
        assertThat(result.value()).isEqualTo(new byte[] {});
    }

    @Test
    void partitionIdReturnsPartitioningWithPartitionIdKindAndExpectedValue() {
        var result = Partitioning.partitionId(1234L);

        assertThat(result.kind()).isEqualTo(PartitioningKind.PartitionId);
        assertThat(result.value()).isEqualTo(new byte[] {-46, 4, 0, 0});
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "    "})
    @NullSource
    void messagesKeyThrowsIggyInvalidArgumentExceptionWhenGivenBlankValues(String id) {
        assertThatThrownBy(() -> Partitioning.messagesKey(id)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void messagesKeyThrowsIggyInvalidArgumentExceptionWhenGivenValueThatIsTooLong() {
        var id = "0".repeat(256);
        assertThatThrownBy(() -> Partitioning.messagesKey(id)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void messagesKeyReturnsPartitioningWithMessagesKeyKindAndExpectedValueWhenGivenValidArguments() {
        var id = "the-key";
        var result = Partitioning.messagesKey(id);

        assertThat(result.kind()).isEqualTo(PartitioningKind.MessagesKey);
        assertThat(result.value()).isEqualTo(new byte[] {116, 104, 101, 45, 107, 101, 121});
    }

    @Test
    void getSizeReturnsLengthOfValuePlus2() {
        assertThat(Partitioning.balanced().getSize()).isEqualTo(2);
        assertThat(Partitioning.partitionId(12345678L).getSize()).isEqualTo(6); // 4 bytes for long ID
        assertThat(Partitioning.messagesKey("foo").getSize()).isEqualTo(5);
    }
}
