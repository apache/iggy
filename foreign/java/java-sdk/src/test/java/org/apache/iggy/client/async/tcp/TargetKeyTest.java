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

package org.apache.iggy.client.async.tcp;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TargetKeyTest {

    @Test
    void keysWithEqualBytesAreEqualWithSameHashCode() {
        var first = TargetKey.of(new byte[] {1, 2, 3, 4});
        var second = TargetKey.of(new byte[] {1, 2, 3, 4});

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }

    @Test
    void keysWithDifferentBytesAreNotEqual() {
        var first = TargetKey.of(new byte[] {1, 2, 3, 4});
        var differentContent = TargetKey.of(new byte[] {1, 2, 3, 5});
        var differentLength = TargetKey.of(new byte[] {1, 2, 3});

        assertThat(first).isNotEqualTo(differentContent);
        assertThat(first).isNotEqualTo(differentLength);
    }

    @Test
    void topicDropsTrailingPartitionIdBytes() {
        var streamAndTopic = new byte[] {1, 0, 0, 0, 2, 0, 0, 0};
        var partitionTarget = new byte[] {1, 0, 0, 0, 2, 0, 0, 0, 7, 0, 0, 0};

        var topicKey = TargetKey.of(partitionTarget).topic();
        var expected = TargetKey.of(streamAndTopic);

        assertThat(topicKey).isEqualTo(expected);
        assertThat(topicKey.hashCode()).isEqualTo(expected.hashCode());
    }

    @Test
    void startsWithMatchesPrefixAndSelfOnly() {
        var key = TargetKey.of(new byte[] {1, 2, 3, 4, 5, 6});
        var prefix = TargetKey.of(new byte[] {1, 2, 3});
        var self = TargetKey.of(new byte[] {1, 2, 3, 4, 5, 6});
        var longer = TargetKey.of(new byte[] {1, 2, 3, 4, 5, 6, 7});
        var differentPrefix = TargetKey.of(new byte[] {1, 9, 3});

        assertThat(key.startsWith(prefix)).isTrue();
        assertThat(key.startsWith(self)).isTrue();
        assertThat(key.startsWith(longer)).isFalse();
        assertThat(key.startsWith(differentPrefix)).isFalse();
    }

    @Test
    void equalsReturnsFalseForNullAndOtherTypes() {
        var key = TargetKey.of(new byte[] {1, 2, 3, 4});

        assertThat(key.equals(null)).isFalse();
        assertThat(key.equals("other")).isFalse();
    }
}
