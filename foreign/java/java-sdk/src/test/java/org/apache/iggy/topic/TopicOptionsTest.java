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

import org.apache.iggy.message.HeaderKind;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TopicOptionsTest {

    @Test
    void shouldEmitOnlyTheKeysThatWereSet() {
        var options = TopicOptions.builder().enforceFsync(true).build();

        assertThat(options).containsOnlyKeys("enforce_fsync");
        assertThat(options.get("enforce_fsync").kind()).isEqualTo(HeaderKind.Bool);
        assertThat(options.get("enforce_fsync").value()).containsExactly(1);
    }

    @Test
    void shouldEncodeEveryKeyInItsCatalogKind() {
        var options = TopicOptions.builder()
                .segmentSize(BigInteger.valueOf(134_217_728))
                .enforceFsync(false)
                .messagesRequiredToSave(1024)
                .sizeOfMessagesRequiredToSave(BigInteger.valueOf(1_048_576))
                .preallocateSegments(true)
                .build();

        assertThat(options)
                .containsOnlyKeys(
                        "segment_size",
                        "enforce_fsync",
                        "messages_required_to_save",
                        "size_of_messages_required_to_save",
                        "preallocate_segments");
        assertThat(options.get("segment_size").kind()).isEqualTo(HeaderKind.Uint64);
        assertThat(options.get("messages_required_to_save").kind()).isEqualTo(HeaderKind.Uint32);
        assertThat(options.get("size_of_messages_required_to_save").kind()).isEqualTo(HeaderKind.Uint64);
        assertThat(options.get("preallocate_segments").kind()).isEqualTo(HeaderKind.Bool);
        // Little-endian, so the low byte of 128 MiB leads and the high bytes are zero.
        assertThat(options.get("segment_size").value()).containsExactly(0, 0, 0, 8, 0, 0, 0, 0);
    }
}
