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

package org.apache.iggy.client.blocking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class StreamClientBaseTest extends IntegrationTest {

    StreamsClient streamsClient;

    @BeforeEach
    void beforeEachBase() {
        streamsClient = client.streams();

        login();
    }

    @Test
    void shouldCreateAndDeleteStream() {
        // when
        var streamDetails = streamsClient.createStream("test-stream");
        trackStream(streamDetails.id());
        var streamOptional = streamsClient.getStream(streamDetails.id());

        // then
        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.name()).isEqualTo("test-stream");

        // when
        var streams = streamsClient.getStreams();

        // then
        assertThat(streams).hasSize(1);

        // when
        streamsClient.deleteStream(streamDetails.id());
        createdStreamIds.remove(streamDetails.id()); // Remove from tracking since we deleted it
        streams = streamsClient.getStreams();

        // then
        assertThat(streams).isEmpty();
    }

    @Test
    void shouldUpdateStream() {
        // given
        var streamDetails = streamsClient.createStream("test-stream");
        trackStream(streamDetails.id());

        // when
        streamsClient.updateStream(streamDetails.id(), "test-stream-new");

        // then
        var streamOptional = streamsClient.getStream(streamDetails.id());

        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.name()).isEqualTo("test-stream-new");
    }

    @Test
    void shouldReturnEmptyForNonExistingStream() {
        // when
        var stream = streamsClient.getStream(333L);

        // then
        assertThat(stream).isEmpty();
    }
}
