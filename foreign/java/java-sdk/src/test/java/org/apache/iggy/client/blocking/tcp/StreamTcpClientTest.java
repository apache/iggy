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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.StreamClientBaseTest;
import org.apache.iggy.identifier.StreamId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StreamTcpClientTest extends StreamClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    /*
     * TCP only: the HTTP client does not percent-encode path segments yet, so
     * a non-ASCII name cannot be looked up over HTTP.
     */
    @Test
    void shouldCreateAndFetchStreamWithNonAsciiName() {
        // given
        var name = "strumień-世界";

        // when
        var streamDetails = client.streams().createStream(name);
        trackStream(streamDetails.id());
        var streamByName = client.streams().getStream(StreamId.of(name));

        // then
        assertThat(streamDetails.name()).isEqualTo(name);
        assertThat(streamByName).isPresent();
        assertThat(streamByName.get().id()).isEqualTo(streamDetails.id());
    }
}
