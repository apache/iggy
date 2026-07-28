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
import org.apache.iggy.client.blocking.IntegrationTest;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RawCommandTcpClientTest extends IntegrationTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    @BeforeEach
    void authenticate() {
        login();
    }

    @Test
    void shouldReturnRawResponsePayloads() {
        assertThat(client.sendBinaryRequest(1, new byte[0])).isEmpty();
        assertThat(client.sendBinaryRequest(10, new byte[0])).isNotEmpty();
    }

    @Test
    void shouldRejectNullPayload() {
        assertThatThrownBy(() -> client.sendBinaryRequest(1, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload cannot be null");
    }

    @Test
    void shouldRejectAllSessionControlCodes() {
        for (int code : new int[] {38, 39, 40, 44, 45}) {
            assertThatThrownBy(() -> client.sendBinaryRequest(code, new byte[0]))
                    .isInstanceOf(IggyServerException.class)
                    .extracting(exception -> ((IggyServerException) exception).getErrorCode())
                    .isEqualTo(IggyErrorCode.INVALID_COMMAND);
        }
    }

    @Test
    void shouldPropagateUnknownCommandError() {
        assertThatThrownBy(() -> client.sendBinaryRequest(60_000, new byte[0]))
                .isInstanceOf(IggyServerException.class)
                .extracting(exception -> ((IggyServerException) exception).getErrorCode())
                .isEqualTo(IggyErrorCode.INVALID_COMMAND);
    }
}
