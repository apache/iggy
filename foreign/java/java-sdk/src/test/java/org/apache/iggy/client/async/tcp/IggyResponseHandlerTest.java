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

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.iggy.exception.IggyConnectionException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IggyResponseHandlerTest {

    @Test
    void shouldFailPendingRequestsWhenChannelBecomesInactive() {
        var handler = new AsyncTcpConnection.IggyResponseHandler();
        var channel = new EmbeddedChannel(handler);
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        handler.enqueueRequest(pending);

        channel.close();

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyConnectionException.class);
    }

    @Test
    void shouldFailPendingRequestsOnPipelineException() {
        var handler = new AsyncTcpConnection.IggyResponseHandler();
        var channel = new EmbeddedChannel(handler);
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        handler.enqueueRequest(pending);

        var failure = new IllegalStateException("broken pipe");
        channel.pipeline().fireExceptionCaught(failure);

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCause(failure);
    }
}
