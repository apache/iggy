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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RetireGeoOrigin wire format: {@code [origin_len:u8][origin:N]}, replicated as a metadata operation with an empty
 * reply.
 */
class AsyncIggyTcpClientRetireGeoOriginTest {

    @Test
    void shouldSendTheOriginAsALengthPrefixedNameInAMetadataOperation() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> retires = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.RETIRE_GEO_ORIGIN) {
                    retires.add(request);
                    // A committed metadata reply leads with its result count, none for success.
                    return Response.committed(
                            VsrOperation.RETIRE_GEO_ORIGIN, 1, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                client.system().retireGeoOrigin("dc-a").get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);

            byte[] origin = "dc-a".getBytes(StandardCharsets.UTF_8);
            byte[] expected = new byte[1 + origin.length];
            expected[0] = (byte) origin.length;
            System.arraycopy(origin, 0, expected, 1, origin.length);
            assertThat(retires).singleElement().extracting(Request::body).isEqualTo(expected);
        }
    }

    @Test
    void shouldRefuseAnOriginNoNameCanHoldBeforeSendingIt() {
        SystemTcpClient system = new SystemTcpClient(() -> {
            throw new AssertionError("a refused origin needs no connection");
        });
        for (String origin : List.of("", "o".repeat(256))) {
            assertThatThrownBy(() -> system.retireGeoOrigin(origin))
                    .as("origin of %d bytes", origin.length())
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }
    }
}
