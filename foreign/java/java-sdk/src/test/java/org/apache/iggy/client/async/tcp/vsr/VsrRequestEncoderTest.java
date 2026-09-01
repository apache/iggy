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

package org.apache.iggy.client.async.tcp.vsr;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrRequestEncoderTest {

    private static final int LOGIN_USER_CODE = 38;
    private static final int PING_CODE = 1;
    private static final int GET_CLUSTER_METADATA_CODE = 12;
    private static final int CREATE_STREAM_CODE = 202;
    private static final int SEND_MESSAGES_CODE = 101;

    private final ConsensusSession session = new ConsensusSession();
    private final VsrRequestEncoder encoder = new VsrRequestEncoder(session);
    private final ByteBufAllocator alloc = ByteBufAllocator.DEFAULT;

    @Test
    void shouldBuildRegisterFrameForLogin() {
        ByteBuf payload = loginUserPayload();
        ByteBuf frame = encoder.encode(alloc, LOGIN_USER_CODE, payload);
        payload.release();

        try {
            assertThat(frame.getUnsignedByte(VsrHeaders.COMMAND_OFFSET)).isEqualTo((short) VsrHeaders.COMMAND_REQUEST);
            assertThat(frame.getUnsignedByte(VsrHeaders.REQUEST_OPERATION_OFFSET))
                    .isEqualTo((short) VsrOperation.REGISTER);
            assertThat(frame.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isZero();
            assertThat(frame.getLongLE(VsrHeaders.REQUEST_SESSION_OFFSET)).isZero();
            assertThat(frame.getUnsignedIntLE(VsrHeaders.SIZE_OFFSET)).isEqualTo(frame.readableBytes());

            // Body starts with the ClientVersionInfo prefix.
            int bodyStart = VsrHeaders.HEADER_SIZE;
            assertThat(frame.getIntLE(bodyStart)).isEqualTo(VsrLoginCodec.PROTOCOL_VERSION);
            int sdkNameLength = frame.getUnsignedByte(bodyStart + 4);
            byte[] sdkName = new byte[sdkNameLength];
            frame.getBytes(bodyStart + 5, sdkName);
            assertThat(new String(sdkName, StandardCharsets.UTF_8)).isEqualTo(VsrLoginCodec.SDK_NAME);
        } finally {
            frame.release();
        }
    }

    @Test
    void shouldStampCodeAndZeroSessionForPing() {
        ByteBuf frame = encoder.encode(alloc, PING_CODE, Unpooled.EMPTY_BUFFER);
        try {
            assertThat(frame.getUnsignedByte(VsrHeaders.REQUEST_OPERATION_OFFSET))
                    .isEqualTo((short) VsrOperation.NON_REPLICATED);
            assertThat(frame.getIntLE(VsrHeaders.REQUEST_RESERVED_CODE_OFFSET)).isEqualTo(PING_CODE);
            assertThat(frame.getLongLE(VsrHeaders.REQUEST_SESSION_OFFSET)).isZero();
        } finally {
            frame.release();
        }
    }

    @Test
    void shouldKeepPreAuthClusterMetadataSessionlessWithoutAdvancingRequestId() {
        ByteBuf frame = encoder.encode(alloc, GET_CLUSTER_METADATA_CODE, Unpooled.EMPTY_BUFFER);
        try {
            assertThat(frame.getUnsignedByte(VsrHeaders.REQUEST_OPERATION_OFFSET))
                    .isEqualTo((short) VsrOperation.NON_REPLICATED);
            assertThat(frame.getIntLE(VsrHeaders.REQUEST_RESERVED_CODE_OFFSET)).isEqualTo(GET_CLUSTER_METADATA_CODE);
            assertThat(frame.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(1);
            assertThat(frame.getLongLE(VsrHeaders.REQUEST_SESSION_OFFSET)).isZero();
            assertThat(session.currentRequestId()).isEqualTo(1);
        } finally {
            frame.release();
        }
    }

    @Test
    void shouldRejectReplicatedCommandBeforeLogin() {
        assertThatThrownBy(() -> encoder.encode(alloc, CREATE_STREAM_CODE, Unpooled.EMPTY_BUFFER))
                .isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void shouldAdvanceRequestIdsForReplicatedCommands() {
        session.beginRegister();
        session.bind(42);

        ByteBuf first = encoder.encode(alloc, CREATE_STREAM_CODE, Unpooled.EMPTY_BUFFER);
        ByteBuf second = encoder.encode(alloc, CREATE_STREAM_CODE, Unpooled.EMPTY_BUFFER);
        try {
            assertThat(first.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(1);
            assertThat(second.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(2);
            assertThat(second.getLongLE(VsrHeaders.REQUEST_SESSION_OFFSET)).isEqualTo(42);
        } finally {
            first.release();
            second.release();
        }
    }

    @Test
    void shouldConsumeTheDedupRequestIdForPartitionOps() {
        // Dedup identity requires each send to carry a distinct number, so
        // partition ops advance the dedup counter exactly like metadata ops
        // and the two planes interleave on one sequence.
        session.beginRegister();
        session.bind(42);

        ByteBuf firstPayload = sendMessagesPayload(2, 3, 4);
        ByteBuf secondPayload = sendMessagesPayload(2, 3, 4);
        ByteBuf first = encoder.encode(alloc, SEND_MESSAGES_CODE, firstPayload);
        ByteBuf second = encoder.encode(alloc, SEND_MESSAGES_CODE, secondPayload);
        firstPayload.release();
        secondPayload.release();
        ByteBuf metadata = encoder.encode(alloc, CREATE_STREAM_CODE, Unpooled.EMPTY_BUFFER);
        try {
            assertThat(first.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(1);
            assertThat(second.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(2);
            assertThat(metadata.getLongLE(VsrHeaders.REQUEST_ID_OFFSET)).isEqualTo(3);
            assertThat(session.currentRequestId()).isEqualTo(4);
        } finally {
            first.release();
            second.release();
            metadata.release();
        }
    }

    @Test
    void shouldPassPartitioningThroughForTheServerToResolve() {
        // The client no longer inspects the payload to route: balanced partitioning
        // reaches the server byte-identical instead of failing at encode time.
        session.beginRegister();
        session.bind(42);

        ByteBuf payload = balancedSendMessagesPayload(2, 3);
        ByteBuf frame = encoder.encode(alloc, SEND_MESSAGES_CODE, payload);
        try {
            assertThat(frame.getUnsignedIntLE(VsrHeaders.SIZE_OFFSET)).isEqualTo(frame.readableBytes());
            byte[] encodedBody = new byte[payload.readableBytes()];
            frame.getBytes(VsrHeaders.HEADER_SIZE, encodedBody);
            byte[] originalBody = new byte[payload.readableBytes()];
            payload.getBytes(payload.readerIndex(), originalBody);
            assertThat(encodedBody).isEqualTo(originalBody);
        } finally {
            frame.release();
            payload.release();
        }
    }

    @Test
    void shouldReArmWithFreshClientIdOnSecondLogin() {
        ByteBuf firstLogin = loginUserPayload();
        encoder.encode(alloc, LOGIN_USER_CODE, firstLogin).release();
        firstLogin.release();
        long firstLow = session.clientIdLow();
        long firstHigh = session.clientIdHigh();
        session.bind(7);

        ByteBuf secondLogin = loginUserPayload();
        encoder.encode(alloc, LOGIN_USER_CODE, secondLogin).release();
        secondLogin.release();

        assertThat(session.isBound()).isFalse();
        assertThat(session.clientIdLow() != firstLow || session.clientIdHigh() != firstHigh)
                .isTrue();
    }

    private static ByteBuf loginUserPayload() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeIntLE(0);
        payload.writeIntLE(0);
        return payload;
    }

    private static ByteBuf sendMessagesPayload(long streamId, long topicId, long partitionId) {
        ByteBuf payload = Unpooled.buffer();
        // metadata: stream ident (6) + topic ident (6) + partitioning (6) + count (4)
        payload.writeIntLE(22);
        writeNumericIdentifier(payload, streamId);
        writeNumericIdentifier(payload, topicId);
        payload.writeByte(2);
        payload.writeByte(4);
        payload.writeIntLE((int) partitionId);
        payload.writeIntLE(0);
        return payload;
    }

    private static ByteBuf balancedSendMessagesPayload(long streamId, long topicId) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeIntLE(18);
        writeNumericIdentifier(payload, streamId);
        writeNumericIdentifier(payload, topicId);
        payload.writeByte(1);
        payload.writeByte(0);
        payload.writeIntLE(0);
        return payload;
    }

    private static void writeNumericIdentifier(ByteBuf payload, long id) {
        payload.writeByte(1);
        payload.writeByte(4);
        payload.writeIntLE((int) id);
    }
}
