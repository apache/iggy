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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.serde.CommandCode;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * The login that registers a session again. A connection that binds the session of another connection holds
 * the instance of that connection, so a committed credential change on any of them reaches all of them, and
 * none of them signs in with an old copy.
 */
final class SessionLogin {
    // Flags the optional username of an UpdateUser body.
    private static final int USERNAME_PRESENT = 1;

    private int commandCode;
    private byte[] payload;
    private long version;

    synchronized void set(int commandCode, byte[] payload) {
        this.commandCode = commandCode;
        this.payload = payload;
        version++;
    }

    synchronized void clear() {
        payload = null;
        version++;
    }

    synchronized boolean clear(long attemptedVersion) {
        if (version != attemptedVersion) {
            return false;
        }
        clear();
        return true;
    }

    synchronized long version() {
        return version;
    }

    synchronized Optional<AsyncTcpConnection.AuthenticationSnapshot> snapshot() {
        return payload == null
                ? Optional.empty()
                : Optional.of(new AsyncTcpConnection.AuthenticationSnapshot(
                        commandCode, Unpooled.wrappedBuffer(payload), version));
    }

    /**
     * Applies a committed password change or rename of the signed-in user {@code userId}, which the request
     * {@code frame} of {@code requestCode} made, to this login. A body that does not decode changes nothing.
     *
     * @return the change, or empty when the request leaves this login as it is
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    synchronized Optional<Change> refresh(int requestCode, ByteBuf frame, long userId) {
        boolean passwordChange = requestCode == CommandCode.User.CHANGE_PASSWORD.getValue();
        if ((!passwordChange && requestCode != CommandCode.User.UPDATE.getValue())
                || payload == null
                || commandCode != CommandCode.User.LOGIN.getValue()) {
            return Optional.empty();
        }
        ByteBuf change = frame.slice(
                frame.readerIndex() + VsrHeaders.HEADER_SIZE, frame.readableBytes() - VsrHeaders.HEADER_SIZE);
        ByteBuf current = Unpooled.wrappedBuffer(payload);
        String oldUsername = current.readCharSequence(current.readUnsignedByte(), StandardCharsets.UTF_8)
                .toString();
        if (!skipUser(change, UserId.of(userId))
                // UserId refuses a blank name, and no request can name one.
                && (StringUtils.isBlank(oldUsername) || !skipUser(change, UserId.of(oldUsername)))) {
            return Optional.empty();
        }
        Optional<String> username = Optional.empty();
        Optional<String> password = Optional.empty();
        if (passwordChange) {
            password = readField(change).flatMap(currentPassword -> readField(change));
        } else if (change.isReadable() && change.readUnsignedByte() == USERNAME_PRESENT) {
            username = readField(change);
        }
        if (username.isEmpty() && password.isEmpty()) {
            return Optional.empty();
        }
        String oldPassword = current.readCharSequence(current.readUnsignedByte(), StandardCharsets.UTF_8)
                .toString();
        byte[] nextUsername = username.orElse(oldUsername).getBytes(StandardCharsets.UTF_8);
        byte[] nextPassword = password.orElse(oldPassword).getBytes(StandardCharsets.UTF_8);
        byte[] next = new byte[2 + nextUsername.length + nextPassword.length];
        Unpooled.wrappedBuffer(next)
                .clear()
                .writeByte(nextUsername.length)
                .writeBytes(nextUsername)
                .writeByte(nextPassword.length)
                .writeBytes(nextPassword);
        payload = next;
        version++;
        return Optional.of(new Change(oldUsername, username, password));
    }

    /** Skips the user identifier at the start of {@code change} when it names {@code user}. */
    private static boolean skipUser(ByteBuf change, UserId user) {
        ByteBuf encoded = user.toBytes();
        try {
            int length = encoded.readableBytes();
            if (change.readableBytes() < length
                    || !ByteBufUtil.equals(change, change.readerIndex(), encoded, 0, length)) {
                return false;
            }
            change.skipBytes(length);
            return true;
        } finally {
            encoded.release();
        }
    }

    /** Reads a u8-length-prefixed UTF-8 field, or nothing when the body ends first. */
    private static Optional<String> readField(ByteBuf body) {
        if (!body.isReadable() || body.readableBytes() <= body.getUnsignedByte(body.readerIndex())) {
            return Optional.empty();
        }
        return Optional.of(body.readCharSequence(body.readUnsignedByte(), StandardCharsets.UTF_8)
                .toString());
    }

    /** A credential change that {@link #refresh} applied, with the username that the login had before it. */
    record Change(String previousUsername, Optional<String> username, Optional<String> password) {}
}
