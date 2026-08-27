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

import org.apache.iggy.exception.IggyNotConnectedException;

import java.security.SecureRandom;

/**
 * VSR client identity and dedup state, mirroring
 * {@code core/sdk/src/session.rs}.
 *
 * <p>The (client id, request id) pair is the server's dedup key for
 * replicated operations, and the session value is the fence epoch of the
 * latest committed {@code Register}. None of these are bearer tokens; auth
 * is bound to the transport connection server-side.
 */
public final class ConsensusSession {

    private static final SecureRandom RANDOM = new SecureRandom();

    private long clientIdLow;
    private long clientIdHigh;
    private Long session;
    private long requestCounter = 1;
    private long correlationCounter = 1;
    private boolean registerConsumed;

    public ConsensusSession() {
        regenerateClientId();
    }

    /**
     * Arms a {@code Register}: on re-login (or a consumed one-shot register)
     * the whole identity re-arms with a fresh client id so the server sees a
     * brand-new registration. Returns the request id a Register carries,
     * which is always zero.
     */
    synchronized long beginRegister() {
        if (registerConsumed || session != null) {
            regenerateClientId();
            session = null;
            requestCounter = 1;
        }
        registerConsumed = true;
        return 0;
    }

    /** Binds the fence epoch returned by a committed Register reply. */
    synchronized void bind(long sessionEpoch) {
        if (sessionEpoch <= 0) {
            throw new IllegalStateException("Register reply carried a non-positive session epoch: " + sessionEpoch);
        }
        this.session = sessionEpoch;
    }

    /**
     * Replicated ops (metadata and partition) consume the monotonic VSR dedup
     * counter. The wire field is a u64 but Java has no unsigned long, so the
     * counter is refused at {@link Long#MAX_VALUE} rather than wrapping
     * negative and sending an id below the server's watermark.
     */
    synchronized long nextRequestId() {
        if (session == null) {
            throw new IggyNotConnectedException("Not authenticated, call login first");
        }
        if (requestCounter == Long.MAX_VALUE) {
            throw new IllegalStateException("VSR request counter exhausted, reconnect to register a fresh session");
        }
        return requestCounter++;
    }

    /**
     * Non-replicated ops use an independent sequence for reply correlation,
     * so they do not create gaps in the dedup sequence.
     */
    synchronized long nextCorrelationId() {
        return correlationCounter++;
    }

    synchronized long currentRequestId() {
        return requestCounter;
    }

    synchronized long sessionOrZero() {
        return session == null ? 0 : session;
    }

    synchronized long boundSession() {
        if (session == null) {
            throw new IggyNotConnectedException("Not authenticated, call login first");
        }
        return session;
    }

    synchronized boolean isBound() {
        return session != null;
    }

    /** Clears the bound epoch (logout / eviction); next login re-registers. */
    synchronized void reset() {
        session = null;
    }

    synchronized long clientIdLow() {
        return clientIdLow;
    }

    synchronized long clientIdHigh() {
        return clientIdHigh;
    }

    private void regenerateClientId() {
        do {
            clientIdLow = RANDOM.nextLong();
            clientIdHigh = RANDOM.nextLong();
        } while (clientIdLow == 0 && clientIdHigh == 0);
    }
}
