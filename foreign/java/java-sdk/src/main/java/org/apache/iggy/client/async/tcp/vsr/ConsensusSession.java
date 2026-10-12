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

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyNotConnectedException;

import java.security.SecureRandom;

/**
 * VSR client identity and dedup state, mirroring
 * {@code core/sdk/src/session.rs}.
 *
 * <p>The (client id, request id) pair is the server's dedup key for
 * replicated operations, and the session value is the fence epoch of the
 * latest committed {@code Register}. The bind secret is a bearer credential
 * that authenticates another connection to this identity. Keep it private to
 * the client and never log or expose it.
 */
public final class ConsensusSession {

    static final int BIND_SECRET_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();
    private final byte[] bindSecret = new byte[BIND_SECRET_BYTES];

    private long clientIdLow;
    private long clientIdHigh;
    private Long session;
    private long requestCounter = 1;
    private long correlationCounter = 1;
    private long generation;
    private long metadataWatermark;
    private boolean shared;
    private ConsensusSession parent;

    public ConsensusSession() {
        regenerateClientId();
    }

    /**
     * Returns the zero request id of Register without changing its identity.
     * Repeating a registration after a lost reply must resolve the same epoch.
     *
     * <p>The request counter survives explicit resets too, so late replies
     * cannot collide with pending requests from a replacement identity.
     */
    synchronized long beginRegister() {
        return 0;
    }

    /** Binds the fence epoch returned by a committed Register reply. */
    synchronized void bind(long sessionEpoch) {
        if (sessionEpoch == 0) {
            throw new IllegalStateException("Register reply carried a zero session epoch");
        }
        if (session != null && session != sessionEpoch) {
            throw new IllegalStateException("Register reply changed the bound session epoch");
        }
        this.session = sessionEpoch;
        generation++;
    }

    /**
     * Replicated ops (metadata and partition) consume the monotonic VSR dedup
     * counter. The wire field is a u64 but Java has no unsigned long, so the
     * counter is refused at {@link Long#MAX_VALUE} rather than wrapping
     * negative and sending an id below the server's watermark.
     *
     * <p>Exhaustion is terminal for this instance. {@link #beginRegister()}
     * deliberately carries the counter across a re-login to keep pending-reply
     * correlation keys unique, so reconnecting cannot rewind it; only a new
     * client instance starts a fresh sequence.
     *
     * <p>A shared session draws the id from its parent, see {@link #bindShared}.
     */
    long nextRequestId() {
        ConsensusSession numbering;
        synchronized (this) {
            if (session == null) {
                throw new IggyNotConnectedException("Not authenticated, call login first");
            }
            numbering = parent;
            if (numbering == null) {
                if (requestCounter == Long.MAX_VALUE) {
                    throw new IllegalStateException(
                            "VSR request counter exhausted, create a fresh client instance (reconnecting preserves the counter)");
                }
                return requestCounter++;
            }
        }
        // Outside this monitor, so the monitors of two sessions never nest.
        return numbering.nextRequestId();
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

    public synchronized long boundSession() {
        if (session == null) {
            throw new IggyNotConnectedException("Not authenticated, call login first");
        }
        return session;
    }

    public synchronized boolean isBound() {
        return session != null;
    }

    /** Ends the local identity after explicit logout or a refused session bind. */
    public synchronized void reset() {
        session = null;
        metadataWatermark = 0;
        regenerateClientId();
        generation++;
    }

    public synchronized byte[] bindSecret() {
        return bindSecret.clone();
    }

    public synchronized byte[] bindSecret(long clientLow, long clientHigh, long epoch) {
        if (session == null || session != epoch || clientIdLow != clientLow || clientIdHigh != clientHigh) {
            throw new IggyNotConnectedException("Poll attachment no longer belongs to the parent session");
        }
        return bindSecret.clone();
    }

    /**
     * Binds another connection to the identity of {@code parent}. Replicated requests
     * take their ids from the parent's counter: the partition deduplicates by client
     * and request id whichever connection carries them, so a counter of this
     * connection would reuse ids the server already answered.
     */
    public synchronized void bindShared(
            ConsensusSession parent, long clientLow, long clientHigh, long epoch, byte[] secret) {
        if (parent == null
                || parent == this
                || (clientLow == 0 && clientHigh == 0)
                || epoch == 0
                || secret == null
                || secret.length != BIND_SECRET_BYTES) {
            throw new IggyInvalidArgumentException("Shared session requires another parent session, a nonzero client,"
                    + " a nonzero epoch and a " + BIND_SECRET_BYTES + "-byte bind secret");
        }
        bind(epoch);
        clientIdLow = clientLow;
        clientIdHigh = clientHigh;
        System.arraycopy(secret, 0, bindSecret, 0, BIND_SECRET_BYTES);
        this.parent = parent;
        shared = true;
    }

    synchronized void onChannelCreated() {
        if (shared) {
            generation++;
        }
    }

    public synchronized long generation() {
        return generation;
    }

    public synchronized long metadataWatermark() {
        return metadataWatermark;
    }

    synchronized void observeMetadata(long commit) {
        if (Long.compareUnsigned(commit, metadataWatermark) > 0) {
            metadataWatermark = commit;
        }
    }

    synchronized long clientIdLow() {
        return clientIdLow;
    }

    synchronized long clientIdHigh() {
        return clientIdHigh;
    }

    private void regenerateClientId() {
        RANDOM.nextBytes(bindSecret);
        do {
            clientIdLow = RANDOM.nextLong();
            clientIdHigh = RANDOM.nextLong();
        } while (clientIdLow == 0 && clientIdHigh == 0);
    }
}
