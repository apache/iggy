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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * The pauses between the attempts of one call that the server keeps refusing. Each pause is a random time from zero
 * to a ceiling, and the ceiling doubles from the base to the maximum, as {@code backoff} and
 * {@code pause_before_retry} in core/sdk/src/links/mod.rs. One instance paces one call, so the next call starts at
 * the base again.
 */
final class RetryBackoff {
    static final long BASE_MILLIS = 50;
    static final long MAX_MILLIS = 1000;

    private final DoubleSupplier draw;
    private long ceilingMillis = BASE_MILLIS;

    RetryBackoff() {
        this(() -> ThreadLocalRandom.current().nextDouble());
    }

    /** {@code draw} gives the random fraction of the ceiling that a pause takes, from {@code [0, 1]}. */
    RetryBackoff(DoubleSupplier draw) {
        this.draw = draw;
    }

    /** The pause before the next attempt. */
    long nextMillis() {
        long ceiling = ceilingMillis;
        ceilingMillis = Math.min(ceiling * 2, MAX_MILLIS);
        return Math.min(ceiling, Math.max(0, (long) (ceiling * draw.getAsDouble())));
    }
}
