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

package org.apache.iggy.conformance;

import java.util.concurrent.CompletableFuture;

/**
 * One running call. A cancel cancels the SDK future that the call waits for, as an application drops a call that it
 * no longer wants, and every future that the call waits for after it.
 */
final class Call {

    private CompletableFuture<?> waiting;
    private boolean cancelled;

    /** The value of an SDK future, or its error as a {@link java.util.concurrent.CompletionException}. */
    <T> T await(CompletableFuture<T> future) {
        synchronized (this) {
            if (cancelled) {
                future.cancel(true);
            }
            waiting = future;
        }
        try {
            return future.join();
        } finally {
            synchronized (this) {
                waiting = null;
            }
        }
    }

    synchronized void cancel() {
        cancelled = true;
        if (waiting != null) {
            waiting.cancel(true);
        }
    }
}
