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

package org.apache.iggy.serde;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The zero-id mint: ids fill the full 16 bytes, never repeat within a thread (the AES-CTR counter
 * always advances), and never collide across threads (each thread seeds an independent keystream).
 */
class MessageIdGeneratorTest {

    @Test
    void shouldMintSixteenBytes() {
        assertThat(MessageIdGenerator.mint()).hasSize(16);
    }

    @Test
    void shouldMintDistinctIdsWithinAThread() {
        var count = 100_000;
        Set<BigInteger> ids = new HashSet<>(count * 2);
        for (var i = 0; i < count; i++) {
            ids.add(new BigInteger(1, MessageIdGenerator.mint()));
        }
        assertThat(ids).hasSize(count);
    }

    @Test
    void shouldMintDistinctIdsAcrossThreads() throws InterruptedException {
        var threads = 4;
        var perThread = 25_000;
        List<List<BigInteger>> perThreadIds = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (var t = 0; t < threads; t++) {
            List<BigInteger> mine = new ArrayList<>(perThread);
            perThreadIds.add(mine);
            var worker = new Thread(() -> {
                for (var i = 0; i < perThread; i++) {
                    mine.add(new BigInteger(1, MessageIdGenerator.mint()));
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (var worker : workers) {
            worker.join();
        }

        Set<BigInteger> all = new HashSet<>(threads * perThread * 2);
        for (var ids : perThreadIds) {
            all.addAll(ids);
        }
        assertThat(all).hasSize(threads * perThread);
    }
}
