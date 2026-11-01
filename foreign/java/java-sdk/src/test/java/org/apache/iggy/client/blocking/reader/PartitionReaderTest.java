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

package org.apache.iggy.client.blocking.reader;

import org.apache.iggy.client.async.reader.PartitionReaderTestAccess;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The blocking reader keeps a batch that its caller stopped waiting for. */
class PartitionReaderTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void shouldReturnNothingAtTheTimeoutAndKeepTheBatchForTheNextCall() throws Exception {
        var access = PartitionReaderTestAccess.standalone(PollOptions.waitFor(Duration.ofSeconds(1)), 1L);
        try (PartitionReader reader = PartitionReader.of(access.reader())) {
            var read = access.nextRead();

            assertThat(reader.next(Duration.ofMillis(100))).isEmpty();
            access.answer(read, 1L, 4L);

            Optional<PolledMessages> polled = reader.next(TIMEOUT);
            assertThat(polled).isPresent();
            assertThat(polled.get().messages()).hasSize(1);
        }
    }

    @Test
    void shouldKeepTheBatchOfACallThatItsThreadInterrupted() throws Exception {
        var access = PartitionReaderTestAccess.standalone(PollOptions.waitFor(Duration.ofSeconds(1)), 1L);
        try (PartitionReader reader = PartitionReader.of(access.reader())) {
            var read = access.nextRead();
            CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
            Thread caller = new Thread(() -> {
                try {
                    reader.next();
                    interrupted.complete(false);
                } catch (RuntimeException error) {
                    interrupted.complete(Thread.currentThread().isInterrupted());
                }
            });
            caller.start();
            caller.interrupt();
            assertThat(interrupted.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .isTrue();

            access.answer(read, 1L, 4L);

            assertThat(reader.next().messages()).hasSize(1);
        }
    }
}
