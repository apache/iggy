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

package org.apache.iggy.client.async.reader;

import org.apache.iggy.client.async.reader.FakeTransport.Read;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.refused;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.loseMembership;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.RETRY_INTERVAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.assignment;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.closeGroupReader;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.member;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.next;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.offsets;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.standalone;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reader paces its requests: the requests in flight at once, and the wait after a refusal. */
class PartitionReaderPacingTest {
    // Short, so six waits that double each time stay well under the timeout of a test read.
    static final Duration SHORT_RETRY = Duration.ofMillis(20);
    private static final int NOT_OWNED = IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode();
    // One short of the report, so a count that outlives the lost membership reports at the next refusal.
    private static final int REFUSALS_BEFORE_THE_LOSS = 5;

    /**
     * A check polls one partition and can wait for its whole wait, as a gap read does. So checks count against the
     * same limit, and a request is left for the read of each group.
     */
    @Test
    void shouldLeaveARequestForTheReadOfTheGroupWhileChecksWait() throws Exception {
        Long[] ids = LongStream.rangeClosed(1, PartitionReader.MAX_READS_IN_FLIGHT + 1)
                .boxed()
                .toArray(Long[]::new);
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, PollingStrategy.last(), MANUAL, ids);
        try {
            List<Read> checks = new ArrayList<>();
            for (int check = 0; check < PartitionReader.MAX_READS_IN_FLIGHT; check++) {
                checks.add(transport.nextRead());
            }
            assertThat(checks).allMatch(Read::poll);
            assertThat(transport.readsNothingFor(QUIET))
                    .as("the checks hold every request of the reader")
                    .isTrue();

            Read found = checks.get(0);
            long partition = found.targets().keySet().iterator().next();
            found.answer(section(partition, 5));

            Read freed = transport.nextRead();
            assertThat(freed.poll())
                    .as("the freed request goes to the read of the group")
                    .isFalse();
            assertThat(freed.targets()).containsOnlyKeys(partition);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A read that the server refuses because another member owns the partition reads again after the retry interval,
     * doubled for each refusal in a row. The sixth in a row is reported once, and it waits 32 times the interval.
     */
    @Test
    void shouldDoubleTheWaitAfterEachNotOwnedRefusalInARow() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = memberRetryingSoon(transport);
        try {
            assignment(transport, 1L);
            Read read = transport.nextRead();
            for (int refusal = 1; refusal <= 6; refusal++) {
                long refusedAt = System.nanoTime();
                read.answer(refused(1, NOT_OWNED));
                read = transport.nextRead();
                assertThat(System.nanoTime() - refusedAt)
                        .as("the wait after refusal %d in a row", refusal)
                        .isGreaterThanOrEqualTo(SHORT_RETRY.toNanos() << (refusal - 1));
            }

            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(NOT_OWNED));
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * The refusals of a lost membership, as after a lost session, end with it. So after the join, a partition that
     * waited after five refusals in a row reads at once, and its next refusal waits the retry interval and reports
     * nothing.
     */
    @Test
    void shouldStartTheRefusalsAgainAfterALostMembership() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            assignment(transport, 1L);
            Read read = transport.nextRead();
            for (int refusal = 1; refusal < REFUSALS_BEFORE_THE_LOSS; refusal++) {
                read.answer(refused(1, NOT_OWNED));
                read = transport.nextRead();
            }
            long refusedAt = System.nanoTime();
            read.answer(refused(1, NOT_OWNED));

            loseMembership(transport, 1L);

            Read afterJoin = transport.nextRead();
            assertThat(System.nanoTime() - refusedAt)
                    .as("the read after the join waits for no refusal of the lost membership")
                    .isLessThan(RETRY_INTERVAL.toNanos() << (REFUSALS_BEFORE_THE_LOSS - 1));
            long refusedAgainAt = System.nanoTime();
            afterJoin.answer(refused(1, NOT_OWNED));
            Read again = transport.nextRead();
            assertThat(System.nanoTime() - refusedAgainAt).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            again.answer(section(1, 0));
            assertThat(offsets(next(reader)))
                    .as("the refusal after the join reports nothing")
                    .containsExactly(0L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    static PartitionReader memberRetryingSoon(FakeTransport transport) throws Exception {
        return PartitionReader.builder(transport)
                .options(MANUAL)
                .retryInterval(SHORT_RETRY)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
