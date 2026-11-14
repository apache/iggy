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
import org.apache.iggy.client.async.reader.ReadException.OffsetRange;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.refused;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.ON_READ;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.next;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.offsets;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.serverError;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.standalone;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A read that the object store refuses for good stops its partition at once. */
class PartitionReaderTieredReadTest {

    /** The object store failed for good, so the first refusal stops the partition without a retry. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldStopAPartitionAtTheFirstTieredReadFailureUntilASeek(boolean wholeRequest) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            Read refusedRead = transport.nextRead();
            if (wholeRequest) {
                refusedRead.reply().completeExceptionally(serverError(IggyErrorCode.TIERED_READ_FAILED));
            } else {
                refusedRead.answer(refused(1, IggyErrorCode.TIERED_READ_FAILED.getCode()));
            }

            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(IggyErrorCode.TIERED_READ_FAILED.getCode()));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();

            reader.seek(1L, PollingStrategy.first()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.nextRead().targets()).containsKey(1L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A stop during the read again of what a lost reply stored leaves the partition to a seek, which drops the range
     * and the batch read after it, and a seek to {@code Next} reads after both. So the stop reports the range as lost
     * and hands over that batch before its own error.
     */
    @Test
    void shouldReportTheLostRangeAndHandOverTheHeldBatchBeforeAStopUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<PolledMessages> second = reader.next();
            transport
                    .nextRead()
                    .reply()
                    .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            transport.nextRead().answer(section(1, 5, 6));
            Read gapRead = transport.nextRead();
            assertThat(gapRead.commit()).as("the read of the gap").isFalse();

            gapRead.answer(refused(1, IggyErrorCode.TIERED_READ_FAILED.getCode()));

            assertThatThrownBy(() -> second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).lost())
                            .contains(new OffsetRange(BigInteger.TWO, BigInteger.valueOf(4))));
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(IggyErrorCode.TIERED_READ_FAILED.getCode()));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
