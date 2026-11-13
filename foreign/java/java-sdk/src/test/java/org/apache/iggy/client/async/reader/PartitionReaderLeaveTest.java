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
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.await;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.ON_READ;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.assignment;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.closeGroupReader;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.member;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.offsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reader when a partition leaves the assignment of its member. */
class PartitionReaderLeaveTest {
    private static final long LOST_OPERATION = 42;

    /** Under Manual the server stored nothing for the queued batches, so a partition that leaves drops them. */
    @Test
    void shouldDropTheQueuedBatchOfAPartitionThatLeavesUnderManual() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, MANUAL);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            transport.nextRead().answer(section(1, 0, 1), section(2, 4, 5));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            await(() -> reader.partitions().equals(List.of(1L)));
            CompletableFuture<PolledMessages> next = reader.next();
            nextReadOf(transport, 1L).answer(section(1, 2, 3));

            PolledMessages batch = next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(batch.partitionId()).isEqualTo(1L);
            assertThat(offsets(batch)).containsExactly(2L, 3L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * Under OnRead a partition that leaves reports the offsets that a lost read stored once a later read showed them,
     * then hands over the batch after them. A lost read whose stored offsets no read showed reports nothing.
     */
    @ParameterizedTest(name = "a later read showed the stored offsets: {0}")
    @ValueSource(booleans = {true, false})
    void shouldReportOnlyTheShownOffsetsOfALostReadWhenItsPartitionLeavesUnderOnRead(boolean shown) throws Exception {
        FakeTransport transport = new FakeTransport(true)
                .groupedBy(partitions -> partitions.stream().map(List::of).toList());
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<PolledMessages> second = reader.next();
            transport
                    .nextRead()
                    .reply()
                    .completeExceptionally(
                            new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, LOST_OPERATION));
            if (shown) {
                transport.nextRead().answer(section(1, 5, 6));
                assertThat(transport.nextRead().commit())
                        .as("the read of the gap")
                        .isFalse();
            }

            assignment(transport, 2L);

            if (shown) {
                assertThatThrownBy(() -> second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .satisfies(error -> assertThat(error.getCause())
                                .isInstanceOf(ReadException.class)
                                .hasMessageContaining("Offsets 2 to 4 were stored by a read whose reply was lost"))
                        .satisfies(error -> assertThat(((ReadException) error.getCause()).lost())
                                .contains(new OffsetRange(BigInteger.TWO, BigInteger.valueOf(4))));
                assertThat(offsets(reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                        .containsExactly(5L, 6L);
            } else {
                nextReadOf(transport, 2L).answer(section(2, 0));
                assertThat(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).partitionId())
                        .isEqualTo(2L);
            }
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * The close never moves back the offset of a partition that left. After the handoff the server refuses the store,
     * and during the handoff a stored offset below the last served one blocks it. So under OnRead the batches of a left
     * partition that the application did not read before the close are lost for the group.
     */
    @Test
    void shouldMoveNothingBackOnCloseForAPartitionThatLeftUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            transport.nextRead().answer(section(1, 0, 1), section(2, 4, 5));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            await(() -> reader.partitions().equals(List.of(1L)));
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.written)
                .as("the close writes no offset for the queued batch of the partition that left")
                .isEmpty();
    }

    /**
     * Under Manual the close sends the leave at once. A sync that answers after it finds the member gone, and the
     * closed reader joins no more, so no member holds partitions that nobody reads.
     */
    @Test
    void shouldNotJoinAgainForASyncThatAnswersAfterTheLeaveOfTheClose() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        assignment(transport, 1L);
        CompletableFuture<Optional<ConsumerGroupAssignment>> sync =
                transport.assignments.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(sync).as("the reader reads its assignment again").isNotNull();
        transport.leaveReply = new CompletableFuture<>();
        CompletableFuture<Void> closed = reader.closeAsync();
        await(() -> transport.membership.contains("leave"));

        sync.complete(Optional.empty());

        assertThat(transport.assignments.poll(QUIET.toMillis(), TimeUnit.MILLISECONDS))
                .as("a closed reader joins and syncs no more")
                .isNull();
        transport.leaveReply.complete(null);
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.membership).containsExactly("join", "leave");
    }

    /** The next read of {@code partition}. A read of other partitions stays without a reply. */
    private static Read nextReadOf(FakeTransport transport, long partition) throws InterruptedException {
        Read read = transport.nextRead();
        while (!read.targets().containsKey(partition)) {
            read = transport.nextRead();
        }
        return read;
    }
}
