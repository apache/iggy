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
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.RETRY_INTERVAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.serverError;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.standalone;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reader against failed lookups of the groups of its partitions, and against ids that a lookup refuses. */
class PartitionReaderGroupingTest {
    /** The client refuses a partition id outside the wire range unsent, so the build refuses it first. */
    @ParameterizedTest
    @ValueSource(longs = {-1, 0x1_0000_0000L})
    void shouldRefuseAPartitionIdOutsideTheWireRange(long id) {
        FakeTransport transport = new FakeTransport(false);

        assertThatThrownBy(() -> PartitionReader.builder(transport)
                        .partitions(List.of(id))
                        .build())
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThat(transport.groupings).isEmpty();
    }

    /** A lookup that throws before it sends fails as a failed one does, so the reader looks up again and reads. */
    @Test
    void shouldLookUpAgainAfterALookupThatThrowsBeforeItSends() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.groupsRefusal = new IggyClientException("The client refused the lookup");
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            assertThat(transport.nextRead().targets()).containsOnlyKeys(1L);
            assertThat(transport.groupings).hasSize(2);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A lookup runs beside the reads, and a failed one keeps the last good groups. A partition that a primary refused
     * waits for the next lookup, so while lookups fail the moved primary gets one read per lookup.
     */
    @Test
    void shouldReadOnWithTheLastGoodGroupsAndHoldANotPrimaryPartitionUntilTheNextLookup() throws Exception {
        AtomicBoolean failing = new AtomicBoolean();
        List<Long> lookups = new CopyOnWriteArrayList<>();
        FakeTransport transport = new FakeTransport(false).groupedBy(ids -> {
            lookups.add(System.nanoTime());
            if (failing.get()) {
                throw new IggyClientException("No route to the partitions");
            }
            return ids.stream().map(List::of).toList();
        });
        PartitionReader reader = standalone(transport, MANUAL, 1L, 2L);
        try {
            Read first = transport.nextRead();
            Read second = transport.nextRead();
            Read ofOne = first.targets().containsKey(1L) ? first : second;
            Read ofTwo = ofOne == first ? second : first;
            failing.set(true);
            transport.groupsReply = new CompletableFuture<>();
            ofOne.reply().completeExceptionally(serverError(IggyErrorCode.NOT_PRIMARY));

            ofTwo.answer(section(2, 0));
            Read whileLooking = transport.nextRead();
            assertThat(whileLooking.targets())
                    .as("the reads go on during the lookup")
                    .containsOnlyKeys(2L);
            assertThat(transport.readsNothingFor(QUIET))
                    .as("the refused partition waits for the lookup")
                    .isTrue();

            transport.groupsReply.complete(null);
            Read again = transport.nextRead();
            assertThat(again.targets())
                    .as("a failed lookup keeps the last good groups")
                    .containsOnlyKeys(1L);
            whileLooking.answer(section(2, 1));
            assertThat(transport.nextRead().targets()).containsOnlyKeys(2L);
            int lookedUp = lookups.size();
            again.reply().completeExceptionally(serverError(IggyErrorCode.NOT_PRIMARY));

            Read afterLookup = transport.nextRead();
            assertThat(afterLookup.targets()).containsOnlyKeys(1L);
            assertThat(lookups)
                    .as("the refused partition reads again only after the next lookup")
                    .hasSizeGreaterThan(lookedUp);
            assertThat(lookups.get(2) - lookups.get(1))
                    .as("a failed lookup runs again after the retry interval")
                    .isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            PolledMessages readDuringLookup = reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(readDuringLookup.partitionId()).isEqualTo(2L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
