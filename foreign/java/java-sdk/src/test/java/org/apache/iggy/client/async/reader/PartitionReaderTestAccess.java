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

import org.apache.iggy.message.PollOptions;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** A reader on a {@link FakeTransport} for the tests of other packages. */
public final class PartitionReaderTestAccess {
    private final FakeTransport transport;
    private final PartitionReader reader;

    private PartitionReaderTestAccess(FakeTransport transport, PartitionReader reader) {
        this.transport = transport;
        this.reader = reader;
    }

    public static PartitionReaderTestAccess standalone(PollOptions options, Long... partitions) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = PartitionReader.builder(transport)
                .partitions(List.of(partitions))
                .options(options)
                .retryInterval(Duration.ofMillis(100))
                .build()
                .get(FakeTransport.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return new PartitionReaderTestAccess(transport, reader);
    }

    /** {@link PartitionReaderCancelTest#memberWithANewOwner} on a group transport. */
    public static PartitionReaderTestAccess memberWithANewOwner() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        return new PartitionReaderTestAccess(transport, PartitionReaderCancelTest.memberWithANewOwner(transport));
    }

    public PartitionReader reader() {
        return reader;
    }

    /** The next read that the reader sends, as an opaque handle. */
    public Object nextRead() throws InterruptedException {
        return transport.nextRead();
    }

    /** Answers {@code read} with a refusal that the reader retries after its retry interval. */
    public void refuseTransiently(Object read) {
        ((FakeTransport.Read) read).answer(PartitionReaderCancelTest.TRANSIENT_REFUSAL);
    }

    public boolean readsNothingForTwoRetryIntervals() throws InterruptedException {
        return transport.readsNothingFor(PartitionReaderTest.RETRY_INTERVAL.multipliedBy(2));
    }

    /** Answers {@code read} with one message of {@code partition} at each of {@code offsets}. */
    public void answer(Object read, long partition, long... offsets) {
        ((FakeTransport.Read) read).answer(FakeTransport.section(partition, offsets));
    }
}
