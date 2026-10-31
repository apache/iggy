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

import org.apache.iggy.client.async.reader.PartitionReaderBuilder;
import org.apache.iggy.client.async.reader.ReadException;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A blocking view of {@link org.apache.iggy.client.async.reader.PartitionReader}. A call that waits for a batch keeps
 * the batch in the reader when its thread is interrupted or its timeout ends, so no message is lost.
 */
public final class PartitionReader implements Closeable {
    private final org.apache.iggy.client.async.reader.PartitionReader reader;

    private PartitionReader(org.apache.iggy.client.async.reader.PartitionReader reader) {
        this.reader = reader;
    }

    /** Builds the reader of {@code builder} and waits for it, a group reader until it joined its group. */
    public static PartitionReader build(PartitionReaderBuilder builder) {
        return new PartitionReader(resolve(builder.build()));
    }

    /** Wraps an async reader, which the blocking one closes. */
    public static PartitionReader of(org.apache.iggy.client.async.reader.PartitionReader reader) {
        return new PartitionReader(reader);
    }

    /**
     * Waits for the next batch of one partition.
     *
     * @throws ReadException for an error of a read, after which the reader goes on
     */
    public PolledMessages next() {
        return resolve(reader.next());
    }

    /** Waits up to {@code timeout} for the next batch, and returns empty when none came. */
    public Optional<PolledMessages> next(Duration timeout) {
        CompletableFuture<PolledMessages> next = reader.next();
        try {
            return Optional.of(next.get(timeout.toNanos(), TimeUnit.NANOSECONDS));
        } catch (TimeoutException ended) {
            next.cancel(false);
            return Optional.empty();
        } catch (InterruptedException interrupted) {
            next.cancel(false);
            Thread.currentThread().interrupt();
            throw new IggyClientException("Interrupted while waiting for a batch", interrupted);
        } catch (ExecutionException failed) {
            throw unwrap(failed.getCause());
        }
    }

    /** See {@link org.apache.iggy.client.async.reader.PartitionReader#commit}. */
    public void commit(Checkpoint checkpoint) {
        resolve(reader.commit(checkpoint));
    }

    /** See {@link org.apache.iggy.client.async.reader.PartitionReader#mark}. */
    public void mark(Checkpoint checkpoint) {
        resolve(reader.mark(checkpoint));
    }

    /** See {@link org.apache.iggy.client.async.reader.PartitionReader#seek}. */
    public void seek(long partitionId, PollingStrategy start) {
        resolve(reader.seek(partitionId, start));
    }

    public List<Long> partitions() {
        return reader.partitions();
    }

    /** See {@link org.apache.iggy.client.async.reader.PartitionReader#closeAsync()}. */
    @Override
    public void close() {
        resolve(reader.closeAsync());
    }

    private static <T> T resolve(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            future.cancel(false);
            Thread.currentThread().interrupt();
            throw new IggyClientException("Interrupted while waiting for the reader", interrupted);
        } catch (ExecutionException failed) {
            throw unwrap(failed.getCause());
        }
    }

    private static RuntimeException unwrap(Throwable cause) {
        return cause instanceof RuntimeException runtime ? runtime : new IggyClientException("Read failed", cause);
    }
}
