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

package org.apache.iggy.client.async;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async client interface for stream management operations.
 *
 * <p>Streams are the top-level organizational unit in Iggy. Each stream contains
 * topics, which in turn contain partitions that hold the actual messages. All
 * methods return {@link CompletableFuture} for non-blocking execution.
 *
 * <p>Usage example:
 * <pre>{@code
 * StreamsClient streams = client.streams();
 *
 * // Create a stream
 * streams.createStream("orders")
 *     .thenAccept(details -> System.out.println("Created stream: " + details.id()));
 *
 * // List all streams
 * streams.getStreams()
 *     .thenAccept(list -> list.forEach(s -> System.out.println(s.name())));
 * }</pre>
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#streams()
 */
public interface StreamsClient {

    /**
     * Gets detailed information about a specific stream by its numeric ID.
     *
     * <p>This is a convenience overload that wraps the numeric ID into a {@link StreamId}.
     *
     * @param streamId the numeric stream identifier
     * @return a {@link CompletableFuture} that completes with an {@link Optional} containing
     *         the {@link StreamDetails} if the stream exists, or empty if not found
     */
    default CompletableFuture<Optional<StreamDetails>> getStream(Long streamId) {
        return getStream(StreamId.of(streamId));
    }

    /**
     * Gets detailed information about a specific stream.
     *
     * <p>The returned {@link StreamDetails} includes the stream's metadata (ID, name, size,
     * creation time) as well as the list of topics within the stream.
     *
     * @param streamId the stream identifier (numeric or string-based)
     * @return a {@link CompletableFuture} that completes with an {@link Optional} containing
     *         the {@link StreamDetails} if the stream exists, or empty if not found
     */
    CompletableFuture<Optional<StreamDetails>> getStream(StreamId streamId);

    /**
     * Gets a list of all streams on the server.
     *
     * <p>Returns basic information about each stream without topic details. Use
     * {@link #getStream(StreamId)} for full details about a specific stream.
     *
     * @return a {@link CompletableFuture} that completes with a list of {@link StreamBase}
     *         objects for all streams
     */
    CompletableFuture<List<StreamBase>> getStreams();

    /**
     * Creates a new stream with the given name and no options.
     *
     * <p>The stream ID is assigned by the server. Stream names must be unique across
     * the server. A server with encryption enabled generates the stream's own data key.
     *
     * @param name the name for the new stream
     * @return a {@link CompletableFuture} that completes with the created {@link StreamDetails}
     * @throws org.apache.iggy.exception.IggyException if a stream with the same name
     *         already exists
     */
    default CompletableFuture<StreamDetails> createStream(String name) {
        return createStream(name, StreamOptions.empty());
    }

    /**
     * Creates a new stream with typed options, such as its own encryption key of 32 random bytes.
     *
     * <p>The key travels inside the request over this client's transport, so protecting it on the
     * network is the job of TLS. The server never returns it, only its source and fingerprints.
     *
     * @param name    the name for the new stream
     * @param options the stream's options
     * @return a {@link CompletableFuture} that completes with the created {@link StreamDetails}
     */
    default CompletableFuture<StreamDetails> createStream(String name, StreamOptions options) {
        return createStream(name, options.entries());
    }

    /**
     * Creates a new stream with options keyed by option name, for keys newer than this build.
     *
     * <p>The server refuses a key outside its catalog by name. Prefer {@link StreamOptions} for the
     * keys this build knows.
     *
     * @param name    the name for the new stream
     * @param options option values keyed by option name
     * @return a {@link CompletableFuture} that completes with the created {@link StreamDetails}
     */
    CompletableFuture<StreamDetails> createStream(String name, Map<String, HeaderValue> options);

    /**
     * Updates an existing stream identified by its numeric ID.
     *
     * <p>This is a convenience overload that wraps the numeric ID into a {@link StreamId}.
     *
     * @param streamId the numeric stream identifier
     * @param update   what to change, every key left out keeps its value
     * @return a {@link CompletableFuture} that completes when the update is done
     */
    default CompletableFuture<Void> updateStream(Long streamId, StreamUpdate update) {
        return updateStream(StreamId.of(streamId), update);
    }

    /**
     * Updates an existing stream: renames it. Its encryption key cannot change or be reset.
     *
     * @param streamId the stream identifier (numeric or string-based)
     * @param update   what to change, every key left out keeps its value
     * @return a {@link CompletableFuture} that completes when the update is done
     * @throws org.apache.iggy.exception.IggyException if the stream does not exist
     */
    CompletableFuture<Void> updateStream(StreamId streamId, StreamUpdate update);

    /**
     * Deletes a stream and all of its topics, partitions, and messages by numeric ID.
     *
     * <p>This is a convenience overload that wraps the numeric ID into a {@link StreamId}.
     *
     * <p><strong>Warning:</strong> This operation is irreversible and will permanently
     * delete all data within the stream.
     *
     * @param streamId the numeric stream identifier
     * @return a {@link CompletableFuture} that completes when the deletion is done
     */
    default CompletableFuture<Void> deleteStream(Long streamId) {
        return deleteStream(StreamId.of(streamId));
    }

    /**
     * Deletes a stream and all of its topics, partitions, and messages.
     *
     * <p><strong>Warning:</strong> This operation is irreversible and will permanently
     * delete all data within the stream.
     *
     * @param streamId the stream identifier (numeric or string-based)
     * @return a {@link CompletableFuture} that completes when the deletion is done
     * @throws org.apache.iggy.exception.IggyException if the stream does not exist
     */
    CompletableFuture<Void> deleteStream(StreamId streamId);
}
