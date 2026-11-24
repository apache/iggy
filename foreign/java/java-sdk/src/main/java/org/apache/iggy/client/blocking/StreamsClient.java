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

package org.apache.iggy.client.blocking;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface StreamsClient {

    default Optional<StreamDetails> getStream(Long streamId) {
        return getStream(StreamId.of(streamId));
    }

    Optional<StreamDetails> getStream(StreamId streamId);

    /**
     * Gets the streams the caller may read. Over TCP, reads the list in pages of up to 1000 streams, so the list is not
     * one consistent read. All pages share the sign-in and the request timeout of the call, so a new sign-in during the
     * read fails it. Over TCP, the call fails with {@link org.apache.iggy.exception.IggyMalformedResponseException}
     * when the server answers the page cursor it was sent. Over HTTP, reads the whole list in one reply, which the
     * server builds from pages of 1000, so it is not one consistent read either.
     */
    List<StreamBase> getStreams();

    /**
     * Gets one page of the streams the caller may read. A query that breaks the page rules fails with an invalid
     * command error.
     */
    Page<StreamBase> getStreamsPage(NamedPageQuery query);

    /** Creates a stream without options. A server with encryption enabled generates the stream's own data key. */
    default StreamDetails createStream(String name) {
        return createStream(name, StreamOptions.empty());
    }

    /**
     * Creates a stream with typed options, such as its own encryption key of 32 random bytes.
     *
     * <p>The key travels inside the request over this client's transport, so protecting it on the network
     * is the job of TLS. The server never returns it, only its source and fingerprints.
     */
    default StreamDetails createStream(String name, StreamOptions options) {
        return createStream(name, options.entries());
    }

    /**
     * Creates a stream with options keyed by option name, for keys newer than this build. The server
     * refuses a key outside its catalog by name.
     */
    StreamDetails createStream(String name, Map<String, HeaderValue> options);

    default void updateStream(Long streamId, StreamUpdate update) {
        updateStream(StreamId.of(streamId), update);
    }

    /** Renames a stream. Every key left out keeps its value. */
    void updateStream(StreamId streamId, StreamUpdate update);

    default void deleteStream(Long streamId) {
        deleteStream(StreamId.of(streamId));
    }

    void deleteStream(StreamId streamId);
}
