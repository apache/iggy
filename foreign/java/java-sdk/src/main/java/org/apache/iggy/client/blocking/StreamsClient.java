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

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface StreamsClient {

    default Optional<StreamDetails> getStream(Long streamId) {
        return getStream(StreamId.of(streamId));
    }

    Optional<StreamDetails> getStream(StreamId streamId);

    List<StreamBase> getStreams();

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
