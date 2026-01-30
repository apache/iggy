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
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface StreamsClient {

    default CompletableFuture<Optional<StreamDetails>> getStream(Long streamId) {
        return getStream(StreamId.of(streamId));
    }

    CompletableFuture<Optional<StreamDetails>> getStream(StreamId streamId);

    CompletableFuture<List<StreamBase>> getStreams();

    CompletableFuture<StreamDetails> createStream(String name);

    default CompletableFuture<Void> updateStream(Long streamId, String name) {
        return updateStream(StreamId.of(streamId), name);
    }

    CompletableFuture<Void> updateStream(StreamId streamId, String name);

    default CompletableFuture<Void> deleteStream(Long streamId) {
        return deleteStream(StreamId.of(streamId));
    }

    CompletableFuture<Void> deleteStream(StreamId streamId);
}
